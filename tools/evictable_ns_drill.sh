#!/usr/bin/env bash
# The evictable-namespace hooks must feed the policy for a DECLARED namespace
# and be completely absent for every other one.
#
# WHY THIS EXISTS. Everything under it is unit-tested — the guard refuses
# undeclared namespaces, the policy is scan-resistant, the floors enforce
# batching. None of that proves the request path is CONNECTED to any of it. A
# hook that is never called passes every unit test in the crate, and its
# symptom is a cache that quietly never evicts until a disk fills.
#
# It is also the inverse claim that matters most here. Flint's position is that
# it never silently drops what a user put there, so the assertion a durable
# deployment needs is not "eviction works" but "eviction is not present". That
# one cannot be made from inside the eviction code, because the code that would
# make it is the code under suspicion.
#
# WHAT IS ASSERTED, each a control rather than an observation:
#   1. NEGATIVE, and the important one: with nothing declared, FLINTINFO's
#      `evict:` field is EMPTY after real traffic. No policy, no counters, no
#      state for a feature nobody opted into.
#   2. POSITIVE: with `--evictable-ns cache`, writes to `cache` reach the
#      policy — `policy_keys` moves. Without this the negative control is
#      satisfied by hooks that never work at all.
#   3. ISOLATION: on that SAME node, writes to an undeclared namespace do not
#      reach the policy. `policy_keys` counts the declared namespace's keys and
#      no others.
#   4. The declaration is visible: `evictable_ns` names it, so an operator can
#      see which namespaces on this node are eligible to lose data.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-evns 6492
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-evns; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }
fleet_warm ./target/release/flint-server

# The whole `evict:` field, or empty when the feature is off.
evict_field() { valkey-cli -p 6492 FLINTINFO | tr '\r' '\n' | grep '^evict:' | cut -d: -f2-; }
# One counter out of it.
evict_num() { evict_field | tr ' ' '\n' | grep "^$1=" | cut -d= -f2; }
info() { valkey-cli -p 6492 FLINTINFO | tr '\r' '\n' | grep "^$1:" | cut -d: -f2-; }

# FLINTNS is CONNECTION-SCOPED, so the namespace and the traffic must share one
# socket. valkey-cli opens a fresh connection per invocation, which sends every
# command in the DEFAULT namespace and makes this drill assert nothing about the
# declared one. It did exactly that on the first run: 60 writes, policy_keys=0,
# and the failure looked like a disconnected hook rather than a broken test.
write_some() { # write_some <ns> <prefix> <count> [reads]
  python3 - "$1" "$2" "$3" "${4:-0}" <<'PYX'
import socket, sys
ns, prefix, count, reads = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
def resp(a):
    return ("*%d\r\n" % len(a)).encode() + b"".join(
        ("$%d\r\n%s\r\n" % (len(x), x)).encode() for x in a)
s = socket.create_connection(("127.0.0.1", 6492), timeout=10); s.settimeout(10)
s.sendall(resp(["FLINTNS", ns])); s.recv(64)
val = "v" * 200
for i in range(count):
    s.sendall(resp(["SET", "%s%d" % (prefix, i), val])); s.recv(64)
for i in range(min(reads, count)):
    s.sendall(resp(["GET", "%s%d" % (prefix, i)])); s.recv(512)
s.close()
PYX
}

echo "== 1. NOTHING DECLARED — the durable default"
$B --port 6492 --engine rocks --data-dir "$D/n" 2>"$D/n.log" &
fleet_wait_ping 6492
write_some cache k 50 10
FIELD=$(evict_field)
if [ -n "${FIELD// /}" ]; then
  echo "FAIL: evict: is '$FIELD' on a node with nothing declared evictable."
  echo "      A durable deployment must carry no eviction state at all."
  exit 1
fi
echo "   evict: empty after 50 writes and a read — correct"
fleet_kill server; sleep 0.5

echo "== 2/3. DECLARED 'cache' — the hooks feed it, and only for it"
$B --port 6492 --engine rocks --data-dir "$D/d" --evictable-ns cache 2>"$D/d.log" &
fleet_wait_ping 6492

DECL=$(info evictable_ns)
[ "$DECL" = "cache" ] || { echo "FAIL: evictable_ns is '$DECL', expected 'cache'"; exit 1; }
echo "   evictable_ns=$DECL"

write_some cache c 60 20
K_CACHE=$(evict_num policy_keys)
[ -n "$K_CACHE" ] || { echo "FAIL: no policy_keys in evict: '$(evict_field)'"; exit 1; }
if [ "$K_CACHE" -lt 1 ]; then
  echo "FAIL: policy_keys=$K_CACHE after 60 writes to the declared namespace."
  echo "      The request-path hook is not reaching the policy, which every"
  echo "      unit test in the crate would still pass."
  exit 1
fi
echo "   policy_keys=$K_CACHE after 60 declared-namespace writes"

# THE READ HOOK, which is the difference between S3-FIFO and plain FIFO. Without
# an access signal nothing is ever promoted out of `small`, and the scan
# resistance the policy was chosen for is gone — silently, because every other
# counter here moves on writes alone. `policy_keys` above would look identical.
A=$(evict_num accesses)
if [ -z "$A" ] || [ "$A" -lt 1 ]; then
  echo "FAIL: accesses=${A:-<missing>} after 20 GETs in the declared namespace."
  echo "      The read hook is not firing, so nothing is ever promoted out of"
  echo "      small and the policy degrades to FIFO — which no unit test and no"
  echo "      write-side counter would show."
  exit 1
fi
echo "   accesses=$A after 20 declared-namespace reads (dropped=$(evict_num accesses_dropped))"

# ISOLATION: same node, undeclared namespace.
write_some other o 60 0
K_AFTER=$(evict_num policy_keys)
# EXACT, not a tolerance. `note_write` returns before touching the policy for a
# namespace nobody declared, so the correct number of new keys is ZERO and any
# drift is a defect worth failing on. An earlier version of this allowed +5 as
# "the policy's own bookkeeping", which was a fudge factor invented to describe
# behaviour that does not exist -- and a check with unexplained slack in it is
# how a real leak gets to look normal.
if [ "$K_AFTER" -ne "$K_CACHE" ]; then
  echo "FAIL: policy_keys went $K_CACHE -> $K_AFTER after writing 60 keys to an"
  echo "      UNDECLARED namespace. Those keys are now eviction candidates in a"
  echo "      namespace whose contract is that they are not."
  exit 1
fi
echo "   policy_keys=$K_AFTER after 60 more writes to an undeclared namespace — unchanged"

echo "PASS: evictable-ns hooks are connected, scoped, and absent by default"
