#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Transaction failure semantics (ADR-0012 D7/D8): every way a transaction can
# fail must end as an ABORT the client can see, never as a partial apply and
# never as an ack for a write that went nowhere.
#
# Five ways to lose a transaction, and the drill asserts each one aborts AND
# that the keys it named are absent afterwards:
#
#   1. the master is KILLED between MULTI and EXEC, through the proxy
#   2. the same, for a client dialling the node DIRECTLY
#   3. the master is DEMOTED between MULTI and EXEC (failover fenced it)
#   4. the write QUORUM is unmet at EXEC (the RPO bound the writes must clear)
#   5. the slot is FROZEN mid-cutover at EXEC
#
# Cases 3-5 are the ones that made this drill necessary: before Phase E the
# node checked only the slot gate at EXEC, so a transaction on a demoted
# master answered `+OK` and wrote nothing, and one on a widowed master
# applied writes the single-command path was shedding with -THROTTLED. A
# false ack is worse than an error, so each case here asserts the ERROR and
# then asserts the ABSENCE.
#
# The FIRST section is the capability assert, and it is not optional: every
# other section passes trivially on a build where transactions never apply
# anything. "Nothing was written" is only evidence if writing was possible.
#
# Not covered here, deliberately: a kill DURING the apply. EXEC commits one
# engine WriteBatch — one WAL group — so there is no interval in which half
# of it is durable, and no timing this drill could hit would prove otherwise.
# That atomicity is the chaos drills' subject; this one is about the paths
# that reach the commit, or must not.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-txnfail 6960 6961 6962
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-txnfail
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

# One RESP helper for every section: a connection the drill drives by hand,
# because MULTI is per-CONNECTION state and valkey-cli gives us a new one per
# invocation.
mkdir -p "$D"
cat >"$D/resp.py" <<'PY'
import socket, sys

def conn(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.settimeout(5)
    return s

def enc(*a):
    return f"*{len(a)}\r\n".encode() + b"".join(
        f"${len(x)}\r\n{x}\r\n".encode() for x in a)

def call(s, *a):
    """Send one command, return one reply as text. '<EOF>' if the peer hung
    up — which for a direct client IS the abort signal, so the caller has to
    be able to tell it apart from a reply."""
    try:
        s.sendall(enc(*a))
    except OSError as e:
        return f"<EOF> {e}"
    buf = b""
    while True:
        try:
            chunk = s.recv(65536)
        except OSError as e:
            return f"<EOF> {e}"
        if not chunk:
            return "<EOF> peer closed"
        buf += chunk
        if buf.endswith(b"\r\n"):
            return buf.decode(errors="replace").strip()

def fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)

def want_abort(reply, where):
    """An abort is any reply that is NOT a successful EXEC array. A success
    looks like `*N` followed by the per-command replies."""
    if reply.startswith("*") and not reply.startswith("*-1"):
        fail(f"{where}: EXEC reported success ({reply!r}) — "
             "a transaction that cannot run must abort, not ack")
    print(f"  abort seen: {reply.splitlines()[0]}")
PY
export PYTHONPATH="$D"

start_pair() {
  rm -rf "$D/m" "$D/r"
  $B --port 6960 --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
  disown                       # this drill kills seats on purpose; the shell's
  fleet_wait_listen 6960       # "Killed: 9" job notices are noise in the log
  $B --port 6961 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6960 2>"$D/r.log" &
  disown
  fleet_wait_listen 6961
  # The replica must be ATTACHED, not merely listening: the quorum section
  # below reads live-replica count, and a drill that starts measuring before
  # the link is up measures the wrong thing.
  for _ in $(seq 1 60); do
    valkey-cli -p 6960 FLINTINFO | tr -d '\r' | grep -q '^live_replicas:1' && return 0
    sleep 0.2
  done
  echo "FAIL: replica never attached"; cat "$D/r.log"; exit 1
}

echo "== fleet: master :6960, replica :6961, proxy :6962"
start_pair
./target/release/flint-proxy --port 6962 \
  --pairs "127.0.0.1:6960,127.0.0.1:6961" 2>"$D/proxy.log" &
disown
fleet_wait_listen 6962
fleet_wait_ping 6962
[ "$(valkey-cli -p 6962 PING)" = "PONG" ] || { echo "FAIL: proxy not up"; exit 1; }

echo
echo "== 0. CAPABILITY ASSERT — a transaction on a healthy master DOES apply"
echo "     (without this, every 'nothing was written' below is vacuous)"
python3 - <<'PY' || exit 1
from resp import *
s = conn(6962)
call(s, "MULTI")
call(s, "SET", "{cap}a", "1")
call(s, "SET", "{cap}b", "2")
r = call(s, "EXEC")
if not r.startswith("*2"):
    fail(f"a healthy transaction did not apply: {r!r}")
a, b = call(s, "GET", "{cap}a"), call(s, "GET", "{cap}b")
if "1" not in a or "2" not in b:
    fail(f"EXEC acked but the keys are not there: {a!r} {b!r}")
print("  both keys present — transactions work on this build")
PY

echo
echo "== 1. the SLOT is frozen mid-cutover at EXEC -> abort, nothing applied"
# Learn the slot the honest way — ask the node which slot it counted, rather
# than reimplementing CRC16 here. Reading heat BEFORE and AFTER one keyed
# command and taking the row that moved works whatever traffic came earlier;
# taking "the first row" only worked on a node nobody had touched, which the
# capability assert above has already stopped being true.
# (FLINTSLOTHEAT leads with an `uptime_ms <n>` row, hence the numeric filter.)
heat() { valkey-cli -p 6960 FLINTSLOTHEAT | tr -d '\r' | awk '$1 ~ /^[0-9]+$/ {print $1, $2}'; }
heat >"$D/heat.before"
cli_ok valkey-cli -p 6960 SET "{frz}probe" seed
heat >"$D/heat.after"
SLOT=$(awk 'NR==FNR {b[$1]=$2; next} ($2+0) > (b[$1]+0) {print $1; exit}' \
       "$D/heat.before" "$D/heat.after")
[ -n "$SLOT" ] || { echo "FAIL: FLINTSLOTHEAT reported no slot for the probe key"; exit 1; }
echo "  {frz} lives in slot $SLOT"
valkey-cli -p 6960 FLINTSLOTFREEZE "$SLOT" 127.0.0.1:6999 | tr -d '\r'
python3 - <<'PY' || exit 1
from resp import *
s = conn(6960)
# The single-command path sheds this with -TRYAGAIN; the transaction must
# reach the same verdict rather than committing behind the freeze.
one = call(s, "SET", "{frz}single", "x")
if not one.startswith("-TRYAGAIN"):
    fail(f"control: a single write to a frozen slot should be shed, got {one!r}")
call(s, "MULTI")
call(s, "SET", "{frz}k1", "v1")
call(s, "SET", "{frz}k2", "v2")
want_abort(call(s, "EXEC"), "frozen slot")
PY
valkey-cli -p 6960 FLINTSLOTABORT "$SLOT" >/dev/null   # unfreeze for later sections
for k in '{frz}k1' '{frz}k2'; do
  [ -z "$(valkey-cli -p 6960 GET "$k")" ] || { echo "FAIL: $k applied behind a frozen slot"; exit 1; }
done
echo "  neither key applied"

echo
echo "== 2. the write QUORUM is unmet at EXEC -> abort, nothing applied"
valkey-cli -p 6960 FLINTCONFIG min-replicas-to-write 5 | tr -d '\r'
python3 - <<'PY' || exit 1
from resp import *
s = conn(6960)
one = call(s, "SET", "{qrm}single", "x")
if not one.startswith("-THROTTLED"):
    fail(f"control: a single write below quorum should be shed, got {one!r}")
call(s, "MULTI")
call(s, "SET", "{qrm}k1", "v1")
call(s, "INCR", "{qrm}n")
want_abort(call(s, "EXEC"), "quorum unmet")
PY
valkey-cli -p 6960 FLINTCONFIG min-replicas-to-write 0 >/dev/null
for k in '{qrm}k1' '{qrm}n'; do
  [ -z "$(valkey-cli -p 6960 GET "$k")" ] || { echo "FAIL: $k applied below write quorum"; exit 1; }
done
echo "  neither key applied"

echo
echo "== 3. the master is DEMOTED between MULTI and EXEC -> abort, nothing applied"
python3 - <<'PY' || exit 1
from resp import *
import subprocess
s = conn(6960)
call(s, "MULTI")
call(s, "SET", "{dem}k1", "v1")
call(s, "SET", "{dem}k2", "v2")
out = subprocess.run(["valkey-cli", "-p", "6960", "FLINTDEMOTE", "9", "9"],
                     capture_output=True, text=True).stdout.strip()
print(f"  FLINTDEMOTE -> {out}")
r = call(s, "EXEC")
want_abort(r, "demoted master")
if not r.startswith("-READONLY"):
    fail(f"a demoted master should refuse with -READONLY, got {r!r}")
PY
# Read back from the node the transaction actually targeted. A demoted node
# fences its own reads, so promote it again first — asking the replica instead
# would prove only that the write never REPLICATED, which is a weaker claim
# than the one being made here.
valkey-cli -p 6960 FLINTPROMOTE 10 0 | tr -d '\r'
for k in '{dem}k1' '{dem}k2'; do
  V=$(valkey-cli -p 6960 GET "$k" 2>&1)
  case "$V" in "") ;; *) echo "FAIL: $k present after a demoted EXEC: $V"; exit 1;; esac
done
echo "  neither key applied"

echo
echo "== 4. the master is KILLED between MULTI and EXEC — DIRECT client"
fleet_kill server; fleet_kill proxy; sleep 0.4
start_pair
python3 - <<'PY' || exit 1
from resp import *
import subprocess
s = conn(6960)
call(s, "MULTI")
call(s, "SET", "{kil}k1", "v1")
call(s, "SET", "{kil}k2", "v2")
subprocess.run(["pkill", "-9", "-f", "flint-server --port 6960"])
import time; time.sleep(0.5)
r = call(s, "EXEC")
want_abort(r, "killed master, direct client")
if not r.startswith("<EOF>"):
    print(f"  (node answered rather than dying: {r!r})")
PY
echo "  promoting the replica and checking the survivor"
valkey-cli -p 6961 FLINTPROMOTE 1 1 | tr -d '\r'
for k in '{kil}k1' '{kil}k2'; do
  [ -z "$(valkey-cli -p 6961 GET "$k")" ] || { echo "FAIL: $k survived a killed master's lost transaction"; exit 1; }
done
echo "  neither key applied"

echo
echo "== 5. the master is KILLED between MULTI and EXEC — THROUGH THE PROXY"
fleet_kill server; fleet_kill proxy; sleep 0.4
start_pair
./target/release/flint-proxy --port 6962 \
  --pairs "127.0.0.1:6960,127.0.0.1:6961" 2>"$D/proxy2.log" &
disown
fleet_wait_listen 6962
fleet_wait_ping 6962
python3 - <<'PY' || exit 1
from resp import *
import subprocess, time
s = conn(6962)
# Bind the transaction to a backend first, so the proxy is genuinely pinned
# to the master we are about to kill.
call(s, "MULTI")
call(s, "SET", "{pxy}k1", "v1")
call(s, "SET", "{pxy}k2", "v2")
subprocess.run(["pkill", "-9", "-f", "flint-server --port 6960"])
time.sleep(0.5)
r = call(s, "EXEC")
want_abort(r, "killed master, via proxy")
# D7 in one assertion: the proxy must FAIL the transaction rather than repair
# it by re-dialling, and it must say so in a form the client can act on.
if not r.startswith("-EXECABORT"):
    fail(f"the proxy must abort a transaction whose backend died, got {r!r}")
print("  the client's own connection survived the abort")
PY
echo "  promoting the replica and checking the survivor"
valkey-cli -p 6961 FLINTPROMOTE 1 1 | tr -d '\r'
for k in '{pxy}k1' '{pxy}k2'; do
  [ -z "$(valkey-cli -p 6961 GET "$k")" ] || { echo "FAIL: $k survived a killed backend's lost transaction"; exit 1; }
done
echo "  neither key applied"

echo
echo "== 6. the aborted connection is still USABLE afterwards"
# The failure mode this catches is Phase D's own: an abort that leaves state
# armed on either side turns one lost transaction into every later one.
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 6962 SET '{after}ping' ok 2>/dev/null | tr -d '\r')" = "OK" ] && break
  sleep 0.25
done
python3 - <<'PY' || exit 1
from resp import *
s = conn(6962)
call(s, "MULTI")
call(s, "SET", "{after}k", "v")
r = call(s, "EXEC")
if not r.startswith("*1"):
    fail(f"a fresh transaction after the abort should commit, got {r!r}")
if "v" not in call(s, "GET", "{after}k"):
    fail("the post-abort transaction acked but did not apply")
print("  a fresh transaction commits against the promoted master")
PY

echo
echo "PASS: every way to lose a transaction ends as a visible abort, and none applied"
