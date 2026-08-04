#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The regression test for a false data-loss verdict.
#
# A full gate run once reported "iter 2: REPLICA kill lost acked key key9" —
# acked-write loss on the one path with no async-contract excuse. It was not
# loss. In the master-kill pass, a reply the oracle could not READ was skipped
# while the key stayed in the ledger still claiming its pre-kill acks; the next
# replica kill then demanded that key, found it gone, and panicked. A
# master-kill artefact became a data-loss verdict.
#
# The fix retries such a reply and, if it stays unreadable, retires the key's
# pre-kill acks so nothing judges it on evidence that could not be gathered.
# But the triggering condition is load- and timing-dependent: every run since
# reported `unreadable: 0`, so the fix shipped never having executed. A fix
# nothing has run is a belief.
#
# So: inject the condition deterministically. --inject-unreadable DELs a key
# from the survivor AND forces the unreadable classification — the two things
# that co-occur in the wild when a key is lost in a failover and read back
# while the proxy is still chasing the promotion. The injector creates the
# FAULT only; the retire that follows is the real code path.
#
# Seed 1 orders the kills REPLICA, MASTER, REPLICA. Injection lands on the
# master kill at iteration 2, and iteration 3's replica kill follows it. That
# ordering is ASSERTED below, not assumed: if the seed ever walks a different
# sequence this drill says so instead of passing vacuously.
#
# WHAT THIS PROVES, AND WHAT IT DOES NOT.
#
# Proves: the retry-retire-count path executes, retires exactly the keys the
# fault was injected into, and reports them. Before this drill that path had
# never run once — every chaos run in the suite reported `unreadable: 0`.
# Against the ORIGINAL code, which had neither a counter nor a retire, this
# fails outright.
#
# Does NOT prove end to end that a retired key is never judged later. Making
# that observable needs the injected key to stay absent until the next replica
# kill, and the writer rewrites every key thousands of times a second, so it is
# resurrected within milliseconds. Freezing the writer was tried: it breaks the
# RTO measurement (which needs a live writer to ack), and when moved after that
# measurement it destabilised the run into a separate latent race — an ack
# timestamped just after kill_ms but served by the dying master. That race is
# worth its own investigation and is NOT worth destabilising a green chaos gate
# to chase today.
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-chaos-
fleet_guard
fleet_kill server
sleep 0.5
cargo build --release -q -p flint-server --features rocks
cargo build --release -q -p flint-chaos

D=$(mktemp -d /tmp/flint-unreadable.XXXXXX)
trap 'fleet_kill server; rm -rf "$D"' EXIT
INJECT=3

echo "== master kill with $INJECT keys made unreadable AND really gone"
set +e
./target/release/flint-chaos --iterations 3 --keys 4000 --seed 1 --mode mixed \
  --inject-unreadable "$INJECT" >"$D/out.log" 2>&1
RC=$?
set -e
sed 's/^/  | /' "$D/out.log" | grep -E "iter |unreadable|PASS|FAIL|panick" || true

# 1. POSITIVE CONTROL ON THE TEST ITSELF: the scenario has to have happened.
#    A master kill with nothing after it proves nothing, and a run where the
#    injector never fired proves less.
SEQ=$(grep -oE "iter [0-9]+: pair 0: killed (MASTER|REPLICA)" "$D/out.log" | sed 's/.*killed //' | tr '\n' ' ')
case "$SEQ" in
  *MASTER*REPLICA*) : ;;
  *) echo "FAIL: scenario not exercised — need a MASTER kill followed by a REPLICA kill, got: $SEQ"
     echo "      the seed's kill order changed; pick a seed that yields MASTER then REPLICA"
     exit 1 ;;
esac

# 2. The retire path actually RAN. Without this the run could pass simply by
#    never injecting, which is the failure mode this whole drill exists to
#    close: `unreadable: 0` on every run is what hid the bug for a day.
N=$(grep -oE "retired and NOT judged as loss: [0-9]+" "$D/out.log" | grep -oE "[0-9]+$" | tail -1)
[ "${N:-0}" -eq "$INJECT" ] \
  || { echo "FAIL: injected $INJECT unreadable keys but the oracle retired ${N:-0}"; exit 1; }
echo "  retire path executed: $N key(s) retired"

# 3. And the replica kill that follows must NOT have blamed them. This is the
#    assertion that fails on the pre-fix binary.
if grep -q "REPLICA kill lost acked key" "$D/out.log"; then
  echo "FAIL: a retired key was still judged on the following REPLICA kill —"
  echo "      a master-kill artefact is being reported as data loss again"
  exit 1
fi
[ "$RC" -eq 0 ] || { echo "FAIL: chaos run exited $RC"; tail -20 "$D/out.log"; exit 1; }

echo "PASS: an unreadable reply during a master kill is retried, retired from the ledger and counted ($INJECT of $INJECT), and the replica kill that follows reports no acked-write loss. See the header for what this does and does not establish."
