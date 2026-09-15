#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# CAN THE FENCE ANSWER "OK" TO TWO MASTERS AT ONCE? (BUG-0151)
#
# A lease row is `(pair members, master-of-record, generation)` and every site
# resolves it by MEMBERSHIP CONTAINMENT -- the first row whose members include
# the address (ADR-0018, BUG-0065, BUG-0150). Containment keys on the address
# being asked about, so it cannot find a row for a member that was not in the
# pair when the row was written.
#
# `CPSETPAIR` replaces a pair's member vector and migrates nothing: in
# `registry.rs` the word `leases` appears only in Fence and LeaseAdopt, and
# main.rs's CPSETPAIR arm touches `st.pairs` alone. So after a member swap the
# row still names the OLD membership, a fence of the NEW member finds no row
# and pushes a second one, and two rows now cover one pair.
#
# `CPLEASE` answers OK when the FIRST row containing the caller names the
# caller as master. With two rows that can be true for two different addresses,
# which is the state the fencing record exists to make impossible.
#
# This was established at the RegistryState level and by reading CPLEASE. What
# it had never been is REACHED -- no drill in the suite exercises CPSETPAIR or
# swap-node at all, which is a large part of why nobody has seen it. This drill
# is the missing step: it runs the ordinary operator sequence against a real
# control plane and asks the product, not a unit test.
#
# CP-LEVEL, DELIBERATELY. No servers are started and none are needed. CPADDPAIR
# registers ADDRESSES rather than processes, and CPLEASE/CPFENCE/CPSETPAIR are
# answered by the control plane out of its own registry. Starting three nodes
# would add a failover's worth of timing to a question that is pure
# bookkeeping -- and would make an intermittent drill out of a deterministic
# one.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# 6430 is the CP. 6431-6433 are the member ADDRESSES -- nothing binds them, and
# they are declared anyway so no future drill is handed a port this one names
# (the collision check cannot tell a fake address from a live seat).
fleet_init $FLINT_DRILL_ROOT/flint-repoint-state 6430 6431 6432 6433
fleet_guard
fleet_kill controlplane; sleep 0.4
CP=./target/release/flint-controlplane
STATE=$FLINT_DRILL_ROOT/flint-repoint-state
CPPORT=6430
A=127.0.0.1:6431   # the incumbent master
B=127.0.0.1:6432   # the peer it is swapped out for
N=127.0.0.1:6433   # the new member, promoted after the swap
cleanup() { fleet_kill controlplane; rm -rf "$STATE" "$STATE.tmp"; }
trap cleanup EXIT
rm -rf "$STATE"

$CP --port $CPPORT --state "$STATE" 2>"$FLINT_DRILL_ROOT/flint-repoint-cp.log" &
fleet_wait_listen $CPPORT
sleep 0.4

# Raw, because these replies are the SUBJECT. `fleet_cp` asserts OK* and prints
# nothing on success, which is right for setup and useless for a verb whose
# error reply is the finding.
cp_() { valkey-cli -p $CPPORT "$@" 2>&1 | tr -d '\r'; }

echo "== a pair, and a master adopted on first touch"
fleet_cp $CPPORT CPADDPAIR "$A,$B"
ADOPT=$(cp_ CPLEASE "$A")
[ "$ADOPT" = "OK" ] || { echo "FAIL: first-touch adoption did not return OK ($ADOPT)"; exit 1; }
echo "  $A holds the lease"

# POSITIVE CONTROL, and the drill is worthless without it. Every assertion
# below is of the form "this address did NOT get OK", and an instrument that
# never says SUPERSEDED would satisfy all of them while proving nothing. Prove
# the refusal is reachable BEFORE relying on its absence.
echo "== control: the peer reads as superseded, so a refusal is observable here"
PEER=$(cp_ CPLEASE "$B")
case "$PEER" in
  *SUPERSEDED*) echo "  $B -> $PEER" ;;
  *) echo "FAIL: the control did not fire -- $B answered '$PEER', not SUPERSEDED."
     echo "      Every later assertion reads 'not OK', so without this the drill"
     echo "      cannot tell a fixed control plane from a broken instrument."
     exit 1 ;;
esac

echo "== repoint: $B is replaced by $N, then $N is promoted and fenced"
fleet_cp $CPPORT CPSETPAIR 0 "$A,$N"
FENCE=$(cp_ CPFENCE "$N")
case "$FENCE" in
  OK*) echo "  $FENCE" ;;
  *) echo "FAIL: the fence itself was refused ($FENCE) -- the drill never"
     echo "      reached the state it is about."; exit 1 ;;
esac

echo "== the question: how many addresses does the CP call master?"
LA=$(cp_ CPLEASE "$A")
LN=$(cp_ CPLEASE "$N")
echo "  $A -> $LA"
echo "  $N -> $LN"
OKS=0
[ "$LA" = "OK" ] && OKS=$((OKS + 1))
[ "$LN" = "OK" ] && OKS=$((OKS + 1))

# The fenced member MUST hold it. "Nobody is master" is not a pass -- it is a
# different fault, and one this drill would otherwise report as success.
[ "$LN" = "OK" ] || {
  echo "FAIL: the freshly fenced master $N does not hold the lease ($LN)."
  echo "      That is not BUG-0151; it is the fence failing to take at all."
  cp_ CPSUBSETS >/dev/null 2>&1
  exit 1; }

if [ "$OKS" -ne 1 ]; then
  echo "FAIL: $OKS addresses hold the write lease for one pair at the same time."
  echo "      BUG-0151 REACHED THROUGH THE PRODUCT. The sequence was ordinary:"
  echo "        CPADDPAIR $A,$B -> CPLEASE $A -> CPSETPAIR 0 $A,$N -> CPFENCE $N"
  echo "      which is replace-a-failed-replica followed by lose-the-master."
  echo "      The stale row still names $A and nothing migrated it, so the"
  echo "      containment read finds a different row for each caller."
  exit 1
fi

echo "PASS: lease after repoint — a pair whose membership changed still has exactly one master of record; the fenced member holds it, the displaced incumbent does not, and the refusal was proven observable first"
