#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# flintctl + 3-seat Raft CP: the inventory contract "3 cp lines = a Raft
# group" — previously a comment reading "3 = Raft, later".
#
# controlplane_ha_drill proves the Raft CP itself (election, redirect,
# leader kill, durability). THIS drill proves flintctl can be its OPERATOR:
# bootstrap starts all three seats with derived raft args, registration
# routes to the leader, tenant mutations FOLLOW a -LEADER redirect (via
# call_cp), and — the part that matters in production — a mutation issued
# AFTER the leader is killed still lands, because call_cp walks to the new
# leader instead of reporting "the CP rejected the command".
#
# The production stake: the 4-host Limited-mode env runs a 3-seat CP
# co-located on existing hosts. Without this tooling those seats would be
# hand-managed, which is how one of them quietly stops being restarted.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-cpha-ctl 6930 6931 7561 7562 7563 7861
fleet_guard
CTL=./target/release/flintctl
D=$FLINT_DRILL_ROOT/flint-cpha-ctl; INV=$D/cluster.flint
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() {
  $CTL -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cat > "$INV" <<EOF
statedir $D/state
bins ./target/release
disposable on
cp 127.0.0.1:7561
cp 127.0.0.1:7562
cp 127.0.0.1:7563
pair 127.0.0.1:6930,127.0.0.1:6931
proxy 127.0.0.1:7861
EOF

echo "== bootstrap: three seats, one leader, registration lands"
$CTL -f "$INV" bootstrap >"$D/boot.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -15 "$D/boot.log"; exit 1; }
grep -q "3 raft seats up, leader elected" "$D/boot.log" \
  || { echo "FAIL: bootstrap did not report an elected raft leader"; tail -5 "$D/boot.log"; exit 1; }

echo "== all three seats visible in status"
ST=$($CTL -f "$INV" status 2>&1)
N=$(echo "$ST" | grep -c "^cp .*up")
[ "$N" = "3" ] || { echo "FAIL: status shows $N/3 CP seats up"; echo "$ST"; exit 1; }
echo "  3/3 seats up"

echo "== a tenant mutation lands regardless of which seat leads"
$CTL -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1 \
  || { echo "FAIL: tenant add against the raft CP"; exit 1; }
sleep 1.5
[ "$(valkey-cli -p 7861 -a tok-acme --no-auth-warning SET k1 v1 2>/dev/null)" = "OK" ] \
  || { echo "FAIL: tenant not serving through the proxy"; exit 1; }
echo "  tenant added and serving"

echo "== an unknown name is REFUSED by the group, not committed as a no-op (BUG-0160)"
# ctl_error_drill asserts these refusals against ONE control-plane seat, and
# the Raft arms used to propose a no-op and reply OK instead -- so the suite
# certified refusals the three-seat topology production runs did not make.
# Asked through flintctl where it has a verb, and raw at the leader where not.
LEADER_ID=$(valkey-cli -p 7561 CPINFO 2>/dev/null | tr -d '\r' | grep '^leader:' | cut -d: -f2)
[ -n "$LEADER_ID" ] || { echo "FAIL: no seat reports a leader before the refusals"; exit 1; }
LPORT=$((7560 + LEADER_ID))
refused_ctl() {  # <want> <flintctl args...>: must exit non-zero AND say why
  local want=$1 out rc; shift
  out=$($CTL -f "$INV" "$@" 2>&1); rc=$?
  [ "$rc" != "0" ] || { echo "FAIL: flintctl $* exited 0 for a name that does not exist: $out"; exit 1; }
  case "$out" in
    *"$want"*) echo "  refused: flintctl $*" ;;
    *) echo "FAIL: flintctl $* failed, but not with '$want': $out"; exit 1 ;;
  esac
}
refused_raw() {  # <want> <CP command...>, sent to the leader seat
  local want=$1 out; shift
  out=$(valkey-cli -p "$LPORT" "$@" 2>&1)
  case "$out" in
    *"ERR "*"$want"*) echo "  refused: $*" ;;   # with or without "(error) "
    *) echo "FAIL: $* at the leader answered '$out', want an ERR naming '$want'"; exit 1 ;;
  esac
}
refused_ctl "no such tenant" tenant-reads ghost on
refused_ctl "no such tenant" tenant-async ghost on
refused_ctl "no such tenant" tenant-federate ghost on
refused_ctl "no such tenant" tenant-cache ghost on
refused_ctl "no such tenant" tenant-quota ghost 100 1000
refused_ctl "no such proxy" retire-proxy 127.0.0.1:1
refused_raw "no such tenant" CPTENANTOVERQUOTA ghost on
refused_raw "no such tenant" CPSETSUBSET ghost 127.0.0.1:7861
refused_raw "no such tenant" CPDROPPREV ghost
refused_raw "no such exception" CPCLEARSLOT acme 100
refused_raw "no such pair index" CPSETPAIR 7 127.0.0.1:6930,127.0.0.1:6931
# THE CONTROL. Every refusal above would also pass if these verbs now failed
# for everyone, so the same verbs must still succeed for a name that exists.
# `0 0` is "unlimited", so the control leaves acme exactly as it found it.
$CTL -f "$INV" tenant-quota acme 0 0 >"$D/quota-ok.log" 2>&1 \
  || { echo "FAIL: tenant-quota refused a tenant that exists"; cat "$D/quota-ok.log"; exit 1; }
for cmd in "CPTENANTOVERQUOTA acme off" "CPDROPPREV acme" "CPSETSUBSET acme 127.0.0.1:7861"; do
  # shellcheck disable=SC2086 -- word splitting is the point
  out=$(valkey-cli -p "$LPORT" $cmd 2>&1)
  case "$out" in
    OK*) ;;
    *) echo "FAIL: $cmd at the leader answered '$out' for a tenant that exists"; exit 1 ;;
  esac
done
echo "  and the same verbs still succeed for acme"

echo "== KILL THE LEADER: the next mutation must still land"
# Find the leader by asking any seat; kill exactly that seat's process.
LEADER_ID=$(valkey-cli -p 7561 CPINFO 2>/dev/null | tr -d '\r' | grep '^leader:' | cut -d: -f2)
[ -n "$LEADER_ID" ] || LEADER_ID=$(valkey-cli -p 7562 CPINFO 2>/dev/null | tr -d '\r' | grep '^leader:' | cut -d: -f2)
[ -n "$LEADER_ID" ] || { echo "FAIL: no seat reports a leader"; exit 1; }
# Match on the seat's unique STATE DIR, not on flag order: cp_seat_args
# emits --state before --raft, so "--node-id N .*statedir" never matches.
# SCOPED (BUG-0136). `cp-state-n<id>` is flintctl's own naming, so this
# pattern was unique only because one drill uses it -- accident, not
# construction. $D/state is where this drill's inventory puts it.
pkill -9 -f "flint-controlplane.*$D/state/cp-state-n$LEADER_ID " \
  || { echo "FAIL: could not kill leader seat n$LEADER_ID"; exit 1; }
echo "  killed leader seat n$LEADER_ID"
sleep 2   # election

# The mutation that used to fail here with "the CP rejected the command":
# flintctl still dials cp[0] first; if that WAS the leader it is now dead,
# and if it was a follower it now redirects somewhere new. Either way
# call_cp must walk to the survivor quorum's leader.
$CTL -f "$INV" tenant add globex tok-glx globex 1 >"$D/add2.log" 2>&1 \
  || { echo "FAIL: tenant add after leader kill"; cat "$D/add2.log"; exit 1; }
sleep 1.5
[ "$(valkey-cli -p 7861 -a tok-glx --no-auth-warning SET k2 v2 2>/dev/null)" = "OK" ] \
  || { echo "FAIL: post-failover tenant not serving"; exit 1; }
echo "  mutation landed on the NEW leader; tenant serving"

echo "== the dead seat shows as DOWN, the survivors as up"
ST=$($CTL -f "$INV" status 2>&1)
UP=$(echo "$ST" | grep -c "^cp .*up"); DOWN=$(echo "$ST" | grep -c "^cp .*DOWN")
[ "$UP" = "2" ] && [ "$DOWN" = "1" ] \
  || { echo "FAIL: expected 2 up / 1 DOWN, got $UP up / $DOWN DOWN"; echo "$ST"; exit 1; }
echo "  status is honest: 2 up, 1 DOWN"

echo "PASS: flintctl bootstraps a 3-seat raft CP, follows the leader through a redirect, survives a leader kill mid-operation, and reports seat health honestly"
