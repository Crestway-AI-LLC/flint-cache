#!/usr/bin/env bash
# BUG-0076: after a promotion, does the survivor that was NEITHER killed nor
# promoted find the new master?
#
# Nothing used to tell it. A replica reads --replica-of once at startup, and
# on a two-member pair the re-point is a side effect of `restart-node`
# bringing the dead seat back -- which every survivor gets, because there is
# only one. On three members the untouched survivor dialled the dead address
# forever: every member UP, roles and epochs correct, and the pair silently
# running single-copy. Found by the first three-member chaos run.
#
# The assertion is the bug's exact signature, and it is deliberately made
# BEFORE the dead seat is restarted: with the master killed, a three-member
# pair has one promoted master and one survivor, so the master must report
# live_replicas 1. Unfixed, it reports 0 forever. Restarting the dead node
# first would hide the bug -- that restart carries its own re-point.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-repoint 7466 7467 7468 7469 7470
fleet_guard
D=$FLINT_DRILL_ROOT/flint-repoint; INV=$D/cluster.flint
CTL=./target/release/flintctl
fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7470
pair 127.0.0.1:7466,127.0.0.1:7467,127.0.0.1:7468
proxy 127.0.0.1:7469
controller on
min-replicas 1
poll-ms ${FLINT_POLL_MS:-150}
confirm ${FLINT_CONFIRM:-3}
EOF

$CTL -f "$INV" bootstrap >"$D-boot.log" 2>&1 || {
  # The reason bootstrap failed is in ITS OWN output, and this line
  # used to send that to /dev/null and then report a bare failure --
  # so the largest cluster of gate reds ("FAIL: bootstrap") could not
  # be diagnosed from the artifact at all. Two drills that captured it
  # showed the actual cause immediately: a replica still `loading`
  # when verify ran (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$D-boot.log"; exit 1; }

master_of() { $CTL -f "$INV" status 2>/dev/null | awk '/ master /{print $3; exit}'; }
live_of()   { $CTL -f "$INV" status 2>/dev/null | awk -v a="$1" '$3==a{for(i=1;i<=NF;i++) if($i=="live_replicas") print $(i+1)}'; }

# The fixture itself must be a three-member pair with both replicas streaming,
# or the assertion below proves nothing about re-pointing.
M0=$(master_of)
for _ in $(seq 1 60); do [ "$(live_of "$M0")" = 2 ] && break; sleep 0.5; done
[ "$(live_of "$M0")" = 2 ] \
  || { echo "FAIL: fixture — $M0 never had 2 live replicas (got '$(live_of "$M0")')"; exit 1; }
echo "  fixture OK: master $M0 with 2 live replicas"

echo "== kill the master; the controller promotes ONE survivor, leaving one untouched"
$CTL -f "$INV" kill-node "$M0" >/dev/null 2>&1 || { echo "FAIL: kill-node"; exit 1; }
M1=""
for _ in $(seq 1 60); do M1=$(master_of); [ -n "$M1" ] && [ "$M1" != "$M0" ] && break; sleep 0.5; done
[ -n "$M1" ] && [ "$M1" != "$M0" ] || { echo "FAIL: no promotion within 30s"; exit 1; }
echo "  promoted: $M1"

# THE ASSERTION. No restart-node in between, on purpose.
OK=0
for _ in $(seq 1 60); do [ "$(live_of "$M1")" = 1 ] && { OK=1; break; }; sleep 0.5; done
if [ "$OK" != 1 ]; then
  echo "FAIL: the untouched survivor never followed $M1 (live_replicas '$(live_of "$M1")', want 1)"
  $CTL -f "$INV" status 2>&1 | sed 's/^/    /'
  exit 1
fi
echo "  PASS: the untouched survivor re-pointed at $M1 without a restart"

# And the pair returns to full strength once the dead seat comes back.
$CTL -f "$INV" restart-node "$M0" >/dev/null 2>&1 || { echo "FAIL: restart-node"; exit 1; }
for _ in $(seq 1 120); do [ "$(live_of "$M1")" = 2 ] && break; sleep 0.5; done
[ "$(live_of "$M1")" = 2 ] \
  || { echo "FAIL: pair never returned to 2 live replicas (got '$(live_of "$M1")')"; exit 1; }
$CTL -f "$INV" verify >/dev/null 2>&1 || { echo "FAIL: fleet does not verify after recovery"; exit 1; }
echo "  PASS: three members again, verify clean"
echo "PASS three_member_repoint"
