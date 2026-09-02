#!/usr/bin/env bash
# ADR-0035: a DETACHED PAIR needs a repair that keeps the seat's data, and
# `restart-node` is not it.
#
# WHY THIS EXISTS. On 2026-08-31 a playground pair ran twenty minutes with
# one disk holding the only copy: master up, every member reachable, epochs
# agreeing, and no replica streaming. The agent refused to act and paged
# twice, correctly — the only repair it had was `restart-node`, which calls
# roll_node with Rejoin::Reseed and marks the seat for reseed
# unconditionally. That is right for a DEAD seat, whose lineage is unknown,
# and wrong for a member sitting there holding good data and merely not
# tailing.
#
# `reattach-node` is the missing verb: warm restart on the SAME data dir, and
# REFUSE rather than fall back when the seat is not an observed live replica.
#
# WHAT IT PROVES, and the second and third matter more than the first:
#
#   1. A streaming replica is re-attached and converges.
#   2. It does NOT reseed — asserted against the server's own `rewind:` lines,
#      with `restart-node` run on the same fixture as the positive control.
#      Without that control, "no rewind happened" is also what you get from a
#      check that cannot see one.
#   3. A refusal STOPS NOTHING. The seat is still serving afterwards. A
#      conservative "I will not do this" that leaves the fleet a seat down is
#      an outage wearing a guard's clothes, and the ordering that prevents it
#      (decide before `stop_seat`) is invisible in the source unless asserted.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-reattach 7471 7472 7473 7474
fleet_guard
D=$FLINT_DRILL_ROOT/flint-reattach; INV=$D/cluster.flint; STATE=$D/state
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

cat > "$INV" <<INVEOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7474
pair 127.0.0.1:7471,127.0.0.1:7472
proxy 127.0.0.1:7473
controller on
min-replicas 1
poll-ms ${FLINT_POLL_MS:-150}
confirm ${FLINT_CONFIRM:-3}
INVEOF

$CTL -f "$INV" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }

master_of() { $CTL -f "$INV" status 2>/dev/null | awk '/ master /{print $3; exit}'; }
live_of()   { $CTL -f "$INV" status 2>/dev/null | awk -v a="$1" '$3==a{for(i=1;i<=NF;i++) if($i=="live_replicas") print $(i+1)}'; }
up()        { $CTL -f "$INV" status 2>/dev/null | grep -q "$1"; }
# `grep -c` PRINTS 0 and EXITS 1 on no match, so `grep -c ... || echo 0`
# emits TWO lines and every later [ -gt ] is a syntax error, not a comparison.
# Caught by this drill's own first run.
rewinds()   { local n; n=$(grep -cE "rewind:|full re-seed|NEEDS_RESEED" "$STATE/logs/node-$1.log" 2>/dev/null); echo "${n:-0}"; }

# The fixture must be a pair with the replica genuinely streaming, or every
# assertion below is about a cluster that was never healthy.
M=$(master_of)
[ -n "$M" ] || { echo "FAIL: no master after bootstrap"; exit 1; }
for _ in $(seq 1 60); do [ "$(live_of "$M")" = 1 ] && break; sleep 0.5; done
[ "$(live_of "$M")" = 1 ] || { echo "FAIL: replica never streamed; fixture is not a healthy pair"; exit 1; }
case "$M" in *7471) R=127.0.0.1:7472; RPORT=7472 ;; *) R=127.0.0.1:7471; RPORT=7471 ;; esac
echo "  fixture: master $M, streaming replica $R"

echo "== a streaming replica is re-attached, and NOT reseeded"
BEFORE=$(rewinds "$RPORT")
$CTL -f "$INV" reattach-node "$R" >/dev/null 2>&1 || { echo "FAIL: reattach-node $R"; exit 1; }
for _ in $(seq 1 60); do [ "$(live_of "$M")" = 1 ] && break; sleep 0.5; done
[ "$(live_of "$M")" = 1 ] || { echo "FAIL: $R did not converge after reattach-node"; exit 1; }
AFTER=$(rewinds "$RPORT")
[ "$BEFORE" = "$AFTER" ] || {
  echo "FAIL: reattach-node reseeded. rewind: lines went $BEFORE -> $AFTER."
  echo "      The whole point of this verb is that the data dir survives."
  exit 1; }
[ ! -f "$STATE/node-$RPORT/NEEDS_RESEED" ] || { echo "FAIL: reattach-node left a NEEDS_RESEED marker"; exit 1; }
echo "  converged, rewind: lines unchanged at $AFTER, no reseed marker"

echo "== POSITIVE CONTROL: restart-node on the same seat DOES reseed"
# Without this, "rewind: lines unchanged" above is equally satisfied by a
# fixture where no reseed could ever be observed -- a wrong log path, a
# server that does not log the line, a replica that never restarts at all.
$CTL -f "$INV" restart-node "$R" >/dev/null 2>&1 || { echo "FAIL: restart-node $R"; exit 1; }
for _ in $(seq 1 60); do [ "$(rewinds "$RPORT")" -gt "$AFTER" ] && break; sleep 0.5; done
CTRL=$(rewinds "$RPORT")
[ "$CTRL" -gt "$AFTER" ] || {
  echo "FAIL: restart-node produced no rewind: line either ($AFTER -> $CTRL)."
  echo "      So the assertion above proves nothing: this drill cannot SEE a"
  echo "      reseed, and would pass with reattach-node reseeding every time."
  exit 1; }
echo "  restart-node: rewind: lines $AFTER -> $CTRL — the check can see a reseed"

echo "== a refusal STOPS NOTHING: the seat is still serving afterwards"
# The master is the cheapest live seat that reattach-node must refuse, and
# what matters is not the refusal but that the fleet is whole afterwards.
# roll_node decides before `stop_seat` precisely so this holds.
for _ in $(seq 1 60); do [ "$(live_of "$M")" = 1 ] && break; sleep 0.5; done
OUT=$($CTL -f "$INV" reattach-node "$M" 2>&1); RC=$?
[ "$RC" -ne 0 ] || { echo "FAIL: reattach-node accepted the pair MASTER"; exit 1; }
up "$M" || { echo "FAIL: the refusal stopped the master. $OUT"; exit 1; }
[ "$(live_of "$M")" = 1 ] || { echo "FAIL: pair no longer streaming after a refusal"; exit 1; }
echo "  refused, master still serving, replica still streaming"

echo "== and a seat whose role cannot be read is refused, not marked"
# The WarmOnly test is `role: != replica`, which covers unreachable, reporting
# master, and reporting nothing. This constructs the unreachable one, which is
# the same branch; the live-reporting-master variant (BUG-0008's two durable
# masters) is not separately built here and shares this code path.
$CTL -f "$INV" kill-node "$R" >/dev/null 2>&1 || { echo "FAIL: kill-node $R"; exit 1; }
sleep 1
OUT=$($CTL -f "$INV" reattach-node "$R" 2>&1); RC=$?
[ "$RC" -ne 0 ] || { echo "FAIL: reattach-node accepted a seat it could not read"; exit 1; }
case "$OUT" in
  *"does not report role:replica"*) ;;
  *) echo "FAIL: refused for the wrong reason: $OUT"; exit 1 ;;
esac
[ ! -f "$STATE/node-$RPORT/NEEDS_RESEED" ] || {
  echo "FAIL: a REFUSAL marked the seat for reseed. Refusing and then doing"
  echo "      the destructive thing anyway is worse than not refusing."
  exit 1; }
echo "  refused naming what it read, and wrote no marker"

echo "PASS: a live replica re-attaches with its data intact, restart-node still reseeds, and every refusal leaves the fleet exactly as it found it"
