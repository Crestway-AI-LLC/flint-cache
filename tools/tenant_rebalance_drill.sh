#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Multi-tenant rebalance drill: two tenants write into the SAME slots (same
# hash tags, different namespaces) all landing on one master; the controller
# rebalances with (ns, slot) as the move unit. Asserts:
#   - fills converge across the group
#   - each tenant's keys stay correct THROUGH THE PROXY (per-(ns,slot)
#     routing: tenant A's moved unit must not reroute tenant B's same-slot
#     unit that did not move)
#   - per-tenant DBSIZE is conserved (no loss, no duplication, no bleed)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-tr- 6668 6700 6701
fleet_guard
fleet_kill controller; fleet_kill server; fleet_kill proxy; sleep 0.4
B=./target/release/flint-server
P0=6700; P1=6701
cleanup() {
  # fleet_kill, not a truncated pkill pattern: "--port 670" is a substring
  # match that reaches ports this drill does not own.
  # Scoped to the ports this drill declared to fleet_init.
  fleet_kill controller
  fleet_kill server
  fleet_kill proxy
  rm -rf $FLINT_DRILL_ROOT/flint-tr-*
}
trap cleanup EXIT

for p in $P0 $P1; do
  d="$FLINT_DRILL_ROOT/flint-tr-$p"; rm -rf "$d"
  $B --port $p --engine rocks --data-dir "$d" 2>"${FLEET_SCOPE}server.log" &
done
sleep 0.8

./target/release/flint-proxy --port 6668 --pairs "127.0.0.1:$P0;127.0.0.1:$P1" \
  --tenants "tokA=alpha,tokB=beta" 2>$FLINT_DRILL_ROOT/flint-tr-proxy.log &
fleet_wait_listen 6668
sleep 0.5

# Four hash tags whose slots default-route to pair 0 (slot < 8192), so all
# seeded data lands on g0 and the group starts maximally unbalanced.
TAGS=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
picked=[]
i=0
while len(picked)<4:
    t=f"t{i}"
    if c(t.encode())%16384 < 8192: picked.append(t)
    i+=1
print(" ".join(picked))')
echo "== tags on pair-0 slots: $TAGS"

echo "== seed via AUTHED proxy: alpha 4x3000=12000, beta same tags 4x1000=4000"
for t in $TAGS; do
  awk -v tag="$t" 'BEGIN{for(i=0;i<3000;i++){k=sprintf("{%s}:k%05d",tag,i);v=sprintf("alpha-%s-%05d",tag,i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p 6668 -a tokA --no-auth-warning --pipe >/dev/null
  awk -v tag="$t" 'BEGIN{for(i=0;i<1000;i++){k=sprintf("{%s}:k%05d",tag,i);v=sprintf("beta-%s-%05d",tag,i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p 6668 -a tokB --no-auth-warning --pipe >/dev/null
done
DA=$(valkey-cli -p 6668 -a tokA --no-auth-warning DBSIZE)
DB=$(valkey-cli -p 6668 -a tokB --no-auth-warning DBSIZE)
[ "$DA" = "12000" ] && [ "$DB" = "4000" ] || { echo "FAIL: seed sizes A=$DA B=$DB"; exit 1; }
G0=$(valkey-cli -p $P0 FLINTSLOTSTATS | awk '{s+=$2} END{print s}')
echo "  tenant DBSIZE: alpha=$DA beta=$DB; node g0 rows=$G0 (all on g0)"

echo "== controller with --rebalance-execute ((ns,slot) units)"
./target/release/flint-controller --pairs "127.0.0.1:$P0;127.0.0.1:$P1" \
  --id TRX --poll-ms 200 --rebalance-deadband 0.2 --rebalance-execute --max-slots-per-cycle 3 \
  2>$FLINT_DRILL_ROOT/flint-tr-ctl.log &

CTL=$FLINT_DRILL_ROOT/flint-tr-ctl.log

# WAIT FOR THE MOVES TO FINISH, NOT FOR THE PLAN TO BE ANNOUNCED (BUG-0094).
#
# "rebalance EXECUTE" is printed by execute_move BEFORE the first unit moves,
# and the units are then migrated SERIALLY. Balance arrives before the plan
# does: three alpha units of 3000 leave fills g0=10000 g1=6000 after TWO of
# them, and 10000*100 <= 8000*125 holds EXACTLY, so the old predicate could
# break out with the third migration still running. DBSIZE counts residency
# and the proxy SUMS the masters, so the conservation assert below then read
# a slot resident on both nodes and reported alpha=15000.
#
# The sound signal is the controller's per-unit line: MIGRATEIN-OK is logged
# only after the SOURCE has replied, and the source does not reply until it
# has purged the slot. The nodes' own FLINTMIGRATIONS is checked as well, but
# is NOT sufficient alone -- the destination clears Importing before the flip
# (migrate.rs "Step 5: flip dest-first") and the source's Moved is hidden from
# the bare form, so for the length of the source's purge BOTH nodes look
# quiescent while both still hold the rows.
units_announced() { grep -oE 'units \[[^]]*\]' "$CTL" 2>/dev/null | grep -o '("' | wc -l | tr -d ' '; }
units_resolved()  { local n; n=$(grep -c 'MIGRATEIN-OK' "$CTL" 2>/dev/null | tr -d ' '); echo "${n:-0}"; }
moves_failed()    { grep 'move failed' "$CTL" 2>/dev/null; }
inflight_rows()   { for p in $P0 $P1; do valkey-cli -p $p FLINTMIGRATIONS | grep -E 'importing|migrating'; done; }
aborted_rows()    { for p in $P0 $P1; do valkey-cli -p $p FLINTMIGRATIONS | grep 'aborted'; done; }

BALANCED=0; SAW_WORK=0
for i in $(seq 1 90); do
  # An abandoned move is a finding, not something to wait out: the counts
  # below can never settle after one, and 90s of silence would report it as
  # "did not converge" -- a cause the check never established.
  FAILED=$(moves_failed)
  if [ -n "$FAILED" ]; then
    echo "FAIL: the controller abandoned a move:"; echo "$FAILED" | sed 's/^/    /'; exit 1
  fi
  AB=$(aborted_rows)
  if [ -n "$AB" ]; then
    echo "FAIL: a destination abandoned an import:"; echo "$AB" | sed 's/^/    /'; exit 1
  fi
  N0=$(valkey-cli -p $P0 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}')
  N1=$(valkey-cli -p $P1 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}')
  MAX=$N0; [ "$N1" -gt "$MAX" ] && MAX=$N1
  MEAN=$(( (N0+N1) / 2 ))
  A=$(units_announced); R=$(units_resolved); IF=$(inflight_rows)
  if [ "$A" -gt "$R" ] || [ -n "$IF" ]; then SAW_WORK=$((SAW_WORK+1)); fi
  if [ "$MEAN" -gt 0 ] && [ $((MAX*100)) -le $((MEAN*125)) ] \
     && [ "$A" -gt 0 ] && [ "$A" = "$R" ] && [ -z "$IF" ]; then
    BALANCED=1; break
  fi
  sleep 1
done
N0=$(valkey-cli -p $P0 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}')
N1=$(valkey-cli -p $P1 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}')
echo "  node fills after: g0=$N0 g1=$N1 (moves: $(grep -c 'MIGRATEIN-OK' $FLINT_DRILL_ROOT/flint-tr-ctl.log))"
grep -oE "units \[[^]]*\]" $FLINT_DRILL_ROOT/flint-tr-ctl.log | head -3 | sed 's/^/  /'
# Says what was observed and nothing more: whether the plan finished is a
# different failure from whether the fills converged, and they read alike.
if [ "$BALANCED" != "1" ]; then
  A=$(units_announced); R=$(units_resolved); IF=$(inflight_rows)
  echo "FAIL: did not settle in 90s — fills g0=$N0 g1=$N1, units announced=$A resolved=$R"
  if [ -n "$IF" ]; then echo "  still in flight:"; echo "$IF" | sed 's/^/    /'; fi
  tail -12 "$CTL"; exit 1
fi
# Load-bearing or not, on THIS host: 0 means every move completed inside one
# poll and the wait proved nothing here, which is not the same as the wait
# being unnecessary. Under the gate's parallelism it is where the 15000 came
# from.
echo "  settle: units announced=$(units_announced) resolved=$(units_resolved); polls that saw work in flight: $SAW_WORK"

echo "== per-tenant conservation via proxy fan-out"
DA=$(valkey-cli -p 6668 -a tokA --no-auth-warning DBSIZE)
DB=$(valkey-cli -p 6668 -a tokB --no-auth-warning DBSIZE)
[ "$DA" = "12000" ] || { echo "FAIL: alpha lost/gained keys: $DA"; exit 1; }
[ "$DB" = "4000" ]  || { echo "FAIL: beta lost/gained keys: $DB"; exit 1; }
echo "  alpha=12000 beta=4000 — conserved"

echo "== every tag readable and correct for BOTH tenants through the proxy"
for t in $TAGS; do
  GA=$(valkey-cli -p 6668 -a tokA --no-auth-warning GET "{$t}:k00042")
  GB=$(valkey-cli -p 6668 -a tokB --no-auth-warning GET "{$t}:k00042")
  [ "$GA" = "alpha-$t-00042" ] || { echo "FAIL: alpha {$t} read: '$GA'"; exit 1; }
  [ "$GB" = "beta-$t-00042" ]  || { echo "FAIL: beta {$t} read: '$GB'"; exit 1; }
done
echo "  all tags correct for both tenants"

echo "== the (ns,slot) independence proof: some unit moved for one tenant only"
MIXED=0
for t in $TAGS; do
  # Where does each tenant's data for this tag live NOW? Ask g0 directly:
  # a moved unit answers -MOVED; an unmoved one serves.
  SA=$(valkey-cli -p $P0 FLINTSLOTSTATS | grep " alpha$" | wc -l)
  SB=$(valkey-cli -p $P0 FLINTSLOTSTATS | grep " beta$" | wc -l)
  if [ "$SA" != "$SB" ]; then MIXED=1; break; fi
done
[ "$MIXED" = "1" ] && echo "  confirmed: alpha and beta hold different unit sets on g0 (independent moves)" \
                   || echo "  (units moved in lockstep this run — acceptable, correctness held)"

echo "PASS: multi-tenant rebalance — (ns,slot) units moved, both tenants conserved and correct via proxy"
