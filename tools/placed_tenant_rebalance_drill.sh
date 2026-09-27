#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The balancer and a tenant placed on one pair (ADR-0053).
#
# A placed tenant lives whole on its pair: its transactions and scripts span
# its slots there, so one of its slots moved elsewhere would split them. The
# controller must therefore never move it, and must still count it, since its
# keys fill the pair like any others. A pair heavy with a placed tenant then
# looks overloaded for ever, and what the balancer does about that is the
# point of this drill: it moves every spread tenant's slot off that pair, and
# none of the placed one's.
#
# Both tenants start on pair 0: `jobs` placed there, `acme` spread, with every
# key in a slot the slot table gives pair 0. Rebalancing is armed. Asserts:
#   - every acme key reaches pair 1, hands-free
#   - no jobs key ever does, and the controller never names a jobs unit
#   - both tenants read back whole through the proxy
#   - jobs' transaction across slots still commits afterwards
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-placedrb-state 6473 6474 6475 6476 6477 6478
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-placedrb-state; INV=$FLINT_DRILL_ROOT/flint-placedrb.flint
PORT=6477; P0=6473; P1=6475

fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

# No `tls`: the seats are read directly below, one namespace at a time.
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:6478
pair 127.0.0.1:$P0,127.0.0.1:6474
pair 127.0.0.1:$P1,127.0.0.1:6476
proxy 127.0.0.1:$PORT
controller on
placed-tenants on
rebalance-deadband 0.2
rebalance-execute on
EOF

echo "== bootstrap 2 pairs; jobs placed on pair 0, acme spread"
./target/release/flintctl -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
./target/release/flintctl -f "$INV" tenant add-on-pair jobs tok-jobs jobs 0 1 >"$STATE-tenant.log" 2>&1 \
  && ./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >>"$STATE-tenant.log" 2>&1 || {
  echo "FAIL: tenant add"; cat "$STATE-tenant.log"; exit 1; }
for tok in tok-jobs tok-acme; do
  for _ in $(seq 1 30); do
    [ "$(valkey-cli -p $PORT -a $tok --no-auth-warning PING 2>/dev/null)" = "PONG" ] && break
    sleep 0.3
  done
done

# A namespace's key count on one seat.
seat_count() { printf 'FLINTNS %s\nDBSIZE\n' "$2" | valkey-cli -p "$1" 2>/dev/null | tail -1 | tr -cd '0-9'; }

# jobs: 8000 keys with no hash tag, so across thousands of slots, half of
# them pair 1's by the slot table. acme: 4 tags in pair 0's half.
JOBS=8000; PER=1000
_jobs_gen() {
  awk -v n=$JOBS 'BEGIN{for(i=0;i<n;i++){k=sprintf("j:%05d",i);v=sprintf("jv%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
}
TAGS=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
picked=[]; i=0
while len(picked)<4:
    t=f"pr{i}"
    if c(t.encode())%16384 < 8192: picked.append(t)
    i+=1
print(" ".join(picked))')
_acme_gen() {
  awk -v tag="$t" -v n=$PER 'BEGIN{for(i=0;i<n;i++){k=sprintf("{%s}k%05d",tag,i);v=sprintf("%s:%05d",tag,i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
}
echo "== seed through the proxy: jobs $JOBS keys, acme 4 x $PER on tags $TAGS"
fleet_load_resp $PORT _jobs_gen $JOBS "" tok-jobs || exit 1
for t in $TAGS; do
  fleet_load_resp $PORT _acme_gen $PER "" tok-acme || exit 1
done
ACME=$(( PER * 4 ))
J0=$(seat_count $P0 jobs); J1=$(seat_count $P1 jobs)
echo "  jobs: pair 0 = $J0, pair 1 = $J1"
[ "$J0" = "$JOBS" ] && [ "$J1" = "0" ] || {
  echo "FAIL: the placed tenant was not written whole to its pair"; exit 1; }

echo "== wait for the balancer to move every acme key to pair 1"
DEADLINE=$(( $(date +%s) + ${PLACED_REBALANCE_BUDGET_S:-240} ))
while :; do
  A1=$(seat_count $P1 acme)
  [ "${A1:-0}" = "$ACME" ] && break
  [ "$(date +%s)" -ge "$DEADLINE" ] && {
    echo "FAIL: acme on pair 1 = ${A1:-?} of $ACME after ${PLACED_REBALANCE_BUDGET_S:-240}s. Controller log:"
    grep rebalance "$STATE/logs/controller.log" | tail -15 | sed 's/^/  | /'; exit 1; }
  sleep 2
done
echo "  acme on pair 1 = $A1 of $ACME"

echo "== two more cycles: the placed tenant stays, and nothing is named to move it"
sleep 12
J0=$(seat_count $P0 jobs); J1=$(seat_count $P1 jobs)
echo "  jobs: pair 0 = $J0, pair 1 = $J1"
[ "$J0" = "$JOBS" ] && [ "$J1" = "0" ] || { echo "FAIL: the balancer moved the placed tenant"; exit 1; }
if grep "rebalance EXECUTE" "$STATE/logs/controller.log" | grep -q '"jobs"'; then
  echo "FAIL: the controller chose a unit of the placed tenant:"
  grep "rebalance EXECUTE" "$STATE/logs/controller.log" | grep '"jobs"' | head -3 | sed 's/^/  | /'; exit 1
fi
MOVES=$(grep -c "rebalance EXECUTE" "$STATE/logs/controller.log")
echo "  $MOVES move(s), every one of them acme's"

echo "== both tenants read back whole through the proxy"
J="valkey-cli -p $PORT -a tok-jobs --no-auth-warning"
A="valkey-cli -p $PORT -a tok-acme --no-auth-warning"
[ "$($J DBSIZE)" = "$JOBS" ] || { echo "FAIL: jobs DBSIZE $($J DBSIZE), want $JOBS"; exit 1; }
[ "$($A DBSIZE)" = "$ACME" ] || { echo "FAIL: acme DBSIZE $($A DBSIZE), want $ACME"; exit 1; }
for i in 00000 04321 07999; do
  [ "$($J GET j:$i)" = "jv$i" ] || { echo "FAIL: jobs j:$i reads '$($J GET j:$i)'"; exit 1; }
done
for t in $TAGS; do
  [ "$($A GET "{$t}k00999")" = "$t:00999" ] || { echo "FAIL: acme {$t}k00999 reads '$($A GET "{$t}k00999")'"; exit 1; }
done

echo "== jobs' transaction across slots still commits"
# `a` (15495) and `b` (3300): two slots, and two pairs by the slot table.
OUT=$(printf 'MULTI\nSET a 1\nINCR b\nEXEC\n' | $J 2>&1)
case "$OUT" in
  *EXECABORT*|*CROSSSLOT*|*ERR*) echo "FAIL: the transaction was refused: $OUT"; exit 1 ;;
esac
[ "$($J GET a)" = "1" ] && [ "$($J GET b)" = "1" ] || { echo "FAIL: the transaction did not apply whole: $OUT"; exit 1; }

echo "PASS: the balancer moved all $ACME of the spread tenant's keys off a pair a placed tenant fills, in $MOVES move(s), and none of the placed tenant's $JOBS"
