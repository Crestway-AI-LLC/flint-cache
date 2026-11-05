#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0279: a replica whose first full sync is cut off part way can start
# again. The download writes into the data directory in the master's
# read_dir order, so a cut copy can hold CURRENT beside a MANIFEST or SST it
# never received, and RocksDB refused that at open on every start. Asserts,
# on real seats:
#   - a replica killed part way through its download leaves the
#     FULLSYNC_INCOMPLETE marker in its directory
#   - with CURRENT there too (as when it arrives first), started again as a
#     replica, it discards the copy, seeds again, and holds every key
#   - started without --replica-of, the same cut copy is refused with the
#     reason, and kept: there is nothing to seed it from
#   - a sync that finishes removes the marker (the control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-fscut- 6556 6557
fleet_guard
fleet_kill server; sleep 0.4
B=./target/release/flint-server
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-fscut-d.XXXXXX)
cleanup() {
  fleet_kill server
  rm -rf "$D"
}
trap cleanup EXIT
info() { valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$2://p"; }
M=6556; R=6557

echo "== a master with 8 MB a replica must copy at 1 MB/s"
$B --port $M --engine rocks --data-dir "$D/m" --fullsync-rate-bytes 1048576 2>"$D/m.log" &
fleet_wait_listen $M; fleet_wait_ping $M
python3 - <<'PY' | valkey-cli -p $M --pipe | tail -1
import sys, os, base64
for i in range(8000):
    k = f"k:{i}"
    v = base64.b64encode(os.urandom(750)).decode()
    sys.stdout.write(f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")
PY
[ "$(valkey-cli -p $M DBSIZE)" = 8000 ] || { echo "FAIL: the master holds $(valkey-cli -p $M DBSIZE) keys"; exit 1; }

echo "== a replica killed part way through its download leaves the marker"
$B --port $R --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:$M 2>"$D/r1.log" & RP=$!
CUT=0
for _ in $(seq 1 100); do
  [ -f "$D/r/FULLSYNC_INCOMPLETE" ] && [ "$(ls "$D/r" | wc -l)" -ge 2 ] && { CUT=1; break; }
  sleep 0.1
done
kill -9 $RP 2>/dev/null; wait $RP 2>/dev/null
[ "$CUT" = 1 ] || { echo "FAIL (BUG-0279): no FULLSYNC_INCOMPLETE beside a received file before the kill:"; ls -la "$D/r" | sed 's/^/    /'; exit 1; }
[ -f "$D/r/FULLSYNC_INCOMPLETE" ] || { echo "FAIL (BUG-0279): the cut copy has no marker"; exit 1; }
# CURRENT first, as read_dir order can send it: it names a MANIFEST this
# copy does not have, which is the state RocksDB refused on every start.
cp "$D/m/CURRENT" "$D/r/CURRENT"
echo "  marker present beside $(($(ls "$D/r" | wc -l) - 1)) received file(s); CURRENT added"

echo "== without --replica-of, the cut copy is refused and kept"
cp -a "$D/r" "$D/r-solo"
$B --port $R --engine rocks --data-dir "$D/r-solo" 2>"$D/solo.log" & SP=$!
for _ in $(seq 1 100); do kill -0 $SP 2>/dev/null || break; sleep 0.1; done
if kill -0 $SP 2>/dev/null; then
  kill -9 $SP; echo "FAIL (BUG-0279): a cut copy started as a master:"; sed 's/^/    /' "$D/solo.log"; exit 1
fi
grep -q "start it with --replica-of" "$D/solo.log" \
  || { echo "FAIL (BUG-0279): the refusal does not say why:"; sed 's/^/    /' "$D/solo.log"; exit 1; }
[ -f "$D/r-solo/CURRENT" ] || { echo "FAIL (BUG-0279): the refused copy was discarded"; exit 1; }
echo "  refused, naming --replica-of; the copy kept"

echo "== started again as a replica, it discards the cut copy and seeds again"
$B --port $R --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:$M 2>"$D/r2.log" &
fleet_wait_listen $R; fleet_wait_ping $R
SEEDED=0
for _ in $(seq 1 600); do
  [ "$(info $R loading)" = 0 ] && [ "$(valkey-cli -p $R DBSIZE 2>/dev/null)" = 8000 ] && { SEEDED=1; break; }
  sleep 0.1
done
[ "$SEEDED" = 1 ] || { echo "FAIL (BUG-0279): the restarted replica holds $(valkey-cli -p $R DBSIZE 2>&1) keys:"; sed 's/^/    /' "$D/r2.log"; exit 1; }
grep -q "FULLSYNC_INCOMPLETE present" "$D/r2.log" \
  || { echo "FAIL (BUG-0279): the restart does not say it discarded the cut copy:"; sed 's/^/    /' "$D/r2.log"; exit 1; }
for i in 0 4321 7999; do
  [ "$(valkey-cli -p $R GET "k:$i")" = "$(valkey-cli -p $M GET "k:$i")" ] || { echo "FAIL (BUG-0279): k:$i differs from the master"; exit 1; }
done
[ -f "$D/r/FULLSYNC_INCOMPLETE" ] && { echo "FAIL (BUG-0279): a finished sync left its marker"; exit 1; }
echo "  discarded, seeded again: 8000 keys, sampled equal to the master; the marker gone"

echo "PASS: a replica cut off part way through its full sync starts again by seeding afresh; the cut"
echo "      copy is refused, and kept, without --replica-of; a finished sync leaves no marker (BUG-0279)."
