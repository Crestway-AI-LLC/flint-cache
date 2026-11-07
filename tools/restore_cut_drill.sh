#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0281: a restore whose copy of an object was cut short, or read other
# bytes, said OK. Restore verifies every object, then reads each one AGAIN to
# copy it, and the second read was trusted: the S3 body reader took a
# connection closed before its Content-Length for the end of the object. A
# MANIFEST cut part way opened as an older version of the database, its newer
# SSTs deleted as obsolete, and the restore printed OK.
#
# Runs the real flint-backup against tools/lib/s3_cut_stub.py, an S3
# stand-in over plain http that spoils one download. Asserts:
#   - the control: an untouched set restores OK, and a seat on the copy
#     serves every key
#   - the copy's read of the MANIFEST cut part way: refused, naming the
#     object, the bytes never received, and the partial restore left behind
#   - the copy's read of an SST returning one byte changed at full length:
#     refused, naming both hashes
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rcut- 6594 6595
fleet_guard
fleet_kill server; sleep 0.4
B=./target/release/flint-server
BK=./target/release/flint-backup
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-rcut-d.XXXXXX)
SP=""
cleanup() {
  [ -n "$SP" ] && kill "$SP" 2>/dev/null
  fleet_kill server
  rm -rf "$D"
}
trap cleanup EXIT
M=6594; P=6595
for b in "$B" "$BK"; do
  [ -x "$b" ] || { echo "FAIL: $b not built (cargo build --release --features flint-server/rocks,flint-backup/rocks)"; exit 1; }
done

echo "== a set of 20000 keys over four flushes, so its MANIFEST records four SSTs"
$B --port $M --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
fleet_wait_listen $M; fleet_wait_ping $M
printf 'version 1\npairs 1\n' >"$D/cp-state"
for r in 0 1 2 3; do
  python3 - $((r * 5000)) $(((r + 1) * 5000)) <<'PY' | valkey-cli -p $M --pipe >/dev/null
import sys
for i in range(int(sys.argv[1]), int(sys.argv[2])):
    k = f"k:{i}"; v = f"v-{i}-" + "x" * 200
    sys.stdout.write(f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")
PY
  # Each run checkpoints the master, and a checkpoint flushes. One set per
  # millisecond id, so the runs are a second apart.
  $BK run --pairs "127.0.0.1:$M" --cp-state "$D/cp-state" --to "$D/sets" --snap-root "$D/snaps" >"$D/run$r.log" 2>&1 \
    || { echo "FAIL: backup run $r:"; sed 's/^/    /' "$D/run$r.log"; exit 1; }
  sleep 1.1
done
SET=$(ls "$D/sets" | sort | tail -1)
[ "$(ls "$D/sets/$SET/pairs/0" | grep -c '\.sst$')" -ge 2 ] \
  || { echo "FAIL: the set has fewer than two SSTs, so a cut MANIFEST proves nothing:"; ls -l "$D/sets/$SET/pairs/0"; exit 1; }
fleet_kill server; sleep 0.4
echo "  $SET: $(ls "$D/sets/$SET/pairs/0" | grep -c '\.sst$') SSTs"

export FLINT_S3_ENDPOINT=http://127.0.0.1:$P AWS_ACCESS_KEY_ID=drill AWS_SECRET_ACCESS_KEY=drill AWS_REGION=us-east-1
# restore_through <case> <match> <mode>: one restore through a stub that
# spoils the second GET of the object whose key contains <match>.
restore_through() {
  python3 tools/lib/s3_cut_stub.py $P "$D/sets/$SET" bkt "sets/$SET" "$2" 2 "$3" 2>"$D/$1.stub" & SP=$!
  fleet_wait_listen $P
  $BK restore --from "s3://bkt/sets/$SET" --into "$D/$1" >"$D/$1.out" 2>&1; RC=$?
  kill $SP 2>/dev/null; wait $SP 2>/dev/null; SP=""
  return $RC
}

echo "== the control: an untouched set restores, and a seat on it serves every key"
restore_through whole no-such-key cut \
  || { echo "FAIL: an untouched set did not restore:"; sed 's/^/    /' "$D/whole.out"; exit 1; }
$B --port $M --engine rocks --data-dir "$D/whole/pair0" 2>"$D/whole.seat" &
fleet_wait_listen $M; fleet_wait_ping $M
[ "$(valkey-cli -p $M DBSIZE)" = 20000 ] || { echo "FAIL: the restored copy serves $(valkey-cli -p $M DBSIZE) of 20000 keys"; exit 1; }
fleet_kill server; sleep 0.4
echo "  OK restored; 20000 keys served"

echo "== the copy's read of the MANIFEST cut part way: refused"
if restore_through cutman MANIFEST- cut; then
  echo "FAIL (BUG-0281): a restore whose MANIFEST copy was cut said OK:"; sed 's/^/    /' "$D/cutman.out"
  if [ -d "$D/cutman/pair0" ]; then
    $B --port $M --engine rocks --data-dir "$D/cutman/pair0" 2>/dev/null &
    fleet_wait_listen $M; fleet_wait_ping $M
    echo "    and a seat on it serves $(valkey-cli -p $M DBSIZE) of 20000 keys"
  fi
  exit 1
fi
grep -q 'CUT GET #2' "$D/cutman.stub" || { echo "FAIL: the stub never cut the MANIFEST's second read"; exit 1; }
grep -q "copy MANIFEST-.*Content-Length bytes unread" "$D/cutman.out" \
  || { echo "FAIL (BUG-0281): the refusal does not name the object and the bytes never received:"; sed 's/^/    /' "$D/cutman.out"; exit 1; }
grep -q "holds a partial restore" "$D/cutman.out" \
  || { echo "FAIL (BUG-0281): the refusal does not say a partial restore was left:"; sed 's/^/    /' "$D/cutman.out"; exit 1; }
echo "  $(grep -o 'copy MANIFEST-[^;]*' "$D/cutman.out")"

echo "== the copy's read of an SST with one byte changed: refused"
if restore_through flipsst .sst flip; then
  echo "FAIL (BUG-0281): a restore whose SST copy read other bytes said OK:"; sed 's/^/    /' "$D/flipsst.out"; exit 1
fi
grep -q 'FLIP GET #2' "$D/flipsst.stub" || { echo "FAIL: the stub never changed the SST's second read"; exit 1; }
grep -q "was copied as .* but the verified set records" "$D/flipsst.out" \
  || { echo "FAIL (BUG-0281): the refusal does not compare the copy with the verified set:"; sed 's/^/    /' "$D/flipsst.out"; exit 1; }
echo "  refused: the copy's sha256 is not the verified set's"

echo "PASS: restore checks what it writes against the verified set: a copy cut part way, or one that"
echo "      read other bytes, is refused naming the object, never restored as OK (BUG-0281)."
