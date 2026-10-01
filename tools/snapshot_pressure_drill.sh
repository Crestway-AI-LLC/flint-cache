#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0195: do rewind snapshots give way when the disk fills, or do writes?
#
# WHY THIS EXISTS. The 2026-09-30 soak (20 MB/s through a 20 GB window, a
# snapshot every 30 s) failed at cycle 9 with every write refused:
# `QUOTA server is low on disk space`. A snapshot is a set of hard links, so on
# a churning pair it pins every SST compaction has since replaced, and
# retention kept every snapshot for a day. 108 minutes of them filled a 468 GB
# NVMe. A snapshot only makes a rejoin faster; refusing every write to keep one
# is the wrong way round.
#
# THE SHAPE, small: a 512 MB filesystem holding the data directory and the
# snapshot root, a 25 MB live set rewritten in full every round with a
# snapshot and a forced compaction after each. Every round orphans the previous
# round's SSTs, so only the snapshots hold them. Thirty rounds write ~750 MB:
# without relief the disk crosses the 10% floor around round sixteen and writes
# shed. The WAL archive is pinned small so it cannot be what fills the disk.
#
# ASSERTED: not one write refused across all rounds; the server says it
# released snapshots under pressure; at least the newest four plus LATEST
# survive; and the final round's values read back byte for byte.
#
# Uses a real small filesystem, as disk_pressure and evictable_pressure do,
# because the point is genuine fullness, not a threshold moved until it fires.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-snappressure 6411
fleet_guard
PORT=6411

# BUG-0019: the image lives where the boot volume is, not necessarily where the
# drill root is.
IMGROOT=$FLINT_DRILL_ROOT
if [ "$(uname)" = "Darwin" ]; then
  _root_dev=$(df -P "$FLINT_DRILL_ROOT" 2>/dev/null | awk 'END{print $1}')
  _boot_dev=$(df -P "${TMPDIR:-/tmp}" 2>/dev/null | awk 'END{print $1}')
  if [ -n "$_root_dev" ] && [ "$_root_dev" != "$_boot_dev" ]; then
    IMGROOT=${TMPDIR:-/tmp}
  fi
fi
if [ "$(uname)" = "Darwin" ]; then IMG=$IMGROOT/flint-snappressure.dmg; else IMG=$IMGROOT/flint-snappressure.img; fi
MNT=$IMGROOT/flint-snappressure-mnt
LOG=$FLINT_DRILL_ROOT/flint-snappressure.log
SIZE_MB=512
ROUNDS=30
KEYS=400   # x 64 KB = a 25 MB live set

fail() { echo "FAIL: $*"; exit 1; }
cleanup() {
  pkill -9 -f "flint-server --port $PORT" 2>/dev/null
  if [ "$(uname)" = "Darwin" ]; then hdiutil detach "$MNT" -force -quiet 2>/dev/null
  else sudo umount "$MNT" 2>/dev/null; fi
  rm -rf "$IMG" "$MNT" 2>/dev/null
}
trap cleanup EXIT
cleanup

echo "== a ${SIZE_MB}MB filesystem for the data and its snapshots"
if [ "$(uname)" = "Darwin" ]; then
  mkdir -p "$MNT"
  hdiutil create -size "${SIZE_MB}m" -fs HFS+ -volname flintsnp -quiet "$IMG" || fail "create image"
  hdiutil attach "$IMG" -mountpoint "$MNT" -quiet || fail "attach image"
else
  command -v mkfs.ext4 >/dev/null || { echo "SKIP: mkfs.ext4 not available"; exit 0; }
  sudo -n true 2>/dev/null || { echo "SKIP: needs passwordless sudo to mount a loop device"; exit 0; }
  mkdir -p "$MNT"
  dd if=/dev/zero of="$IMG" bs=1M count="$SIZE_MB" status=none
  mkfs.ext4 -q "$IMG"
  sudo mount -o loop "$IMG" "$MNT" || fail "mount loop"
  sudo chown "$(id -u):$(id -g)" "$MNT" || fail "chown mount"
fi

cargo build --release -q -p flint-server --features flint-server/rocks || fail "build"

echo "== node on it: shed below 10% free, so snapshot relief starts below 25%"
./target/release/flint-server --port "$PORT" --engine rocks --data-dir "$MNT/data" \
  --disk-min-free-pct 10 --disk-min-free-bytes 0 --disk-sample-ms 200 \
  --wal-size-limit-mb 16 --wal-ttl-seconds 2 >"$LOG" 2>&1 &
ready() {
  [ "$(valkey-cli -p $PORT PING 2>/dev/null)" = "PONG" ] &&
    ! valkey-cli -p $PORT FLINTINFO 2>/dev/null | tr -d '\r' | grep -qx 'loading:1'
}
for _ in $(seq 1 120); do ready && break; sleep 0.25; done
ready || fail "server did not become ready; see $LOG"

# One round: rewrite every key with values that cannot compress (a repeated
# pattern would shrink to nothing and fill no disk), and count refusals.
# Values are seeded by (round, key) so the last round can be checked exactly.
round() { # round <n> -> prints the number of refused writes
  python3 - "$PORT" "$1" "$KEYS" <<'PYR'
import random, socket, sys
port, rnd, keys = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
def resp(a):
    out = b"*%d\r\n" % len(a)
    for x in a:
        x = x if isinstance(x, bytes) else x.encode()
        out += b"$%d\r\n%s\r\n" % (len(x), x)
    return out
s = socket.create_connection(("127.0.0.1", port), timeout=60)
s.sendall(resp(["FLINTNS", "churn"])); s.recv(64)
refused = 0
BLOCK = 25
for i in range(0, keys, BLOCK):
    n = min(BLOCK, keys - i)
    s.sendall(b"".join(resp(["SET", "k%d" % j, random.Random(rnd * 100003 + j).randbytes(65536)])
                       for j in range(i, i + n)))
    buf = b""
    while buf.count(b"\r\n") < n:
        chunk = s.recv(65536)
        if not chunk:
            sys.exit("connection closed")
        buf += chunk
    refused += sum(1 for line in buf.split(b"\r\n")[:n] if line.startswith(b"-"))
s.close()
print(refused)
PYR
}

REFUSED=0
for r in $(seq 1 "$ROUNDS"); do
  n=$(round "$r") || fail "round $r: the writer died ($n)"
  REFUSED=$((REFUSED + n))
  valkey-cli -p $PORT FLINTCOMPACT churn >/dev/null
  SNAP=$(valkey-cli -p $PORT FLINTSNAPSHOT "$MNT/snaps")
  case "$SNAP" in OK\ snap-*) ;; *) fail "round $r: FLINTSNAPSHOT answered '$SNAP'" ;; esac
  FREE=$(valkey-cli -p $PORT FLINTINFO | tr '\r' '\n' | sed -n 's/^disk_free_pct://p')
  # What holds the disk: the WAL archive, the live DB, and what ONLY the
  # snapshots pin. One du for the last two, so a hard link shared with the
  # live DB is charged to it and the snapshot figure is pinned garbage alone.
  ARCH=$(du -sm "$MNT/data/archive" 2>/dev/null | cut -f1)
  set -- $(du -sm "$MNT/data" "$MNT/snaps" 2>/dev/null | cut -f1)
  printf '  round %2d: %d refused, %s held, disk free %s%% (archive %sM, data %sM, snapshot-only %sM)\n' \
    "$r" "$n" "$(ls "$MNT/snaps" | grep -c '^snap-')" "$FREE" "${ARCH:-0}" "${1:-?}" "${2:-?}"
done

[ "$REFUSED" -eq 0 ] || { tail -5 "$LOG" | sed 's/^/    log: /'
  fail "$REFUSED write(s) refused across $ROUNDS rounds -- the disk filled behind snapshots"; }
grep -q "under disk pressure" "$LOG" \
  || fail "no write was refused, but nothing was released either: this run never reached
      pressure, so it tested nothing (raise ROUNDS or KEYS)"
HELD=$(ls "$MNT/snaps" | grep -c '^snap-')
[ "$HELD" -ge 5 ] || fail "only $HELD snapshot(s) left -- the newest four and LATEST must survive"
LATEST=$(cat "$MNT/snaps/LATEST")
[ -d "$MNT/snaps/$LATEST" ] || fail "LATEST names $LATEST, which is gone"

# The data under all of this is intact: every key holds the last round's value.
python3 - "$PORT" "$ROUNDS" "$KEYS" <<'PYV' || fail "the final values did not read back"
import random, socket, sys
port, rnd, keys = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
s = socket.create_connection(("127.0.0.1", port), timeout=60)
f = s.makefile("rb")
def cmd(*a):
    s.sendall(b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a))
    line = f.readline()
    if line.startswith(b"$"):
        n = int(line[1:-2])
        return None if n < 0 else f.read(n + 2)[:-2]
    return line
cmd(b"FLINTNS", b"churn")
for j in range(keys):
    want = random.Random(rnd * 100003 + j).randbytes(65536)
    if cmd(b"GET", b"k%d" % j) != want:
        sys.exit("k%d does not hold round %d's value" % (j, rnd))
PYV

echo "PASS: snapshot pressure -- ${ROUNDS} rounds of churn on a ${SIZE_MB}MB disk, 0 writes refused, old snapshots released (${HELD} held), every key intact"
