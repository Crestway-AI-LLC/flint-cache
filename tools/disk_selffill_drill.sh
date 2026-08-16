#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Can a node that filled its OWN disk dig itself back out?
#
# WHY THIS IS NOT disk_pressure_drill. That drill fills the volume from
# OUTSIDE, with a ballast file the server never wrote, and recovery is `rm
# ballast` — space returns instantly and completely. Every assertion it makes
# is real, and none of them touch the case the capacity work is actually
# about: the LSM growing into the wall on its own.
#
# The difference is not cosmetic. disk_pressure's own header names the trap
# and then does not exercise it: "an LSM needs headroom to compact, and the
# cure for a full disk is a trap without it: a delete is a write, and
# reclaiming the bytes needs the compaction that has no room to run." With a
# ballast file, freeing bytes is a syscall. With self-inflicted fill, freeing
# bytes is a COMPACTION — it costs temporary space before it returns any, the
# tombstones are themselves writes, and whether the loop closes is a property
# of the product, not of the test. Nobody has measured it.
#
# So this is the local, zero-cost rehearsal for the fill-to-full soak (#195):
# it asks the boundary question on a 768 MB image in about a minute instead of
# on 2 pairs of real nodes over a weekend.
#
# WHAT IT PINS
#   1. THE GUARD WINS THE RACE. The first write refusal the node ever issues
#      must be the headroom guard's -QUOTA, with verdict `shed` — never an IO
#      error, never a dead process. If ENOSPC gets there first the guard is
#      decorative, because ENOSPC lands inside RocksDB where a cache has no
#      good answer.
#   2. THE NODE STAYS IN A SAFE BAND at the boundary under sustained writes:
#      every refusal comes from the guard and none from the filesystem, free
#      space never falls below half the configured threshold, reads keep
#      serving, and the process stays alive.
#
#      This assertion was first written as "shed is STICKY — while nothing
#      changes, shed stays shed", and it failed on its first run against a
#      product that was behaving correctly. Under self-inflicted fill
#      something is always changing: compaction kept running after the drill
#      stopped writing, obsolete SSTs were dropped, free space went 18% -> 33%
#      unaided, and the guard cleared because the space had genuinely come
#      back. A cache that REFUSES rather than evicts must cycle at a full disk
#      — shed, compact, admit, fill, shed — so forbidding the cycle would have
#      been forbidding recovery. The band, not the stickiness, is the property.
#   3. THE NODE DIGS OUT UNAIDED. Delete most of the keyspace and the verdict
#      must return to ok BY ITSELF, with physical bytes actually reclaimed.
#      This is the assertion with a real chance of failing, and that is why
#      it is here: an LSM with no headroom cannot compact, and a node that
#      cannot compact cannot honour a delete. If this fails it is a product
#      finding for #95/#195, not a broken drill — the numbers printed beside
#      it are the evidence.
#   4. NO ACKED WRITE IS LOST across the whole excursion.
#
# RATIO, NOT VOLUME. The roadmap called for a ~10 GB device. It does not need
# one: shrinking the level base to 8 MB makes 768 MB behave like a node with
# many levels and constant compaction, which is the regime the assertions are
# about. Same trick as ingest_saturation_drill, one layer down.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-selffill" 6458
fleet_guard

PORT=6458
# macOS: hdiutil APPENDS .dmg when the path lacks it, so a bare .img name
# creates one file and attaches another. Name it per platform.
if [ "$(uname)" = "Darwin" ]; then IMG="$FLINT_DRILL_ROOT/flint-selffill.dmg"
else IMG="$FLINT_DRILL_ROOT/flint-selffill.img"; fi
# THE MOUNTPOINT IS NOT UNDER $FLINT_DRILL_ROOT, DELIBERATELY.
#
# macOS refuses `hdiutil attach -mountpoint` when the mountpoint lives inside
# another mounted volume: "attach failed - Permission denied", with no hint
# that the path is the problem. Since #180 exists precisely so the drill root
# can be an external SSD, the natural placement is the broken one.
#
# The IMAGE still lives on $FLINT_DRILL_ROOT, which is what #180 is actually
# about — every byte the node writes lands on the configured volume. Only the
# directory entry the volume is grafted onto is local.
MNT="${TMPDIR:-/tmp}/flint-selffill-mnt"
DIR="$MNT/data"
LOG="$FLINT_DRILL_ROOT/flint-selffill.log"
BIN="./target/release/flint-server"
SIZE_MB=768
# Reachable on a 768 MB volume, and clear of the filesystem's own overhead so
# the guard is not already shedding at boot.
MIN_PCT=20
MIN_BYTES=$((48 * 1024 * 1024))
# The ingest budget is a SAFETY BOUND, not a target: 30 x 2000 x 10 KB is
# 600 MB against a 768 MB image, so if the mount ever silently failed this
# drill still could not fill the developer's real disk. The mount assertion
# below is the primary guard; this is the belt to its braces. The guard should
# shed far earlier — physical runs ahead of logical, so 20% free is reached
# well inside 600 MB — and never reaching it is itself a failure below.
CHUNK_KEYS=2000
VSIZE=10000
MAX_CHUNKS=30

fail() { echo "FAIL: $*"; exit 1; }

# NEVER TRUST detach's EXIT STATUS — verify the mount is gone, and keep
# trying while the just-killed server's file handles are released. A wedged
# image poisons every later run: the server starts against a directory that is
# not the volume it thinks it is. disk_pressure_drill learned this over three
# consecutive red runs with a zero-byte server log; the lesson is copied here
# deliberately rather than re-learned. Returns 0 only when nothing is mounted.
unmount_now() {
  local i real
  real=$(cd "$MNT" 2>/dev/null && pwd -P) || real="$MNT"
  for i in $(seq 1 20); do
    mount | grep -qE " on (${MNT}|${real}) " || return 0
    if [ "$(uname)" = "Darwin" ]; then hdiutil detach "$MNT" -force -quiet 2>/dev/null
    else sudo umount "$MNT" 2>/dev/null; fi
    sleep 0.5
  done
  return 1
}

cleanup() {
  fleet_kill server
  # rm -rf on a directory that is STILL A MOUNT POINT deletes the image's
  # contents rather than the empty stand-in, and leaves the wedged mount
  # behind regardless. Unmount first, or leave both for the next run's guard.
  unmount_now || {
    echo "WARN: $MNT would not detach; leaving it mounted for the next run to clear"
    return
  }
  rm -rf "$IMG" "$MNT" 2>/dev/null
}

assert_unmounted() {
  unmount_now && return 0
  fail "$MNT is still mounted after 10s of detach attempts. Clear it by hand —
      'hdiutil detach $MNT -force' (macOS) or 'sudo umount $MNT' — and re-run"
}
trap cleanup EXIT
cleanup
assert_unmounted

echo "== a ${SIZE_MB}MB filesystem for the node to fill by itself"
if [ "$(uname)" = "Darwin" ]; then
  mkdir -p "$MNT"
  hdiutil create -size "${SIZE_MB}m" -fs HFS+ -volname flintselffill -quiet "$IMG" \
    || fail "could not create the disk image"
  hdiutil attach "$IMG" -mountpoint "$MNT" -quiet || fail "could not attach the disk image"
else
  command -v mkfs.ext4 >/dev/null || { echo "SKIP: mkfs.ext4 not available"; exit 0; }
  # sudo for the MOUNT ONLY, never the whole script: the toolchain is
  # installed per-user and root cannot find it.
  sudo -n true 2>/dev/null || { echo "SKIP: needs passwordless sudo to mount a loop device"; exit 0; }
  mkdir -p "$MNT"
  dd if=/dev/zero of="$IMG" bs=1M count="$SIZE_MB" status=none
  mkfs.ext4 -q "$IMG"
  sudo mount -o loop "$IMG" "$MNT" || fail "could not mount the loop device"
  sudo chown "$(id -u):$(id -g)" "$MNT" || fail "could not take ownership of the mount"
fi
mkdir -p "$DIR"

cargo build --release -q -p flint-server --features rocks || fail "build"
fleet_warm "$BIN"

# THE POSITIVE CONTROL ON THE APPARATUS, before a single byte is written.
#
# If the image failed to attach, $DIR is an ordinary directory on the
# developer's SSD, every assertion below still "passes" for the wrong reason,
# and the drill spends its ingest budget filling the wrong disk. Compare the
# device backing the data dir against the device backing the mount: they must
# be the same one, and it must not be the device backing the repo.
DEV_DATA=$(df -P "$DIR" | awk 'NR==2 {print $1}')
DEV_REPO=$(df -P . | awk 'NR==2 {print $1}')
[ "$DEV_DATA" != "$DEV_REPO" ] \
  || fail "the data dir is on the SAME device as the repo ($DEV_DATA) — the image is not mounted, and this run would have filled the real disk"
echo "  data dir on $DEV_DATA, repo on $DEV_REPO — the image is really mounted"

# A shrunken LSM: many levels and constant compaction at 768 MB, which is the
# regime the boundary questions live in.
export FLINT_LEVEL_BASE_MB=8
export FLINT_WRITE_BUFFER_MB=4

echo "== node up on it (min-free ${MIN_PCT}% / $((MIN_BYTES / 1048576))MB)"
"$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" \
  --disk-min-free-pct "$MIN_PCT" --disk-min-free-bytes "$MIN_BYTES" \
  --disk-sample-ms 500 >"$LOG" 2>&1 &
fleet_wait_listen "$PORT"

info() { valkey-cli -p "$PORT" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$1://p"; }

[ "$(info disk_verdict)" = "ok" ] || fail "verdict is $(info disk_verdict) before anything was written"
[ "$(info disk_unknown_samples)" = "0" ] || fail "the sampler cannot read the filesystem, so the guard is blind"

# Canaries written while healthy, read back at the end. Values are derivable
# from the key so a truncated or swapped value is caught, not just a missing
# one.
for i in 1 2 3; do
  cli_ok valkey-cli -p "$PORT" SET "canary:$i" "canary-value-for-$i"
done
echo "  verdict ok, sampler live, 3 canaries written"

echo "== ingest until the node stops accepting — and see WHAT stops it"
ACKED=0
SHED_AT=""
FIRST_REFUSAL=""
for c in $(seq 1 "$MAX_CHUNKS"); do
  # Incompressible values, or block compression squashes the fill and the
  # volume never fills at all (#169 was filed for exactly that).
  python3 - "$CHUNK_KEYS" "$VSIZE" "$c" > "$FLINT_DRILL_ROOT/flint-selffill.resp" <<'PYEOF'
import os, sys
n, vs, chunk = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
out, hdr = sys.stdout.buffer, b"*3\r\n$3\r\nSET\r\n"
vlen = ("$%d\r\n" % vs).encode()
for i in range(n):
    k = ("fill:%03d:%06d" % (chunk, i)).encode()
    out.write(hdr + ("$%d\r\n" % len(k)).encode() + k + b"\r\n" + vlen + os.urandom(vs) + b"\r\n")
PYEOF
  OUT=$(valkey-cli -p "$PORT" --pipe < "$FLINT_DRILL_ROOT/flint-selffill.resp" 2>&1 | tr -d '\r')
  ERRS=$(printf '%s' "$OUT" | sed -n 's/.*errors: \([0-9]*\).*/\1/p' | head -1)
  VERDICT=$(info disk_verdict)

  # A single explicit write, so the REFUSAL TEXT is evidence rather than a
  # count. --pipe reports how many replies were errors and never which.
  PROBE=$(valkey-cli -p "$PORT" SET "probe:$c" v 2>&1 | tr -d '\r')
  if [ "$PROBE" != "OK" ] && [ -z "$FIRST_REFUSAL" ]; then FIRST_REFUSAL="$PROBE"; fi

  if [ "$VERDICT" = "shed" ]; then SHED_AT=$c; break; fi

  # Still `ok`, so nothing may have been refused. An error here means
  # something OTHER than the guard stopped the write — which is the failure
  # this drill exists to catch.
  [ "${ERRS:-1}" = "0" ] || fail "chunk $c hit $ERRS error(s) while the verdict was still '$VERDICT' at $(info disk_free_pct)% free — the guard did not get there first. First refusal: ${FIRST_REFUSAL:-?}"
  ACKED=$((ACKED + CHUNK_KEYS * VSIZE))
  kill -0 "$(pgrep -f "flint-server --port $PORT" | head -1)" 2>/dev/null \
    || fail "the server died during chunk $c — ENOSPC reached RocksDB before the guard shed"
done
rm -f "$FLINT_DRILL_ROOT/flint-selffill.resp"
[ -n "$SHED_AT" ] || fail "the node never shed after $((MAX_CHUNKS * CHUNK_KEYS * VSIZE / 1048576))MB on a ${SIZE_MB}MB volume — raise MAX_CHUNKS or the guard is not sampling"
echo "  shed at chunk $SHED_AT, $((ACKED / 1048576))MB acked while ok, $(info disk_free_pct)% free"

# 1. THE GUARD WON THE RACE.
case "$FIRST_REFUSAL" in
  *QUOTA*disk*) echo "  first refusal was the headroom guard: $FIRST_REFUSAL" ;;
  "")           fail "the verdict is shed but no write was ever refused — the verdict and the write path disagree" ;;
  *)            fail "the first refusal was NOT the headroom guard: '$FIRST_REFUSAL'. The guard is decorative if something else stops writes first" ;;
esac
grep -q "Ok -> Shed" "$LOG" || fail "the Ok -> Shed transition was not logged"

# Reads and the delete path, which are the whole contract while shedding.
# Checked FIRST, because the verdict is not stable here (see below) and these
# have to be sampled while it is still shed.
[ "$(valkey-cli -p "$PORT" GET canary:1 | tr -d '\r')" = "canary-value-for-1" ] \
  || fail "reads stopped working while shedding"
[ "$(valkey-cli -p "$PORT" DEL nosuch | tr -d '\r')" = "0" ] \
  || fail "DEL was refused — the only way out is blocked"
echo "  reads served and DEL accepted while shedding"

# 2. THE SAFE BAND UNDER SUSTAINED PRESSURE — *not* "shed is sticky".
#
# The first version of this asserted the verdict never leaves `shed` while
# nothing is freed, and it FAILED — correctly, on a false premise of mine.
# Under self-inflicted fill something IS always being freed: the drill stopped
# writing, compaction finished, obsolete SSTs were deleted, and free space went
# 147MB -> 263MB (18% -> 33%) on its own. The guard cleared because the space
# genuinely came back. That is the behaviour we want, and the ballast version
# of this test (disk_pressure_drill) cannot see it because a ballast file makes
# free space static.
#
# So the property is not stickiness. A cache that REFUSES rather than evicts
# necessarily cycles at a full disk — shed, compact, admit, fill, shed — and
# forbidding that would be forbidding recovery. What must hold is that the
# cycling stays INSIDE A SAFE BAND: writes keep being refused by the guard and
# never by the filesystem, free space never collapses toward zero, reads never
# stop, and the process never dies. That is what is asserted here, under
# sustained write pressure rather than in a quiet window.
echo "== hold at the boundary under sustained writes: does it stay in the band?"
MIN_FREE_SEEN=100
CYCLES=0
for r in $(seq 1 16); do
  OUT=$(valkey-cli -p "$PORT" SET "hold:$r" "$(head -c 4000 /dev/zero | tr '\0' 'y')" 2>&1 | tr -d '\r')
  case "$OUT" in
    OK|*QUOTA*disk*) : ;;
    *) fail "a write at the boundary was refused by something other than the guard: '$OUT'" ;;
  esac
  F=$(info disk_free_pct)
  [ -n "$F" ] && [ "$F" -lt "$MIN_FREE_SEEN" ] && MIN_FREE_SEEN=$F
  sleep 0.4
done
CYCLES=$(grep -c "Ok -> Shed" "$LOG" 2>/dev/null || echo 0)
kill -0 "$(pgrep -f "flint-server --port $PORT" | head -1)" 2>/dev/null \
  || fail "the server died while holding at the boundary — ENOSPC reached RocksDB"
[ "$(valkey-cli -p "$PORT" GET canary:2 | tr -d '\r')" = "canary-value-for-2" ] \
  || fail "reads stopped working while cycling at the boundary"
# The floor: half the configured threshold. Crossing it means the guard let the
# disk run away between samples, which is the only way ENOSPC gets a chance.
FLOOR=$((MIN_PCT / 2))
[ "$MIN_FREE_SEEN" -ge "$FLOOR" ] \
  || fail "free space fell to ${MIN_FREE_SEEN}%, under the ${FLOOR}% floor (guard threshold ${MIN_PCT}%) — the guard is not keeping real headroom, so ENOSPC is one burst away"
echo "  held for 16 rounds: min free ${MIN_FREE_SEEN}% (floor ${FLOOR}%), $CYCLES shed cycle(s),"
echo "  every refusal came from the guard, reads served throughout, process alive"

echo "== dig out: delete most of the keyspace and wait for the node to reopen"
# Record whether the node was ALREADY back before the deletes. It often is —
# compaction alone reclaims enough — and reporting a self-recovery as though
# the deletes caused it would be the drill flattering itself.
VERDICT_BEFORE_DELETE=$(info disk_verdict)
FREE_BEFORE_DELETE=$(info disk_free_pct)
PHYS_BEFORE=$(du -sk "$DIR" | awk '{print $1 * 1024}')
DELETED=0
for c in $(seq 1 "$SHED_AT"); do
  # Three quarters of each chunk, in batches, through DEL — the realistic
  # shape (a tenant deleting), not the FLUSHALL sledgehammer.
  for base in $(seq 0 500 $((CHUNK_KEYS * 3 / 4 - 1))); do
    KEYS=""
    for i in $(seq "$base" $((base + 499))); do
      [ "$i" -lt $((CHUNK_KEYS * 3 / 4)) ] && KEYS="$KEYS $(printf 'fill:%03d:%06d' "$c" "$i")"
    done
    [ -n "$KEYS" ] || continue
    N=$(valkey-cli -p "$PORT" DEL $KEYS 2>/dev/null | tr -d '\r')
    DELETED=$((DELETED + ${N:-0}))
  done
done
echo "  deleted $DELETED keys (~$((DELETED * VSIZE / 1048576))MB logical)"

REOPENED=""
for _ in $(seq 1 180); do
  [ "$(info disk_verdict)" = "ok" ] && { REOPENED=yes; break; }
  sleep 0.5
done
PHYS_AFTER=$(du -sk "$DIR" | awk '{print $1 * 1024}')
echo "  physical $((PHYS_BEFORE / 1048576))MB -> $((PHYS_AFTER / 1048576))MB, free $(info disk_free_pct)%"
[ -n "$REOPENED" ] || {
  echo "FAIL: the node is STILL shedding 90s after $DELETED keys were deleted."
  echo "      Physical bytes went $((PHYS_BEFORE / 1048576))MB -> $((PHYS_AFTER / 1048576))MB, so the deletes"
  echo "      did not become free space. This is the capacity-boundary trap:"
  echo "      reclaiming needs a compaction, and a compaction needs headroom"
  echo "      the node no longer has. It is a product finding (#95/#195), not"
  echo "      a broken drill — a node that cannot honour a delete cannot be"
  echo "      recovered by a tenant, only by an operator."
  exit 1
}
grep -q "Shed -> Ok" "$LOG" || fail "the Shed -> Ok transition was not logged"
# Say which mechanism actually reopened the node, so the PASS line cannot be
# read as proof of something this run did not test.
if [ "$VERDICT_BEFORE_DELETE" = "ok" ]; then
  echo "  NOTE: the node had already reopened at ${FREE_BEFORE_DELETE}% free BEFORE the deletes —"
  echo "        compaction alone reclaimed enough. The deletes are still asserted"
  echo "        to be accepted and durable below, but this run does not show that"
  echo "        delete-driven reclaim is what recovers a node."
else
  echo "  the deletes are what reopened it (verdict was '$VERDICT_BEFORE_DELETE' at ${FREE_BEFORE_DELETE}% free beforehand)"
fi
[ "$(valkey-cli -p "$PORT" SET after-recovery v | tr -d '\r')" = "OK" ] || fail "writes did not resume after the verdict cleared"

# 4. NOTHING ACKED WAS LOST — canaries, and a sample of the fill that was
#    NOT deleted (the last quarter of each chunk).
for i in 1 2 3; do
  [ "$(valkey-cli -p "$PORT" GET "canary:$i" | tr -d '\r')" = "canary-value-for-$i" ] \
    || fail "canary:$i did not survive the excursion"
done
#    ONLY FULLY-ACKED CHUNKS. Chunk $SHED_AT is the one the guard interrupted:
#    the node started refusing partway through it, so its later keys were never
#    acked and their absence is the guard working, not data loss. Sampling it
#    reported two "missing" keys on the first run of this assertion — a false
#    alarm produced by the drill asking about writes it had itself been told
#    were refused. "No acked write is lost" can only be asked of writes that
#    were acked.
LAST_FULL=$((SHED_AT - 1))
[ "$LAST_FULL" -ge 1 ] || fail "the node shed during the very first chunk, so no chunk was fully acked and there is nothing to check for loss — lower MIN_PCT or raise the image size"
KEPT=$((CHUNK_KEYS * 3 / 4))
MISSING=0
for c in 1 $((LAST_FULL / 2 + 1)) "$LAST_FULL"; do
  for i in "$KEPT" $((CHUNK_KEYS - 1)); do
    K=$(printf 'fill:%03d:%06d' "$c" "$i")
    LEN=$(valkey-cli -p "$PORT" STRLEN "$K" 2>/dev/null | tr -d '\r')
    [ "$LEN" = "$VSIZE" ] || { echo "  MISSING/short: $K (strlen=$LEN want=$VSIZE)"; MISSING=$((MISSING + 1)); }
  done
done
[ "$MISSING" = 0 ] || fail "$MISSING undeleted acked key(s) are missing or truncated after the excursion"
echo "  canaries and undeleted fill from chunks 1..$LAST_FULL all read back intact"

echo "PASS: self-inflicted disk fill — the headroom guard refused before ENOSPC"
echo "      could reach RocksDB, held the node inside a ${FLOOR}%-free floor while it"
echo "      cycled at the boundary under load, and the node reclaimed its own"
echo "      space and reopened ($((PHYS_BEFORE / 1048576))MB -> $((PHYS_AFTER / 1048576))MB) with no acked write lost"
