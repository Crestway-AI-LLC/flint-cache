#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Capacity eviction, under a disk that is actually full.
#
# WHY THIS EXISTS. Everything below it is tested in pieces: the policy picks
# cold keys, the guard refuses undeclared namespaces, the floors batch, the
# hooks are connected. Not one of those exercises the path this feature IS —
# a disk fills, the trigger fires, keys are marked, compaction drops them, and
# space comes back. Until that runs once, "eviction works" is an inference
# across five components, and the failure mode of an inference like that is a
# node that fills up and refuses writes with a cache full of cold data.
#
# THE ASSERTION THAT MATTERS MOST IS THE ONE ABOUT THE OTHER NAMESPACE. Flint's
# position is that it never silently drops what a user put there, and this
# feature deletes data on a background thread. So the question is not only
# "did the evictable namespace shrink" but "did the durable one survive being
# next to it while it did". That is asserted key by key, not by a count.
#
# Uses a real small filesystem, as disk_pressure_drill does, because the point
# is genuine fullness rather than a threshold moved until it fires.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-evpressure 6493
fleet_guard
PORT=6493

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
if [ "$(uname)" = "Darwin" ]; then IMG=$IMGROOT/flint-evpressure.dmg; else IMG=$IMGROOT/flint-evpressure.img; fi
MNT=$IMGROOT/flint-evpressure-mnt
SIZE_MB=512
# floor = max(30% of 512MB, 64MB) = 153MB; reclaim engages below 230MB free and
# targets 307MB free. Both inside the device, which the clamp in reclaim_action
# now guarantees but which is worth choosing deliberately anyway.
MIN_PCT=30
MIN_BYTES=$((64 * 1024 * 1024))

fail() { echo "FAIL: $*"; exit 1; }
cleanup() {
  pkill -9 -f "flint-server --port $PORT" 2>/dev/null
  if [ "$(uname)" = "Darwin" ]; then hdiutil detach "$MNT" -force -quiet 2>/dev/null
  else sudo umount "$MNT" 2>/dev/null; fi
  rm -rf "$IMG" "$MNT" 2>/dev/null
}
trap cleanup EXIT
cleanup

echo "== a ${SIZE_MB}MB filesystem to fill"
if [ "$(uname)" = "Darwin" ]; then
  mkdir -p "$MNT"
  hdiutil create -size "${SIZE_MB}m" -fs HFS+ -volname flintevp -quiet "$IMG" || fail "create image"
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

echo "== node on it, 'cache' declared evictable and 'durable' not"
./target/release/flint-server --port "$PORT" --engine rocks --data-dir "$MNT/data" \
  --evictable-ns cache \
  --disk-min-free-pct "$MIN_PCT" --disk-min-free-bytes "$MIN_BYTES" \
  --disk-sample-ms 500 >"$FLINT_DRILL_ROOT/flint-evpressure.log" 2>&1 &
ready() {
  [ "$(valkey-cli -p $PORT PING 2>/dev/null)" = "PONG" ] &&
    ! valkey-cli -p $PORT FLINTINFO 2>/dev/null | tr -d '\r' | grep -qx 'loading:1'
}
for _ in $(seq 1 120); do ready && break; sleep 0.25; done
ready || fail "server did not become ready; see $FLINT_DRILL_ROOT/flint-evpressure.log"

evict_field() { valkey-cli -p $PORT FLINTINFO | tr '\r' '\n' | grep '^evict:' | cut -d: -f2-; }
evict_num() { evict_field | tr ' ' '\n' | grep "^$1=" | cut -d= -f2; }
info() { valkey-cli -p $PORT FLINTINFO | tr '\r' '\n' | grep "^$1:" | cut -d: -f2-; }

# One socket per namespace: FLINTNS is connection-scoped.
fill() { # fill <ns> <prefix> <from> <count> <valkb>
  python3 - "$PORT" "$1" "$2" "$3" "$4" "$5" <<'PYX'
import socket, sys
port, ns, prefix, start, count, kb = (sys.argv[1], sys.argv[2], sys.argv[3],
                                      int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
def resp(a):
    return ("*%d\r\n" % len(a)).encode() + b"".join(
        ("$%d\r\n%s\r\n" % (len(x), x)).encode() for x in a)
s = socket.create_connection(("127.0.0.1", int(port)), timeout=30); s.settimeout(30)
s.sendall(resp(["FLINTNS", ns])); s.recv(64)
val = "v" * (kb * 1024)
# Pipelined in blocks: one round trip per key would dominate the run.
BLOCK = 50
i = start
while i < start + count:
    n = min(BLOCK, start + count - i)
    buf = b"".join(resp(["SET", "%s%d" % (prefix, j), val]) for j in range(i, i + n))
    s.sendall(buf)
    got = 0
    while got < n:
        chunk = s.recv(65536)
        if not chunk:
            sys.exit("connection closed after %d/%d" % (got, n))
        got += chunk.count(b"\r\n")
    i += n
s.close()
PYX
}

echo "== 200 durable keys first — these must all survive what follows"
fill durable d 0 200 20 || fail "durable fill"

# BALLAST, as disk_pressure_drill does. Without it the node has to write the
# whole 512 MB itself to reach the threshold: the first version of this drill
# wrote 6000 keys, reported policy_keys=6000 with 117 MB tracked, and failed
# with the disk 75% FREE. The trigger was right not to fire; the drill was
# wrong to expect it to.
echo "== 200MB of ballast, so the node only has to write the last stretch"
dd if=/dev/zero of="$MNT/ballast" bs=1m count=200 2>/dev/null \
  || dd if=/dev/zero of="$MNT/ballast" bs=1M count=200 status=none \
  || fail "could not write ballast"
sync 2>/dev/null || true
echo "   disk_free_pct=$(info disk_free_pct) after ballast"

echo "== filling 'cache' until the reclaim trigger engages"
ENGAGED=0
for round in $(seq 1 12); do
  fill cache c $(( (round - 1) * 500 )) 500 20 || break
  [ "$(info reclaim_active)" = "1" ] && { ENGAGED=1; echo "   engaged after $(( round * 500 )) keys"; break; }
done
# POSITIVE CONTROL on the premise. If the trigger never fired, everything below
# would be asserting about a node under no pressure at all, and would pass most
# easily in exactly that case.
[ "$ENGAGED" = "1" ] || {
  echo "FAIL: reclaim never engaged after 6000 keys on a ${SIZE_MB}MB disk."
  echo "      disk_free_pct=$(info disk_free_pct) verdict=$(info disk_verdict)"
  echo "      evict: $(evict_field)"
  exit 1
}
echo "   reclaim_target_free_bytes=$(info reclaim_target_free_bytes) disk_free_pct=$(info disk_free_pct)"

echo "== the chain: marked -> forced pass -> rows actually dropped"
for _ in $(seq 1 60); do
  [ "$(evict_num dropped)" -gt 0 ] 2>/dev/null && break
  sleep 1
done
M=$(evict_num marked_total); F=$(evict_num forced_passes); D=$(evict_num dropped)
R=$(evict_num refused);     O=$(evict_num overflow)
echo "   marked_total=$M forced_passes=$F dropped=$D refused=$R overflow=$O"
[ "${M:-0}" -gt 0 ] || fail "nothing was ever marked, so the policy offered no candidates"
[ "${F:-0}" -ge 1 ] || fail "no compaction pass was forced, so nothing could be reclaimed promptly"
[ "${D:-0}" -gt 0 ] || fail "no row was dropped by the compaction filter: marked=$M forced=$F"
# The guard must not have refused anything: every mark came from the policy for
# a declared namespace, so a nonzero count here is a policy bug, not pressure.
[ "${R:-0}" -eq 0 ] || fail "the guard refused $R marks — the policy chose keys outside the declared namespace"

echo "== and the durable namespace lost NOTHING"
python3 - "$PORT" <<'PYX'
import socket, sys
port = int(sys.argv[1])
def resp(a):
    return ("*%d\r\n" % len(a)).encode() + b"".join(
        ("$%d\r\n%s\r\n" % (len(x), x)).encode() for x in a)
s = socket.create_connection(("127.0.0.1", port), timeout=30); s.settimeout(30)
s.sendall(resp(["FLINTNS", "durable"])); s.recv(64)
missing = []
for i in range(200):
    s.sendall(resp(["GET", "d%d" % i]))
    buf = b""
    while not buf.endswith(b"\r\n") or buf.count(b"\r\n") < 2:
        c = s.recv(65536)
        if not c:
            sys.exit("connection closed reading d%d" % i)
        buf += c
        if buf.startswith(b"$-1"):
            break
    if buf.startswith(b"$-1"):
        missing.append(i)
s.close()
if missing:
    sys.exit("FAIL: %d durable keys were DELETED by eviction: %s%s" % (
        len(missing), missing[:10], "..." if len(missing) > 10 else ""))
print("   all 200 durable keys still readable")
PYX
[ $? -eq 0 ] || exit 1

echo "PASS: the disk filled, eviction ran, rows left, and the durable namespace was untouched"
