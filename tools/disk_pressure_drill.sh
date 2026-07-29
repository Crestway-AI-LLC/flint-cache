#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A node whose disk fills up, and digs itself back out.
#
# Per-tenant quotas bound each namespace; nothing bounds the HOST, and the
# sum of quotas is MEANT to exceed the disk — that oversubscription is the
# packing economics. So a full node is a normal consequence of the business
# model, and the contract under it has to be exactly the one the tenant
# guide already promises for -QUOTA:
#
#   - ordinary writes are refused, with a message that says why;
#   - reads keep serving;
#   - DEL/UNLINK/EXPIRE/FLUSHALL keep working, because deleting is the only
#     way out and blocking it would make the state self-sustaining;
#   - nothing already written is lost;
#   - and when space comes back the node reopens BY ITSELF, no operator.
#
# The last one is the point. An LSM needs headroom to compact, and the cure
# for a full disk is a trap without it: a delete is a write, and reclaiming
# the bytes needs the compaction that has no room to run. A node allowed to
# fill completely can be unable to recover. This asserts we stop early
# enough that it never gets there.
#
# Uses a small disk image so the thresholds are reachable in seconds rather
# than by writing a real terabyte. Linux and macOS.
set -u
cd "$(dirname "$0")/.."

PORT=6396
# macOS: hdiutil APPENDS .dmg when the path lacks it, so a bare .img name
# creates one file and attaches another. Name it per platform.
if [ "$(uname)" = "Darwin" ]; then IMG=/tmp/flint-diskpressure.dmg; else IMG=/tmp/flint-diskpressure.img; fi
MNT=/tmp/flint-diskpressure-mnt
SIZE_MB=512
# Thresholds chosen so a 512 MB volume trips them with a ~400 MB ballast
# file, not so tight that filesystem overhead alone trips them at boot.
MIN_PCT=30
MIN_BYTES=$((64 * 1024 * 1024))

fail() { echo "FAIL: $*"; exit 1; }

cleanup() {
  pkill -9 -f "flint-server.*$PORT" 2>/dev/null
  if [ "$(uname)" = "Darwin" ]; then
    hdiutil detach "$MNT" -force -quiet 2>/dev/null
  else
    umount "$MNT" 2>/dev/null
  fi
  rm -rf "$IMG" "$MNT" 2>/dev/null
}
trap cleanup EXIT
cleanup

echo "== a $SIZE_MB MB filesystem to run out of"
if [ "$(uname)" = "Darwin" ]; then
  mkdir -p "$MNT"
  hdiutil create -size "${SIZE_MB}m" -fs HFS+ -volname flintpressure -quiet "$IMG" \
    || fail "could not create the disk image"
  hdiutil attach "$IMG" -mountpoint "$MNT" -quiet || fail "could not attach the disk image"
else
  command -v mkfs.ext4 >/dev/null || { echo "SKIP: mkfs.ext4 not available"; exit 0; }
  [ "$(id -u)" = "0" ] || { echo "SKIP: needs root to mount a loop device"; exit 0; }
  mkdir -p "$MNT"
  dd if=/dev/zero of="$IMG" bs=1M count="$SIZE_MB" status=none
  mkfs.ext4 -q "$IMG"
  mount -o loop "$IMG" "$MNT" || fail "could not mount the loop device"
fi

cargo build --release -q -p flint-server --features rocks || fail "build"

echo "== node up on it"
./target/release/flint-server --port "$PORT" --engine rocks --data-dir "$MNT/data" \
  --disk-min-free-pct "$MIN_PCT" --disk-min-free-bytes "$MIN_BYTES" \
  --disk-sample-ms 500 >/tmp/flint-diskpressure.log 2>&1 &
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p $PORT PING 2>/dev/null)" = "PONG" ] && break; sleep 0.25
done
[ "$(valkey-cli -p $PORT PING 2>/dev/null)" = "PONG" ] || {
  tail -5 /tmp/flint-diskpressure.log; fail "server did not start"; }

info() { valkey-cli -p "$PORT" FLINTINFO 2>/dev/null | tr -d '\r' | grep "^$1:" | cut -d: -f2; }

echo "== healthy: writes land, and the gauge reports real numbers"
valkey-cli -p $PORT SET keep-a aaa >/dev/null
valkey-cli -p $PORT SET keep-b bbb >/dev/null
[ "$(info disk_verdict)" = "ok" ] || fail "verdict is $(info disk_verdict), expected ok"
[ "$(info disk_total_bytes)" -gt 0 ] || fail "disk_total_bytes not reported"
[ "$(info disk_unknown_samples)" = "0" ] || fail "the sampler could not read the filesystem"
echo "  free $(info disk_free_pct)%, verdict ok, 2 keys written"

echo "== fill it from OUTSIDE the server: space it did not consume itself"
dd if=/dev/zero of="$MNT/ballast" bs=1m count=400 2>/dev/null \
  || dd if=/dev/zero of="$MNT/ballast" bs=1M count=400 status=none
sleep 2
[ "$(info disk_verdict)" = "shed" ] || fail "verdict is $(info disk_verdict) at $(info disk_free_pct)% free, expected shed"
grep -q "Ok -> Shed" /tmp/flint-diskpressure.log || fail "the transition was not logged"
echo "  free $(info disk_free_pct)%, verdict shed, transition logged"

echo "== the contract while shedding"
OUT=$(valkey-cli -p $PORT SET newkey v 2>&1)
case "$OUT" in
  QUOTA*disk*) : ;;
  *) fail "ordinary write got '$OUT', expected a QUOTA error naming the disk" ;;
esac
[ "$(valkey-cli -p $PORT GET keep-a)" = "aaa" ] || fail "reads stopped working"
[ "$(valkey-cli -p $PORT DEL nosuch)" = "0" ] || fail "DEL was refused — the recovery path is blocked"
[ "$(valkey-cli -p $PORT UNLINK nosuch)" = "0" ] || fail "UNLINK was refused"
[ "$(valkey-cli -p $PORT EXPIRE keep-b 3600)" = "1" ] || fail "EXPIRE was refused"
echo "  writes refused, reads served, DEL/UNLINK/EXPIRE all still work"

echo "== nothing already written was lost"
[ "$(valkey-cli -p $PORT GET keep-a)" = "aaa" ] || fail "keep-a lost"
[ "$(valkey-cli -p $PORT GET keep-b)" = "bbb" ] || fail "keep-b lost"

echo "== free the space: the node reopens on its own"
rm -f "$MNT/ballast"
REOPENED=""
for _ in $(seq 1 40); do
  [ "$(info disk_verdict)" = "ok" ] && { REOPENED=yes; break; }
  sleep 0.5
done
[ -n "$REOPENED" ] || fail "still shedding at $(info disk_free_pct)% free — no operator should be needed"
grep -q "Shed -> Ok" /tmp/flint-diskpressure.log || fail "the recovery transition was not logged"
[ "$(valkey-cli -p $PORT SET newkey v)" = "OK" ] || fail "writes did not resume"
[ "$(valkey-cli -p $PORT GET keep-a)" = "aaa" ] || fail "keep-a lost across recovery"
echo "  free $(info disk_free_pct)%, verdict ok, writes resumed, data intact"

echo "PASS: disk pressure — writes shed with a QUOTA error while reads and the delete path keep working, nothing written is lost, and the node reopens by itself once space returns"
