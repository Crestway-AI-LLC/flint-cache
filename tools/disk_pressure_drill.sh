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
. "$(dirname "$0")/lib/fleet.sh"
# Declared so the SET of drills can be checked for port collisions —
# fleet_init only records the scope, it changes no behaviour here. A
# drill that declares nothing is invisible to assert_no_port_overlap,
# which is how failover and controller came to share 6440/6441 and
# reseed and lag_cap to share 6471/6472, unseen.
fleet_init /tmp/flint-diskpressure 6396

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
    sudo umount "$MNT" 2>/dev/null
  fi
  rm -rf "$IMG" "$MNT" 2>/dev/null
}

# NEVER TRUST detach's EXIT — verify the mount is gone.
#
# `hdiutil detach ... 2>/dev/null` in cleanup discards its status, so a mount
# that refuses to detach survives, and every later run attaches ON TOP of the
# wedged one. The server then starts against a directory that is not the
# volume it thinks it is, dies before writing a single log line, and the drill
# reports "server did not start" with a ZERO-BYTE server log. It failed three
# runs in a row that way, then passed immediately after the stale mount was
# detached by hand — which is the whole diagnosis.
#
# Same rule the suite already applies to pkill: the kill is not the evidence,
# the absence afterwards is.
assert_unmounted() {
  local i real
  # macOS reports /private/tmp/... in `mount` even when the path given was
  # /tmp/... , so matching the literal never fires and the guard silently
  # passes. Resolve the path first. (Caught by its own positive control:
  # a deliberately wedged mount was reported as absent.)
  real=$(cd "$MNT" 2>/dev/null && pwd -P) || real="$MNT"
  for i in $(seq 1 20); do
    mount | grep -qE " on (${MNT}|${real}) " || return 0
    [ "$(uname)" = "Darwin" ] && hdiutil detach "$MNT" -force -quiet 2>/dev/null       || sudo umount "$MNT" 2>/dev/null
    sleep 0.5
  done
  echo "FAIL: $MNT is still mounted after 10s of detach attempts."
  echo "      A wedged image poisons every later run of this drill; clear it with"
  echo "      'hdiutil detach $MNT -force' (macOS) or 'sudo umount $MNT' and re-run."
  exit 1
}
trap cleanup EXIT
cleanup
assert_unmounted

echo "== a $SIZE_MB MB filesystem to run out of"
if [ "$(uname)" = "Darwin" ]; then
  mkdir -p "$MNT"
  hdiutil create -size "${SIZE_MB}m" -fs HFS+ -volname flintpressure -quiet "$IMG" \
    || fail "could not create the disk image"
  hdiutil attach "$IMG" -mountpoint "$MNT" -quiet || fail "could not attach the disk image"
else
  command -v mkfs.ext4 >/dev/null || { echo "SKIP: mkfs.ext4 not available"; exit 0; }
  # sudo for the MOUNT ONLY, never for the whole script: cargo and the
  # toolchain are installed per-user, and running everything as root loses
  # them ("rustup could not choose a version of cargo to run").
  sudo -n true 2>/dev/null || { echo "SKIP: needs passwordless sudo to mount a loop device"; exit 0; }
  mkdir -p "$MNT"
  dd if=/dev/zero of="$IMG" bs=1M count="$SIZE_MB" status=none
  mkfs.ext4 -q "$IMG"
  sudo mount -o loop "$IMG" "$MNT" || fail "could not mount the loop device"
  sudo chown "$(id -u):$(id -g)" "$MNT" || fail "could not take ownership of the mount"
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
  # Capture STATE, not just the log.
  #
  # This drill has failed intermittently under the full suite and never in
  # isolation, and every previous investigation dead-ended on the same
  # evidence: "server did not start" plus a log that said nothing. That
  # framing is a guess, and it sent two investigations after the wrong
  # thing — a wedged mount (none was present) and a cold binary (it starts
  # in 0.3s on a fresh image, measured, with a 293-byte log). What was
  # never captured is whether the process was even alive, whether the
  # volume was mounted, and who else was on the port.
  #
  # So the failure path now records those, because the next occurrence is
  # the only chance to learn anything and it has been wasted three times.
  echo "---- diagnosis (the drill did not get a PONG within its budget) ----"
  echo "process:"; pgrep -lf "flint-server --port $PORT" | sed 's/^/  /' || echo "  none alive"
  echo "port $PORT:"; lsof -nP -iTCP:$PORT 2>/dev/null | sed 's/^/  /' | head -4 || echo "  free"
  echo "mount:"; mount | grep -i diskpressure | sed 's/^/  /' || echo "  NOT MOUNTED"
  echo "data dir:"; ls -la "$MNT/data" 2>&1 | head -4 | sed 's/^/  /'
  echo "free space:"; df -h "$MNT" 2>&1 | tail -1 | sed 's/^/  /'
  echo "server log ($(wc -c </tmp/flint-diskpressure.log 2>/dev/null || echo 0) bytes):"
  sed 's/^/  /' /tmp/flint-diskpressure.log 2>/dev/null | tail -10
  echo "load:"; uptime | sed 's/^/  /'
  echo "other flint seats (a busy box is the leading remaining hypothesis):"
  pgrep -lf 'flint-(server|proxy|controlplane|controller|chaos)' | grep -v "port $PORT" \
    | sed 's/^/  /' | head -6 || echo "  none"
  echo "--------------------------------------------------------------------"
  fail "server did not start"; }

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
