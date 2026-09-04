#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0079: the WAL archive budget must be answerable to the volume it claims
# to describe, and the seat must SAY which volume it asked.
#
# The budget was only ever announced in one boot log line. A 3.5 TB seat came
# up clamped to the 1 GiB FLOOR -- a 256x under-provisioned archive on a
# healthy disk -- because `statvfs` was called moments before the data
# directory existed, and `unwrap_or(0)` turned "I could not measure" into "a
# zero-byte volume". Its replica, starting 1.1 s later, measured the same disk
# correctly and took 256 GiB. Nothing compared the two, nothing surfaced
# either, and the gate was green.
#
# The number ALONE cannot carry this check: 1024 MB is correct on a 4 GB dev
# box and a catastrophe on an NVMe, and an operator who pinned it is not a
# defect at all. So this asserts the budget against the volume the seat is
# STILL sampling, and only where the seat says it derived one -- which is what
# `wal_archive_src` is for.
#
# THE SEAT'S OWN INFO CONTRADICTS ITSELF in the failure, which is why one
# process is enough and no fleet is needed: the bad sample happens once, at
# boot, while the disk guard goes on sampling the same directory successfully
# for the life of the process. `wal_archive_mb` remembers the failure;
# `disk_total_bytes` reports the success.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-walb- 6416 6417 6418
fleet_guard
fleet_kill server; sleep 0.3
B=./target/release/flint-server
DERIVED=6416; PINNED=6417; MEM=6418
cleanup() { fleet_kill server; rm -rf $FLINT_DRILL_ROOT/flint-walb-*; }
trap cleanup EXIT

# A short sample interval so the guard's first reading lands inside the wait
# below; the DEFAULT 2 s is not the thing under test.
for p in $DERIVED $PINNED; do
  d="$FLINT_DRILL_ROOT/flint-walb-$p"; rm -rf "$d"
  extra=""
  [ "$p" = "$PINNED" ] && extra="--wal-size-limit-mb 4096"
  $B --port $p --engine rocks --data-dir "$d" --disk-sample-ms 200 $extra 2>"${FLEET_SCOPE}server.log" &
done
$B --port $MEM --engine mem --disk-sample-ms 200 2>"${FLEET_SCOPE}server2.log" &
sleep 1.0
for p in $DERIVED $PINNED $MEM; do
  [ "$(valkey-cli -p $p PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: :$p never answered PING"; exit 1; }
done

field() { valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr -d '\r' | awk -F: -v k="$2" '$1==k {print $2; exit}'; }

# CAPABILITY ASSERT, FIRST. Every comparison below reads two INFO fields, and
# `field` prints an empty string both when a field says nothing and when it is
# not there at all. Without this, deleting the fields from INFO turns every
# assertion into an empty-vs-empty comparison and the drill reports PASS on a
# build that surfaces nothing -- the precise failure this bug is about.
echo "== the fields exist at all"
for k in wal_archive_mb wal_archive_src disk_total_bytes; do
  v=$(field $DERIVED $k)
  [ -n "$v" ] || { echo "FAIL: FLINTINFO has no '$k'. This drill cannot check"; \
                   echo "      a budget it cannot read; absent is not clean."; exit 1; }
done
echo "  wal_archive_mb, wal_archive_src and disk_total_bytes are all reported"

echo "== a derived seat says so, and agrees with the volume it is sampling"
SRC=$(field $DERIVED wal_archive_src)
[ "$SRC" = "measured" ] || { echo "FAIL: derived seat reports wal_archive_src='$SRC', want 'measured'"; exit 1; }
# POSITIVE CONTROL on the comparand. A total of 0 would make the expected
# budget the floor, and a floor-clamped seat would then MATCH -- the check
# would agree with the exact defect it exists to catch.
TOTAL=""
for _ in $(seq 1 40); do
  t=$(field $DERIVED disk_total_bytes)
  case "$t" in ''|*[!0-9]*) : ;; *) [ "$t" -gt 0 ] && { TOTAL=$t; break; } ;; esac
  sleep 0.25
done
[ -n "$TOTAL" ] || { echo "FAIL: disk_total_bytes never became positive in 10s."; \
                     echo "      The volume comparison would have been against 0, which"; \
                     echo "      makes the floor look correct. Refusing to assert."; exit 1; }
# Mirrors rocks::wal_size_limit_mb_for_volume: (total/4) in MiB, clamped.
EXPECT=$(( (TOTAL / 4) / 1048576 ))
[ "$EXPECT" -lt 1024 ] && EXPECT=1024
[ "$EXPECT" -gt 262144 ] && EXPECT=262144
GOT=$(field $DERIVED wal_archive_mb)
[ "$GOT" = "$EXPECT" ] || {
  echo "FAIL: derived archive budget disagrees with the volume the seat is sampling"
  echo "      disk_total_bytes=$TOTAL -> expected ${EXPECT} MB, seat reports ${GOT} MB"
  [ "$GOT" = "1024" ] && echo "      ${GOT} MB is the FLOOR: the boot-time measurement failed and was believed."
  exit 1; }
echo "  volume $TOTAL bytes -> ${GOT} MB archive, as the volume implies"

echo "== a pinned seat says 'override', and is NOT held to the volume"
PSRC=$(field $PINNED wal_archive_src)
PGOT=$(field $PINNED wal_archive_mb)
[ "$PSRC" = "override" ] || { echo "FAIL: pinned seat reports wal_archive_src='$PSRC', want 'override'"; exit 1; }
[ "$PGOT" = "4096" ] || { echo "FAIL: pinned seat reports ${PGOT} MB, want the pinned 4096"; exit 1; }
# THE RULE'S OWN NEGATIVE CONTROL. 4096 is deliberately not what this volume
# implies, so a check that asserted the invariant unconditionally would fail
# here. That it passes is what proves the invariant is conditioned on
# provenance rather than applied to every seat.
[ "$PGOT" = "$EXPECT" ] && {
  echo "FAIL: the pinned budget happens to equal the derived one ($EXPECT MB), so this"
  echo "      control proves nothing. Pin a different value."
  exit 1; }
echo "  pinned ${PGOT} MB stands against a volume implying ${EXPECT} MB, and is not a finding"

echo "== a seat with no archive says 'none' rather than a number nobody chose"
MSRC=$(field $MEM wal_archive_src)
[ "$MSRC" = "none" ] || {
  echo "FAIL: mem seat reports wal_archive_src='$MSRC', want 'none'"
  echo "      The mem engine derives no archive budget. Reporting the compiled"
  echo "      default as though it had been chosen is a fabricated measurement."
  exit 1; }
echo "  mem seat reports 'none', so the default is never mistaken for a derivation"

echo "PASS: the archive budget is visible, says how it was chosen, and a derived one matches its volume"
