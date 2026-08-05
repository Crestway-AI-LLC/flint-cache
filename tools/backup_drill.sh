#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Backup set production and integrity (ADR-0011 Phase A/B).
#
# What this asserts, in the order the assertions depend on each other:
#
#   0. CAPABILITY — a set is produced from a live two-pair fleet and verifies.
#      Every refusal below is vacuous without it: a backup tool that produces
#      nothing, or a verify that refuses everything, passes them all.
#   1. the set is COMPLETE — every pair contributed, the control plane's
#      state came along, and the manifest names the master each checkpoint
#      was actually cut on
#   2. the MASTER is backed up, not whichever member is listed first. Backing
#      up a replica would inherit the RPO window on top of the backup's own
#      age, and the mistake is invisible on a fleet that never failed over —
#      so this drill fails one over FIRST and then backs up.
#   3. one flipped byte is REFUSED (same length; a size check would pass it)
#   4. an object the manifest never listed is REFUSED — the direction a
#      checksum loop structurally cannot see
#   5. the on-node checkpoint is CLEANED UP. A checkpoint hard-links the live
#      SSTs, so one left behind pins them against compaction reclaim: free
#      the moment it is cut, and a disk leak that grows with every scheduled
#      run if it is not.
#   6. the controller's snapshot root is UNTOUCHED. FLINTSNAPSHOT repoints
#      <root>/LATEST, and spare-restore seeds a replacement node from
#      whatever LATEST names — so a backup sharing that root would silently
#      aim disaster recovery at a checkpoint the backup then deletes.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-bkp 6950 6951 6952 6953
fleet_guard
B=./target/release/flint-server
BK=./target/release/flint-backup
D=/tmp/flint-bkp
fleet_kill server; sleep 0.4
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

echo "== two pairs: (6950 master, 6951 replica) and (6952 master, 6953 replica)"
for spec in "6950:" "6951:127.0.0.1:6950" "6952:" "6953:127.0.0.1:6952"; do
  p=${spec%%:*}; m=${spec#*:}
  if [ -n "$m" ]; then
    $B --port "$p" --engine rocks --data-dir "$D/n$p" --replica-of "$m" 2>"$D/n$p.log" &
  else
    $B --port "$p" --engine rocks --data-dir "$D/n$p" 2>"$D/n$p.log" &
  fi
  disown
  fleet_wait_listen "$p"
done
for p in 6950 6952; do
  for _ in $(seq 1 60); do
    valkey-cli -p $p FLINTINFO | tr -d '\r' | grep -q '^live_replicas:1' && break
    sleep 0.2
  done
done

echo "== a known corpus on both pairs"
for p in 6950 6952; do
  for i in $(seq 1 500); do printf 'SET bk:%s:%04d v-%s-%04d\r\n' "$p" "$i" "$p" "$i"; done \
    | valkey-cli -p $p --pipe 2>&1 | tail -1
done

echo
echo "== fail pair 0 over BEFORE backing up, so 'the master' is not 'the first member'"
# 6951 becomes the master of pair 0. A backup that reads position rather than
# role would now capture 6950 — a replica — and look completely healthy.
fleet_signal_port 6950 9 2>/dev/null || pkill -9 -f "flint-server --port 6950"
sleep 0.5
valkey-cli -p 6951 FLINTPROMOTE 1 1 | tr -d '\r'
for _ in $(seq 1 40); do
  [ "$(valkey-cli -p 6951 FLINTINFO | tr -d '\r' | grep '^role:' | cut -d: -f2)" = "master" ] && break
  sleep 0.2
done

echo
echo "== a stand-in control plane state file, and a controller snapshot root"
printf 'version 7\npairs 2\n' >"$D/cp-state"
mkdir -p "$D/ctl-snaps"; printf 'snap-controller-0001' >"$D/ctl-snaps/LATEST"
CTL_LATEST_BEFORE=$(cat "$D/ctl-snaps/LATEST")

echo
echo "== 0. CAPABILITY ASSERT — produce a set from the live fleet"
$BK run --pairs "127.0.0.1:6950,127.0.0.1:6951;127.0.0.1:6952,127.0.0.1:6953" \
        --cp-state "$D/cp-state" --to "$D/sets" --snap-root "$D/snaps" || {
  echo "FAIL: backup run refused"; exit 1; }
SET=$(ls "$D/sets")
[ -n "$SET" ] || { echo "FAIL: no set produced"; exit 1; }
echo "  set: $SET"

echo
echo "== 1. the set is COMPLETE"
M="$D/sets/$SET/manifest"
grep -q '^cp-source ' "$M" || { echo "FAIL: manifest records no cp source"; exit 1; }
[ -f "$D/sets/$SET/cp-state" ] || { echo "FAIL: control plane state not captured"; exit 1; }
for i in 0 1; do
  grep -q "^pair $i " "$M" || { echo "FAIL: pair $i missing from the manifest"; exit 1; }
  ls "$D/sets/$SET/pairs/$i"/*.sst >/dev/null 2>&1 \
    || { echo "FAIL: pair $i contributed no SSTs"; exit 1; }
done
echo "  $(grep -c '^object ' "$M") objects, both pairs and the control plane present"

echo
echo "== 2. the MASTER was backed up, not the first-listed member"
P0=$(grep '^pair 0 ' "$M" | awk '{print $3}')
[ "$P0" = "127.0.0.1:6951" ] || {
  echo "FAIL: pair 0 was backed up from $P0, but 6951 is the master after the failover"
  echo "      (a replica backup inherits the RPO window on top of its own age)"; exit 1; }
echo "  pair 0 taken from $P0 — the promoted member"

echo
echo "== 5. the on-node checkpoint was cleaned up"
LEFT=$(find "$D/snaps" -name '*.sst' 2>/dev/null | wc -l | tr -d ' ')
[ "$LEFT" = "0" ] || {
  echo "FAIL: $LEFT checkpoint SSTs left on the node — each pins a live SST against compaction"
  find "$D/snaps" -name '*.sst' | head -3; exit 1; }
echo "  no checkpoint files left behind"

echo
echo "== 6. the controller's snapshot root is untouched"
[ "$(cat "$D/ctl-snaps/LATEST")" = "$CTL_LATEST_BEFORE" ] || {
  echo "FAIL: backup repointed the controller's LATEST — spare-restore now aims at a backup"; exit 1; }
echo "  LATEST still names $CTL_LATEST_BEFORE"

echo
echo "== verify passes on the untouched set (the control for 3 and 4)"
$BK verify --from "$D/sets/$SET" || { echo "FAIL: an intact set did not verify"; exit 1; }

echo
echo "== 3. one flipped byte is refused"
cp -R "$D/sets/$SET" "$D/flipped"
VICTIM=$(find "$D/flipped/pairs/0" -name '*.sst' | head -1)
python3 - "$VICTIM" <<'PY'
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read())
# Same length, one bit different — the corruption a size check cannot see.
b[len(b) // 2] ^= 0x01
open(p, 'wb').write(bytes(b))
PY
if $BK verify --from "$D/flipped" >"$D/flip.out" 2>&1; then
  echo "FAIL: a corrupt set verified"; cat "$D/flip.out"; exit 1
fi
grep -q 'corrupt' "$D/flip.out" || { echo "FAIL: refusal did not name corruption"; cat "$D/flip.out"; exit 1; }
echo "  $(head -1 "$D/flip.out")"

echo
echo "== 4. an object the manifest never listed is refused"
cp -R "$D/sets/$SET" "$D/extra"
printf 'not from this backup' >"$D/extra/pairs/0/999999.sst"
if $BK verify --from "$D/extra" >"$D/extra.out" 2>&1; then
  echo "FAIL: a set with an unlisted object verified"; cat "$D/extra.out"; exit 1
fi
grep -q 'not listed' "$D/extra.out" || { echo "FAIL: refusal did not name the unlisted object"; cat "$D/extra.out"; exit 1; }
echo "  $(head -1 "$D/extra.out")"

echo
echo "PASS: a set is produced from the live masters, cleans up after itself, and refuses corruption in both directions"
