#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The S3 store, against a REAL S3-compatible endpoint (ADR-0011 D8).
#
# MANUAL, like the multi-host chaos drill: it needs credentials and a
# bucket, so it is not in the gate's CORE list. Without credentials it
# SKIPs; with them it proves the transport the way nothing local can —
# the SigV4 signer, the TLS trust path, the pagination, and the
# round-trip integrity checks all against the genuine article.
#
#   FLINT_S3_BUCKET=<bucket> tools/backup_s3_drill.sh
#
# What it asserts:
#   0. CAPABILITY — a set produced from a live fleet lands in the bucket
#      and verifies FROM the bucket (checksums recomputed over downloaded
#      bytes, listing compared in both directions).
#   1. restore --from s3:// materialises a bootable copy whose corpus
#      matches, streaming straight from the store.
#   2. the manifest's both-directions discipline holds remotely: an object
#      planted in the set's prefix that the manifest never listed makes
#      verify REFUSE (this is where ListObjectsV2 pagination and prefix
#      scoping earn their keep).
#   3. the set in the bucket is byte-intact after being read (reading a
#      backup must never mutate it, remote included).
# Cleanup deletes every object under the drill's unique prefix, and ONLY
# under it.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

command -v aws >/dev/null || { echo "SKIP: no aws cli for cleanup"; exit 0; }
BUCKET="${FLINT_S3_BUCKET:-}"
[ -n "$BUCKET" ] || { echo "SKIP: set FLINT_S3_BUCKET to run against real S3"; exit 0; }
# The seat reads the standard AWS env; hydrate it from the profile the
# operator uses so one variable is enough to opt in.
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  eval "$(aws configure export-credentials --profile "${AWS_PROFILE:-default}" --format env 2>/dev/null)"
fi
[ -n "${AWS_ACCESS_KEY_ID:-}" ] || { echo "SKIP: no AWS credentials in env or profile"; exit 0; }
export AWS_REGION="${AWS_REGION:-us-east-1}"

fleet_init $FLINT_DRILL_ROOT/flint-s3bk 6946 6947
fleet_guard
B=./target/release/flint-server
BK=./target/release/flint-backup
D=$FLINT_DRILL_ROOT/flint-s3bk
PREFIX="drill-$(date +%s)-$$"
SPEC="s3://$BUCKET/$PREFIX"
fleet_kill server; sleep 0.4
cleanup() {
  fleet_kill server; rm -rf "$D"
  aws s3 rm "s3://$BUCKET/$PREFIX" --recursive --quiet 2>/dev/null
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

echo "== a single-pair fleet with a known corpus"
$B --port 6946 --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
disown
fleet_wait_listen 6946
for i in $(seq 1 300); do printf 'SET s3k:%04d val-%04d\r\n' "$i" "$i"; done \
  | valkey-cli -p 6946 --pipe 2>&1 | tail -1
printf 'cp-state-stand-in\n' >"$D/cp-state"

echo
echo "== 0. CAPABILITY: back up TO the bucket, verify FROM it"
$BK run --pairs "127.0.0.1:6946,127.0.0.1:6946" \
        --cp-state "$D/cp-state" --to "$SPEC" --snap-root "$D/snaps" || {
  echo "FAIL: backup to S3 refused"; exit 1; }
SET_ID=$(aws s3 ls "s3://$BUCKET/$PREFIX/" | awk '{print $2}' | tr -d '/' | head -1)
[ -n "$SET_ID" ] || { echo "FAIL: nothing landed in the bucket"; exit 1; }
echo "  set: $SET_ID"
$BK verify --from "$SPEC/$SET_ID" || { echo "FAIL: remote verify refused an intact set"; exit 1; }

echo
echo "== 1. restore --from s3:// streams into a bootable copy"
$BK restore --from "$SPEC/$SET_ID" --into "$D/restored" | tail -1 || {
  echo "FAIL: restore from S3 refused"; exit 1; }
$B --port 6947 --engine rocks --data-dir "$D/restored/pair0" 2>"$D/r.log" &
disown
fleet_wait_listen 6947
fleet_wait_ping 6947
[ "$(valkey-cli -p 6947 GET s3k:0042)" = "val-0042" ] || {
  echo "FAIL: corpus key missing after S3 restore"; exit 1; }
[ "$(valkey-cli -p 6947 DBSIZE)" = "300" ] || {
  echo "FAIL: restored keyspace is $(valkey-cli -p 6947 DBSIZE) keys, wanted 300"; exit 1; }
echo "  restored copy boots and serves all 300 keys"

echo
echo "== 2. an unlisted object in the bucket is refused"
printf 'planted' | aws s3 cp - "s3://$BUCKET/$PREFIX/$SET_ID/pairs/0/999999.sst" --quiet
if $BK verify --from "$SPEC/$SET_ID" >"$D/unlisted.out" 2>&1; then
  echo "FAIL: verify passed with a planted object in the set's prefix"; exit 1
fi
grep -q 'not listed' "$D/unlisted.out" || {
  echo "FAIL: refusal did not name the unlisted object"; cat "$D/unlisted.out"; exit 1; }
echo "  $(head -1 "$D/unlisted.out" | cut -c1-90)"
aws s3 rm "s3://$BUCKET/$PREFIX/$SET_ID/pairs/0/999999.sst" --quiet

echo
echo "== 3. reading never wrote: the set still verifies after all of the above"
$BK verify --from "$SPEC/$SET_ID" || {
  echo "FAIL: the set no longer verifies — something wrote into it"; exit 1; }

echo
echo "PASS: S3 store — signed round trip against the real endpoint, restore streams from the bucket, and the manifest's both-directions discipline holds remotely"
