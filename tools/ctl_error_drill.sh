#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# flintctl reports a REFUSED command like a CLI, not like a crash.
#
# The bug this guards: `flintctl tenant add` on an existing tenant used to
# panic —
#   thread 'main' panicked at crates/flint-ctl/src/main.rs:1010:18:
#   tenant add failed: Ok(Error("ERR tenant exists"))
# — burying the control plane's perfectly good sentence inside a
# Debug-formatted Result and exiting with a panic code. Operators read the
# CP's error, scripts read the exit status; both deserve better.
#
# Asserts, for every refusal path: the SERVER's message on stderr, nothing
# on stdout, exit 1, and no 'panicked' / 'Ok(Error(' anywhere in the output.
# The last case pulls the CP out from under flintctl entirely, so the
# transport failure (not a -ERR reply) reports the same way.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-ctlerr-state 7701 7702 7830
fleet_guard
STATE=/tmp/flint-ctlerr-state; INV=/tmp/flint-ctlerr.flint
DEAD=/tmp/flint-ctlerr-dead.flint
CTL=./target/release/flintctl
fleet_kill server; fleet_kill controlplane
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill controlplane
  rm -rf "$STATE" "$INV" "$DEAD" /tmp/flint-ctlerr.out /tmp/flint-ctlerr.err
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$DEAD"

# --features flint-server/rocks even though THIS drill never starts a rocks
# engine: the build output is shared, so omitting it replaces
# ./target/release/flint-server with a mem-only binary and the next drill
# that wants rocks dies with "unknown --engine". Harmless here only by
# accident of ordering — reseed happens to rebuild with rocks a few drills
# later. gates.sh now lints for this.
cargo build --release -q -p flint-server -p flint-controlplane -p flint-ctl \
  --features flint-server/rocks

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7830
pair 127.0.0.1:7701,127.0.0.1:7702
EOF
# Same fleet, CP address nothing listens on: the transport-failure case.
sed 's/7830/7839/' "$INV" > "$DEAD"

# fails_cleanly <expected-stderr-substring> <flintctl args...>
fails_cleanly() {
  local want=$1; shift
  "$@" >/tmp/flint-ctlerr.out 2>/tmp/flint-ctlerr.err
  local rc=$?
  local out err
  out=$(cat /tmp/flint-ctlerr.out); err=$(cat /tmp/flint-ctlerr.err)
  [ "$rc" = "1" ] || { echo "FAIL: [$*] exit $rc (want 1)"; echo "  stderr: $err"; exit 1; }
  case "$err" in
    *"$want"*) ;;
    *) echo "FAIL: [$*] stderr missing '$want': $err"; exit 1;;
  esac
  case "$err$out" in
    *panicked*|*"Ok(Error("*) echo "FAIL: [$*] crashed instead of reporting: $err"; exit 1;;
  esac
  [ -z "$out" ] || { echo "FAIL: [$*] wrote the error to stdout: $out"; exit 1; }
  echo "  ok: $err"
}

echo "== bootstrap + the tenant that will collide"
$CTL -f "$INV" bootstrap >/dev/null 2>&1
R=$($CTL -f "$INV" tenant add acme tok-acme acme 1 2>&1) || { echo "FAIL: first add: $R"; exit 1; }
echo "  tenant acme added ($R)"

echo "== the same name again: the CP's own '-ERR tenant exists'"
fails_cleanly "tenant exists" $CTL -f "$INV" tenant add acme tok-acme acme 1

echo "== commands aimed at a tenant that was never there"
fails_cleanly "no such tenant" $CTL -f "$INV" tenant remove ghost
fails_cleanly "no such tenant" $CTL -f "$INV" tenant-reads ghost on
fails_cleanly "no such tenant" $CTL -f "$INV" tenant-quota ghost 100 1000
fails_cleanly "no such tenant" $CTL -f "$INV" tenant-cache ghost on

echo "== the tenant that DOES exist is untouched by the refusals"
$CTL -f "$INV" tenant-reads acme on >/dev/null 2>&1 || { echo "FAIL: live tenant broke"; exit 1; }
echo "  tenant-reads acme on still succeeds"

echo "== no CP at all: the transport failure reports the same way"
fails_cleanly "flintctl: tenant add failed" $CTL -f "$DEAD" tenant add beta tok-beta beta 1

echo "PASS: refusals are the server's sentence on stderr + exit 1 — no panic, no Debug-formatted Result"
