#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Tenant drill: token auth at the proxy + namespace isolation on the nodes.
#   - pre-auth commands get -NOAUTH; bad tokens get -WRONGPASS
#   - two tenants write the SAME key names; each reads only its own values
#   - DBSIZE is per-tenant; FLUSHALL of one tenant leaves the other intact
#   - re-AUTH to a different tenant on one connection is rejected
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-tn 6650 6667
fleet_guard
fleet_kill server; fleet_kill proxy; sleep 0.4
B=./target/release/flint-server
D=$(mktemp -d /tmp/flint-tn.XXXXXX)
cleanup() {
  pkill -9 -f "flint-server --port 6650" 2>/dev/null
  fleet_kill proxy
  rm -rf "$D"
}
trap cleanup EXIT

$B --port 6650 --engine rocks --data-dir "$D" 2>/dev/null &
sleep 0.6
./target/release/flint-proxy --port 6667 --pairs "127.0.0.1:6650" \
  --tenants "tokA=alpha,tokB=beta" 2>/tmp/flint-tn-proxy.log &
sleep 0.5

echo "== pre-auth: everything but AUTH/QUIT is refused"
R=$(valkey-cli -p 6667 GET k 2>&1)
echo "$R" | grep -q "NOAUTH" || { echo "FAIL: expected NOAUTH, got: $R"; exit 1; }
R=$(valkey-cli -p 6667 PING 2>&1)
echo "$R" | grep -q "NOAUTH" || { echo "FAIL: PING pre-auth should be NOAUTH, got: $R"; exit 1; }
R=$(valkey-cli -p 6667 AUTH wrong-token 2>&1)
echo "$R" | grep -q "WRONGPASS" || { echo "FAIL: expected WRONGPASS, got: $R"; exit 1; }
echo "  NOAUTH + WRONGPASS enforced"

echo "== two tenants, same key names, different values"
valkey-cli -p 6667 -a tokA --no-auth-warning SET shared "from-alpha" >/dev/null
valkey-cli -p 6667 -a tokA --no-auth-warning SET alpha-only 1 >/dev/null
valkey-cli -p 6667 -a tokB --no-auth-warning SET shared "from-beta" >/dev/null
GA=$(valkey-cli -p 6667 -a tokA --no-auth-warning GET shared)
GB=$(valkey-cli -p 6667 -a tokB --no-auth-warning GET shared)
[ "$GA" = "from-alpha" ] || { echo "FAIL: tenant A sees '$GA'"; exit 1; }
[ "$GB" = "from-beta" ]  || { echo "FAIL: tenant B sees '$GB'"; exit 1; }
GX=$(valkey-cli -p 6667 -a tokB --no-auth-warning GET alpha-only)
[ -z "$GX" ] || { echo "FAIL: tenant B can see tenant A's key: '$GX'"; exit 1; }
echo "  same key name, isolated values; cross-tenant read is nil"

echo "== per-tenant DBSIZE"
DA=$(valkey-cli -p 6667 -a tokA --no-auth-warning DBSIZE)
DB=$(valkey-cli -p 6667 -a tokB --no-auth-warning DBSIZE)
[ "$DA" = "2" ] || { echo "FAIL: tenant A DBSIZE=$DA (want 2)"; exit 1; }
[ "$DB" = "1" ] || { echo "FAIL: tenant B DBSIZE=$DB (want 1)"; exit 1; }
echo "  A=2 B=1"

echo "== FLUSHALL is tenant-scoped"
valkey-cli -p 6667 -a tokA --no-auth-warning FLUSHALL >/dev/null
DA=$(valkey-cli -p 6667 -a tokA --no-auth-warning DBSIZE)
DB=$(valkey-cli -p 6667 -a tokB --no-auth-warning DBSIZE)
GB=$(valkey-cli -p 6667 -a tokB --no-auth-warning GET shared)
[ "$DA" = "0" ] || { echo "FAIL: tenant A not flushed: $DA"; exit 1; }
[ "$DB" = "1" ] && [ "$GB" = "from-beta" ] || { echo "FAIL: tenant A's FLUSHALL damaged tenant B (DBSIZE=$DB, shared='$GB')"; exit 1; }
echo "  A flushed to 0; B untouched (DBSIZE=1, value intact)"

echo "== re-AUTH to a different tenant on one connection is rejected"
R=$(valkey-cli -p 6667 -a tokA --no-auth-warning AUTH tokB 2>&1)
echo "$R" | grep -q "another tenant" || { echo "FAIL: tenant switch not rejected: $R"; exit 1; }
echo "  switch rejected (reconnect required)"

echo "== admin surface still sealed for authed tenants"
R=$(valkey-cli -p 6667 -a tokA --no-auth-warning FLINTINFO 2>&1)
echo "$R" | grep -q "not available" || { echo "FAIL: FLINT* leaked through: $R"; exit 1; }

echo "PASS: token auth + namespace isolation — same keys, separate worlds; scoped DBSIZE/FLUSHALL"
