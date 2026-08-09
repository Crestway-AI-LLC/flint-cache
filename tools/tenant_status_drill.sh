#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0014 D3: CPMYSTATUS — what a tenant may know about itself.
#
# CPMYCONFIG is a SETTER and CPMYUSAGE returns a bare positional line, so a
# tenant had no way to ask "what is my quota, which flags am I on, what am
# I connected to". The console's /api/overview covers some of it for the
# SaaS path only; self-hosted and marketplace tenants had nothing — and on
# the marketplace AMI valkey-cli is not even installed, so the CP response
# is the whole surface.
#
# The ADR states the red condition, and states HOW to check it: tenant A's
# token must return A's quota and flags and must contain no string
# identifying tenant B — "asserted by provisioning both and grepping the
# response, not by reading the code". A tenant-scoping bug is exactly the
# kind that reads correct and behaves otherwise, so this drill provisions
# two tenants with deliberately distinctive names and greps.
#
# Requires: valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-tstatus-state 7431 7432 7433 7434
fleet_guard
STATE=/tmp/flint-tstatus-state
CP=7434
fleet_kill controlplane
sleep 0.3
cleanup() { fleet_kill controlplane; rm -rf "$STATE"; }
trap cleanup EXIT
rm -rf "$STATE"; mkdir -p "$STATE"

cargo build --release -q -p flint-controlplane

./target/release/flint-controlplane --port $CP --state "$STATE/cp" \
  > "$STATE/cp.log" 2>&1 &
for _ in $(seq 1 40); do
  [ "$(valkey-cli -p $CP PING 2>/dev/null)" = "PONG" ] && break
  sleep 0.25
done
[ "$(valkey-cli -p $CP PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: CP never came up"; exit 1; }

# Register a proxy BEFORE the tenants, so each gets a real subset. Without
# one, `endpoint` renders "-" and the assertion below would pass whatever
# the field did -- non-emptiness proving nothing, the way "[ -n $OUT ]"
# once certified a wiped database.
fleet_cp $CP CPADDPROXY 127.0.0.1:7433
# Distinctive names/namespaces, so a leak cannot hide inside a common
# substring. "acme"/"beta" would both appear in unrelated words.
fleet_cp $CP CPADDTENANT alphaco tok-alpha-zzz nsalphaqq 1
fleet_cp $CP CPADDTENANT betaco  tok-beta-yyy  nsbetaww  1
fleet_cp $CP CPTENANTQUOTA alphaco 4321 987654321
fleet_cp $CP CPTENANTREADS alphaco on

echo "== a tenant sees its own quota and flags"
A=$(valkey-cli -p $CP CPMYSTATUS tok-alpha-zzz 2>&1 | tr -d '\r')
echo "$A" | sed 's/^/  | /'
for want in "tenant:alphaco" "namespace:nsalphaqq" "quota_ops_per_sec:4321" \
            "quota_max_bytes:987654321" "replica_reads:1" "usage_bytes:" "build:" \
            "endpoint:127.0.0.1:7433"; do
  echo "$A" | grep -q "^$want" || { echo "FAIL: missing '$want'"; exit 1; }
done

echo "== and NOTHING identifying the other tenant"
# The check the ADR asks for. Grep, not review.
for leak in "betaco" "nsbetaww" "tok-beta-yyy"; do
  echo "$A" | grep -q "$leak" && {
    echo "FAIL: tenant A's status leaked '$leak' — a cross-tenant disclosure"
    exit 1; }
done
echo "  no 'betaco', 'nsbetaww' or 'tok-beta-yyy' in A's response"

# POSITIVE CONTROL on that grep. Three greps that find nothing prove
# nothing unless the same greps can find something: if CPMYSTATUS returned
# an empty string, the loop above would pass and the drill would certify
# isolation it never tested.
B=$(valkey-cli -p $CP CPMYSTATUS tok-beta-yyy 2>&1 | tr -d '\r')
echo "$B" | grep -q "^tenant:betaco" || { echo "FAIL: B's own status does not name B"; exit 1; }
for leak in "alphaco" "nsalphaqq" "tok-alpha-zzz"; do
  echo "$B" | grep -q "$leak" && { echo "FAIL: tenant B's status leaked '$leak'"; exit 1; }
done
echo "  and the mirror holds: B sees B, never A (so the greps do work)"

echo "== no operator surface: no topology, no node addresses, no journal"
# The other half of D3. A tenant asking why they are throttled needs their
# own limits; the fleet's shape is not theirs to have.
for leak in "pair" "cp:" "journal" "admin" "controller" "registry_version"; do
  echo "$A" | grep -qi "^$leak" && { echo "FAIL: operator field '$leak' in a tenant response"; exit 1; }
done
echo "  none present"

echo "== a bad token is refused, and says so as WRONGPASS"
R=$(valkey-cli -p $CP CPMYSTATUS not-a-real-token 2>&1 | tr -d '\r')
case "$R" in
  *WRONGPASS*) echo "  $R" ;;
  *) echo "FAIL: expected WRONGPASS for an unknown token, got: $R"; exit 1 ;;
esac
# And it must not distinguish "no such tenant" from "wrong token" in a way
# that enumerates tenants.
echo "$R" | grep -qi "alphaco\|betaco" && { echo "FAIL: the rejection names a tenant"; exit 1; }

echo "PASS: CPMYSTATUS — a tenant reads its own quota, flags, endpoint and the service build; contains nothing identifying another tenant (grepped, both directions) and no operator topology"
