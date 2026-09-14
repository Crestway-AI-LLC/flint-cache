#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A TENANT'S PROXY SUBSET ONLY EVER SHRINKS (ADR-0030).
#
# Three mutations write `tenant.subset`: AddTenant computes it once, DelProxy
# removes a retired proxy from EVERY tenant, and SetSubset is the operator's
# hand. AddProxy is not among them. So fleet membership already reaches tenant
# subsets -- in one direction, and it is the direction that degrades. A tenant
# at k=2 that loses a proxy runs at k=1 permanently, and the terminus is an
# empty subset, which the control plane itself calls DRAINED and answers
# -WRONGPASS from.
#
# THIS DRILL PINS THE CURRENT BEHAVIOUR ON PURPOSE. The "stays at 1" assertion
# below is a defect being held still, not a property being protected: when
# ADR-0030 is taken it goes RED and tells whoever took it to flip it, rather
# than passing silently against a fleet that now re-widens. Same reason the
# roll-record drill in flint-kv-ops asserts its old message.
#
# REGISTRY-LEVEL, DELIBERATELY. No proxies are started and none are needed:
# the defect is in the registry's mutation logic, CPADDPROXY registers an
# address rather than a process, and "the tenant still serves" is exactly the
# reading ADR-0030 argues against -- a subset that is serving is not a subset
# that is whole.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-ratchet-state 7560 7564 7565 7566
fleet_guard
fleet_kill controlplane; sleep 0.4
CP=./target/release/flint-controlplane
STATE=$FLINT_DRILL_ROOT/flint-ratchet-state
cleanup() { fleet_kill controlplane; rm -rf "$STATE" "$STATE.tmp"; }
trap cleanup EXIT
rm -rf "$STATE"

$CP --port 7560 --state "$STATE" 2>$FLINT_DRILL_ROOT/flint-ratchet-cp.log &
fleet_wait_listen 7560
sleep 0.4

cp_() { valkey-cli -p 7560 "$@"; }
# tr -d '\r' IS LOAD-BEARING. CPSUBSETS is a Bulk of CRLF-terminated lines,
# so awk's last field carries a trailing CR -- and an address with a CR in it
# makes the CP build `OK retired <addr>\r`, a RESP Simple string containing
# CR, which is malformed. The first CPDELPROXY below takes field 1 and worked;
# the second took the last field and failed with "Bad simple string value",
# which names the reply and not the argument that poisoned it.
subset_of() { cp_ CPSUBSETS | tr -d '\r' | awk -v t="$1" '$1==t{print $2}'; }
count_of() { local s; s=$(subset_of "$1"); [ -z "$s" ] || [ "$s" = "-" ] && { echo 0; return; }; echo "$s" | tr ',' '\n' | grep -c .; }

echo "== a fleet of three proxies, one tenant at the default k=2"
# DECLARED in fleet_init even though nothing binds them: these are registry
# entries, not listeners, and borrowing another drill's declared ports for
# them would read as a collision to anyone auditing the port map.
for p in 7564 7565 7566; do cp_ CPADDPROXY "127.0.0.1:$p" >/dev/null; done
cp_ CPADDPAIR 127.0.0.1:6999 >/dev/null
# CPADDTENANT <name> <token> <ns> [k] -- the trailing number is the SUBSET
# WIDTH, not a quota. Copying `1` from the drills that use it placed this
# tenant on ONE proxy and the control below caught it, which is what the
# control is for; asserting the reply here says so at the setup line instead.
ADD=$(cp_ CPADDTENANT acme tok-acme acme 2)
echo "  $ADD"
case "$ADD" in
  OK*) ;;
  *) echo "FAIL: CPADDTENANT refused: $ADD"; exit 1 ;;
esac

# THE CONTROL. Everything below is satisfied by a tenant that started at 1,
# so prove it started at k.
K0=$(count_of acme)
[ "$K0" = "2" ] || { echo "FAIL: tenant did not start at k=2 (got $K0) — the shrink below would prove nothing"; cp_ CPSUBSETS | sed 's/^/  | /'; exit 1; }
BEFORE=$(subset_of acme)
echo "  acme placed on [$BEFORE]"

echo "== retire ONE of the tenant's own proxies"
VICTIM=$(echo "$BEFORE" | cut -d, -f1)
SURVIVOR=$(echo "$BEFORE" | cut -d, -f2)
DEL1=$(cp_ CPDELPROXY "$VICTIM")
case "$DEL1" in OK*) ;; *) echo "FAIL: CPDELPROXY refused: $DEL1"; exit 1 ;; esac
sleep 0.5
K1=$(count_of acme)
[ "$K1" = "1" ] || { echo "FAIL: expected the subset to shrink to 1, got $K1"; cp_ CPSUBSETS | sed 's/^/  | /'; exit 1; }
[ "$(subset_of acme)" = "$SURVIVOR" ] || { echo "FAIL: the survivor is not the proxy that was left: $(subset_of acme) vs $SURVIVOR"; exit 1; }
echo "  acme is down to [$SURVIVOR] — the retired proxy was removed, correctly"

echo "== and nothing re-widens it, though a spare proxy is sitting right there"
# A third proxy was registered and never used, so re-widening is POSSIBLE and
# simply does not happen. Without that spare this assertion would be vacuous.
# CPPROXIES is COMMA-joined, not space-joined: splitting on spaces returned
# the whole list and the "spare" named the survivor among others.
SPARE=$(cp_ CPPROXIES | tr -d '\r' | tr ',' '\n' | grep -v "^$" | grep -vx "$SURVIVOR" | head -1)
[ -n "$SPARE" ] || { echo "FAIL: no spare proxy in the fleet, so 'does not re-widen' is untestable here"; exit 1; }
sleep 3
K2=$(count_of acme)
if [ "$K2" != "1" ]; then
  echo "FAIL (AND THIS MAY BE GOOD NEWS): the subset moved to $K2 after the retirement."
  echo "      This drill PINS the ADR-0030 ratchet — a tenant that loses a proxy"
  echo "      stays shrunk. If something now re-widens it, that behaviour is the"
  echo "      fix and THIS ASSERTION IS WHAT NEEDS UPDATING, not the fleet."
  cp_ CPSUBSETS | sed 's/^/  | /'
  exit 1
fi
echo "  still [$(subset_of acme)] after 3s, with $SPARE idle and eligible"

echo "== the ratchet's terminus: retire the last one and the tenant is DRAINED"
DEL=$(cp_ CPDELPROXY "$SURVIVOR")
case "$DEL" in OK*) ;; *) echo "FAIL: CPDELPROXY refused: $DEL"; exit 1 ;; esac
sleep 0.5
K3=$(count_of acme)
[ "$K3" = "0" ] || { echo "FAIL: expected an empty subset, got $K3"; exit 1; }
echo "  acme now has NO proxies — the state the control plane answers -WRONGPASS from,"
echo "  reached by attrition rather than by any decision about this tenant"

echo "PASS: the subset shrank on retirement, stayed shrunk with a spare available, and reached empty — ADR-0030's ratchet, pinned"
