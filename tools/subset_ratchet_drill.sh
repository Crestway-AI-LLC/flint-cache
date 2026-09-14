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
# THE RATCHET IS CLOSED (ADR-0030, taken 2026-09-14). This file pinned the
# defect first and went RED when the fix landed, which is what it was written
# to do; the assertions below are the flipped ones. `DelProxy` now refills the
# hole it makes, to the subset's OWN width -- so an operator who widened a
# whale by hand keeps that width -- taking members from the shuffle-shard
# ideal so repaired tenants spread instead of piling onto one survivor, and
# never moving a member it did not have to.
#
# Jeff, 2026-09-14: every shrink is an operator action, so the repair belongs
# at the moment of the retirement, with a human present, rather than in a
# background sweeper.
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

echo "== retire ONE of the tenant's own proxies — the hole must be REFILLED"
VICTIM=$(echo "$BEFORE" | cut -d, -f1)
SURVIVOR=$(echo "$BEFORE" | cut -d, -f2)
DEL1=$(cp_ CPDELPROXY "$VICTIM")
case "$DEL1" in OK*) ;; *) echo "FAIL: CPDELPROXY refused: $DEL1"; exit 1 ;; esac
sleep 0.5
K1=$(count_of acme)
AFTER=$(subset_of acme)
[ "$K1" = "2" ] || {
  echo "FAIL: the tenant is at $K1 after a retirement, not back at 2 — [$AFTER]"
  echo "      ADR-0030: DelProxy refills the hole it makes. A tenant left"
  echo "      narrow here is the ratchet, reopened."
  cp_ CPSUBSETS | sed 's/^/  | /'; exit 1; }
case ",$AFTER," in *",$VICTIM,"*) echo "FAIL: the retired proxy $VICTIM is still placed"; exit 1 ;; esac
case ",$AFTER," in *",$SURVIVOR,"*) ;; *) echo "FAIL: the surviving member $SURVIVOR was MOVED, and need not have been: [$AFTER]"; exit 1 ;; esac
echo "  acme is back to [$AFTER] — refilled from the spare, survivor untouched"

echo "== a fleet too small to cover the width leaves it short, and does not duplicate"
# Retire another of ITS OWN: two proxies are now gone, one remains, and the
# tenant wants two. Short is the correct answer -- inventing a duplicate to
# reach the number would be worse than being honest about the fleet.
NEXT=$(echo "$AFTER" | cut -d, -f1)
DEL2=$(cp_ CPDELPROXY "$NEXT")
case "$DEL2" in OK*) ;; *) echo "FAIL: CPDELPROXY refused: $DEL2"; exit 1 ;; esac
sleep 0.5
K2=$(count_of acme); S2=$(subset_of acme)
[ "$K2" = "1" ] || { echo "FAIL: one proxy left in the fleet but the tenant reports $K2 — [$S2]"; exit 1; }
UNIQ=$(echo "$S2" | tr ',' '\n' | sort -u | grep -c .)
[ "$UNIQ" = "1" ] || { echo "FAIL: the refill duplicated a proxy: [$S2]"; exit 1; }
echo "  one proxy left, acme on [$S2] — short and honest rather than padded"

echo "== and a fleet with NO proxies cannot serve anyone"
DEL3=$(cp_ CPDELPROXY "$S2")
case "$DEL3" in OK*) ;; *) echo "FAIL: CPDELPROXY refused: $DEL3"; exit 1 ;; esac
sleep 0.5
K3=$(count_of acme)
[ "$K3" = "0" ] || { echo "FAIL: expected an empty subset on an empty fleet, got $K3"; exit 1; }
echo "  acme has no proxies, because the FLEET has none — a state an operator"
echo "  reached deliberately, not one attrition walked it into"

echo "PASS: a retirement refills the hole it makes, keeps the tenant's own width, moves no member it need not, and goes short only when the fleet is genuinely too small"
