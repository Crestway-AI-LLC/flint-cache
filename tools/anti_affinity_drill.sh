#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A pair whose members share a failure domain must FAIL verify.
#
# WHY THIS EXISTS. The inventory has always required a pair's two members on
# separate HOSTS, and that is the check people assume protects them. It does
# not: two hosts in one availability zone survive a host failure and not the
# loss of the zone, and the zone is by far the likelier event. On
# instance-store families the data goes with them — stopping or retiring an
# instance destroys its copy — so a zone event is dual DATA loss, not a
# temporary outage.
#
# `zone <host> <name>` declares the domain and `verify` asserts pair members
# never share one. This drill proves all three states, because only the first
# is interesting on its own:
#
#   1. distinct domains          -> verify PASSES
#   2. shared domain             -> verify FAILS, naming the pair
#   3. zones declared for SOME   -> verify FAILS
#
# State 3 is the one worth the extra assertion. A half-declared topology
# reports anti-affinity it never checked, which is strictly worse than
# declaring nothing at all — it converts "unknown" into "confirmed safe".
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-affinity 6855 6856 7155 7855
fleet_guard

CTL=./target/release/flintctl
D=/tmp/flint-affinity; rm -rf "$D"; mkdir -p "$D"

cleanup() {
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  [ -n "${KEEP:-}" ] || rm -rf "$D"
}
trap cleanup EXIT
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
cargo build --release -q -p flint-ctl -p flint-proxy -p flint-controlplane -p flint-controller \
  || { echo "FAIL: build"; exit 1; }

# One host, two ports: the addresses differ, so nothing here depends on real
# separate machines. What is under test is the DECLARATION and the check,
# and those are the parts that were missing.
base() {
  cat <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7155
pair 127.0.0.1:6855,127.0.0.1:6856
proxy 127.0.0.1:7855
controller on
EOF
}

echo "== bootstrap (no zones declared: nothing asserted, nothing claimed)"
base > "$D/cluster.flint"
$CTL -f "$D/cluster.flint" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -8 "$D/bootstrap.log"; exit 1; }

OUT=$($CTL -f "$D/cluster.flint" verify 2>&1) || { echo "FAIL: verify on a zone-less inventory should pass"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "not declared" \
  || { echo "FAIL: a zone-less inventory should SAY it is not checking, not stay silent"; echo "$OUT"; exit 1; }
echo "  zone-less inventory verifies, and says the check is not armed"

# ---------------------------------------------------------------------------
echo "== 1. distinct domains -> PASS"
{ base; echo "zone 127.0.0.1 az-a"; } > "$D/cluster.flint"
# Both members share host 127.0.0.1 here, so a single zone line puts them in
# ONE domain. That is state 2, and it is what the next block asserts. For a
# passing case the members must resolve to different hosts, which loopback
# cannot do — so state 1 is asserted through the parser and the check with
# an inventory whose members carry distinct host literals.
sed -i.bak 's|^pair 127.0.0.1:6855,127.0.0.1:6856$|pair 127.0.0.1:6855,127.0.0.2:6856|' "$D/cluster.flint"
{ cat "$D/cluster.flint"; echo "zone 127.0.0.2 az-b"; } > "$D/tmp" && mv "$D/tmp" "$D/cluster.flint"
OUT=$($CTL -f "$D/cluster.flint" verify 2>&1 || true)
echo "$OUT" | grep -q "ok   pair 0 members are in distinct failure domains" \
  || { echo "FAIL: distinct domains should report ok"; echo "$OUT"; exit 1; }
echo "  distinct domains reported ok"

# ---------------------------------------------------------------------------
echo "== 2. shared domain -> FAIL"
{ base; echo "zone 127.0.0.1 az-a"; } > "$D/cluster.flint"
set +e
OUT=$($CTL -f "$D/cluster.flint" verify 2>&1)
RC=$?
set -e
[ "$RC" -ne 0 ] \
  || { echo "FAIL: a pair with both members in az-a must fail verify"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "distinct failure domains" \
  || { echo "FAIL: the failure must NAME the check"; echo "$OUT"; exit 1; }
echo "  shared domain failed verify, naming the check"

# ---------------------------------------------------------------------------
echo "== 3. partial declaration -> FAIL (the false-assurance case)"
sed 's|^pair 127.0.0.1:6855,127.0.0.1:6856$|pair 127.0.0.1:6855,127.0.0.2:6856|' <(base) > "$D/cluster.flint"
echo "zone 127.0.0.1 az-a" >> "$D/cluster.flint"   # 127.0.0.2 deliberately unzoned
set +e
OUT=$($CTL -f "$D/cluster.flint" verify 2>&1)
RC=$?
set -e
[ "$RC" -ne 0 ] \
  || { echo "FAIL: a half-declared topology must be refused, not half-checked"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "127.0.0.2" \
  || { echo "FAIL: the failure must name the host that has no zone"; echo "$OUT"; exit 1; }
echo "  partial declaration refused, naming the unzoned host"

# ---------------------------------------------------------------------------
echo "== 4. a malformed zone line dies rather than being ignored"
{ base; echo "zone 127.0.0.1"; } > "$D/cluster.flint"
set +e
OUT=$($CTL -f "$D/cluster.flint" verify 2>&1)
RC=$?
set -e
[ "$RC" -ne 0 ] \
  || { echo "FAIL: 'zone <host>' with no name must be refused, not dropped"; echo "$OUT"; exit 1; }
echo "  malformed zone line refused"

echo "PASS: failure-domain anti-affinity — distinct domains verify, a shared domain fails, a PARTIAL declaration fails rather than reporting anti-affinity it never checked, and a malformed zone line is refused instead of silently dropped."
