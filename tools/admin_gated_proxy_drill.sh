#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# An admin-GATED proxy must not be mistaken for a dead one.
#
# A fleet with an `admin-token` line has its proxy refuse operator commands
# pre-auth: the control plane pushes the token's digest (ADR-0006 D4) and
# PROXYSTATS answers `-NOAUTH admin token required for this command`. That
# refusal is POSITIVE EVIDENCE the proxy is serving — only a live proxy
# holding the pushed digest can produce it. `proxy_up` has always known
# that and accepted it.
#
# WHY THIS DRILL EXISTS. `proxy_up` moved from `call` to `call_seq_on`,
# which converts a RESP error into an io::Error instead of returning it as
# a value. The arm that accepted the refusal —
#
#     Ok(Value::Error(e)) => e.starts_with("NOAUTH")
#
# — became unreachable, silently. Nothing failed to compile. For two
# releases every fleet with an admin token read its OWN proxy as down:
# `bootstrap` failed, `status` printed DOWN, and `roll_edge` aborted the
# upgrade after rolling every seat.
#
# It was caught by the FLEET repository's admin_rotate drill, because that
# is the only drill anywhere that bootstraps a fleet with an admin-token
# line. This repository's gate was green on the same commit — for a binary
# that ships from HERE. That is the gap this file closes: the coverage now
# lives with the code, where a fork or a source build can run it.
#
# It asserts the three things that actually broke, in the order they broke.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-admingate 7441 7442 7443 7444
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-admingate
INV=$STATE/cluster.flint
A=127.0.0.1:7441
B=127.0.0.1:7442
PROXY=127.0.0.1:7443
CP=127.0.0.1:7444
TAG=admingate-1
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  rm -rf "$STATE"
}
trap cleanup EXIT
rm -rf "$STATE"; mkdir -p "$STATE"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

CLI=""
for c in valkey-cli redis-cli; do command -v "$c" >/dev/null 2>&1 && { CLI=$c; break; }; done
[ -n "$CLI" ] || { echo "SKIP: no valkey-cli or redis-cli"; exit 0; }

# `admin-token` is the whole point of this inventory. Everything else is the
# smallest fleet that has a proxy to gate.
cat > "$INV" <<EOF
disposable on
statedir $STATE/state
bins ./target/release
admin-token seed-admin-token
cp $CP
pair $A,$B
proxy $PROXY
controller on
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap a fleet whose proxy is admin-gated"
# THE FIRST THING THAT BROKE. bootstrap waits on proxy_up for 10s and then
# panics with "did not come up (port busy?)" — a message that sends you
# looking at ports while the proxy sits there serving.
$CTL bootstrap >"$STATE/bootstrap.log" 2>&1 || {
  echo "FAIL: bootstrap"
  tail -12 "$STATE/bootstrap.log" | sed 's/^/  | /'
  echo "      If this says the proxy 'did not come up', check whether a -NOAUTH"
  echo "      refusal is still being read as liveness (proxy_up / is_noauth_error)."
  exit 1; }
echo "  bootstrapped"

# POSITIVE CONTROL, and this drill is worthless without it. If the proxy
# were NOT gated it would answer PROXYSTATS with a Bulk, every assertion
# below would pass through the ordinary path, and the drill would certify
# the -NOAUTH handling while never once exercising it. Assert the gate is
# real before trusting anything that depends on it.
echo "== the gate is real: PROXYSTATS is refused pre-auth"
R=$($CLI -p 7443 --no-auth-warning PROXYSTATS 2>&1 | tr -d '\r' | head -1)
case "$R" in
  *NOAUTH*) echo "  proxy refuses pre-auth: $R" ;;
  *) echo "FAIL: expected a -NOAUTH refusal, got: ${R:-<empty>}"
     echo "      The proxy is not gated, so this drill would prove nothing."
     echo "      Has the CP stopped pushing the admin digest (ADR-0006 D4)?"
     exit 1 ;;
esac

echo "== status reports that proxy UP, not DOWN"
# THE SECOND THING THAT BROKE, and the one an operator sees. A serving
# proxy reported DOWN is worse than no report: it sends someone to restart
# a healthy seat during an incident.
ST=$($CTL status 2>&1)
echo "$ST" | sed 's/^/  | /'
PLINE=$(echo "$ST" | grep "^proxy" | head -1)
case "$PLINE" in
  *" up "*) ;;
  *DOWN*) echo "FAIL: a serving, admin-gated proxy is reported DOWN: $PLINE"; exit 1 ;;
  *) echo "FAIL: could not read a proxy row from status: ${PLINE:-<none>}"; exit 1 ;;
esac
echo "  proxy up"

echo "== upgrade --version-tag completes instead of aborting at the edge"
# THE THIRD AND WORST. roll_edge rolls every seat and THEN waits on
# proxy_up; a false DOWN there aborts with exit 3 after the fleet has
# already been replaced, so the roll both happened and reported failure.
$CTL upgrade --version-tag "$TAG" --soak-ms 1500 >"$STATE/upgrade.log" 2>&1 || {
  echo "FAIL: upgrade exited non-zero on an admin-gated fleet"
  tail -12 "$STATE/upgrade.log" | sed 's/^/  | /'
  exit 1; }
ST=$($CTL status 2>&1)
echo "$ST" | grep -E "^(proxy|cp)" | sed 's/^/  | /'
echo "$ST" | grep -q "^proxy .*build $TAG" || {
  echo "FAIL: after the roll the proxy does not report build '$TAG'"
  echo "$ST" | sed 's/^/  | /'; exit 1; }
echo "  rolled, and the gated proxy still reports its build"

echo "PASS: admin-gated proxy — a -NOAUTH refusal is read as a SERVING proxy, so bootstrap, status and the edge roll all complete on a fleet with an admin token"
