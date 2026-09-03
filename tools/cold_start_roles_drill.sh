#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Cold-start role drill: a pair that has FAILED OVER must still replicate
# after a full stop/start.
#
# Two correct rules used to compose into a broken fleet. `start` falls back
# to inventory ORDER when nothing is up — pair[0] bare, the rest
# `--replica-of pair[0]` — because with no live seat there is nobody to ask.
# And a node's durable manifest role outranks that flag, which is right: a
# flag must never demote a master holding the newest data. After a failover
# the two disagree, and the result is:
#
#   pair[0], durable replica, started bare  -> replica of NOBODY
#   pair[1], durable master, given the flag -> "ignoring --replica-of"
#
# Nothing errors. `status` shows both up, roles coherent, epochs agreed.
# Only live_replicas 0 betrays it. That is the shape this drill exists to
# refuse: a fleet that looks healthy and is storing one copy.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-coldrole-state 7403 7404 7405 7406
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-coldrole-state
INV=$FLINT_DRILL_ROOT/flint-coldrole.flint
A=127.0.0.1:7403   # inventory pair[0]
B=127.0.0.1:7404   # inventory pair[1] — the one we promote
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:7406
pair $A,$B
proxy 127.0.0.1:7405
controller on
EOF

CTL="./target/release/flintctl -f $INV"
role_of() { valkey-cli -p "${1##*:}" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^role://p'; }
# A ROLE IS DECIDED, NOT MERELY PRESENT. Since #176 a node binds and answers
# FLINTINFO from INSIDE its load, and `role:` reads `loading` there --
# non-empty, and not master or replica. Any wait whose test is "role_of is
# non-empty" is therefore satisfied by the state it exists to wait past, and
# breaks out on the first tick. That is how this drill reported
# `roles loading/loading` and reddened main.
role_decided() { case "$(role_of "$1")" in master|replica) return 0 ;; *) return 1 ;; esac; }
replicas_of() { valkey-cli -p "${1##*:}" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p'; }
loading_of() { valkey-cli -p "${1##*:}" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^loading://p'; }
seqlag_of() { valkey-cli -p "${1##*:}" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^seq_lag://p'; }

echo "== bootstrap: pair[0] is master, replication live"
$CTL bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
for _ in $(seq 1 60); do [ "$(replicas_of "$A")" = "1" ] && break; sleep 0.5; done
[ "$(role_of "$A")" = "master" ] || { echo "FAIL: $A is not master after bootstrap"; exit 1; }
[ "$(replicas_of "$A")" = "1" ] || { echo "FAIL: no replication after bootstrap"; exit 1; }
echo "  $A master, live_replicas 1"

echo "== write something, so a lost replica would be losing real data"
FIRST=$(valkey-cli -p 7403 SET cold:1 v1 2>&1)
for i in $(seq 2 200); do cli_ok valkey-cli -p 7403 SET "cold:$i" "v$i"; done
# ASSERT THE SEED LANDED. Without this the drill's final "data lost" could
# equally mean "data never written", and those need different fixes — the
# KB already carries this lesson and this drill was written ignoring it.
SEEDED=$(valkey-cli -p 7403 DBSIZE)
[ "${SEEDED:-0}" -ge 200 ] || {
  echo "FAIL: seed writes not accepted — DBSIZE=$SEEDED on $A after 200 SETs; first SET replied: $FIRST"
  echo "      (nothing downstream can be interpreted; the drill never had data)"
  exit 1
}
echo "  $SEEDED keys on $A"
# WAIT for the replica to hold them before promoting it. This inventory
# leaves min-replicas at 0, so a write acks without any replica confirming
# it, and `failover` immediately after the loop could promote a node still
# behind — losing writes this drill later asserts are present. Correct
# product behaviour for min-replicas 0, and a race the drill should not
# leave open.
#
# Precautionary, not diagnosed: the failure that prompted it turned out to
# be a full disk (see the seed assertion above), not replication timing.
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7403 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^seq_lag://p')" = "0" ] && break
  sleep 0.5
done
[ "$(valkey-cli -p 7403 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^seq_lag://p')" = "0" ] || {
  echo "FAIL: replica never caught up before the failover; promoting now would"
  echo "      lose writes and the data assertion at the end would be measuring"
  echo "      that, not the cold-start path this drill is about"
  exit 1
}

echo "== fail over: the durable roles now disagree with inventory order"
$CTL failover "$A" >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(role_of "$B")" = "master" ] && break; sleep 0.5; done
[ "$(role_of "$B")" = "master" ] || { echo "FAIL: $B never promoted"; exit 1; }
echo "  $B is master, and it is the file's pair[1]"

echo "== full stop, then cold start — the case with nobody left to ask"
$CTL stop >/dev/null 2>&1
sleep 2
for p in 7403 7404; do
  ! valkey-cli -p "$p" PING >/dev/null 2>&1 || { echo "FAIL: $p still serving after stop"; exit 1; }
done
echo "== first, build the broken shape by hand: verify must refuse it"
# Both members up, correct roles, agreed epochs, replicating NOTHING —
# what `start` used to produce. Spawned directly rather than through
# flintctl so the state exists to be judged, with no controller running to
# repair it underneath the assertion.
#
# This is the positive control for verify's new check. Without it, "verify
# passes after the fix" would be equally true of a verify that looks at
# nothing.
./target/release/flint-controlplane --port 7406 --state "$STATE/cp" >/dev/null 2>&1 &
for _ in $(seq 1 40); do [ "$(valkey-cli -p 7406 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.25; done
for p in 7403 7404; do
  ./target/release/flint-server --port "$p" --bind 127.0.0.1 --engine rocks \
    --data-dir "$STATE/node-$p" --journal 127.0.0.1:7406 >/dev/null 2>&1 &
done
for _ in $(seq 1 60); do
  role_decided "$A" && role_decided "$B" && break; sleep 0.5
done
[ "$(role_of "$A")" = "replica" ] && [ "$(role_of "$B")" = "master" ] || {
  echo "FAIL: hand-spawn did not reproduce the detached shape (roles $(role_of "$A")/$(role_of "$B"))"
  exit 1
}
[ "$(replicas_of "$B")" = "0" ] || { echo "FAIL: hand-spawned pair is replicating; no broken state to judge"; exit 1; }
$CTL verify > $FLINT_DRILL_ROOT/flint-coldrole.verify 2>&1
grep -q "SINGLE-COPY: every member up" $FLINT_DRILL_ROOT/flint-coldrole.verify || {
  echo "FAIL: verify passed a pair with both members up and zero replication"
  echo "      — this is the state that ran the playground for five days"
  cat $FLINT_DRILL_ROOT/flint-coldrole.verify; exit 1
}
echo "  verify refuses it: $(grep -o 'SINGLE-COPY: every member up.*' $FLINT_DRILL_ROOT/flint-coldrole.verify | head -1)"
# Clear the hand-spawned seats so `start` below sees a genuine cold fleet;
# flintctl has no pidfiles for these, and a live process would be read as
# "already up" and left alone.
for p in 7403 7404 7406; do pkill -f "port $p " 2>/dev/null; done
sleep 2
rm -f $FLINT_DRILL_ROOT/flint-coldrole.verify

echo "== now the real cold start"
$CTL start > $FLINT_DRILL_ROOT/flint-coldrole.out 2>&1
# The sleep gives `start` a moment to get going; it is NOT the readiness
# check, and on a loaded runner it never was. Same trap one form over: a
# duration that used to be enough is not an answer from the thing being
# waited on, and a role read during the load window reads `loading` and fails
# an assertion about lineage that has nothing to do with timing.
sleep 3
for _ in $(seq 1 80); do role_decided "$A" && role_decided "$B" && break; sleep 0.5; done

echo "== the fleet must replicate, and must say why it had to intervene"
# The substantive property is asserted FIRST, deliberately. Checking the
# log line first meant an edit to the wording failed the drill before it
# ever looked at whether the fleet replicates, reporting a cosmetic change
# as this bug.
MASTER=$B
[ "$(role_of "$B")" = "master" ] || { echo "FAIL: $B lost the lineage across the cold start"; exit 1; }
for _ in $(seq 1 80); do [ "$(replicas_of "$MASTER")" = "1" ] && break; sleep 0.5; done
[ "$(replicas_of "$MASTER")" = "1" ] || {
  echo "FAIL: live_replicas $(replicas_of "$MASTER") after cold start — the pair is storing one copy"
  echo "      and nothing errored, which is exactly the failure this drill exists for"
  # WHICH failure is it? BUG-0064. A replica still INSIDE its load counts as an
  # ABSENT one in the master's live_replicas, so "the replica never attached"
  # and "40s was not enough on a slow runner" produce the identical line above.
  # This drill already knows the distinction — `role_decided` exists because
  # since #176 a node answers FLINTINFO from inside its load — but this
  # assertion never used it, and the two readings have been indistinguishable
  # in CI for as long as it has been failing there (2 of 60 runs).
  #
  # Printed rather than judged, deliberately: the assertion is unchanged, so a
  # genuine replication failure still fails. What changes is that the NEXT
  # firing says which one it was, instead of costing another read of the same
  # ambiguous line. Same shape as BUG-0014's instrument fix.
  echo "      == BUG-0064 DIAGNOSTIC =="
  echo "      waited 40s (80 x 0.5s) for live_replicas to reach 1"
  for n in "$A" "$B"; do
    echo "      seat $n: role=$(role_of "$n") loading=$(loading_of "$n") live_replicas=$(replicas_of "$n") seq_lag=$(seqlag_of "$n")"
  done
  echo "      READ IT THUS: loading=1 on the replica means the budget expired mid-load"
  echo "      (a timing fault, widen the wait); loading=0 with live_replicas=0 on the"
  echo "      master means it finished loading and did NOT attach, which is the"
  echo "      single-copy fleet of BUG-0008 and is a product regression."
  cat $FLINT_DRILL_ROOT/flint-coldrole.out; exit 1
}
[ "$(role_of "$A")" = "replica" ] || { echo "FAIL: $A did not come back as a replica"; exit 1; }
# Corroboration, not the headline: proves the healthy answer came from the
# repair path rather than from a setup that quietly failed to build the
# broken case this drill is named for.
grep -q "durable roles disagree with inventory order" $FLINT_DRILL_ROOT/flint-coldrole.out || {
  echo "FAIL: the pair replicates, but start never reported the role/order"
  echo "      disagreement — so the failover above probably did not leave"
  echo "      pair[1] as the durable master, and this run proved nothing"
  cat $FLINT_DRILL_ROOT/flint-coldrole.out; exit 1
}
echo "  $MASTER master, $A replica, live_replicas 1"

echo "== and the data survived the re-seed"
[ "$(valkey-cli -p 7404 GET cold:200)" = "v200" ] || {
  echo "FAIL: data lost — GET cold:200 on master $B returned '$(valkey-cli -p 7404 GET cold:200)'"
  echo "      master  DBSIZE=$(valkey-cli -p 7404 DBSIZE) latest_seq=$(valkey-cli -p 7404 FLINTINFO | tr -d '\r' | sed -n 's/^latest_seq://p')"
  echo "      replica DBSIZE=$(valkey-cli -p 7403 DBSIZE) latest_seq=$(valkey-cli -p 7403 FLINTINFO | tr -d '\r' | sed -n 's/^latest_seq://p')"
  echo "      sample: $(valkey-cli -p 7404 KEYS 'cold:*' | head -3 | tr '\n' ' ')"
  exit 1
}
COUNT=$(valkey-cli -p 7404 DBSIZE)
[ "${COUNT:-0}" -ge 200 ] || { echo "FAIL: expected >=200 keys, got $COUNT"; exit 1; }
echo "  $COUNT keys on the master"

rm -f $FLINT_DRILL_ROOT/flint-coldrole.out
echo "PASS: cold start of a failed-over pair — durable roles beat inventory order, replication restored, no silent single-copy fleet"
