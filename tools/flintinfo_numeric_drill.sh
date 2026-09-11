#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0095: a FLINTINFO field that is a number in ONE state must be a number
# in EVERY state.
#
# `flint-exporter` emits only values that `parse::<f64>()`, so a field that
# renders a WORD in some state simply disappears from Prometheus in that
# state -- and an absent series is "no data", which most alerting treats as
# not-firing. Five fields did exactly that, each in the one state it exists to
# report: `acked_seq`, `seq_lag` and `lag_ms` with no live replica,
# `cert_days_remaining` with no readable certificate, `disk_free_pct` with an
# unreadable filesystem. `flint_lag_ms` is named in docs/self-hosting.md as a
# metric to watch, and it was present while replication was healthy and gone
# when it was not.
#
# A NODE IN ITS DEFAULT STATE IS THE WORST CASE, which is why this costs one
# seat and no fleet: standalone, no replica, no TLS. That single configuration
# exercises four of the five, and it is the configuration all four were
# already broken in. The defect was reachable by starting a node and looking.
#
# WHAT MAKES THIS MORE THAN A SPELL-CHECK is the exemption list. A check of
# the form "numeric unless exempt" is only as good as its exemptions, and a
# stale one silently re-permits the whole class -- so every exempt key must
# also BE PRESENT, and the count of fields examined has a floor.
set -euo pipefail
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

# TWO ports: the standalone master below, and the replica BUG-0131's phase
# attaches to it. Both must be declared here or the port-overlap preflight
# cannot see the second one.
#
# 6428/6429 rather than the old 6391: the 63xx block is exhausted (6300-6387
# and 6391-6399 are claimed, and gates.sh's own conformance stage takes
# 6388-6390 inline), so there was no adjacent pair left. These two are free in
# BOTH repos.
fleet_init $FLINT_DRILL_ROOT/flint-infonum 6428 6429
PORT=6428
RPORT=6429
fleet_guard
fleet_kill server
sleep 0.3

fail() { echo "FAIL: $*"; exit 1; }
cli() { valkey-cli -p "$PORT" "$@"; }

D="$FLINT_DRILL_ROOT/flint-infonum-data"
LOG="$FLINT_DRILL_ROOT/flint-infonum.log"
R="$FLINT_DRILL_ROOT/flint-infonum-replica"
RLOG="$FLINT_DRILL_ROOT/flint-infonum-replica.log"

# CLEAN UP ON THE FAILING PATH TOO. Every `fail` above exits immediately, so
# without this a red run leaves its seats behind -- and the NEXT run is then
# refused by fleet_guard ("this box already has Flint processes outside ...",
# all orphans, ppid 1), which reads as a fresh environment problem rather than
# as the previous failure's litter. Found while mutation-testing this file:
# two of three mutants left a seat and blocked the next mutant.
cleanup() { fleet_kill server; rm -rf "$D" "$R"; }
trap cleanup EXIT

# Fields that are STRINGS by nature, not numbers that went missing. Each is a
# name, a verdict word, a list, or a pair -- none of them is a quantity, and
# none can be plotted. Adding to this list must be a decision: it is the only
# way to make this check ignore a field.
STRINGS="role role_epoch build wal_archive_src disk_verdict mem_src \
evictable_ns evictable_ns_bytes evict collection_read_mode"

cargo build --release -q -p flint-server --features rocks || fail "build"

rm -rf "$D"
./target/release/flint-server --port "$PORT" --engine rocks --data-dir "$D" \
  >"$LOG" 2>&1 &
fleet_wait_listen "$PORT"
fleet_wait_ping "$PORT"

INFO=$(cli FLINTINFO | tr -d '\r')
[ -n "$INFO" ] || fail "FLINTINFO returned nothing"

echo "== every field is a number, or is declared a string"
# The regex is what `flint-exporter`'s `parse::<f64>()` accepts for every shape
# this product actually renders -- integers, negatives, and the one fixed-point
# field (`pool_batch_mean`). Deliberately STRICTER than f64 (no `1e5`, no
# `NaN`): erring strict flags a value the exporter would have taken, which is a
# false alarm; erring loose passes one it would have dropped, which is this bug.
SCAN=$(awk -F: -v strings=" $STRINGS " '
  NF < 2 { next }
  {
    k = $1
    v = substr($0, index($0, ":") + 1)
    if (index(strings, " " k " ")) next
    examined++
    if (v !~ /^-?[0-9]+(\.[0-9]+)?$/) bad = bad " " k "=[" v "]"
  }
  END { print examined "\t" bad }' <<< "$INFO")
EXAMINED=${SCAN%%$'\t'*}
BAD=${SCAN#*$'\t'}

# A MATCHER THAT EXAMINES NOTHING AGREES WITH EVERYTHING. If the parse stopped
# splitting lines, BAD is empty and this reports agreement -- the same output
# as success.
[ "$EXAMINED" -ge 50 ] \
  || fail "examined only $EXAMINED numeric fields; FLINTINFO has ~77, so this
  check has stopped matching and its empty complaint list means nothing"
[ -z "$BAD" ] \
  || fail "FLINTINFO field(s) rendering a non-number:$BAD
  flint-exporter emits only values that parse, so each of these is ABSENT from
  Prometheus in this state. Render a sentinel outside the field's real range
  (see UNKNOWN_NUMERIC in flint-server, or flint_tls::CERT_DAYS_UNKNOWN), or
  add the key to STRINGS above if it is genuinely not a quantity."
echo "   $EXAMINED numeric fields, all numeric"

echo "== the exemption list is not stale"
MISSING=""
for k in $STRINGS; do
  grep -q "^$k:" <<< "$INFO" || MISSING="$MISSING $k"
done
[ -z "$MISSING" ] \
  || fail "STRINGS names field(s) FLINTINFO does not emit:$MISSING
  A stale exemption is how this whole class comes back: the name survives a
  rename and then silently excuses whatever takes it."
echo "   all $(wc -w <<< "$STRINGS" | tr -d ' ') exemptions still name live fields"

echo "== the unknown states render a sentinel, not a healthy-looking zero"
field() { sed -n "s/^$1://p" <<< "$INFO"; }
for f in acked_seq seq_lag lag_ms; do
  v=$(field "$f")
  [ "$v" = "-1" ] \
    || fail "$f=$v on a node with no live replica; expected -1.
  Zero is a LEGITIMATE reading for all three -- a caught-up replica -- so an
  unknown that renders 0 is the reassuring answer and the wrong one."
done
CDR=$(field cert_days_remaining)
[ "$CDR" = "-99999" ] \
  || fail "cert_days_remaining=$CDR with no certificate configured; expected
  -99999. NOT -1, which this field reaches legitimately: a certificate that
  expired between one and two days ago reports exactly that."
echo "   acked_seq/seq_lag/lag_ms=-1, cert_days_remaining=$CDR"

# POSITIVE CONTROL. Everything above passes on a build that hardcoded every
# field to its sentinel. This node's filesystem IS readable, so the one field
# whose unknown state is NOT reachable here must carry a real reading.
DFP=$(field disk_free_pct)
case "$DFP" in
  ''|*[!0-9]*) fail "disk_free_pct=[$DFP] on a readable filesystem -- expected 0-100" ;;
esac
[ "$DFP" -ge 0 ] && [ "$DFP" -le 100 ] \
  || fail "disk_free_pct=$DFP is outside 0-100, so the sentinels above prove
  nothing: a build rendering -1 everywhere would pass every check but this one"
DUS=$(field disk_unknown_samples)
[ "$DUS" = "0" ] \
  || fail "disk_unknown_samples=$DUS -- the guard could not read the filesystem
  during this run, so disk_free_pct=$DFP is not the control it is meant to be"
echo "   control: disk_free_pct=$DFP% is a real reading (disk_unknown_samples=$DUS)"

echo "== BUG-0131: a REPLICA omits the three master-side fields entirely"
# THE ROLE DIMENSION, which the single seat above cannot reach.
#
# `acked_seq`, `seq_lag` and `lag_ms` describe this seat's OWN outbound
# replicas. A master with none is WIDOWED -- applicable and unknown -- and
# renders the sentinel, which is what the phase above pins. A replica has none
# and never will, so the same sentinel reports a permanent fault about a
# healthy seat. ADR-0018: absence means NOT APPLICABLE, a value means
# applicable, and collapsing those is the defect.
#
# THIS COSTS A SECOND SEAT, and the economy argued at the top of this file --
# "one seat and no fleet" -- is exactly what hid BUG-0131 for two releases.
# Standalone-with-no-replica is a MASTER, so the sentinel was only ever pinned
# in the role where it means a fault, and never in the role where it means the
# question does not apply. The argument was right about BUG-0095 and wrong as
# a permanent boundary for this file.
rm -rf "$R"
./target/release/flint-server --port "$RPORT" --engine rocks --data-dir "$R" \
  --replica-of 127.0.0.1:"$PORT" >"$RLOG" 2>&1 &
fleet_wait_listen "$RPORT"
fleet_wait_ping "$RPORT"
for _ in $(seq 1 60); do
  fleet_ready "$RPORT" && break
  sleep 0.2
done
fleet_ready "$RPORT" \
  || fail "the replica at $RPORT never became READY, so nothing below can be
  read as a statement about a replica's FLINTINFO. Its log: $RLOG"

RINFO=$(valkey-cli -p "$RPORT" FLINTINFO | tr -d '\r')
[ -n "$RINFO" ] || fail "the replica's FLINTINFO returned nothing"

# POSITIVE CONTROLS FIRST, because "the key is absent" is the assertion that
# passes most loudly on an empty body, a truncated reply, or a seat that is
# not a replica at all. Prove this IS a replica's FLINTINFO before reading an
# absence out of it.
grep -q '^role:replica$' <<< "$RINFO" \
  || fail "the seat at $RPORT does not report role:replica -- it reports
  '$(sed -n 's/^role://p' <<< "$RINFO")'. An absent seq_lag on a MASTER would
  be the opposite bug, so this check must not run against one."
for k in latest_seq last_applied live_replicas uptime_ms disk_free_pct; do
  grep -q "^$k:" <<< "$RINFO" \
    || fail "the replica's FLINTINFO is missing $k as well, so the body is
  truncated rather than selectively omitting the master-side fields"
done

for f in acked_seq seq_lag lag_ms; do
  ! grep -q "^$f:" <<< "$RINFO" \
    || fail "a replica still renders $f:$(sed -n "s/^$f://p" <<< "$RINFO").
  This field describes a seat's OWN outbound replicas and a replica has none,
  so a sentinel here reports a permanent widowed pair on a healthy seat --
  and \`flint_seq_lag == -1\`, the only alert that catches the master case,
  then fires on every replica in the fleet (BUG-0131)."
done
echo "   replica omits acked_seq/seq_lag/lag_ms; role/latest_seq/last_applied/live_replicas present"

# THE OTHER HALF, and it is not optional: a build that dropped the three
# fields UNCONDITIONALLY passes every assertion above. That build would
# reopen BUG-0095 -- the series vanishing in the state it exists to report --
# while closing BUG-0131, which is a strictly worse trade than either bug.
for _ in $(seq 1 60); do
  [ "$(cli FLINTINFO | tr -d '\r' | sed -n 's/^seq_lag://p')" = "0" ] && break
  sleep 0.2
done
MINFO=$(cli FLINTINFO | tr -d '\r')
mfield() { sed -n "s/^$1://p" <<< "$MINFO"; }
for f in acked_seq seq_lag lag_ms; do
  grep -q "^$f:" <<< "$MINFO" \
    || fail "the MASTER omits $f with a live replica attached. The fix dropped
  these fields everywhere instead of on the replica alone, which reopens
  BUG-0095 in the state the fields exist for."
done
[ "$(mfield live_replicas)" = "1" ] \
  || fail "the master reports live_replicas=$(mfield live_replicas), so the
  replica is not attached and seq_lag below would be the WIDOWED reading --
  which would let a build that never attaches anything pass this control"
[ "$(mfield seq_lag)" = "0" ] \
  || fail "the master reports seq_lag=$(mfield seq_lag) with a live, attached
  replica; expected 0 once drained"
case "$(mfield acked_seq)" in
  ''|*[!0-9]*) fail "the master's acked_seq=[$(mfield acked_seq)] is not a
  number with a live replica attached -- the sentinel is still being rendered
  where a real reading exists" ;;
esac
echo "   control: master with a live replica renders acked_seq=$(mfield acked_seq) seq_lag=0 lag_ms=$(mfield lag_ms) live_replicas=1"

# AND THE FOURTH STATE, which is the one the role alone gets wrong.
#
# `controlled_failover` DEMOTES the old master and then polls THAT SEAT's
# seq_lag until it reads 0 -- only the demoted seat knows how far its replica
# has drained. So for that window a seat is read_only WITH a live replica
# still attached, and the field very much applies. A first cut of this fix
# keyed the omission on the role alone; every rolling upgrade then hung for
# 30s and panicked with "replica never drained the demoted master's tail",
# which `build_read_failure` caught and this arm exists so it never has to
# again.
echo "== a DEMOTED master still reports the replica draining from it"
EP=$(cli FLINTINFO | tr -d '\r' | sed -n 's/^role_epoch://p' | tr -d '()' | cut -d, -f2)
cli FLINTDEMOTE 0 $((EP + 1)) >/dev/null
for _ in $(seq 1 40); do
  [ "$(cli FLINTINFO | tr -d '\r' | sed -n 's/^role://p')" = "replica" ] && break
  sleep 0.2
done
DINFO=$(cli FLINTINFO | tr -d '\r')
dfield() { sed -n "s/^$1://p" <<< "$DINFO"; }
[ "$(dfield role)" = "replica" ] \
  || fail "the seat did not become read-only after FLINTDEMOTE (role=$(dfield role)),
  so what follows would just be re-testing a master"
[ "$(dfield live_replicas)" = "1" ] \
  || fail "the demoted seat reports live_replicas=$(dfield live_replicas); its
  replica detached before this arm could read it, so the arm proves nothing --
  it would pass identically against the role-keyed build this exists to catch"
for f in acked_seq seq_lag lag_ms; do
  grep -q "^$f:" <<< "$DINFO" \
    || fail "a DEMOTED master with a live replica omits $f. The omission is
  keyed on the role rather than on being a replication source, and the roll's
  drain wait reads exactly this field off exactly this seat (BUG-0131)."
done
echo "   demoted seat: role=replica live_replicas=1, still renders acked_seq=$(dfield acked_seq) seq_lag=$(dfield seq_lag)"

echo "PASSED"
