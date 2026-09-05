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

fleet_init $FLINT_DRILL_ROOT/flint-infonum 6391
PORT=6391
fleet_guard
fleet_kill server
sleep 0.3

fail() { echo "FAIL: $*"; exit 1; }
cli() { valkey-cli -p "$PORT" "$@"; }

D="$FLINT_DRILL_ROOT/flint-infonum-data"
LOG="$FLINT_DRILL_ROOT/flint-infonum.log"

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

fleet_kill server
rm -rf "$D"
echo "PASSED"
