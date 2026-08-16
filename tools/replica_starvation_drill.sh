#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Does the lag cap keep a replica INSIDE the master's retained WAL window?
#
# WHY THIS EXISTS. The 2026-08-16 fleet run (4 pairs, max ingest) ended with a
# master reporting `seq_lag: none` — its replica was not streaming at all —
# and the converge gate expired. The lag-cap backpressure built in #23/#59
# exists precisely so a replica cannot fall arbitrarily behind, and it did not
# prevent this. The evidence was destroyed with the fleet (#197), so this
# drill reproduces the ratio locally, in seconds, instead of at $20/hour.
#
# THE RATIO, NOT THE VOLUME. The failure needs ingest to outrun replica apply
# for longer than the WAL window holds. Both sides shrink:
#   * the window: FLINT_WAL_RETAIN_MB makes it megabytes instead of 1 GB
#   * the replica: SIGSTOP makes its apply rate exactly zero, portably
#     (no cgroups, no taskset — this runs the same on macOS and CI)
#
# WHAT IT MEASURES, AND WHAT IT DOES NOT. This spawns flint-server DIRECTLY,
# so it measures the SERVER's own bounds, not a deployed fleet's: flintctl
# defaults `--widowed-grace-ms` to 10 s on every replicated seat, and this
# drill passes no such flag. Arm B's "accepts everything" is therefore the
# raw-server bound, NOT what a fleet does — do not read it as an exposure in
# shipped clusters. (I read it that way once; see #197 for the correction.)
#
# The shipped bounds, for reference while reading the arms below:
#   live but slow replica  -> lag-hard 1000 ms sheds (server default)
#   no live replica        -> flintctl's 10 s widow grace, then sheds
# min-replicas-to-write stays 0 by product decision: this is a cache with
# async replication, and writes are not blocked on replica health.
#
# THE MECHANISM, AND WHY THE DEFAULT MATTERS. The two bounds are in different
# currencies: the lag cap is TIME (`lag_ms`, the age of the oldest unacked
# write) and the WAL window is BYTES. Nothing converts between them, so a cap
# satisfied in milliseconds says nothing about whether the replica is still
# inside the window. Worse, `effective_acked` returns None when NO replica is
# live, and lag_ms of None means "no cap" — so once a stalled replica is
# declared dead the backpressure DISAPPEARS and the master accelerates,
# guaranteeing the gap it needs to close can never close. That is a positive
# feedback loop, and it is the shape of what the fleet showed.
#
# ARM A (guard on) must shed. ARM B (guard off) documents the exposure: a
# master with no live replica accepts writes without limit. B is asserted too
# — if it ever starts shedding, the default changed and this drill should say
# so rather than silently passing for a new reason.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-starve" 6455 6456
fleet_guard

MPORT=6455
RPORT=6456
MDIR="$FLINT_DRILL_ROOT/flint-starve/m"
RDIR="$FLINT_DRILL_ROOT/flint-starve/r"
BIN="./target/release/flint-server"
# ~8 MB against a 4 MB window. The BYTE volume is what has to outrun the
# window; the round-trip count is pure cost, and this drill now runs two arms,
# so the same 8 MB goes in 400 larger writes rather than 800 small ones.
#
# The fill is a repeated byte, which #169 would normally forbid — but #169 is
# about SST bytes, and the bound here is the WAL, which RocksDB does not
# compress. A compressible value still occupies its full length in the window.
WRITES=400
VSIZE=20000

cleanup() {
  # CONT first: a process left stopped cannot be killed by SIGTERM, and a
  # drill that leaks a stopped seat poisons the next run's port check.
  [ -n "${RPID:-}" ] && kill -CONT "$RPID" 2>/dev/null
  fleet_kill server
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
fleet_warm "$BIN"

# A 4 MB window: small enough that a few MB of writes recycles past a stopped
# replica in seconds, large enough that normal streaming never trips it.
export FLINT_WAL_RETAIN_MB=4
export FLINT_WAL_RETAIN_SECS=3600

# BOTH ARMS RUN, EVERY TIME.
#
# This was one arm selected by a MINREP env var, defaulted to 1 — so the gate
# only ever ran arm A, while the header claimed both were asserted. Arm B is
# the one that describes the SHIPPED cache default (min-replicas-to-write 0:
# writes are never blocked on replica health), so leaving it unrun meant the
# gate said nothing about the configuration customers actually get.
run_arm() {
  local MINREP="$1" ACCEPTED=0 THROTTLED=0 CAUGHT=0 RESUMED=0 ESCALATED i OUT LAGMS LIVE LAG C
  RPID=""
  fleet_kill server
  rm -rf "$MDIR" "$RDIR"; mkdir -p "$MDIR" "$RDIR"

  # lag-hard 2000ms: the master must shed writes (-THROTTLED) once the freshest
  # replica is >2 s behind. With the replica stopped that threshold is crossed
  # almost immediately, so any sustained acceptance past it is the cap failing.
  echo "== ARM $([ "$MINREP" = 0 ] && echo B || echo A): master :$MPORT (WAL window ${FLINT_WAL_RETAIN_MB}MB, lag-hard 2000ms, min-replicas $MINREP), replica :$RPORT"
  "$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" \
    --lag-soft-ms 1000 --lag-hard-ms 2000 --min-replicas-to-write "$MINREP" >"$MDIR/log" 2>&1 &
  fleet_wait_listen "$MPORT"
  "$BIN" --port "$RPORT" --engine rocks --data-dir "$RDIR" \
    --replica-of "127.0.0.1:$MPORT" >"$RDIR/log" 2>&1 &
  fleet_wait_listen "$RPORT"
  RPID=$(pgrep -f "flint-server --port $RPORT" | head -1)
  [ -n "$RPID" ] || { echo "FAIL: no replica pid"; exit 1; }

  C="valkey-cli -p $MPORT"
  # Seed and let the replica catch up, so the starting state is provably
  # healthy (a drill that begins from an unknown state cannot attribute what
  # follows).
  for i in $(seq 1 200); do $C SET "seed:$i" "v$i" >/dev/null; done
  for _ in $(seq 1 50); do
    LAG=$($C FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^seq_lag://p')
    [ "$LAG" = "0" ] && { CAUGHT=1; break; }
    sleep 0.2
  done
  [ "$CAUGHT" = 1 ] || { echo "FAIL: replica never caught up before the test began"; exit 1; }
  echo "  replica streaming, seq_lag 0 — healthy start"

  echo "  1. STOP the replica: its apply rate is now exactly zero"
  kill -STOP "$RPID"

  echo "  2. write hard, and watch whether the master ever stops accepting"
  for i in $(seq 1 "$WRITES"); do
    OUT=$($C SET "load:$i" "$(head -c "$VSIZE" /dev/zero | tr '\0' 'x')" 2>&1 | tr -d '\r')
    case "$OUT" in
      OK) ACCEPTED=$((ACCEPTED+1)) ;;
      *THROTTLED*|*QUOTA*) THROTTLED=$((THROTTLED+1)) ;;
    esac
  done
  LAGMS=$($C FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^lag_ms://p')
  LIVE=$($C FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')
  echo "     accepted=$ACCEPTED throttled=$THROTTLED (master reports lag_ms=$LAGMS live_replicas=$LIVE)"

  if [ "$MINREP" = 0 ]; then
    # ARM B: the exposure, asserted so a default change cannot pass silently.
    [ "$THROTTLED" = 0 ] || {
      echo "FAIL: with min-replicas-to-write 0 the master SHED ($THROTTLED) — the"
      echo "      default changed. That is likely an improvement, but this drill"
      echo "      and the fleet inventories encode the old behaviour; update both."
      exit 1
    }
    echo "     arm B confirmed: guard OFF accepts all $ACCEPTED writes with no live replica"
  else
    # ARM A: a cap that holds cannot let every write past a replica that is
    # 100% stalled and far beyond lag-hard.
    [ "$THROTTLED" != 0 ] || {
      echo "FAIL: the master accepted ALL $ACCEPTED writes while its only replica was"
      echo "      frozen and past lag-hard (lag_ms=$LAGMS, live_replicas=$LIVE)."
      echo "      The lag cap is measured in TIME and the WAL window in BYTES, and"
      echo "      when the replica stops counting as live the cap evaluates to"
      echo "      'no cap' — so backpressure disappears exactly when it is needed"
      echo "      and the master races the replica out of its own WAL window (#197)."
      exit 1
    }
    echo "     the cap engaged ($THROTTLED shed)"
  fi

  echo "  3. resume the replica: can it still tail, or must it full-sync?"
  kill -CONT "$RPID"
  for _ in $(seq 1 60); do
    LAG=$($C FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^seq_lag://p')
    [ "$LAG" = "0" ] && { RESUMED=1; break; }
    sleep 0.5
  done
  if grep -q "full sync\|FULLSYNC\|WALGAP" "$RDIR/log" 2>/dev/null; then ESCALATED=1; else ESCALATED=0; fi
  # Recovery is non-negotiable even when the cap is off by design: escalation
  # to full sync is a legitimate answer (#106) — losing the replica is not.
  [ "$RESUMED" = 1 ] || { echo "FAIL: replica never reconverged after CONT (escalated=$ESCALATED)"; exit 1; }
  echo "     replica reconverged (escalated_to_full_sync=$ESCALATED)"
}

run_arm 1
run_arm 0
echo "PASS: replica starvation drill — with the guard on, a frozen replica drives"
echo "      the master into backpressure rather than out of its own WAL window;"
echo "      with the cache default (guard off) the master accepts without limit,"
echo "      as designed; and the replica reconverges either way"
