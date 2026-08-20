#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The release gate, as a script instead of a checklist.
#
# docs/release-checklist.md has always LISTED these steps. That is not the
# same as having them: a list is retyped, and what gets retyped gets dropped.
# Every claim in this project that "the drills pass" has meant a shell
# one-liner reconstructed from memory, whose failures were summarised to one
# line and whose output was then thrown away — so a flake and a real bug
# looked identical, and telling them apart meant running everything again.
#
# So: the list lives here, the output is KEPT, and the exit status is the
# answer. `tools/gates.sh` before tagging; the checklist stays as the
# explanation of why each step exists.
#
# Usage: tools/gates.sh [stage ...]     (default: all)
#   check        fmt + clippy + tests, both feature configs
#   conformance  the compatibility oracle vs valkey, flint mem, flint rocks
#   drills       the core drills (the CORE list below is the count)
#   chaos        the two randomized chaos drills
#
# Logs land in $FLINT_GATE_LOGS (default $FLINT_DRILL_ROOT/flint-gates), one
# DIRECTORY PER RUN — <utc-stamp>-<short-sha>[-dirty]/<step>.log — kept whether
# the step passed or failed, and kept across the NEXT run. The newest 20 runs
# are retained; $FLINT_GATE_LOGS/latest points at the most recent.
#
# "Kept whether it passed or failed" was true within a run and false across
# runs until 2026-08-18: this was a flat directory the next run opened with
# `rm -rf`. See docs/bugs/0021 — telling a flake from a real bug IS a cross-run
# comparison, so the retention evaporated at exactly the moment it was needed.
#
# FLINT_DRILL_ROOT (default /tmp) is where every drill's scratch, seat logs
# and these gate logs live, and it is the volume the disk guard measures.
# Put drill I/O on another disk with:
#
#   FLINT_DRILL_ROOT=/Volumes/YourSSD/drillscratch tools/gates.sh
set -u

# STAGE ARGUMENTS, validated before anything else runs.
#
# This was one line — `STAGES="${*:-check conformance drills chaos}"`, with a
# `want()` that substring-matched against it — so an argument that was not a
# stage name matched nothing at all: every stage was skipped, FAILED stayed
# empty, and the script printed "GATES PASSED" and exited 0. `gates.sh --help`
# and `gates.sh drill` (the stage is `drills`) were both a green run of
# nothing, in under a second, and the singular/plural slip is the typo made at
# 1am. The release checklist names this script as the authority, so the answer
# it gave for an unrecognised argument was the most expensive one available.
#
# That is the failure this whole file was written against — a check that
# verifies nothing and reads as green — arriving through the argument channel.
# Found 2026-08-11 while adding bloom to CORE;
# docs/bugs/0009-unknown-stage-passes-the-gate.md is the write-up.
#
# So: fail closed on any unrecognised stage, and serve --help deliberately
# rather than through the same hole.
#
# Parsed BEFORE the `cd` (so $0 still resolves against the caller's directory)
# and before the log directory is cleared — `--help` should not delete the
# previous run's logs on its way to printing a usage block.
ALL_STAGES="check conformance drills chaos"
want() { case " ${STAGES} " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# The header comment IS the usage text, printed out of the file itself. One
# copy, for the same reason the CORE list below has no second home.
usage() { awk '/^# Usage:/{u=1} u&&!/^#/{exit} u{sub(/^# ?/,""); print}' "$0"; }

for arg in "$@"; do
  case "$arg" in -h|--help) usage; exit 0 ;; esac
done
for arg in "$@"; do
  case " $ALL_STAGES " in
    *" $arg "*) ;;
    *)
      { echo "gates.sh: unrecognised stage: $arg"
        echo
        usage
        echo
        # Deliberately NOT the words of the verdict line: anything grepping
        # this output for the pass string must not match an explanation of
        # it. The same collision in the other direction (`^error` matching a
        # drill's "errors: 0") is recorded below.
        echo "Refusing to run. An unrecognised stage used to select no stage"
        echo "at all and still report the gate as green, which is worse than"
        echo "an error."
      } >&2
      exit 2 ;;
  esac
done
STAGES="${*:-$ALL_STAGES}"

cd "$(dirname "$0")/.."

# ONE DIRECTORY PER RUN, not one file per step (docs/bugs/0021).
#
# This used to be a flat directory that the next run opened with `rm -rf`. So
# the retention this file was built to provide — "keep the output, a flake and
# a real bug look identical without it" — lasted exactly until the second run,
# which is the first moment the question can be asked. Worse than an
# overwrite: `rm -rf` also took the logs of steps the new run never reached,
# so re-running a subset erased evidence the subset had nothing to do with.
# It cost the failing log for BUG-0024 while that bug was being investigated.
#
# The run id carries the TREE as well as the time, because a log could not
# previously say which commit produced it, and two branches differing in one
# file was already enough to make identical line numbers disagree. A dirty
# tree says so — a bare sha for a tree with uncommitted changes names a
# commit that was never the thing under test.
_gate_tree_id() {
  local sha
  sha="$(git rev-parse --short HEAD 2>/dev/null)" || { echo "nogit"; return; }
  if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    echo "${sha}-dirty"
  else
    echo "$sha"
  fi
}

# Keep the newest N run directories. The id starts with a UTC timestamp, so a
# reverse sort BY NAME is a reverse sort by time and needs no stat(1). Bounded
# retention does not reintroduce the bug: the point is to survive the NEXT
# run, not to keep everything forever.
_gate_prune_runs() {
  local keep="$1" d n=0
  for d in $(ls -1 "$LOGS_ROOT" 2>/dev/null | sort -r); do
    [ -d "$LOGS_ROOT/$d" ] || continue
    [ "$d" = "latest" ] && continue
    n=$((n + 1))
    [ "$n" -le "$keep" ] && continue
    rm -rf "${LOGS_ROOT:?}/$d"
  done
}

# Refusing a drill outright throws a 25-minute run away over a collision that
# would have cleared on its own, so drills QUEUE here rather than fail:
# fleet_guard waits this long for a sibling's build to finish. Standalone
# drills keep the old behaviour (no wait) — the gate's choice, not the
# library's default. See BUG-0036.
#
# THE BUDGET IS PER DRILL, WHICH IS WHY IT IS SMALL. There are ~117 steps, so
# a generous per-drill budget multiplies: 900s each would let one long sibling
# sweep stall a gate for hours while every step waits its turn and times out
# in sequence. Two minutes absorbs the gap between cargo test binaries — the
# thing that actually interrupts a run — and a sweep longer than that is worth
# a human deciding to wait for, not 117 drills each discovering it alone.
export FLINT_DRILL_WAIT="${FLINT_DRILL_WAIT:-120}"

LOGS_ROOT="${FLINT_GATE_LOGS:-${FLINT_DRILL_ROOT:-/tmp}/flint-gates}"
mkdir -p "$LOGS_ROOT"
_gate_run_base="$(date -u +%Y%m%dT%H%M%SZ)-$(_gate_tree_id)"
GATE_RUN_ID="$_gate_run_base"
_gate_n=1
# Two runs inside one second collide on the id — gates_drill.sh does exactly
# that. Suffix rather than let the second run write into the first's directory.
while [ -e "$LOGS_ROOT/$GATE_RUN_ID" ]; do
  GATE_RUN_ID="$_gate_run_base.$_gate_n"; _gate_n=$((_gate_n + 1))
done
LOGS="$LOGS_ROOT/$GATE_RUN_ID"
mkdir -p "$LOGS"
ln -sfn "$GATE_RUN_ID" "$LOGS_ROOT/latest" 2>/dev/null || true
_gate_prune_runs 20

# Section 3 of the checklist, in its order. Adding a drill here is what puts
# it in the gate — there is no second list.
#
# AUDITED 2026-08-10. Until then this list held 48 of the 82 drills in
# tools/, and nothing recorded why the other 34 were absent — so "the gate is
# green" was a quieter claim than it sounded. All 34 were run: 31 passed, and
# they are now here. Two of them had been broken for weeks with nobody
# looking: lease_drill asserted a read behaviour the R1 stale-read fence
# changed on 2026-07-17 (fixed, 432a5d5), and the reply-assertion sweep found
# discarded writes throughout. A drill outside the gate rots, and rots
# silently.
CORE="restart repl failover proxy slot_migrate slot_map rebalance_execute
      bloom ns_escape coproc_cred coproc_channel family_route family_route_cp coproc_forward coproc_budget coproc_exempt coproc_vec coproc_vec_tls coproc_vec_rebuild
      tenant_quota token_rotation cert_reload_fleet controlplane_ha
      decommission config_file federation_plumbing disk_pressure ctl_error
      client_compat proxy_registry reseed lag_cap widowed_grace controller
      promote_notice fleet_guard ctl_cpha upgrade anti_affinity attached_chaos
      async_flag async_writes txn_failure backup restore_ns backup_schedule
      backup_seat gc_sweep keystat start_guard seat_log cold_start_roles
      build_stamp config_drift tenant_status proxy_conformance edge_roll
      cpha_roll admin_gated_proxy edge_ca_trust chaos_edge_tls
      cert_rotate control_tls controller_ha controller_managed controller_slow_master controller_stall
      controller_multipair controlplane cp_publish failover_bystander failover_churn gates internal_mtls json lease
      fanout_timeout m3_exit migrate_slots min_replicas node_tls proxy_backpressure
      proxy_cache proxy_tls replica_reads replica_stale_fence rw_isolation
      scan slot_cutover slot_cutover_recovery slot_moved snapshot_restore
      tenant tenant_rebalance tenant_remove token_hash
      write_deadline fullsync_rate edge_reroute rewind_rejoin"
CHAOS="chaos proxy_chaos chaos_unreadable hotkey_chaos"

# DELIBERATELY OUT, with the reason. An absence with no reason beside it is
# indistinguishable from an oversight, which is what the audit above had to
# spend an evening establishing.
#
#   backup_s3     needs a real bucket: exits 0 with "SKIP: set FLINT_S3_BUCKET".
#                 FLINT_GATE_STRICT=1 turns a SKIP into a FAIL, so adding it
#                 here would make the strict gate unrunnable without AWS.
#                 Run it by hand against a scratch bucket when touching the
#                 backup path.
#   fullsync_cap  FAILS on a fast host: "no replica was throttled — herd
#                 didn't overlap (raise the dataset size)". The drill cannot
#                 reliably create the condition it asserts on. Same family as
#                 the RPO/THROTTLED work — pressure, not scale. Fix before
#                 adding.
#   stop_sweep    FAILS in setup: "fleet B did not start". It declares eight
#                 ports across two fleets (6317-6321, 7820, 7879, 7889), so a
#                 collision is the first thing to check. Fix before adding.

# FLINT_GATE_STRICT=1 turns a SKIPPED drill into a FAILED one.
#
# Several drills exit 0 with a "SKIP:" line when a dependency is missing —
# client_compat without redis-py or node, disk_pressure without mkfs.ext4 or
# passwordless sudo. On a developer's laptop that is right: a macOS box has
# no mkfs.ext4 and should not fail the suite over it.
#
# In CI it is the opposite of right. CI is where "the gate is green" gets
# believed and merges get unblocked, and a drill that skipped is
# indistinguishable there from a drill that passed. A forgotten `pip install`
# would quietly delete client-compatibility coverage from every future run,
# and nothing would say so.
#
# So the environment that trusts the result is the environment that must
# refuse a skip.
FAILED=""
FAILED_LOGS=""
# Steps actually executed. A gate that ran nothing must not report a pass, and
# the argument validation above is only the hole we know about — this counts
# the work instead of trusting the dispatch, so a future refactor cannot
# reintroduce a green run of nothing by some other route.
RAN_STEPS=0
# Seats a drill left behind. The gate starts from a box with no Flint on it
# (assert_clean_box below), and every drill is supposed to clean up after
# itself, so anything alive after drill N came FROM drill N.
#
# Why this exists: on 2026-08-10 a single leaked seat turned into 24 reported
# failures. controlplane_drill passed, leaked its 6740 node — its cleanup
# matched `--port 673`, which covers 6730 and not 6740 — and every drill
# after it hit fleet_guard's refusal to run on a box holding Flint processes
# it does not own. fleet_guard was right; the gate's OUTPUT was the problem.
# It named 24 innocent drills and not the one that did it, which is the most
# expensive way for a suite to be wrong.
#
# So: attribute the leak to the drill that caused it, and clear it, so one
# leak costs one accurate failure rather than a cascade of false ones.
# Flint daemons running anywhere on this box. DELIBERATELY UNSCOPED — the
# scoping happens in _leaked_seats below, which needs to see everything in
# order to tell ours from someone else's.
_all_flint_seats() { pgrep -f 'target/release/flint-(server|proxy|controlplane|controller|agent)' 2>/dev/null; }

# Of those, the ones the NAMED DRILL could plausibly own (docs/bugs/0027).
#
# This used to be the unscoped list, and the caller kill -9'd it. So any Flint
# process on the box was attributed to whichever drill had just finished and
# destroyed — including another session's fleet. Measured on 2026-08-19: a
# peer's 4-seat TLS fleet on 7191-7194 was killed four times in one run, blamed
# on restart, ctl_error, proxy_tls and coproc_vec_rebuild, none of which had
# anything to do with it. fleet_guard had already refused those drills for the
# right reason, one layer down, and then this killed what it had declined to
# touch. tools/lib/fleet.sh exists because of exactly that harm — "anyone with
# a live Flint fleet on the same machine loses it" — and the gate reintroduced
# it above the abstraction written to prevent it.
#
# Ownership is decided the same way fleet.sh decides it: the drill's scope
# directory OR one of the ports it declares. Both are needed — a proxy started
# `--port 6666 --pairs ...` and a controller started `--pairs ... --id PX`
# carry no path at all, so a directory-only match would miss them.
_leaked_seats() {  # <drill-name>
  local drill="tools/${1}_drill.sh" ports pid args
  ports=$(grep -h '^fleet_init' "$drill" 2>/dev/null \
    | awk '{for (i = 3; i <= NF; i++) print $i}' | grep -E '^[0-9]+$' \
    | tr '\n' '|' | sed 's/|$//')
  for pid in $(_all_flint_seats); do
    args=$(ps -o args= -p "$pid" 2>/dev/null) || continue
    case "$args" in
      *"${FLINT_DRILL_ROOT:-/tmp}"*) echo "$pid"; continue ;;
    esac
    [ -n "$ports" ] && echo "$args" | grep -qE "(^|[^0-9])($ports)([^0-9]|$)" && echo "$pid"
  done
}

# Flint daemons this drill cannot be shown to own. Reported, never killed.
#
# NOT "someone else's" — that is more than the check establishes. All that is
# known is that the argv carries neither this drill's scope directory nor a port
# it declares. Another session's fleet looks like that, and so does a drill that
# leaked a seat carrying neither marker. Reporting it as foreign would be the
# same confident attribution this whole check was just fixed for, pointed the
# other way.
#
# The sweep behind it stays GLOBAL on purpose (a peer's point). Scoping the KILL
# is what stops the damage; scoping the SEARCH as well would mean an absent
# report could equally mean "the box is clean" or "the check narrowed until it
# could not see". Global search, scoped kill: no report means no unaccounted
# Flint process anywhere on the machine.
_foreign_seats() {  # <drill-name>
  local mine; mine=$(_leaked_seats "$1" | tr '\n' ' ')
  local pid
  for pid in $(_all_flint_seats); do
    case " $mine " in *" $pid "*) ;; *) echo "$pid" ;; esac
  done
}

step() {  # step <name> <log-suffix> <command...>
  local name="$1" log="$LOGS/$2.log"; shift 2
  local start; start=$(date +%s)
  RAN_STEPS=$((RAN_STEPS + 1))
  if "$@" >"$log" 2>&1; then
    # `SKIP:` and `SKIP (` are the two forms a drill uses to say "I did not
    # test this because a dependency was missing". Deliberately NOT plain
    # /SKIP/: attached_chaos prints "data plane SKIPPED (pass --probe ...)",
    # a known and documented limitation of that drill rather than a broken
    # environment, and failing every pull request over a pre-existing gap
    # would teach people to ignore this gate — the one outcome worse than
    # not having it.
    if [ -n "${FLINT_GATE_STRICT:-}" ] && grep -qE 'SKIP[: (]' "$log"; then
      printf 'FAIL  %-22s (%ss)  %s\n' "$name" "$(( $(date +%s) - start ))" "$log"
      grep -m2 'SKIP' "$log" | sed 's/^/        /'
      echo "        skipped under FLINT_GATE_STRICT: install the dependency"
      echo "        or drop the drill from CORE deliberately, not by accident."
      FAILED="$FAILED $name(skipped)"
      return
    fi
    printf 'PASS  %-22s (%ss)\n' "$name" "$(( $(date +%s) - start ))"
  else
    printf 'FAIL  %-22s (%ss)  %s\n' "$name" "$(( $(date +%s) - start ))" "$log"
    # The drill's OWN verdict first, then the looser patterns.
    #
    # This was one `grep -m1 -E '^(FAIL|error|REFUSING)'`, and `^error`
    # matched repl_drill's benign progress line "errors: 0, replies:
    # 50500". The gate therefore reported a passing statistic as the
    # diagnosis of a failure, and the actual FAIL line -- further down the
    # same file -- was never shown. Two runs were spent reading "errors: 0"
    # and wondering what was wrong with zero errors.
    #
    # A summary that can print a HEALTHY line as the reason for a failure
    # is worse than printing nothing, because it is read as the answer.
    # `^FAIL` is the drills' own contract (every one of them exits through
    # a `FAIL:` line); the rest are fallbacks for a drill that died before
    # reaching its own verdict.
    { grep -m1 -E '^FAIL' "$log" \
        || grep -m1 -E '^(REFUSING|thread .* panicked|error(\[|:))' "$log" \
        || tail -3 "$log"; } | sed 's/^/        /'
    FAILED="$FAILED $name"
    FAILED_LOGS="$FAILED_LOGS $log"
  fi

  # Leak check, on the PASS path as much as the FAIL path — the leak that
  # cost 24 false failures came from a drill that passed.
  #
  # DRILLS ONLY ($LEAKCHECK, set around the CORE/CHAOS loops). Not every step
  # is supposed to leave a clean box: the conformance stage starts its
  # servers in one step and RUNS AGAINST THEM in the four that follow, so a
  # blanket check reads that as a leak and kills the oracle the rest of the
  # stage needs. It did exactly that on 2026-08-10 — "conformance oracle
  # left 2 Flint processes running", then all four conformance steps failed
  # at 0s, a regression introduced by the fix for the previous cascade.
  #
  # "Every step must leave nothing behind" was an assumption, not a contract,
  # and it was wrong for the stage that happens to run first.
  local leaked; leaked=$([ -n "${LEAKCHECK:-}" ] && _leaked_seats "$name")
  local foreign; foreign=$([ -n "${LEAKCHECK:-}" ] && _foreign_seats "$name")
  if [ -n "$foreign" ]; then
    # NOT a failure and NOT killed. Someone else's fleet on a shared box is a
    # fact about the box, not a defect in this drill — and it is exactly what
    # this check used to destroy.
    echo "      note: $(echo "$foreign" | wc -l | tr -d ' ') Flint process(es) on this box are not attributable to $name"
    echo "            (no match on its scope dir or declared ports) — left running, not a failure."
    echo "            Another session's fleet looks like this; so would a leak carrying neither marker."
  fi
  if [ -n "$leaked" ]; then
    echo "      LEAKED: $name left $(echo "$leaked" | wc -l | tr -d ' ') Flint process(es) running"
    ps -o pid=,args= -p $(echo "$leaked" | tr '\n' ' ') 2>/dev/null \
      | sed 's/^/        /' | cut -c1-110
    echo "        Its cleanup does not cover every seat it starts. Prefer"
    echo "        fleet_kill (scoped to the drill's fleet_init ports) over a"
    echo "        hand-written pkill pattern, which goes stale silently."
    kill -9 $(echo "$leaked" | tr '\n' ' ') 2>/dev/null
    case " $FAILED " in
      *" $name "*) ;;
      *) FAILED="$FAILED $name(leaked)"; FAILED_LOGS="$FAILED_LOGS $log" ;;
    esac
  fi
}

# Dependency licences (deny.toml). Flint ships as binaries under a
# proprietary source-available licence, so a copyleft dependency would be a
# distribution problem, not a style one — and the day it arrives is the day
# nobody is looking at the dependency tree. The allow-list only protects
# anything if something runs it, which is what this step is for.
#
# Skips when cargo-deny is absent, like the other optional-dependency
# drills; FLINT_GATE_STRICT=1 promotes that skip to a failure, so CI and the
# gate box cannot quietly lose the check.
licence_check() {
  if ! command -v cargo-deny >/dev/null 2>&1; then
    echo "SKIP: cargo-deny not installed (cargo install --locked cargo-deny)"
    return 0
  fi
  cargo deny check licenses
}

# FREE DISK, checked before anything runs and named as the host problem it is.
#
# Every node starts a disk guard that sheds writes below `min-free 10% or 2 GiB`
# (#95). That guard is doing its job, but it does not distinguish "this cluster
# filled its disk" from "the laptop was already full" — so on a host below the
# threshold, drills fail as an application symptom. On 2026-08-09 the chaos
# drill died with
#
#     iter 12: writer saw no ack within 10000ms x2 of the kill
#
# and the cause was 2 MB: free space was 24,508,858,368 against a threshold of
# 24,510,719,590, and the node log said `disk guard: Ok -> Shed`. Eleven
# iterations had passed. Nothing in the gate's output pointed at the disk, so
# it read exactly like a replication regression on the eve of a release.
#
# A red gate has to name its own cause, or the next person spends an hour
# proving the product is fine. Refuse up front instead, and say the number.
# MEASURE THE VOLUME THE DRILLS ACTUALLY WRITE TO, which is the scratch root
# — not `.`. Those are the same filesystem in a plain checkout and DIFFERENT
# the moment either target/ or the scratch root is moved to another volume, a
# split this repo already has (target/ is commonly a symlink to an external
# SSD). Measuring `.` there reports the volume holding the source while every
# byte lands somewhere else: the guard would refuse on a full source disk the
# drills never touch, and — worse — pass while the volume they DO fill is
# nearly out. A guard that measures the wrong thing is not a weaker guard, it
# is a guard pointed away from the hazard.
_GATE_DISK_TARGET="${FLINT_DRILL_ROOT:-/tmp}"
mkdir -p "$_GATE_DISK_TARGET" 2>/dev/null || true
_gate_free_pct() { df -k "$_GATE_DISK_TARGET" | awk 'NR==2 {printf "%d", $4/$2*100}'; }
_GATE_FREE_PCT=$(_gate_free_pct)
if [ "${_GATE_FREE_PCT:-100}" -lt 12 ]; then
  echo "REFUSING TO RUN: only ${_GATE_FREE_PCT}% of this filesystem is free."
  echo
  echo "  Nodes shed writes below 10% (the disk guard), so drills would fail as"
  echo "  replication or ack timeouts with no mention of the disk. 12% is the"
  echo "  floor because a chaos run needs headroom of its own on top of the 10%."
  echo
  df -h "$_GATE_DISK_TARGET" | sed 's/^/  /'
  echo
  echo "  Free space and re-run, or set FLINT_GATE_SKIP_DISK=1 to override"
  echo "  (and read every failure with this in mind)."
  [ -n "${FLINT_GATE_SKIP_DISK:-}" ] || exit 2
  echo "  FLINT_GATE_SKIP_DISK=1 set — continuing anyway."
fi

# No drill may claim the DEFAULT cluster ports.
#
# fleet.sh decides ownership by scope directory OR port: a process on one of
# the drill's declared ports counts as the drill's own, so fleet_guard lets it
# through and fleet_kill sends it -9. That is right for a leaked seat and
# catastrophic for the real thing, because 7001/7002/7379/7500 is exactly what
# render-inventory.sh and the playground deploy. Four public drills and one
# fleet drill claimed that block, which meant running the suite on a machine
# with a default-port cluster would kill it — silently, since the guard reads
# it as ours. That is the harm the guard exists to prevent, arriving through
# the port channel.
#
# Kept here rather than in fleet.sh because it is a property of the SET of
# drills, which no single drill can check about itself.
assert_no_default_ports() {
  local hits
  hits=$(grep -nE '^fleet_init .*[^0-9](7001|7002|7379|7500)([^0-9]|$)' \
    tools/*_drill.sh 2>/dev/null || true)
  [ -z "$hits" ] && return 0
  echo "FAIL  drills claim the default cluster ports (7001/7002/7379/7500):"
  echo "$hits" | sed 's/^/        /'
  echo "        fleet_kill would -9 a real cluster on those ports and"
  echo "        fleet_guard would not object. Move the drill to a free block."
  FAILED="$FAILED default-ports"
}

# No two drills may claim the SAME port either.
#
# Ownership in fleet.sh is by scope directory OR port, so a shared port makes
# one drill adopt the other's leftovers: fleet_guard waves them through and
# fleet_kill sends them -9. In a serial suite the usual symptom is milder and
# more confusing — a seat from the previous drill that has not finished dying
# still holds the port, and the next drill's control plane times out waiting
# to bind.
#
# 23 collisions across 14 drill pairs were sitting here when this guard was
# written; the same defect in the fleet repo produced two spurious failures
# in two consecutive runs, each drill green in isolation, before anyone
# suspected the port map. A gate whose red means "unlucky ordering" is a gate
# people re-run instead of read.
#
# It also catches its author: the fleet copy of this check fired on a block
# picked by eyeballing adjacent numbers, within an hour of being added. That
# is the argument for a fail-closed list over careful intentions.
#
# There is no exemption any more, and that is the point. flint-chaos used to
# hardcode 6460/6470/7690, so drills driving it bound ports they could not
# declare — invisible to this check, which is how tenant_quota's control
# plane came to share 7690 with the chaos proxy unnoticed. The ports are a
# --port-base now, each chaos drill has its own block, and every port any
# drill binds is a port some drill declares.
assert_no_port_overlap() {
  local dupes p
  dupes=$(grep -h '^fleet_init' tools/*_drill.sh 2>/dev/null \
    | awk '{for (i=3; i<=NF; i++) print $i}' | sort -n | uniq -d)
  [ -z "$dupes" ] && return 0
  echo "FAIL  two or more drills declare the same port(s):"
  for p in $dupes; do
    echo "        $p: $(grep -l "^fleet_init.*[^0-9]$p\([^0-9]\|\$\)" tools/*_drill.sh | tr '\n' ' ')"
  done
  echo "        fleet_guard reads the other drill's seats as this drill's own."
  echo "        Give each drill a disjoint block."
  FAILED="$FAILED port-overlap"
}

# Ports are only HALF of ownership. _fleet_ours takes a pid if the ps line
# contains the scope dir OR a declared port, so two drills sharing a scope dir
# can select each other's seats even with disjoint port blocks — and each
# rm -rf's that dir at start. assert_no_port_overlap checks the ports and was
# read as covering ownership; it never looked at $2. coproc_cred and
# coproc_family both claimed $FLINT_DRILL_ROOT/flint-coproc for months,
# benign only because the gate runs drills sequentially. Found by a peer
# session reading this file for a different reason, which is the second time
# today the check that was missing was the one nobody thought to write.
assert_no_scope_overlap() {
  local dupes d
  dupes=$(grep -h '^fleet_init' tools/*_drill.sh 2>/dev/null \
    | awk '{print $2}' | sort | uniq -d)
  [ -z "$dupes" ] && return 0
  echo "FAIL  two or more drills declare the same scope dir:"
  for d in $dupes; do
    echo "        $d: $(grep -l "^fleet_init $(printf '%s' "$d" | sed 's/[][\.*^$/]/\\&/g') " tools/*_drill.sh | tr '\n' ' ')"
  done
  echo "        scope is the other half of _fleet_ours's ownership test, so each"
  echo "        drill can select the other's seats and rm -rf its state."
  echo "        Give each drill its own directory."
  FAILED="$FAILED scope-overlap"
}

# AN ARGUMENT LIST HAS TWO ENDS. flint-server has no parser: it scans
# env::args() once per flag it cares about, so ANY other token is ignored in
# silence. That is how `--help` came to start a node on the default port and
# `--version` hung a box with a 30-minute TTL (docs/bugs/0034).
#
# The tempting fix is to refuse unrecognised arguments. It was written, and it
# hung the gate twice: slot_map and restore_ns each start a seat with
# `--advertise`, a PROXY flag flint-server has never read, so a silent no-op
# became exit 2 and a drill waited forever for a seat that would never bind.
# The accepted set had been enumerated from the CALLEE, correctly and
# exhaustively — and that proves nothing about what the CALLERS send.
#
# So this asserts the property instead of assuming it: every flag a drill hands
# flint-server must be one flint-server reads. Keep that true and rejection
# becomes safe BY CONSTRUCTION rather than by an enumeration someone has to
# re-run whenever a flag is added.
#
# ON THE ANCHOR, because the discarded method is the part worth copying:
# matching the literal `flint-server` finds nine false positives from
# `cargo build --bin flint-server` lines and MISSES the real ones, because
# drills spawn through `$B`. `--data-dir` is the anchor that works — no cargo
# line and no AWS call carries one — and it is paired with the binary variable
# so a flint-backup line cannot drift in later.
assert_server_flags_are_read() {
  local read_flags passed bad f
  # CALLEE: arg("--x") plus the flags compared directly before the listener.
  read_flags=$( {
      grep -oE 'arg\("--[a-z0-9-]+"\)' crates/flint-server/src/main.rs \
        | sed 's/arg("//; s/")//'
      grep -oE 'a == "--?[a-zA-Z0-9-]+"' crates/flint-server/src/main.rs \
        | sed 's/a == "//; s/"//'
    } | sort -u )
  if [ -z "$read_flags" ]; then
    echo "FAIL  could not read flint-server's accepted flags — this check cannot answer"
    FAILED="$FAILED server-flags"
    return 0
  fi
  # CALLERS: shell lines that spawn a flint-server, doubly anchored.
  passed=$( grep -hE '(\$B|\$BIN|/flint-server)' tools/*_drill.sh tools/lib/*.sh 2>/dev/null \
    | grep -- '--data-dir' \
    | grep -oE ' --[a-z0-9-]+' | tr -d ' ' | sort -u )
  # Symmetric to the read_flags guard. If the anchor stops matching — the glob
  # moves, a drill renames $B — `passed` empties, `bad` empties with it, and the
  # check reports success having examined nothing. Empty is not clean here: the
  # drills always spawn a server with at least --port and --data-dir.
  if [ -z "$passed" ]; then
    echo "FAIL  found no flint-server spawn lines in tools/ — this check cannot answer"
    FAILED="$FAILED server-flags"
    return 0
  fi
  bad=$( comm -23 <(printf '%s\n' "$passed") <(printf '%s\n' "$read_flags") )
  [ -z "$bad" ] && return 0
  echo "FAIL  a drill passes flint-server a flag it does not read:"
  # Blame the LINE, not the file. A first version listed every file containing
  # the flag anywhere, which named eighteen drills for one offender because
  # `--advertise` is legitimate on proxy spawns in most of them. A check that
  # fires correctly and accuses the wrong file is the leak-attribution bug
  # again, and it wastes exactly the time it was meant to save.
  for f in $bad; do
    grep -nE '(\$B|\$BIN|/flint-server)' tools/*_drill.sh tools/lib/*.sh 2>/dev/null \
      | grep -- '--data-dir' | grep -- " $f" \
      | cut -d: -f1,2 | sed "s/^/        $f at /"
  done
  echo "        flint-server IGNORES these, so they do nothing today — and they are"
  echo "        exactly what makes refusing unknown arguments unsafe. Delete them."
  FAILED="$FAILED server-flags"
}

# A drill that STARTS seats but never calls fleet_init declares nothing to
# assert_no_port_overlap above — which then passes because it had nothing to
# check, the same defect one level up. restart_drill.sh sat in that blind spot
# from the day it was written (docs/bugs/0020): its port lived in
# `PORT="${2:-6410}"`, invisible to a parser that reads fleet_init lines, and
# its teardown was a bare `pkill -f` that owned nothing — so a full gate
# reported `restart(leaked)` while the drill's own assertions all passed.
#
# The rule is NOT "every drill declares ports". gates_drill.sh exercises
# gates.sh itself and starts no seats, so it has nothing to declare and a
# blanket rule would need an allowlist, which is how a check starts drifting
# from what it means. The rule is that a drill which starts something must say
# what it owns.
assert_spawning_drills_declare_ports() {
  local bad="" f
  for f in tools/*_drill.sh; do
    grep -q '^fleet_init' "$f" && continue
    grep -qE 'target/release/flint-|flintctl' "$f" || continue
    bad="$bad $f"
  done
  [ -z "$bad" ] && return 0
  echo "FAIL  a drill starts Flint processes but declares no ports:"
  for f in $bad; do echo "        $f"; done
  echo "        With no fleet_init line the port-overlap preflight cannot see it,"
  echo "        and its cleanup is scoped to nothing it owns. Add fleet_init with"
  echo "        the drill's own scope and a LITERAL port block."
  FAILED="$FAILED undeclared-ports"
}

# docs/bugs/0025: `recover_migrations` completes a slot flip onto a destination
# that may hold nothing, purging the source — measured acked-write loss. It is
# gated on a non-empty --recover-nodes list, and the ONLY reason it never fires
# in a real fleet is that flintctl does not pass one. That is load-bearing
# absence: it reads as an oversight, someone "fixes" it, and a latent data-loss
# path goes live with no test failing (the recovery drill asserts the source
# redirects and never reads a key from the destination).
#
# This check is deliberately narrow. It is not "recovery must stay off forever"
# — it is "recovery must not be switched on by editing one argv list while the
# reconcile still infers ownership from an absent record instead of observing
# it". Whoever makes the reconcile observe should delete this check in the same
# commit, and the bug write-up says so too.
assert_recovery_stays_off_until_it_observes() {
  # COMMENTS EXCLUDED, and that is not a detail. The doc comment above
  # controller_args names the flag in order to warn about it, so a naive grep
  # matches the warning as readily as the violation and fires on a clean tree —
  # caught here by running the negative control before the positive one.
  local hit
  hit=$(grep -n -- '--recover-nodes' crates/flint-ctl/src/main.rs 2>/dev/null \
    | grep -v ':[[:space:]]*//')
  [ -z "$hit" ] && return 0
  echo "FAIL  flintctl now passes --recover-nodes to the controller:"
  echo "$hit" | sed 's/^/        /'
  echo "        That switches on recover_migrations, which completes a slot flip"
  echo "        from an INFERENCE (the destination's Importing record is absent)"
  echo "        that an aborted import writes just as readily as a completed one."
  echo "        The source then purges every row of the slot. See"
  echo "        docs/bugs/0025-recovery-completes-a-flip-onto-a-destination-that-never-imported.md"
  FAILED="$FAILED recovery-enabled-on-an-inference"
}

# #182: the lease TTL had four homes — flintctl's DEFAULT_LEASE_TTL_MS, the
# AWS chaos/soak fleet's inventory, and two drills — and when ADR-0018 moved
# it from 3000 to 5000 only flintctl followed. Nothing failed; every chaos run
# afterwards was quietly measured on a fleet fenced tighter than any
# customer's. An inventory that OMITS lease-ttl-ms inherits the constant, so
# omitting it is the correct thing to write and a literal is a copy that will
# go stale in silence. A drill deliberately testing a TTL passes a VARIABLE
# (`lease-ttl-ms $LEASE`) — the number is then that drill's own subject rather
# than a duplicate of the product default, and this check leaves it alone.
assert_lease_ttl_single_source() {
  local bad
  bad=$(grep -rn 'lease-ttl-ms[[:space:]][[:space:]]*[0-9]' tools/ 2>/dev/null \
    | grep -v ':[[:space:]]*#')
  [ -z "$bad" ] && return 0
  echo "FAIL  a literal lease-ttl-ms value is hardcoded outside flintctl:"
  echo "$bad" | sed 's/^/        /'
  echo "        DEFAULT_LEASE_TTL_MS (crates/flint-ctl/src/main.rs) is the only"
  echo "        home for this number. Omit the key to inherit it, or pass a"
  echo "        variable if the drill is deliberately testing another TTL."
  FAILED="$FAILED lease-ttl-copy"
}

if want check; then
  echo "== gates: fmt, clippy, tests (both feature configs)"
  assert_no_default_ports
  assert_no_port_overlap
  assert_no_scope_overlap
  assert_server_flags_are_read
  assert_spawning_drills_declare_ports
  assert_recovery_stays_off_until_it_observes
  assert_lease_ttl_single_source
  step "fmt" fmt cargo fmt --all --check
  step "clippy (mem)" clippy-mem \
    cargo clippy --workspace --all-targets -- -D warnings
  step "clippy (rocks)" clippy-rocks \
    cargo clippy --workspace --all-targets --features flint-server/rocks,flint-backup/rocks -- -D warnings
  step "test (mem)" test-mem cargo test --workspace
  step "test (rocks)" test-rocks cargo test --workspace --features flint-server/rocks,flint-backup/rocks
  step "licences" licences licence_check
fi

if want conformance || want drills || want chaos; then
  echo "== building the release binaries the drills run"
  step "build" build \
    cargo build --release --workspace --features flint-server/rocks,flint-backup/rocks
  # Execute the freshly linked flint-server ONCE, before any drill times it.
  #
  # Not superstition: the first exec of a just-written binary pays for
  # signature validation and a cold page cache, and drills that start a
  # server and then sleep a FIXED interval are racing exactly that. The
  # first suite run after a build failed five drills that then passed
  # individually and on every rerun — the shape of a bug hunted in the
  # product when it lived in the harness.
  #
  # This is a mitigation, not the cure. The cure is for every drill to wait
  # for readiness instead of sleeping; most already do, and the ones that do
  # not are tracked separately. Only flint-server is warmed here because
  # only flint-server has a flag that prints and exits — the other binaries
  # would BOOT on an unrecognised argument and bind their default ports,
  # which is worse than the problem being solved.
  ./target/release/flint-server --build-version >/dev/null 2>&1 || true
fi

if want conformance; then
  echo "== conformance: valkey (oracle), flint mem, flint rocks"
  CDIR=$(mktemp -d "${FLINT_DRILL_ROOT:-/tmp}/flint-gate-conf.XXXXXX")
  # Each target starts inside its own subshell so THIS shell never owns it as
  # a job: a shell that owns a background job announces how it died, and
  # "Killed: 9" lines interleaved with the results make a clean gate look
  # like something went wrong.
  ( valkey-server --port 6399 --save '' --appendonly no --daemonize no \
      >"$LOGS/valkey.log" 2>&1 & )
  ( ./target/release/flint-server --port 6398 --engine mem \
      >"$LOGS/conf-mem.log" 2>&1 & )
  ( ./target/release/flint-server --port 6397 --engine rocks --data-dir "$CDIR/rocks" \
      >"$LOGS/conf-rocks.log" 2>&1 & )
  for p in 6399 6398 6397; do
    for _ in $(seq 1 100); do
      [ "$(valkey-cli -p $p PING 2>/dev/null)" = "PONG" ] && break
      sleep 0.1
    done
  done
  step "conformance oracle" conf-oracle \
    ./target/release/flint-conformance --target 127.0.0.1:6399 --reference
  step "conformance mem" conf-mem-run \
    ./target/release/flint-conformance --target 127.0.0.1:6398
  step "conformance rocks" conf-rocks-run \
    ./target/release/flint-conformance --target 127.0.0.1:6397
  # RESP3, which until now was never gated at all — and it is the dialect
  # redis-py 8 and node-redis negotiate BY DEFAULT, so the protocol most
  # clients actually speak had less coverage than the one they don't.
  #
  # The oracle run is the load-bearing one: RESP3 is not a re-spelling of
  # RESP2 (one null instead of two, maps instead of flat arrays, doubles
  # instead of bulk strings), so the corpus's RESP3 expectations are a claim
  # about real Redis behaviour that only a real Redis can settle. Green here
  # means the folding in `Client::normalize` matches Valkey, and only then
  # does a green Flint run mean anything.
  step "conformance oracle (RESP3)" conf-oracle3 \
    ./target/release/flint-conformance --target 127.0.0.1:6399 --reference --proto 3
  step "conformance mem (RESP3)" conf-mem3-run \
    ./target/release/flint-conformance --target 127.0.0.1:6398 --proto 3
  step "conformance rocks (RESP3)" conf-rocks3-run \
    ./target/release/flint-conformance --target 127.0.0.1:6397 --proto 3
  grep -h '^overall:' "$LOGS"/conf-*-run.log "$LOGS"/conf-oracle.log \
    "$LOGS"/conf-oracle3.log 2>/dev/null | sed 's/^/      /'
  for p in 6399 6398 6397; do valkey-cli -p $p SHUTDOWN NOSAVE >/dev/null 2>&1; done
  sleep 0.3
  # Belt and braces: SHUTDOWN is a request, and a wedged process would
  # otherwise be inherited by the drills as a foreign fleet.
  . tools/lib/fleet.sh
  fleet_init "$CDIR" 6398 6397
  fleet_kill server
  rm -rf "$CDIR"
fi

# Every drill SHARES ./target/release, so a drill that rebuilds flint-server
# WITHOUT `--features flint-server/rocks` replaces the rocks binary with a
# mem-only one. The next drill starts `--engine rocks`, the server prints
# "unknown --engine" and exits, and that drill reports "nothing listening
# after 30s" — pointing at a component that is perfectly fine.
#
# Cost of learning this: on 2026-08-11 one new drill silently took out the
# next three, and the failures read as a proxy bug.
#
# Checked STATICALLY, before anything runs. A runtime probe is not an option:
# `flint-server --engine rocks` ignores `--help` and BOOTS, so asking the
# binary what it supports means starting a server (and writing a data dir)
# after every drill. Reading the scripts is free, deterministic, and names
# the file to fix instead of the drill that tripped over it.
# Every drill that builds must CHECK the build (docs/bugs/0028).
#
# Drills declare `set -u`, not `set -e`, so an unguarded `cargo build` that
# fails does not stop the drill — it carries on against whatever is already in
# target/release. Under the gate that is masked, because the gate builds the
# workspace in its own step first and the drill's rebuild is a no-op. Run by
# hand it is not masked: the drill tests a stale binary, or one from the other
# feature config, and says nothing about which.
#
# That masking is how 35569c8 survived. tenant_remove_drill.sh had a fleet_warm
# call spliced into its build's continuation, so cargo ran with fleet_warm as an
# argument and the remainder ran as a command. Both failed, both silently, and
# the drill passed for months because it only ever ran under the gate.
#
# 27 of 53 drills already guarded, in one verbatim idiom, before this check
# existed — so it went in with 26 mechanical edits and no design argument. Half
# a convention is worse than none: a single unguarded drill among 52 guarded
# ones is a visible omission, 26 of 53 is a coin flip that looks like a standard.
#
# THE ORDERING IS PART OF THE CHECK. `set -e` only guards a build that comes
# after it. Crediting it wherever it appears in the file is the rule this was
# first written with, and it happened not to bite — one drill sets -e below its
# build, and that build carries its own inline guard. "There are none today" is
# not "the rule is right".
assert_no_continuation_splice() {
  local bad
  bad=$(python3 - "$@" <<'PY'
import glob, io, re, sys

# A new command begins here, rather than an argument continuing.
STARTS_COMMAND = re.compile(r'^\s*(fleet_[a-z_]+|step|assert_[a-z_]+|cd|export|trap|source)\b')
# The previous line ends in an operator or opens a group, so a command on the
# next line is correct.
OPERATOR_END = re.compile(r'(\|\||&&|\||;|\{|\(|\bthen|\bdo|\belse)\s*\\$')

def quotes_open(chunk):
    # Cheap and deliberately conservative: an odd count of either quote in
    # the recent window means we are probably inside a multi-line string, so
    # skip rather than risk a false positive on an ssh heredoc-ish payload.
    return chunk.count('"') % 2 == 1 or chunk.count("'") % 2 == 1

hits = []
for f in sorted(set(glob.glob('tools/*.sh') + glob.glob('tools/lib/*.sh')
                    + glob.glob('packaging/**/*.sh', recursive=True))):
    lines = io.open(f, encoding='utf-8', errors='replace').read().splitlines()
    for i, line in enumerate(lines[:-1]):
        s = line.rstrip()
        if not s.endswith('\\') or s.lstrip().startswith('#'):
            continue
        if OPERATOR_END.search(s):
            continue
        if quotes_open('\n'.join(lines[max(0, i - 3):i + 1])):
            continue
        if STARTS_COMMAND.match(lines[i + 1]):
            # Print what MATCHED, not what we expect to be there. The public
            # repo's build-guard census nearly shipped a wrong number because
            # its evidence line was chosen separately from its verdict.
            hits.append(f"{f}:{i+2}\n      continued: {s.strip()[:72]}\n      spliced:   {lines[i+1].strip()[:72]}")
print("\n".join(hits))
PY
)
  [ -z "$bad" ] && return 0
  echo "GATES FAILED: a command is spliced into a backslash continuation:"
  printf '    %s\n' "$bad"
  echo "  The line above it swallows this one as an argument, and the line"
  echo "  BELOW it then runs as a command. Both halves fail silently: the"
  echo "  spliced call never happens, and the drill can still pass if"
  echo "  something else already did its work (the gate pre-builds, which is"
  echo "  how 35569c8 survived in tenant_remove_drill.sh)."
  echo "  Fix: move the spliced line above or below the whole continuation."
  exit 1
}

assert_drill_build_is_checked() {
  local bad
  bad=$(python3 - <<'PY_INNER'
import glob, re
bad = []
for f in sorted(glob.glob("tools/*_drill.sh")):
    lines = open(f, errors="replace").read().split("\n")
    e = next((i for i, l in enumerate(lines) if re.match(r'set -\S*e', l.strip())), None)
    b = next((i for i, l in enumerate(lines) if l.strip().startswith("cargo build")), None)
    if b is None:
        continue
    last = b
    while last < len(lines) - 1 and lines[last].rstrip().endswith("\\"):
        last += 1
    stmt = " ".join(l.rstrip().rstrip("\\") for l in lines[b:last + 1])
    if (e is not None and e < b) or "||" in stmt or "&&" in stmt:
        continue
    bad.append(f"{f}:{b+1}\n      {lines[b].strip()[:78]}")
print("\n".join(bad))
PY_INNER
)
  [ -z "$bad" ] && return 0
  echo "GATES FAILED: drill(s) run cargo build without checking it:"
  printf '    %s\n' "$bad"
  echo "  set -u does not stop a failed build, so the drill continues against"
  echo "  whatever is already in target/release. Under the gate that is hidden"
  echo "  by the pre-build step; run by hand it silently tests a stale binary."
  echo "  Add the idiom the other drills use:"
  echo "      cargo build ... || { echo \"FAIL: build\"; exit 1; }"
  exit 1
}

assert_drill_builds_keep_rocks() {
  local bad=""
  for f in tools/*_drill.sh; do
    # Strip whole-line comments BEFORE matching, then join backslash
    # continuations — most of these build lines wrap. Without the strip,
    # a comment that merely quotes a build command is flagged as one
    # (this check's own first version flagged the drill whose comment
    # explains the rule).
    local joined
    joined=$(grep -v '^[[:space:]]*#' "$f" | sed -e :a -e '/\\$/N; s/\\\n//; ta')
    # `--features rocks` and `--features flint-server/rocks` are equivalent
    # when -p flint-server is the selected package, and drills use both.
    # The rule is simply: if it rebuilds flint-server, it must say rocks.
    echo "$joined" | grep 'cargo build.*-p flint-server' | grep -qv 'rocks' \
      && bad="$bad $f"
  done
  [ -z "$bad" ] && return 0
  echo "GATES FAILED: drill(s) rebuild flint-server WITHOUT flint-server/rocks:"
  for f in $bad; do echo "    $f"; done
  echo "  That downgrades ./target/release/flint-server to a mem-only build,"
  echo "  and every later drill using --engine rocks then reports"
  echo "  'nothing listening'. Add to the build line:"
  echo "      --features flint-server/rocks"
  exit 1
}

if want drills; then
  echo "== core drills"
  assert_drill_builds_keep_rocks
  assert_drill_build_is_checked
  assert_no_continuation_splice
  LEAKCHECK=1
  for d in $CORE; do step "$d" "drill-$d" bash "tools/${d}_drill.sh"; done
  LEAKCHECK=
fi

if want chaos; then
  echo "== chaos drills (randomized; the honesty step)"
  LEAKCHECK=1
  for d in $CHAOS; do step "$d" "chaos-$d" bash "tools/${d}_drill.sh"; done
  LEAKCHECK=
fi

echo
if [ -n "$FAILED" ]; then
  echo "GATES FAILED:$FAILED"
  echo "  logs: $LOGS"
  # DUMP THE FAILING LOGS INLINE (docs/bugs/0021).
  #
  # The one-line summary above names the log and stops. Locally that is a path
  # you can open; in CI it is a path inside an artifact, and reading a red gate
  # then requires knowing artifacts exist and downloading one. That extra step
  # is where someone concludes the evidence is gone — it is how "CI never
  # captures the drill log" got written down as a fact and committed to two
  # repos, when gate.yml had been uploading them all along.
  #
  # Tail, not the whole file: a chaos log runs to tens of thousands of lines
  # and the artifact still holds every one of them. The failure is at the end.
  for _fl in $FAILED_LOGS; do
    [ -f "$_fl" ] || continue
    echo
    echo "  ---- $_fl (last 200 lines; the full log is in the run directory) ----"
    tail -200 "$_fl" | sed 's/^/  | /'
  done
  exit 1
fi
if [ "$RAN_STEPS" -eq 0 ]; then
  echo "GATES DID NOT RUN: stages '$STAGES' executed no steps."
  echo
  echo "  This is not a pass. Every stage runs at least one step, so reaching"
  echo "  here means the dispatch above selected nothing — the shape of the"
  echo "  unknown-stage bug this guard exists to keep closed. Fix gates.sh."
  exit 2
fi
echo "GATES PASSED — $RAN_STEPS steps, logs kept in $LOGS (also $LOGS_ROOT/latest)"
