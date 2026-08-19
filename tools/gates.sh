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
      cpha_roll admin_gated_proxy edge_ca_trust
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
_leaked_seats() { pgrep -f 'target/release/flint-(server|proxy|controlplane|controller|agent)' 2>/dev/null; }

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
      grep -m2 'SKIP' "$log" |  df -h "$_GATE_DISK_TARGET" | sed 's/^/        /'
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
        || tail -3 "$log"; } |  df -h "$_GATE_DISK_TARGET" | sed 's/^/        /'
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
  local leaked; leaked=$([ -n "${LEAKCHECK:-}" ] && _leaked_seats)
  if [ -n "$leaked" ]; then
    echo "      LEAKED: $name left $(echo "$leaked" | wc -l | tr -d ' ') Flint process(es) running"
    ps -o pid=,args= -p $(echo "$leaked" | tr '\n' ' ') 2>/dev/null \
      |  df -h "$_GATE_DISK_TARGET" | sed 's/^/        /' | cut -c1-110
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
  echo "$hits" |  df -h "$_GATE_DISK_TARGET" | sed 's/^/        /'
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
  assert_spawning_drills_declare_ports
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
    "$LOGS"/conf-oracle3.log 2>/dev/null |  df -h "$_GATE_DISK_TARGET" | sed 's/^/      /'
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
assert_drill_builds_keep_rocks() {
  local bad=""
  for f in tools/*_drill.sh; do
    # Strip whole-line comments BEFORE matching, then join backslash
    # continuations — most of these build lines wrap. Without the strip,
    # a comment that merely quotes a build command is flagged as one
    # (this check's own first version flagged the drill whose comment
    # explains the rule).
    local joined
    joined=$(grep -v '^[[:space:]]*#' "$f" |  df -h "$_GATE_DISK_TARGET" | sed -e :a -e '/\\$/N; s/\\\n//; ta')
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
