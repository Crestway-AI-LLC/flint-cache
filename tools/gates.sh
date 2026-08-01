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
#   drills       the 20 core drills
#   chaos        the two randomized chaos drills
#
# Logs land in $FLINT_GATE_LOGS (default /tmp/flint-gates) — one file per
# step, kept whether it passed or failed.
set -u
cd "$(dirname "$0")/.."

LOGS="${FLINT_GATE_LOGS:-/tmp/flint-gates}"
rm -rf "$LOGS"; mkdir -p "$LOGS"

# Section 3 of the checklist, in its order. Adding a drill here is what puts
# it in the gate — there is no second list.
CORE="restart repl failover proxy slot_migrate slot_map rebalance_execute
      tenant_quota token_rotation cert_reload_fleet controlplane_ha
      decommission config_file federation_plumbing disk_pressure ctl_error
      client_compat proxy_registry reseed lag_cap widowed_grace attached_chaos"
CHAOS="chaos proxy_chaos"

FAILED=""
step() {  # step <name> <log-suffix> <command...>
  local name="$1" log="$LOGS/$2.log"; shift 2
  local start; start=$(date +%s)
  if "$@" >"$log" 2>&1; then
    printf 'PASS  %-22s (%ss)\n' "$name" "$(( $(date +%s) - start ))"
  else
    printf 'FAIL  %-22s (%ss)  %s\n' "$name" "$(( $(date +%s) - start ))" "$log"
    # The first line that looks like a diagnosis, so a failure is readable
    # here and fully readable in the log.
    grep -m1 -E '^(FAIL|error|REFUSING)' "$log" | sed 's/^/        /'
    FAILED="$FAILED $name"
  fi
}

want() { case " ${STAGES} " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
STAGES="${*:-check conformance drills chaos}"

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

if want check; then
  echo "== gates: fmt, clippy, tests (both feature configs)"
  assert_no_default_ports
  step "fmt" fmt cargo fmt --all --check
  step "clippy (mem)" clippy-mem \
    cargo clippy --workspace --all-targets -- -D warnings
  step "clippy (rocks)" clippy-rocks \
    cargo clippy --workspace --all-targets --features flint-server/rocks -- -D warnings
  step "test (mem)" test-mem cargo test --workspace
  step "test (rocks)" test-rocks cargo test --workspace --features flint-server/rocks
fi

if want conformance || want drills || want chaos; then
  echo "== building the release binaries the drills run"
  step "build" build \
    cargo build --release --workspace --features flint-server/rocks
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
  CDIR=$(mktemp -d /tmp/flint-gate-conf.XXXXXX)
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
  grep -h '^overall:' "$LOGS"/conf-*-run.log "$LOGS"/conf-oracle.log 2>/dev/null \
    | sed 's/^/      /'
  for p in 6399 6398 6397; do valkey-cli -p $p SHUTDOWN NOSAVE >/dev/null 2>&1; done
  sleep 0.3
  # Belt and braces: SHUTDOWN is a request, and a wedged process would
  # otherwise be inherited by the drills as a foreign fleet.
  . tools/lib/fleet.sh
  fleet_init "$CDIR" 6398 6397
  fleet_kill server
  rm -rf "$CDIR"
fi

if want drills; then
  echo "== core drills"
  for d in $CORE; do step "$d" "drill-$d" bash "tools/${d}_drill.sh"; done
fi

if want chaos; then
  echo "== chaos drills (randomized; the honesty step)"
  for d in $CHAOS; do step "$d" "chaos-$d" bash "tools/${d}_drill.sh"; done
fi

echo
if [ -n "$FAILED" ]; then
  echo "GATES FAILED:$FAILED"
  echo "  logs: $LOGS"
  exit 1
fi
echo "GATES PASSED — logs kept in $LOGS"
