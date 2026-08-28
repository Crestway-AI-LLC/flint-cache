#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Everything, in one command.
#
# The work is spread across seven suites and two languages, and until now
# nothing ran them together. A verification you have to remember how to
# assemble is one you will eventually assemble wrong -- and this project has
# already produced a script that graded the wrong jar, a control that could not
# arm, and a green SSE-C run whose reads would have failed in production.
#
#   tools/gate.sh            # everything
#   tools/gate.sh --quick    # skip the shaded-jar re-run
set -uo pipefail

# Valkey or Redis, whichever this machine has. The suites need a
# Redis-protocol server and do not care which implementation provides it --
# and hardcoding `"$TIER_SERVER"` made the gate unrunnable on any CI image
# where it is not packaged, which is most of them. Same flags work for both.
TIER_SERVER="${TIER_SERVER:-$(command -v valkey-server || command -v redis-server || true)}"
TIER_CLI="${TIER_CLI:-$(command -v valkey-cli || command -v redis-cli || true)}"
# Pass the resolution DOWN. Suite.java spawns a tier of its own for the
# tier-death check, and two layers resolving independently is how they end up
# disagreeing about which server is running.
export FLINT_TIER_SERVER="$TIER_SERVER" FLINT_TIER_CLI="$TIER_CLI"

cd "$(dirname "$0")/.."
ROOT=$PWD

# Resource floors. The gate bounded TIME and nothing else, so every other way a
# run can go wrong -- disk, memory, heap -- arrived as an OOM kill or a corrupt
# target dir rather than as a refusal that names the cause. See tools/resource_guard.sh.
. "$(dirname "$0")/resource_guard.sh"
# Both volumes this gate writes to: maven output and the scratch root the
# suites, tier data and logs land in. They are the same filesystem in a plain
# checkout and different the moment either is moved.
GUARD_SCRATCH="${TMPDIR:-/tmp}"
GUARD_DISK_BEFORE=$(guard_free_disk_gb "$GUARD_SCRATCH")
guard_check "the gate" "$ROOT/jvm-spike" "$GUARD_SCRATCH" || exit 3
GUARD_JAVA_OPTS="$(guard_java_opts)"

# Move the previous run's logs aside. They are named after the suite AND its
# check count, so a suite that gains a check leaves its old log behind forever
# under the old name -- and anyone grepping /tmp/gate_*.log for failures finds
# a stale one and diagnoses a bug that was fixed a day ago. That happened while
# writing this line: a "23 checks" log from the day before showed [FAIL] beside
# a clean current run. Kept rather than deleted, because comparing against the
# previous run is the first thing you want when something goes red.
if ls /tmp/gate_*.log >/dev/null 2>&1; then
  rm -rf /tmp/gate_prev && mkdir -p /tmp/gate_prev
  mv /tmp/gate_*.log /tmp/gate_prev/ 2>/dev/null || true
fi
# Bound the BUILD too. A maven that swaps takes the machine down just as
# thoroughly as a suite that does, and it runs before any suite could report it.
export MAVEN_OPTS="${MAVEN_OPTS:-} -Xmx${GUARD_JVM_MAX_HEAP}"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
export PATH="$JAVA_HOME/bin:$PATH"
QUICK=${1:-}
# The tier ports are VARIABLES. Every stage below begins with `flushall`, so a
# tier this gate did not start is not merely shared with a neighbouring
# harness -- it is destroyed mid-run, and both runs then measure the debris.
TIER_PORT=${TIER_PORT:-9399}
SLOW_PORT=${SLOW_PORT:-9398}

PASS=0; FAIL=0; SKIP=0
declare -a FAILED
PIDS=()
ORIGIN_PORTS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  # By pid, not by telling whatever answers on the port to shut down --
  # `--daemonize yes` used to start it, which reparents to init, so an
  # abnormal exit left a tier holding the port indefinitely and the NEXT run
  # adopted it.
  [ -n "${TIER_PID:-}" ] && kill "$TIER_PID" 2>/dev/null
  # AND WHOEVER HOLDS THE PORT NOW, which is not always that pid. The client
  # suite kills the tier mid-run and restarts one to leave the box as it found
  # it, so the tier alive at teardown is a process this script never spawned
  # and whose pid it never knew. Killing only TIER_PID leaked one flint-server
  # per run -- found by counting processes after a PASSING gate, not by
  # anything the gate said. The port is the identity that survives a restart;
  # the pid is only the identity of the first one.
  if [ -n "${TIER_PORT:-}" ]; then
    for _p in $(lsof -nP -iTCP:"$TIER_PORT" -sTCP:LISTEN -t 2>/dev/null); do
      [ "$_p" != "${TIER_PID:-}" ] && kill "$_p" 2>/dev/null
    done
  fi
  # Prove it, rather than assume the kills landed -- and proving it means
  # WAITING for them to land. `kill` returns once the signal is queued, not
  # once the process is gone, so scanning straight afterwards samples the
  # process table mid-exit and reports a leak that is already exiting. A
  # passing gate that ends with a false "1 PROCESS LEFT" teaches the reader to
  # skip the line, which is how the real leak this check exists for gets
  # missed.
  local w p alive
  for w in $(seq 1 30); do
    alive=0
    for p in "${PIDS[@]:-}" "${TIER_PID:-}"; do
      [ -n "$p" ] && kill -0 "$p" 2>/dev/null && alive=1
    done
    [ "$alive" = 0 ] && break
    sleep 0.1
  done
  local left
  # Attribute a leaked process to THIS gate by something this gate declared --
  # our own fixture script names, or a tier server on one of OUR ports. Two
  # reasons, both learned rather than designed:
  #
  # The previous pattern anchored on ^(/opt/homebrew/bin/)?, a macOS Homebrew
  # path, so on any Linux runner -- where the binary is /usr/bin/redis-server
  # -- a leaked tier server matched nothing and the check printed "clean". A
  # leak check that cannot SEE a process reports the same thing as no leak.
  #
  # And matching any redis-server anywhere would flag OTHER work on a shared
  # machine as our leak. A developer box runs more than this gate; a check that
  # blames a neighbour is worse than no check, because the next person learns
  # to ignore it.
  # Scoped to ports THIS gate bound. Matching counting_s3.py by name alone
  # blamed this gate for a neighbouring session's fixture on a port we never
  # touched -- the precise false attribution the paragraph above forbids.
  local mine
  mine="$(printf '%s|' "${ORIGIN_PORTS[@]:-}" | sed 's/|$//')"
  [ -n "$mine" ] || mine="$TIER_PORT"
  local pat="counting_s3\.py[^|]*--port ($mine)|slow_tier\.py[^|]*--listen ($SLOW_PORT)|narrow_tier\.py[^|]*--listen 9397|(valkey|redis)-server[^|]*--port ($SLOW_PORT|$TIER_PORT)|(valkey|redis)-server[^|]*:($SLOW_PORT|$TIER_PORT)"
  local leaked
  leaked=$(ps -ax -o command= | grep -E "$pat" | grep -v grep || true)
  left=$(printf "%s" "$leaked" | grep -c . || true)
  if [ "${left:-0}" = 0 ]; then
    echo "teardown: clean"
  else
    echo "teardown: $left PROCESS(ES) LEFT"
    printf "%s\n" "$leaked"
  fi
}
trap cleanup EXIT

step() { printf "\n\033[1m== %s\033[0m\n" "$1"; }
verdict() { # name, rc
  if [ "$2" -eq 0 ]; then PASS=$((PASS+1)); printf "   \033[32mPASS\033[0m  %s\n" "$1"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); printf "   \033[31mFAIL\033[0m  %s (rc=$2)\n" "$1"; fi
}

need() { command -v "$1" >/dev/null 2>&1; }
if [ -z "$TIER_SERVER" ] || [ -z "$TIER_CLI" ]; then
  echo "need valkey-server/valkey-cli or redis-server/redis-cli on PATH" >&2
  exit 2
fi
for t in java mvn python3 "$TIER_SERVER" "$TIER_CLI"; do
  need "$t" || { echo "missing prerequisite: $t"; exit 2; }
done

# ------------------------------------------------------------------ the tier
# Started once, here, rather than re-issued by start_svcs on every stage.
# `--daemonize yes` returns 0 whether or not the bind succeeded, so a port
# already held produced a "successful" start and a silent adoption of someone
# else's tier. Prove the port is free by binding it, own the pid, then prove
# the server answering is the one we started.
python3 - "$TIER_PORT" <<'FREE' || { echo "port $TIER_PORT is in use -- refusing to adopt a tier this gate did not start. Set TIER_PORT to a free port." >&2; exit 2; }
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
FREE
# ENGINE. `resp` (default) starts valkey/redis; `flint` starts a real
# flint-server, which is the tier this product is actually for. The two need
# different flags and prove ownership differently, and until this existed the
# whole gate ran against a stand-in -- invisible, because valkey answers the
# same protocol and passes every assertion identically. What it hid was
# everything the suite does not test: Flint has no INFO and no KEYS, and its
# per-value storage overhead is 1.004x where valkey's is 1.250x.
TIER_ENGINE="${FLINT_TIER_ENGINE:-resp}"
if [ "$TIER_ENGINE" = flint ]; then
  TIER_DATA="${FLINT_TIER_DATA:-/tmp/gate_flint_data_$TIER_PORT}"
  rm -rf "$TIER_DATA"
  "$TIER_SERVER" --port "$TIER_PORT" --engine rocks --data-dir "$TIER_DATA" \
      >/tmp/gate_tier.log 2>&1 & TIER_PID=$!
else
  "$TIER_SERVER" --port "$TIER_PORT" --save '' --appendonly no \
      >/tmp/gate_tier.log 2>&1 & TIER_PID=$!
fi
for _ in $(seq 1 40); do "$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
"$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 \
    || { echo "tier never answered on $TIER_PORT (see /tmp/gate_tier.log)" >&2; exit 2; }
# OWNERSHIP. The point is never to flushall a tier we did not start -- this
# gate destroys the keyspace at every stage. INFO is the direct proof and Flint
# does not implement it, so fall back to asking the OS who holds the port
# rather than skipping the check: an ownership check that silently turns off on
# one engine is worse than none, because the flushall still happens.
TIER_OWNED=$("$TIER_CLI" -p "$TIER_PORT" info server 2>/dev/null | tr -d '\r' | awk -F: '/^process_id:/{print $2}')
if [ -z "$TIER_OWNED" ]; then
  TIER_OWNED=$(lsof -nP -iTCP:"$TIER_PORT" -sTCP:LISTEN -t 2>/dev/null | head -1)
  [ -n "$TIER_OWNED" ] \
      || { echo "cannot establish who owns the tier on $TIER_PORT -- refusing to flushall it." >&2; exit 2; }
fi
[ "$TIER_OWNED" = "$TIER_PID" ] \
    || { echo "tier on $TIER_PORT has pid $TIER_OWNED, we started $TIER_PID -- not ours." >&2; exit 2; }
# The suites need the pid to kill a tier whose protocol has no SHUTDOWN.
export FLINT_TIER_PID="$TIER_PID"
export FLINT_TIER_ENGINE FLINT_TIER_DATA="${TIER_DATA:-}"

# ---------------------------------------------------------------- fixtures
# A gate that can hang forever is worse than one that fails: the first run of
# this script burned its whole budget on one stuck suite and produced NO
# verdict at all. macOS has no timeout(1), so enforce it here.
# Wait for a fixture to ANSWER, rather than for a number of seconds to pass.
#
# Every fixture here used to be followed by `sleep 2` (moto, `sleep 4`). That is
# not a weak readiness signal, it is NO signal: it asserts that two seconds is
# enough on every machine this will ever run on, which is false the moment the
# box is loaded -- and this box regularly is, with a neighbouring session
# building. The failure surfaces far downstream as a suite that could not reach
# its origin, and the obvious repair, a longer sleep, only helps when the thing
# waited on is genuinely SLOW rather than merely not started yet.
#
# counting_s3 and moto both answer HTTP; slow_tier is a TCP proxy, so it gets a
# connect rather than a request.
wait_http() { # url [tries]
  local i
  for i in $(seq 1 "${2:-150}"); do
    curl -sf -o /dev/null "$1" 2>/dev/null && return 0
    sleep 0.1
  done
  echo "fixture never answered: $1" >&2
  return 1
}
wait_tcp() { # port [tries]
  local i
  for i in $(seq 1 "${2:-150}"); do
    python3 -c "
import socket,sys
s=socket.socket(); s.settimeout(0.3)
try: s.connect(('127.0.0.1', int(sys.argv[1])))
except OSError: sys.exit(1)
finally: s.close()" "$1" 2>/dev/null && return 0
    sleep 0.1
  done
  echo "fixture never accepted on port $1" >&2
  return 1
}

SUITE_TIMEOUT=${SUITE_TIMEOUT:-180}
run_bounded() { # cmd...
  "$@" & local pid=$! waited=0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 2; waited=$((waited+2))
    if [ "$waited" -ge "$SUITE_TIMEOUT" ]; then
      kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
      return 124
    fi
  done
  wait "$pid"; return $?
}

# Defined HERE, above every use, because it was not: a stage added later called
# run_bounded from line 60 while the definition sat at line 100, and bash
# reported `command not found` -- rc 127, which the gate correctly failed on
# but which reads like a missing interpreter rather than a helper used before
# it exists. A helper defined halfway down a script is a footgun for whoever
# adds the next stage above it.

step "fixtures"
python3 tools/counting_s3.py --self-test >/tmp/gate_harness.log 2>&1
verdict "counting-s3 self-test" $?

# The preflight script is the first thing a customer runs and the only one that
# runs on THEIR cluster, so its version predicates are gated like anything else.
# Each carries a negative control, because a warning nobody has watched fire is
# indistinguishable from a warning that cannot.
bash tools/preflight.sh --self-test >/tmp/gate_preflight.log 2>&1
verdict "preflight version predicates (4 checks)" $?

# Do our ports belong to us alone? Claimed once that they did, from a grep
# pattern listing the ports already believed to be in use -- which found
# exactly those and nothing else. Two of ours were in fact a sibling harness's,
# and our teardown SHUTS DOWN whatever answers on the tier port, so the
# collision would have stopped a neighbour's node via a clean RESP shutdown
# that looks like their seat exiting for no reason.
run_bounded bash "$ROOT/tools/port_exclusivity_check.sh" >/tmp/gate_ports.log 2>&1
verdict "port exclusivity vs the sibling harness (3 checks)" $?

# The sick-tier proxy is an instrument like counting_s3, and gets the same
# treatment: a proxy that quietly added no delay would make a broken client
# look healthy, so it self-tests with a zero-delay transparency control.
run_bounded python3 tools/slow_tier.py --self-test >/tmp/gate_slowtier.log 2>&1
verdict "slow-tier proxy self-test (4 checks)" $?

# Its companion: slow_tier makes the FIRST byte late, narrow_tier makes every
# byte late by a little. The two are different failures and the two clients do
# not agree about the second one -- see ADR-0023 D17.
run_bounded python3 "$ROOT/tools/narrow_tier.py" --self-test >/tmp/gate_narrowtier.log 2>&1
verdict "narrow-tier proxy self-test (4 checks)" $?

bash tools/shim_guard_test.sh >/tmp/gate_shim.log 2>&1
verdict "shim guard (5 classpath states)" $?

# ---------------------------------------------------------------- build
step "build"
( cd jvm-spike && mvn -q package -DskipTests ) >/tmp/gate_build.log 2>&1
verdict "maven package" $?

bash tools/verify_shading.sh >/tmp/gate_shade.log 2>&1
verdict "shading (relocation + shim isolation)" $?

CP_FILE=/tmp/gate_cp.txt
( cd jvm-spike && mvn -q dependency:build-classpath -Dmdep.outputFile=$CP_FILE ) >/dev/null 2>&1
CP=$(cat $CP_FILE 2>/dev/null)
# Test scope too: the Iceberg suite needs iceberg-data and a newer Avro, both
# test-scoped because customers bring their own Hadoop and Iceberg.
CP_TEST_FILE=/tmp/gate_cp_test.txt
( cd jvm-spike && mvn -q dependency:build-classpath -Dmdep.outputFile=$CP_TEST_FILE \
    -DincludeScope=test ) >/dev/null 2>&1
CP_TEST=$(cat $CP_TEST_FILE 2>/dev/null)
CLASSES=$ROOT/jvm-spike/target/classes
SHADED=$(ls jvm-spike/target/*.jar 2>/dev/null | grep -v original | grep -v hadoop-shim | head -1)
SHIM=$(ls jvm-spike/target/*hadoop-shim.jar 2>/dev/null | head -1)

# ---------------------------------------------------------------- services
start_svcs() { # port, extra-args
  # The tier is started once at the top and owned; this only proves it is
  # still there, so a stage that flushes a DEAD tier fails here rather than
  # further along as an inexplicable pile of cache misses.
  "$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 \
      || { echo "tier on $TIER_PORT stopped answering" >&2; exit 2; }
  # Re-take ownership if a suite restarted the tier under us. Suite.java kills
  # the tier deliberately (to prove readers survive it) and then starts a
  # DAEMONIZED replacement to leave the box as it found it -- which the gate
  # could not then clean up, because the pid it owned had already been shut
  # down. Tracking whoever holds the port is safe here and only here: the gate
  # proved the port free before binding it and has held it since, so anything
  # answering on it now descends from this run.
  local now
  now=$("$TIER_CLI" -p "$TIER_PORT" info server 2>/dev/null | tr -d '\r' \
        | awk -F: '/^process_id:/{print $2}')
  [ -n "$now" ] && [ "$now" != "${TIER_PID:-}" ] && TIER_PID="$now"
  ORIGIN_PORTS+=("$1")
  python3 tools/counting_s3.py --port "$1" --objects 8 --object-bytes 8388608 ${2:-} \
      >/tmp/gate_origin_$1.log 2>&1 &
  PIDS+=($!)
  wait_http "http://127.0.0.1:$1/__stats" || exit 2
}
# Stop the fixtures and return when they are actually GONE.
#
# This was `kill; sleep 1`. The sleep was a guess at how long a python process
# takes to die, and the next stage begins with a bind to the same port -- so on
# a loaded box the guess expiring early is a bind failure in a stage that has
# nothing to do with the fixture. `wait` is the signal: the shell already knows
# when its own child exits.
#
# It also silences the `Terminated: 15` lines that trailed the PASS output.
# Those were bash reporting a job it reaped asynchronously; reaping it here
# deliberately is what makes them stop, and a clean gate now looks clean.
stop_origin() {
  local p
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null; done
  PIDS=()
}


run_suite() { # label, mainclass, port, classpath, extra-origin-args
  start_svcs "$3" "${5:-}"
  "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
  local log="/tmp/gate_$(echo "$1" | tr ' /()' '____').log"
  run_bounded java $GUARD_JAVA_OPTS -cp "$4" "$2" "http://127.0.0.1:$3" redis://127.0.0.1:$TIER_PORT >"$log" 2>&1
  local rc=$?
  [ $rc -eq 124 ] && printf "   \033[31mHUNG\033[0m  %s (>${SUITE_TIMEOUT}s)\n" "$1"
  verdict "$1" $rc
  stop_origin
}

step "suites (classes)"
# --delay-ms is not decoration: the single-flight suites need a window wide
# enough for readers to actually collide, and these are the values each suite
# was developed and validated against.
run_suite "client suite (24 checks)"   ai.crestway.flintaccel.client.Suite   9301 "$ROOT/jvm-spike/target/classes:$CP" "--delay-ms 120"
run_suite "S3A properties (12 checks)"  ai.crestway.flintaccel.s3a.S3aSuite   9302 "$ROOT/jvm-spike/target/classes:$CP" "--delay-ms 150"
run_suite "adoption paths (9 checks)"  ai.crestway.flintaccel.s3a.AdoptionSuite 9303 "$ROOT/jvm-spike/target/classes:$CP"
run_suite "SSE-C bypass (5 checks)"    ai.crestway.flintaccel.s3a.SseCSuite  9304 "$ROOT/jvm-spike/target/classes:$CP"
# BUG-0058. This suite KILLS AND RESTARTS THE TIER, which the ownership re-take
# above is already written for. Gated rather than left as a spike: ADR-0023
# D12.9 calls this the property that decides deployability, and it had a spike
# (ResilienceSpike) that the gate never ran, which is how a tier down at
# submission time reached a real Spark job and failed it outright.
run_suite "tier down at build + mid-job on path 1 (16 checks)" ai.crestway.flintaccel.client.TierDownSuite 9306 "$ROOT/jvm-spike/target/classes:$CP"
# ResilienceSpike RETIRED here (BUG-0067). It was gated as "tier killed mid-job
# (4 checks)" and did not use FlintObjectClient: it defined its own
# ResilientObjectClient inside the spike file, so the stage proved a
# reimplementation of the degradation logic survived a dead tier and said
# nothing about the product. All four of its assertions have a stronger
# counterpart in the client suite, against the real client -- reads correct
# cold and warm, readers SURVIVE the tier dying, they finish promptly rather
# than eventually, and tierFailures moved so the failure was observed -- and
# that suite also covers the joined-in-flight interaction the spike did not.
# A stage that reports coverage it does not have costs a slot AND the
# confidence; that is how the mid-job gap on path 1 stayed invisible.
# BUG-0057: identically configured mounts must share one stack. Under
# fs.s3a.impl.disable.cache=true Hadoop builds a FileSystem per get() and Spark
# never closes them, so this ran per READ -- measured at +4 threads each, +48
# for twelve, until the JVM could not create another Netty event loop group.
run_suite "connection sharing (8 checks)" ai.crestway.flintaccel.client.TierSharingSuite 9312 "$ROOT/jvm-spike/target/classes:$CP"
# ADR-0023 D17.5: the cap is on the PART, not the object. The suite issues the
# SAME request under two caps, because "a big request was not cached" is also
# true of a cache that is simply broken.
# BUG-0066/BUG-0067: every config key path 1 DECLARES must be one somebody has
# shown to do something. Reflection finds the keys, so adding a constant without
# classifying it fails here rather than being silently untested -- which is the
# defect itself. Needs the SLOW proxy: a budget is not observable without one.
python3 tools/slow_tier.py --listen "$SLOW_PORT" --upstream "$TIER_PORT" --delay-ms 200 \
    >/tmp/gate_creach_proxy.log 2>&1 &
PIDS+=($!)
wait_tcp "$SLOW_PORT" || exit 2
start_svcs 9314
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.s3a.ConfigReachSuite \
    "http://127.0.0.1:9314" "redis://127.0.0.1:$TIER_PORT" "redis://127.0.0.1:$SLOW_PORT" \
    >/tmp/gate_config_reach.log 2>&1
verdict "every declared path-1 key does something (28 checks)" $?
stop_origin

run_suite "part cap admission, read and write sides (13 checks)" ai.crestway.flintaccel.client.PartCapSuite 9313 "$ROOT/jvm-spike/target/classes:$CP"

# The tier is the one dependency every other suite trusts implicitly. This one
# corrupts it on purpose: absent, truncated, wrong-bytes, and right-bytes-wrong-
# offset. The last two returned wrong data as truth until chunks were sealed
# with their own identity, and no other suite could have seen it.
run_suite "tier integrity, adversarial (20 checks)" ai.crestway.flintaccel.client.IntegritySuite 9305 "$ROOT/jvm-spike/target/classes:$CP"

# Every fixture object had been single-part, so the ETag string that D3
# content-addresses by had only ever taken one shape. Anything Spark writes at
# size reports "<hash>-<partcount>" instead -- the workload this product exists
# for. The whole 18-check suite runs again in that regime, and the drill checks
# the suffix survives into the KEY, which correct bytes alone would not show.
run_suite "client suite under MULTIPART etags (24 checks)" ai.crestway.flintaccel.client.Suite \
    9307 "$ROOT/jvm-spike/target/classes:$CP" "--multipart-parts 4 --delay-ms 120"

# Its own tier port, like the cross-language drill: this one starts and tears
# down its tier, and tearing down the GATE's tier is what it used to do.
CP_FILE="$CP_FILE" TIER_PORT=9400 run_bounded bash "$ROOT/tools/multipart_etag_drill.sh" \
    >/tmp/gate_multipart.log 2>&1
verdict "multipart etag key shape (7 checks)" $?

# SSE-KMS bypasses the cache by default (D13.3). Needs TWO origins: one that
# reports aws:kms and one that does not, because "it cached nothing" is only
# evidence of the rule if the same client demonstrably caches a plain object.
start_svcs 9308 "--sse-kms"
python3 tools/counting_s3.py --port 9309 --objects 4 --object-bytes 1048576 \
    >/tmp/gate_origin_9309.log 2>&1 &
PIDS+=($!)
wait_http "http://127.0.0.1:9309/__stats" || exit 2
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.client.SseKmsSuite \
    http://127.0.0.1:9308 http://127.0.0.1:9309 redis://127.0.0.1:$TIER_PORT \
    >/tmp/gate_ssekms.log 2>&1
verdict "SSE-KMS bypass + opt-in (12 checks)" $?

# The same rule on ALL THREE adoption paths. Written because it was implemented
# on two and absent from the one preflight recommends FIRST: the stream factory
# built its client through the older constructor and could not detect KMS at
# all, while fs.s3a.impl mapped config through a switch that dropped the opt-in
# key. A check on any single path passes while the others are wrong.
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.s3a.SseKmsPathsSuite \
    http://127.0.0.1:9308 redis://127.0.0.1:$TIER_PORT >/tmp/gate_kmspaths.log 2>&1
verdict "SSE-KMS on all 3 adoption paths (7 checks)" $?

# The counters, read the way an OPERATOR reads them -- through the platform
# MBeanServer by object name, never from the reference we registered. Sixteen
# counters existed and nothing surfaced them, so a customer could install this
# and have no way to answer "is it working?" -- and the two
# silent-zero-acceleration cases (an SSE-KMS bucket, a sick tier) were
# invisible by construction.
python3 tools/counting_s3.py --port 9311 --objects 4 --object-bytes 1048576 \
    >/tmp/gate_origin_9311.log 2>&1 &
PIDS+=($!)
wait_http "http://127.0.0.1:9311/__stats" || exit 2
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.client.MetricsSuite \
    http://127.0.0.1:9311 http://127.0.0.1:9308 redis://127.0.0.1:$TIER_PORT \
    >/tmp/gate_metrics.log 2>&1
verdict "JMX metrics, read via MBeanServer (13 checks)" $?

# An immutability declaration should stop the revalidation HEADs. Measured as
# origin HEAD count across a read taken AFTER the mutable TTL expires --
# correct bytes or a hit counter would look identical whether or not the
# declaration did anything.
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.client.ImmutableSuite \
    http://127.0.0.1:9311 redis://127.0.0.1:$TIER_PORT >/tmp/gate_immutable.log 2>&1
verdict "immutability declaration skips revalidation (3 checks)" $?
stop_origin

# A SICK tier -- one that answers slowly rather than dying -- used to make
# reads 165% SLOWER than having no cache at all, because every request burned
# the whole budget and then went to the origin anyway. The reference here is
# deliberately the no-tier client, not the healthy one: a cache may be slower
# than a fast cache, and may never be slower than no cache.
start_svcs 9310 "--delay-ms 20"
python3 tools/slow_tier.py --listen "$SLOW_PORT" --upstream "$TIER_PORT" --delay-ms 200 \
    >/tmp/gate_proxy.log 2>&1 &
PIDS+=($!)
wait_tcp "$SLOW_PORT" || exit 2
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.client.SickTierSuite \
    http://127.0.0.1:9310 redis://127.0.0.1:$TIER_PORT redis://127.0.0.1:$SLOW_PORT \
    >/tmp/gate_sicktier.log 2>&1
verdict "sick tier is not slower than NO tier (4 checks)" $?
stop_origin

# ------------------------------------------------------------ iceberg io-impl
# The one adoption route with NO inheritable contract suite, so this is bespoke
# on purpose: a real table, created by a catalog configured with nothing but
# io-impl, written as Avro through the FileIO, and read back through Iceberg's
# own planner. Each read phase uses a FRESH catalog, because one FileIO holds
# one AAL factory and reading twice through it measures AAL's memory rather
# than our tier.
step "iceberg io-impl, end to end (real tables, avro + parquet)"
start_svcs 9306
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
( cd jvm-spike && run_bounded mvn -q test-compile ) >/tmp/gate_ice_build.log 2>&1
run_bounded java $GUARD_JAVA_OPTS -cp "$ROOT/jvm-spike/target/classes:$ROOT/jvm-spike/target/test-classes:$CP_TEST" \
    ai.crestway.flintaccel.iceberg.IcebergSuite \
    http://127.0.0.1:9306 redis://127.0.0.1:$TIER_PORT >/tmp/gate_iceberg.log 2>&1
verdict "iceberg table read via io-impl, avro + parquet (20 checks)" $?
stop_origin

# --------------------------------------------------- inherited contract suite
step "hadoop contract suite (45 tests we did not write)"
start_svcs 9310
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
( cd jvm-spike && run_bounded mvn -q test -Dtest=ITestFlintSeek,ITestFlintOpen \
    -DfailIfNoTests=false -Dflint.test.endpoint=http://127.0.0.1:9310 \
    -Dflint.test.tier=redis://127.0.0.1:$TIER_PORT ) >/tmp/gate_contract.log 2>&1
verdict "Hadoop AbstractContract{Seek,Open}Test" $?
stop_origin

# ---------------------------------------------------------------- python
step "python path"
PYENV="${FLINT_PYENV:-$ROOT/python/.venv}"
if [ -x "$PYENV/bin/python" ]; then
  start_svcs 9401 "--delay-ms 80"
  "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
  ( cd python && run_bounded "$PYENV/bin/python" suite.py \
      http://127.0.0.1:9401 redis://127.0.0.1:$TIER_PORT ) >/tmp/gate_python.log 2>&1
  verdict "python suite (45 checks)" $?
  stop_origin

  # fsspec's own abstract suite, against MOTO rather than counting_s3.
  # counting_s3 exists to count and cannot serve DeleteObjects or CopyObject;
  # running a correctness suite against it measures the fixture. Each
  # instrument for the question it can answer.
  if "$PYENV/bin/python" -c "import moto, pytest" 2>/dev/null; then
    "$PYENV/bin/python" -m moto.server -p 9810 >/tmp/gate_moto.log 2>&1 &
    PIDS+=($!); wait_http "http://127.0.0.1:9810" || exit 2
    "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
    ( cd python && FLINT_TEST_ENDPOINT=http://127.0.0.1:9810 \
        FLINT_TEST_TIER=redis://127.0.0.1:$TIER_PORT \
        run_bounded "$PYENV/bin/python" -m pytest test_fsspec_contract.py \
        -q --no-header --deselect \
        test_fsspec_contract.py::TestFlintOpen::test_open_exclusive \
      ) >/tmp/gate_fsspec.log 2>&1
    verdict "fsspec abstract suite (90 tests)" $?
    stop_origin
  else
    SKIP=$((SKIP+1))
    printf "   \033[33mSKIP\033[0m  fsspec abstract suite (moto/pytest not installed)\n"
  fi

  # ------------------------------------------------- one tier, two languages
  # The only check that can see whether the JVM and Python clients actually
  # SHARE a cache. Every other suite speaks one language and populates the
  # tier itself, so both stayed green while the two wrote different key
  # prefixes and different value formats -- two copies of identical bytes and
  # double the S3 bill, invisible by construction.
  #
  # The drill owns its own origin and tier and shuts BOTH down on exit, so any
  # later stage must start its own. start_svcs does.
  cp "$CP_FILE" /tmp/cp.txt 2>/dev/null
  # Its OWN tier port, distinct from the gate's. The drill starts, owns and
  # shuts down its tier, and now refuses to adopt one it did not start -- so
  # sharing the gate's port makes it refuse to run at all, correctly. 9400 is
  # declared and nothing binds it.
  FLINT_PYENV="$PYENV" PORT=9407 TIER_PORT=9400 \
    run_bounded bash "$ROOT/tools/cross_language_drill.sh" \
    >/tmp/gate_xlang.log 2>&1
  verdict "cross-language cache sharing (20 checks)" $?

  # The same SSE-KMS rule on the Python path. Not a duplicate of the JVM
  # stage: the two clients SHARE one tier, so a rule only one of them enforces
  # is not a rule -- a Python reader would cache the very plaintext the JVM
  # reader refuses to cache.
  start_svcs 9318 "--sse-kms"
  python3 tools/counting_s3.py --port 9319 --objects 4 --object-bytes 1048576 \
      >/tmp/gate_origin_9319.log 2>&1 &
  PIDS+=($!)
  wait_http "http://127.0.0.1:9319/__stats" || exit 2
  "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
  ( cd python && run_bounded "$PYENV/bin/python" sse_kms_suite.py \
      http://127.0.0.1:9318 http://127.0.0.1:9319 redis://127.0.0.1:$TIER_PORT \
    ) >/tmp/gate_ssekms_py.log 2>&1
  verdict "SSE-KMS on the python path (11 checks)" $?
  stop_origin

  # The python counterpart of SickTierSuite, and NOT a duplicate of it: the two
  # clients did not mean the same thing by the 50 ms budget. redis-py applies
  # socket_timeout per recv(), so a tier that answered promptly and then
  # delivered slowly was invisible to the python client -- an 8 MiB read over
  # an 8-second tier reported no failure at all, where the JVM degraded. Both
  # shapes of "slow" are asserted, which needs both proxies: slow_tier is late
  # to the FIRST byte, narrow_tier is slow to FINISH.
  start_svcs 9319
  python3 tools/slow_tier.py --listen "$SLOW_PORT" --upstream "$TIER_PORT" --delay-ms 200 \
      >/tmp/gate_proxy_py.log 2>&1 &
  PIDS+=($!)
  python3 "$ROOT/tools/narrow_tier.py" --listen 9397 --upstream "$TIER_PORT" --kbps 40000 \
      >/tmp/gate_narrow_py.log 2>&1 &
  PIDS+=($!)
  wait_tcp "$SLOW_PORT" || exit 2
  wait_tcp 9397 || exit 2
  "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
  ( cd python && run_bounded "$PYENV/bin/python" budget_suite.py \
      http://127.0.0.1:9319 redis://127.0.0.1:$TIER_PORT \
      redis://127.0.0.1:9397 redis://127.0.0.1:$SLOW_PORT \
    ) >/tmp/gate_budget_py.log 2>&1
  verdict "tier budget bounds the COMMAND, python (11 checks)" $?
  stop_origin
else
  SKIP=$((SKIP+1))
  printf "   \033[33mSKIP\033[0m  python suite (no venv; set FLINT_PYENV)\n"
fi

if [ "$QUICK" != "--quick" ]; then
  step "suites (SHADED jar -- relocation can break reflection)"
  if [ -f "$SHADED" ] && [ -f "$SHIM" ]; then
    ( cd jvm-spike && mvn -q dependency:build-classpath -Dmdep.outputFile=/tmp/gate_cp_prov.txt -Dmdep.includeScope=provided ) >/dev/null 2>&1
    PROV=$(cat /tmp/gate_cp_prov.txt 2>/dev/null)
    run_suite "adoption paths on shaded jar" ai.crestway.flintaccel.s3a.AdoptionSuite 9305 \
        "$ROOT/$SHADED:$ROOT/$SHIM:$PROV"
  else
    SKIP=$((SKIP+1)); printf "   \033[33mSKIP\033[0m  shaded jar not built\n"
  fi
fi

# ---------------------------------------------------------------- verdict
step "gate"
printf "   %d passed, %d failed" "$PASS" "$FAIL"
[ $SKIP -gt 0 ] && printf ", %d skipped" "$SKIP"
printf "\n"
for f in "${FAILED[@]:-}"; do [ -n "$f" ] && printf "   failed: %s  (see /tmp/gate_*.log)\n" "$f"; done
# A run that leaves the volume near full makes the NEXT run fail for reasons
# that have nothing to do with it, so say so while the cause is still attached.
guard_report_after "the gate" "$GUARD_SCRATCH" "$GUARD_DISK_BEFORE"
[ $FAIL -eq 0 ] && echo "   GATE PASSED" || echo "   GATE FAILED"
exit $FAIL
