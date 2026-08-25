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
  # Prove it, rather than assume the kills landed.
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
  local pat="counting_s3\.py[^|]*--port ($mine)|slow_tier\.py[^|]*--listen ($SLOW_PORT)|(valkey|redis)-server[^|]*--port ($SLOW_PORT|$TIER_PORT)|(valkey|redis)-server[^|]*:($SLOW_PORT|$TIER_PORT)"
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
"$TIER_SERVER" --port "$TIER_PORT" --save '' --appendonly no \
    >/tmp/gate_tier.log 2>&1 & TIER_PID=$!
for _ in $(seq 1 40); do "$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
"$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 \
    || { echo "tier never answered on $TIER_PORT (see /tmp/gate_tier.log)" >&2; exit 2; }
TIER_OWNED=$("$TIER_CLI" -p "$TIER_PORT" info server 2>/dev/null | tr -d '\r' | awk -F: '/^process_id:/{print $2}')
[ "$TIER_OWNED" = "$TIER_PID" ] \
    || { echo "tier on $TIER_PORT has pid $TIER_OWNED, we started $TIER_PID -- not ours." >&2; exit 2; }

# ---------------------------------------------------------------- fixtures
# A gate that can hang forever is worse than one that fails: the first run of
# this script burned its whole budget on one stuck suite and produced NO
# verdict at all. macOS has no timeout(1), so enforce it here.
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
  sleep 2
}
stop_origin() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; PIDS=(); sleep 1; }


run_suite() { # label, mainclass, port, classpath, extra-origin-args
  start_svcs "$3" "${5:-}"
  "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
  local log="/tmp/gate_$(echo "$1" | tr ' /()' '____').log"
  run_bounded java -cp "$4" "$2" "http://127.0.0.1:$3" redis://127.0.0.1:$TIER_PORT >"$log" 2>&1
  local rc=$?
  [ $rc -eq 124 ] && printf "   \033[31mHUNG\033[0m  %s (>${SUITE_TIMEOUT}s)\n" "$1"
  verdict "$1" $rc
  stop_origin
}

step "suites (classes)"
# --delay-ms is not decoration: the single-flight suites need a window wide
# enough for readers to actually collide, and these are the values each suite
# was developed and validated against.
run_suite "client suite (18 checks)"   ai.crestway.flintaccel.client.Suite   9301 "$ROOT/jvm-spike/target/classes:$CP" "--delay-ms 120"
run_suite "S3A properties (9 checks)"  ai.crestway.flintaccel.s3a.S3aSuite   9302 "$ROOT/jvm-spike/target/classes:$CP" "--delay-ms 150"
run_suite "adoption paths (9 checks)"  ai.crestway.flintaccel.s3a.AdoptionSuite 9303 "$ROOT/jvm-spike/target/classes:$CP"
run_suite "SSE-C bypass (5 checks)"    ai.crestway.flintaccel.s3a.SseCSuite  9304 "$ROOT/jvm-spike/target/classes:$CP"

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
run_suite "client suite under MULTIPART etags (18 checks)" ai.crestway.flintaccel.client.Suite \
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
sleep 2
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.client.SseKmsSuite \
    http://127.0.0.1:9308 http://127.0.0.1:9309 redis://127.0.0.1:$TIER_PORT \
    >/tmp/gate_ssekms.log 2>&1
verdict "SSE-KMS bypass + opt-in (12 checks)" $?

# The same rule on ALL THREE adoption paths. Written because it was implemented
# on two and absent from the one preflight recommends FIRST: the stream factory
# built its client through the older constructor and could not detect KMS at
# all, while fs.s3a.impl mapped config through a switch that dropped the opt-in
# key. A check on any single path passes while the others are wrong.
run_bounded java -cp "$ROOT/jvm-spike/target/classes:$CP" \
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
sleep 2
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java -cp "$ROOT/jvm-spike/target/classes:$CP" \
    ai.crestway.flintaccel.client.MetricsSuite \
    http://127.0.0.1:9311 http://127.0.0.1:9308 redis://127.0.0.1:$TIER_PORT \
    >/tmp/gate_metrics.log 2>&1
verdict "JMX metrics, read via MBeanServer (13 checks)" $?

# An immutability declaration should stop the revalidation HEADs. Measured as
# origin HEAD count across a read taken AFTER the mutable TTL expires --
# correct bytes or a hit counter would look identical whether or not the
# declaration did anything.
run_bounded java -cp "$ROOT/jvm-spike/target/classes:$CP" \
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
sleep 2
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
run_bounded java -cp "$ROOT/jvm-spike/target/classes:$CP" \
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
run_bounded java -cp "$ROOT/jvm-spike/target/classes:$ROOT/jvm-spike/target/test-classes:$CP_TEST" \
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
  verdict "python suite (19 checks)" $?
  stop_origin

  # fsspec's own abstract suite, against MOTO rather than counting_s3.
  # counting_s3 exists to count and cannot serve DeleteObjects or CopyObject;
  # running a correctness suite against it measures the fixture. Each
  # instrument for the question it can answer.
  if "$PYENV/bin/python" -c "import moto, pytest" 2>/dev/null; then
    "$PYENV/bin/python" -m moto.server -p 9810 >/tmp/gate_moto.log 2>&1 &
    PIDS+=($!); sleep 4
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
  verdict "cross-language cache sharing (16 checks)" $?

  # The same SSE-KMS rule on the Python path. Not a duplicate of the JVM
  # stage: the two clients SHARE one tier, so a rule only one of them enforces
  # is not a rule -- a Python reader would cache the very plaintext the JVM
  # reader refuses to cache.
  start_svcs 9318 "--sse-kms"
  python3 tools/counting_s3.py --port 9319 --objects 4 --object-bytes 1048576 \
      >/tmp/gate_origin_9319.log 2>&1 &
  PIDS+=($!)
  sleep 2
  "$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null 2>&1
  ( cd python && run_bounded "$PYENV/bin/python" sse_kms_suite.py \
      http://127.0.0.1:9318 http://127.0.0.1:9319 redis://127.0.0.1:$TIER_PORT \
    ) >/tmp/gate_ssekms_py.log 2>&1
  verdict "SSE-KMS on the python path (11 checks)" $?
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
[ $FAIL -eq 0 ] && echo "   GATE PASSED" || echo "   GATE FAILED"
exit $FAIL
