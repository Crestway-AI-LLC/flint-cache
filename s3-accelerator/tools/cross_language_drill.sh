#!/usr/bin/env bash
# One tier, two languages.
#
# The claim under test is the one that makes the Python path a FEATURE rather
# than a second product: a Spark job and a PyTorch job over the same dataset
# pay S3 once between them, not once each.
#
# Nothing else could have caught the bug this drill was written for. Every JVM
# suite ran against a JVM-populated tier and every Python suite against a
# Python-populated one, so both stayed green while the two clients wrote
# different key prefixes and different value formats -- separate copies of
# identical bytes, double the S3 bill, and invisible to any suite that only
# ever speaks one language.
#
# READAHEAD IS ASYMMETRIC and the assertions below respect it. s3fs hands the
# Python client ~4 MiB per read where AAL hands the JVM client ~128 KiB, so
# "both directions cost zero" is FALSE and asserting it would be asserting a
# wish. The honest claims are: the narrow reader pays nothing against a wide
# reader's cache, and the wide reader genuinely reuses what the narrow one left.
#
# Counts are DELTAS, never absolutes. A delta is right even when a reset is
# not -- and a reset silently failing is exactly how the first version of this
# drill produced three false failures.
set -uo pipefail

# Valkey or Redis, whichever this machine has. The suites need a
# Redis-protocol server and do not care which implementation provides it --
# and hardcoding `"$TIER_SERVER"` made the gate unrunnable on any CI image
# where it is not packaged, which is most of them. Same flags work for both.
TIER_SERVER="$(command -v valkey-server || command -v redis-server || true)"
TIER_CLI="$(command -v valkey-cli || command -v redis-cli || true)"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
export PATH="$JAVA_HOME/bin:$PATH"
PYENV="${FLINT_PYENV:-$ROOT/python/.venv}"
PORT=${PORT:-9407}
# The tier port is a VARIABLE, because two harnesses on one machine must not
# share a tier. Every assertion below starts with `flushall`, so an adopted
# tier is not merely shared -- it is destroyed mid-run by whichever run
# flushes first, and both then measure each other's garbage.
TIER_PORT=${TIER_PORT:-9399}
TIER_URI="redis://127.0.0.1:$TIER_PORT"
PASS=0; FAIL=0
ck() { if [ "$1" = 0 ]; then PASS=$((PASS+1)); printf "[ok] %s\n" "$2";
       else FAIL=$((FAIL+1)); printf "[FAIL] %s\n" "$2"; fi; }
# `kill ${VAR:-0}` is a process-group suicide: when the variable is unset the
# command becomes `kill 0`, which signals EVERY process in the current process
# group -- the calling shell included. It only fires on the paths that exit
# before the pid is assigned, so it survived every local run and killed the CI
# job with exit 143 the first time a SKIP guard tripped.
cleanup() {
  [ -n "${ORIGIN_PID:-}" ] && kill "$ORIGIN_PID" 2>/dev/null
  # Kill the tier we started, by pid, rather than telling whatever answers on
  # the port to shut down. `--daemonize yes` used to be how this started, which
  # reparents the server to init -- so any abnormal exit left a tier running
  # forever, and the NEXT run adopted it instead of starting its own.
  [ -n "${TIER_PID:-}" ] && kill "$TIER_PID" 2>/dev/null
  return 0
}
trap cleanup EXIT

# Tests the RESOLVED server, not `valkey-server` -- which is what it used to
# test, so a redis-only machine skipped the whole drill while TIER_SERVER sat
# there correctly pointing at redis-server. A skip that reads as a pass.
[ -n "$TIER_SERVER" ] && [ -n "$TIER_CLI" ] || { echo "SKIP: no redis/valkey server on PATH"; exit 0; }
[ -x "$PYENV/bin/python" ] || { echo "SKIP: no python env"; exit 0; }
[ -f /tmp/cp.txt ] || { echo "SKIP: no maven classpath at /tmp/cp.txt"; exit 0; }

# `java` on PATH is the macOS stub on this machine: it prints "Unable to locate
# a Java Runtime" and exits nonzero. jread() sends output to /dev/null, so a
# stub java costs ZERO origin requests -- and "0 origin GETs, want 0" is
# precisely what a perfect cache hit looks like. The negative controls below DO
# catch it, but they report it as "the cache is not working", which sends the
# reader after the wrong bug. Resolve a java that actually runs, and name the
# problem if none does.
if [ -z "${JAVA:-}" ]; then
  for c in ${JAVA_HOME:+"$JAVA_HOME/bin/java"} java /opt/homebrew/opt/openjdk/bin/java; do
    "$c" -version >/dev/null 2>&1 && { JAVA="$c"; break; }
  done
fi
[ -n "${JAVA:-}" ] || { echo "SKIP: no java that runs (PATH java may be the macOS stub)"; exit 0; }

# Prove the port is free BEFORE binding it. `--daemonize yes` returns 0
# whether or not the bind succeeded, so a taken port produced a "successful"
# start and a drill that silently adopted a stranger's tier -- then flushed it.
# Bind-to-prove-it rather than parse a listing, and FAIL rather than skip: a
# busy port is a collision to resolve, not a reason to report nothing.
python3 - "$TIER_PORT" <<'FREE' || { echo "FAIL: port $TIER_PORT in use -- refusing to adopt a tier this drill did not start. Set TIER_PORT to a free port."; exit 1; }
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
FREE
"$TIER_SERVER" --port "$TIER_PORT" --save "" --appendonly no \
  >/tmp/xlang_tier.log 2>&1 & TIER_PID=$!
for _ in $(seq 1 40); do "$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
"$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 \
  || { echo "FAIL: tier never answered on $TIER_PORT (see /tmp/xlang_tier.log)"; exit 1; }
# And prove the server answering is the one we started, not a survivor that won
# the race between the bind test and the launch.
TIER_OWNED=$("$TIER_CLI" -p "$TIER_PORT" info server 2>/dev/null | tr -d '\r' | awk -F: '/^process_id:/{print $2}')
[ "$TIER_OWNED" = "$TIER_PID" ] \
  || { echo "FAIL: tier on $TIER_PORT has pid $TIER_OWNED, we started $TIER_PID -- not ours."; exit 1; }
# Wait for the origin to ANSWER rather than for seconds to pass. `sleep 3` is
# not a weak readiness signal, it is NO signal: it asserts three seconds is
# enough on every machine this will ever run on, which stops being true the
# moment the box is loaded. And the obvious repair -- a longer sleep -- only
# helps when the thing waited on is genuinely slow rather than not yet started.
wait_http() { # url [tries]
  local i
  for i in $(seq 1 "${2:-150}"); do
    curl -sf -o /dev/null "$1" 2>/dev/null && return 0
    sleep 0.1
  done
  echo "FAIL: fixture never answered: $1" >&2
  return 1
}
python3 "$ROOT/tools/counting_s3.py" --port "$PORT" --objects 4 --object-bytes 4194304 \
  >/tmp/xlang_origin.log 2>&1 & ORIGIN_PID=$!
wait_http "http://127.0.0.1:$PORT/__stats" || exit 1
CP="$ROOT/jvm-spike/target/classes:$(cat /tmp/cp.txt)"
KEY="data/000001.bin"; LEN=100000

st() { curl -s "http://127.0.0.1:$PORT/__stats" \
       | python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])"; }
jread() { "$JAVA" -cp "$CP" ai.crestway.flintaccel.client.CrossLangProbe \
          "http://127.0.0.1:$PORT" bucket "$KEY" "$LEN" "$TIER_URI" >/dev/null 2>&1; }
# Same probe, arbitrary key and length -- section 5 rewrites one object at two
# different sizes, so neither can be the file-scoped $KEY/$LEN.
jread_key() { "$JAVA" -cp "$CP" ai.crestway.flintaccel.client.CrossLangProbe \
          "http://127.0.0.1:$PORT" bucket "$1" "$2" "$TIER_URI" >/dev/null 2>&1; }
# Writes through the PYTHON client, so s3fs's own mutation path runs and our
# invalidate_cache hook fires exactly as it would for a customer.
pywrite() {
  "$PYENV/bin/python" - "$ROOT" "http://127.0.0.1:$PORT" "$1" "$2" "$3" "$TIER_URI" \
    >/dev/null 2>/tmp/xlang_pw.err <<'PY'
import sys
sys.path.insert(0, sys.argv[1] + "/python")
import flint_accel
so = dict(anon=False, key="p", secret="p",
          client_kwargs={"endpoint_url": sys.argv[2], "region_name": "us-east-1"},
          tier_uri=sys.argv[6])
f = flint_accel.FlintS3FileSystem(skip_instance_cache=True, **so)
f.pipe_file("s3://bucket/" + sys.argv[3], sys.argv[5].encode() * int(sys.argv[4]))
PY
}
pyread() {
  "$PYENV/bin/python" - "$ROOT" "http://127.0.0.1:$PORT" "$KEY" "$LEN" "$TIER_URI" \
    >/tmp/xlang_py.json 2>/tmp/xlang_py.err <<'PY'
import json, sys
sys.path.insert(0, sys.argv[1] + "/python")
import flint_accel
so = dict(anon=False, key="p", secret="p",
          client_kwargs={"endpoint_url": sys.argv[2], "region_name": "us-east-1"},
          tier_uri=sys.argv[5])
f = flint_accel.FlintS3FileSystem(skip_instance_cache=True, **so)
with f.open("s3://bucket/" + sys.argv[3], "rb") as h:
    h.seek(0)
    b = h.read(int(sys.argv[4]))
open("/tmp/xlang_py_bytes", "wb").write(b)
print(json.dumps(dict(f.counters)))
PY
}
pyc() { python3 -c "import json;print(json.load(open('/tmp/xlang_py.json')).get('$1',0))" 2>/dev/null || echo 0; }

# --- 1. the seal, or nothing below can work ---------------------------------
JV=$("$JAVA" -cp "$CP" ai.crestway.flintaccel.client.SealVector 2>/dev/null | tail -1)
PV=$("$PYENV/bin/python" -c "
import sys; sys.path.insert(0,'$ROOT/python')
from flint_accel.tier import FlintTier
print(FlintTier._seal_of('\"abc123\"', 7, b'flint-interop-vector'))" 2>/dev/null)
[ -n "$JV" ] && [ "$JV" = "$PV" ]; ck $? "the seal agrees across languages (java=$JV python=$PV)"

# --- 2. wide writer, narrow reader: must cost NOTHING ------------------------
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null
pyread
PK=$("$TIER_CLI" -p "$TIER_PORT" --scan --pattern 'c2/*' | wc -l | tr -d ' ')
[ "$PK" -gt 0 ]; ck $? "armed: the Python client populated the tier ($PK chunks)"
# Metadata is the OTHER half of the shared keyspace, and nothing here asserted
# it. That is how the two clients came to write `m/bucket/key` and
# `m1/s3://bucket/key` -- different prefix AND different path form -- while
# every chunk assertion above stayed green. D3 puts most of the request saving
# in metadata, so the unasserted half was carrying most of the unclaimed bill.
PM=$("$TIER_CLI" -p "$TIER_PORT" --scan --pattern 'm1/*' | sort)
PMK=$(printf '%s\n' "$PM" | grep -c . || true)
[ "${PMK:-0}" -gt 0 ]; ck $? "armed: the Python client populated METADATA ($PMK entries)"
B=$(st gets); HB=$(st heads); jread; A=$(st gets); HA=$(st heads)
[ $((A-B)) -eq 0 ]; ck $? "THE JVM READS PYTHON'S CACHE FOR FREE ($((A-B)) origin GETs, want 0)"
[ $((HA-HB)) -eq 0 ]; ck $? "AND PYTHON'S METADATA TOO ($((HA-HB)) origin HEADs, want 0)"

"$PYENV/bin/python" -c "
import hashlib,sys
b=open('/tmp/xlang_java_bytes','rb').read()
w=bytes(hashlib.md5(f'$KEY:0:{a//16}'.encode()).digest()[a%16] for a in range(len(b)))
sys.exit(0 if b==w and len(b)==$LEN else 1)"
ck $? "and the bytes the JVM got are correct, not merely cheap"

# --- 3. narrow writer, wide reader: must REUSE, cannot be free ---------------
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null
jread
JK=$("$TIER_CLI" -p "$TIER_PORT" --scan --pattern 'c2/*' | wc -l | tr -d ' ')
[ "$JK" -gt 0 ]; ck $? "armed: the JVM client populated the tier ($JK chunks)"
JM=$("$TIER_CLI" -p "$TIER_PORT" --scan --pattern 'm1/*' | sort)
JMK=$(printf '%s\n' "$JM" | grep -c . || true)
[ "${JMK:-0}" -gt 0 ]; ck $? "armed: the JVM client populated METADATA ($JMK entries)"
# The assertion that would have named the original bug on sight. The two
# behavioural checks bracketing it prove that sharing WORKS; this one says why
# when it does not, by putting the two key sets against each other.
[ "$PM" = "$JM" ]; ck $? "both languages write the SAME metadata key"
HB=$(st heads); pyread; HA=$(st heads)
[ $((HA-HB)) -eq 0 ]; ck $? "PYTHON READS THE JVM'S METADATA FOR FREE ($((HA-HB)) origin HEADs, want 0)"
HITS=$(pyc chunk_hits)
[ "${HITS:-0}" -ge "$JK" ]; ck $? "PYTHON REUSES EVERY CHUNK THE JVM LEFT ($HITS hits >= $JK cached)"
"$PYENV/bin/python" -c "
import hashlib,sys
b=open('/tmp/xlang_py_bytes','rb').read()
w=bytes(hashlib.md5(f'$KEY:0:{a//16}'.encode()).digest()[a%16] for a in range(len(b)))
sys.exit(0 if b==w and len(b)==$LEN else 1)"
ck $? "and the bytes Python got are correct"

# --- 4. negative controls ----------------------------------------------------
# Without these, a broken origin counter or a client that never ran would
# produce the same all-zero output as perfect sharing.
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null
B=$(st gets); HB=$(st heads); jread; A=$(st gets); HA=$(st heads)
[ $((A-B)) -gt 0 ]; ck $? "negative control: a COLD JVM read DOES hit the origin ($((A-B)) GETs)"
# Without this, "0 HEADs when warm" would also be the reading for a client
# that never HEADs at all, and both metadata assertions would pass vacuously.
[ $((HA-HB)) -gt 0 ]; ck $? "negative control: a COLD JVM read DOES HEAD the origin ($((HA-HB)) HEADs)"
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null
HB=$(st heads); pyread; HA=$(st heads)
CM=$(pyc chunk_misses)
[ "${CM:-0}" -gt 0 ]; ck $? "negative control: a COLD Python read DOES miss ($CM misses)"
[ $((HA-HB)) -gt 0 ]; ck $? "negative control: a COLD Python read DOES HEAD the origin ($((HA-HB)) HEADs)"

# --- 5. a rewrite in ONE language must not leave the other reading stale ----
#
# Both clients invalidate on write -- Python through s3fs's invalidate_cache,
# the JVM through FlintS3AFileSystem.create/delete/rename -- and both delete
# the SAME m1/ key. That the two therefore protect each other's readers is an
# inference, and it had never once been executed.
#
# It matters more than ordinary staleness because the entry carries the object
# LENGTH and the ETAG. A reader trusting a stale entry addresses chunks by the
# OLD etag, hits them, and returns the ENTIRE PREVIOUS OBJECT -- not a short
# read and not a torn one: a coherent, wrong, confidently-served answer.
XK="scratch/xlang_rewrite.bin"
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null
pywrite "$XK" 40000 L
jread_key "$XK" 40000
LB=$(head -c 40000 /tmp/xlang_java_bytes 2>/dev/null | tr -d 'L' | wc -c | tr -d ' ')
[ "${LB:-1}" = 0 ]; ck $? "armed: the JVM reads what Python wrote (40000 bytes, all L)"
MK=$("$TIER_CLI" -p "$TIER_PORT" --scan --pattern 'm1/*' | grep -c "$XK" || true)
[ "${MK:-0}" -gt 0 ]; ck $? "armed: and that read cached its metadata ($MK entry)"

pywrite "$XK" 900 S
MK2=$("$TIER_CLI" -p "$TIER_PORT" --scan --pattern 'm1/*' | grep -c "$XK" || true)
[ "${MK2:-1}" = 0 ]; ck $? "PYTHON'S REWRITE DROPPED THE SHARED METADATA ENTRY"
jread_key "$XK" 900
SB=$(head -c 900 /tmp/xlang_java_bytes 2>/dev/null | tr -d 'S' | wc -c | tr -d ' ')
[ "${SB:-1}" = 0 ]; ck $? "AND THE JVM READS THE NEW OBJECT, not the old one it had cached"

echo "--- $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
