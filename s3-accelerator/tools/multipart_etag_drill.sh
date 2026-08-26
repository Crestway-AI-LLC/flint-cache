#!/usr/bin/env bash
# Multipart ETags, which every fixture object had silently avoided.
#
# S3 does not report an MD5 for a multipart upload. It reports
# "<md5-of-the-concatenated-part-md5s>-<partcount>", and ANYTHING Spark writes
# at size arrives that way -- which is precisely the workload this product
# targets. D3 content-addresses cache entries BY ETag, so that string flows
# straight into key derivation, and until now every object the suites ever saw
# was single-part.
#
# Correct bytes alone would not prove this works: a client that dropped the
# ETag entirely, or normalised the suffix away, would still return correct
# bytes while collapsing two distinct objects onto one key. So the drill
# asserts the KEY SHAPE, with the single-part run as its control.
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
MP=${MP_PORT:-9528}; SP=${SP_PORT:-9529}
# Its own tier, on its own port. This used to start a tier on the gate's 9399
# -- adopting it, since --daemonize returns 0 on a failed bind -- and then
# SHUT IT DOWN from its EXIT trap. Every later gate stage depended on that
# tier. It survived only because start_svcs silently restarted one, so the
# breakage looked like nothing at all.
TIER_PORT=${TIER_PORT:-9399}
TIER_URI="redis://127.0.0.1:$TIER_PORT"
PASS=0; FAIL=0
ck() { if [ "$1" = 0 ]; then PASS=$((PASS+1)); printf "[ok] %s\n" "$2";
       else FAIL=$((FAIL+1)); printf "[FAIL] %s\n" "$2"; fi; }
# See cross_language_drill.sh: `kill ${VAR:-0}` becomes `kill 0` when unset,
# which signals the whole process group including the calling shell.
cleanup() {
  [ -n "${A:-}" ] && kill "$A" 2>/dev/null
  [ -n "${B:-}" ] && kill "$B" 2>/dev/null
  [ -n "${TIER_PID:-}" ] && kill "$TIER_PID" 2>/dev/null
  return 0
}
trap cleanup EXIT

[ -n "$TIER_SERVER" ] && [ -n "$TIER_CLI" ] || { echo "SKIP: no redis/valkey server on PATH"; exit 0; }
CP_FILE="${CP_FILE:-/tmp/gate_cp.txt}"
[ -f "$CP_FILE" ] || { echo "SKIP: no maven classpath"; exit 0; }
CP="$ROOT/jvm-spike/target/classes:$(cat "$CP_FILE")"

python3 - "$TIER_PORT" <<'FREE' || { echo "FAIL: port $TIER_PORT in use -- refusing to adopt a tier this drill did not start."; exit 1; }
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
  >/tmp/mp_tier.log 2>&1 & TIER_PID=$!
for _ in $(seq 1 40); do "$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
"$TIER_CLI" -p "$TIER_PORT" ping >/dev/null 2>&1 \
  || { echo "FAIL: tier never answered on $TIER_PORT"; exit 1; }
TIER_OWNED=$("$TIER_CLI" -p "$TIER_PORT" info server 2>/dev/null | tr -d '\r' | awk -F: '/^process_id:/{print $2}')
[ "$TIER_OWNED" = "$TIER_PID" ] \
  || { echo "FAIL: tier on $TIER_PORT has pid $TIER_OWNED, we started $TIER_PID -- not ours."; exit 1; }
python3 "$ROOT/tools/counting_s3.py" --port "$MP" --objects 4 --object-bytes 1048576 \
  --multipart-parts 4 >/tmp/mp_origin.log 2>&1 & A=$!
python3 "$ROOT/tools/counting_s3.py" --port "$SP" --objects 4 --object-bytes 1048576 \
  >/tmp/sp_origin.log 2>&1 & B=$!
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
wait_http "http://127.0.0.1:$MP/__stats" || exit 1
wait_http "http://127.0.0.1:$SP/__stats" || exit 1

probe() { java -cp "$CP" ai.crestway.flintaccel.client.CrossLangProbe \
          "http://127.0.0.1:$1" bucket data/000001.bin 100000 "$TIER_URI" >/dev/null 2>&1; }
nkeys() { "$TIER_CLI" -p "$TIER_PORT" --scan --pattern "$1" | wc -l | tr -d ' '; }

# --- 1. the ETag really is multipart-shaped at the origin -------------------
ET=$(curl -sI "http://127.0.0.1:$MP/bucket/data/000001.bin" | tr -d '\r' | grep -i '^etag' | cut -d' ' -f2)
[[ "$ET" == *-4* ]]; ck $? "armed: the origin reports a multipart ETag ($ET)"
ETSP=$(curl -sI "http://127.0.0.1:$SP/bucket/data/000001.bin" | tr -d '\r' | grep -i '^etag' | cut -d' ' -f2)
[[ "$ETSP" != *-4* ]]; ck $? "control: the other origin reports a plain one ($ETSP)"

# --- 2. multipart: bytes correct AND the suffix survives into the key -------
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null; probe "$MP"
T=$(nkeys 'c2/*'); S=$(nkeys 'c2/*-4}/*')
[ "$T" -gt 0 ]; ck $? "armed: the multipart read populated the tier ($T chunks)"
# The pattern ends `-4}` not `-4/`: the etag now sits inside a hash tag, so the
# multipart suffix is the last thing before the closing brace rather than before
# the slash. Colocating an object's chunks in one slot is what makes MGET safe
# on a multi-pair fleet, and it moved this key shape.
[ "$S" -eq "$T" ]; ck $? "EVERY key carries the -N suffix ($S of $T) -- the ETag is not being mangled"
python3 - "$ROOT" <<'PY'
import hashlib, sys
b = open("/tmp/xlang_java_bytes", "rb").read()
w = bytes(hashlib.md5(f"data/000001.bin:0:{a//16}".encode()).digest()[a % 16]
          for a in range(len(b)))
sys.exit(0 if b == w and len(b) == 100000 else 1)
PY
ck $? "and the bytes are correct under a multipart ETag"

# --- 3. the control. Without it, "0 suffixed keys" would be satisfied by a
#        run that cached nothing at all -- which is how a dead origin looks.
"$TIER_CLI" -p "$TIER_PORT" flushall >/dev/null; probe "$SP"
T2=$(nkeys 'c2/*'); S2=$(nkeys 'c2/*-4/*')
[ "$T2" -gt 0 ]; ck $? "armed: the single-part read populated the tier ($T2 chunks)"
[ "$S2" -eq 0 ]; ck $? "control: NO key carries a suffix under single-part ETags ($S2)"

echo "--- $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
