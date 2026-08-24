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
PASS=0; FAIL=0
ck() { if [ "$1" = 0 ]; then PASS=$((PASS+1)); printf "[ok] %s\n" "$2";
       else FAIL=$((FAIL+1)); printf "[FAIL] %s\n" "$2"; fi; }
cleanup() { kill ${A:-0} ${B:-0} 2>/dev/null; "$TIER_CLI" -p 6399 shutdown nosave 2>/dev/null; }
trap cleanup EXIT

command -v valkey-server >/dev/null || { echo "SKIP: no "$TIER_SERVER""; exit 0; }
CP_FILE="${CP_FILE:-/tmp/gate_cp.txt}"
[ -f "$CP_FILE" ] || { echo "SKIP: no maven classpath"; exit 0; }
CP="$ROOT/jvm-spike/target/classes:$(cat "$CP_FILE")"

"$TIER_SERVER" --port 6399 --save '' --appendonly no --daemonize yes
python3 "$ROOT/tools/counting_s3.py" --port "$MP" --objects 4 --object-bytes 1048576 \
  --multipart-parts 4 >/tmp/mp_origin.log 2>&1 & A=$!
python3 "$ROOT/tools/counting_s3.py" --port "$SP" --objects 4 --object-bytes 1048576 \
  >/tmp/sp_origin.log 2>&1 & B=$!
sleep 3

probe() { java -cp "$CP" ai.crestway.flintaccel.client.CrossLangProbe \
          "http://127.0.0.1:$1" bucket data/000001.bin 100000 >/dev/null 2>&1; }
nkeys() { "$TIER_CLI" -p 6399 --scan --pattern "$1" | wc -l | tr -d ' '; }

# --- 1. the ETag really is multipart-shaped at the origin -------------------
ET=$(curl -sI "http://127.0.0.1:$MP/bucket/data/000001.bin" | tr -d '\r' | grep -i '^etag' | cut -d' ' -f2)
[[ "$ET" == *-4* ]]; ck $? "armed: the origin reports a multipart ETag ($ET)"
ETSP=$(curl -sI "http://127.0.0.1:$SP/bucket/data/000001.bin" | tr -d '\r' | grep -i '^etag' | cut -d' ' -f2)
[[ "$ETSP" != *-4* ]]; ck $? "control: the other origin reports a plain one ($ETSP)"

# --- 2. multipart: bytes correct AND the suffix survives into the key -------
"$TIER_CLI" -p 6399 flushall >/dev/null; probe "$MP"
T=$(nkeys 'c1/*'); S=$(nkeys 'c1/*-4/*')
[ "$T" -gt 0 ]; ck $? "armed: the multipart read populated the tier ($T chunks)"
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
"$TIER_CLI" -p 6399 flushall >/dev/null; probe "$SP"
T2=$(nkeys 'c1/*'); S2=$(nkeys 'c1/*-4/*')
[ "$T2" -gt 0 ]; ck $? "armed: the single-part read populated the tier ($T2 chunks)"
[ "$S2" -eq 0 ]; ck $? "control: NO key carries a suffix under single-part ETags ($S2)"

echo "--- $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
