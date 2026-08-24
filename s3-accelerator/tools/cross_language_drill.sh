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
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
export PATH="$JAVA_HOME/bin:$PATH"
PYENV="${FLINT_PYENV:-$ROOT/python/.venv}"
PORT=${PORT:-9407}
PASS=0; FAIL=0
ck() { if [ "$1" = 0 ]; then PASS=$((PASS+1)); printf "[ok] %s\n" "$2";
       else FAIL=$((FAIL+1)); printf "[FAIL] %s\n" "$2"; fi; }
cleanup() { kill ${ORIGIN_PID:-0} 2>/dev/null; valkey-cli -p 6399 shutdown nosave 2>/dev/null; }
trap cleanup EXIT

command -v valkey-server >/dev/null || { echo "SKIP: no valkey-server"; exit 0; }
[ -x "$PYENV/bin/python" ] || { echo "SKIP: no python env"; exit 0; }
[ -f /tmp/cp.txt ] || { echo "SKIP: no maven classpath at /tmp/cp.txt"; exit 0; }

valkey-server --port 6399 --save "" --appendonly no --daemonize yes
python3 "$ROOT/tools/counting_s3.py" --port "$PORT" --objects 4 --object-bytes 4194304 \
  >/tmp/xlang_origin.log 2>&1 & ORIGIN_PID=$!
sleep 3
CP="$ROOT/jvm-spike/target/classes:$(cat /tmp/cp.txt)"
KEY="data/000001.bin"; LEN=100000

st() { curl -s "http://127.0.0.1:$PORT/__stats" \
       | python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])"; }
jread() { java -cp "$CP" ai.crestway.flintaccel.client.CrossLangProbe \
          "http://127.0.0.1:$PORT" bucket "$KEY" "$LEN" >/dev/null 2>&1; }
pyread() {
  "$PYENV/bin/python" - "$ROOT" "http://127.0.0.1:$PORT" "$KEY" "$LEN" \
    >/tmp/xlang_py.json 2>/tmp/xlang_py.err <<'PY'
import json, sys
sys.path.insert(0, sys.argv[1] + "/python")
import flint_accel
so = dict(anon=False, key="p", secret="p",
          client_kwargs={"endpoint_url": sys.argv[2], "region_name": "us-east-1"},
          tier_uri="redis://127.0.0.1:6399")
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
JV=$(java -cp "$CP" ai.crestway.flintaccel.client.SealVector 2>/dev/null | tail -1)
PV=$("$PYENV/bin/python" -c "
import sys; sys.path.insert(0,'$ROOT/python')
from flint_accel.tier import FlintTier
print(FlintTier._seal_of('\"abc123\"', 7, b'flint-interop-vector'))" 2>/dev/null)
[ -n "$JV" ] && [ "$JV" = "$PV" ]; ck $? "the seal agrees across languages (java=$JV python=$PV)"

# --- 2. wide writer, narrow reader: must cost NOTHING ------------------------
valkey-cli -p 6399 flushall >/dev/null
pyread
PK=$(valkey-cli -p 6399 --scan --pattern 'c1/*' | wc -l | tr -d ' ')
[ "$PK" -gt 0 ]; ck $? "armed: the Python client populated the tier ($PK chunks)"
B=$(st gets); jread; A=$(st gets)
[ $((A-B)) -eq 0 ]; ck $? "THE JVM READS PYTHON'S CACHE FOR FREE ($((A-B)) origin GETs, want 0)"

"$PYENV/bin/python" -c "
import hashlib,sys
b=open('/tmp/xlang_java_bytes','rb').read()
w=bytes(hashlib.md5(f'$KEY:0:{a//16}'.encode()).digest()[a%16] for a in range(len(b)))
sys.exit(0 if b==w and len(b)==$LEN else 1)"
ck $? "and the bytes the JVM got are correct, not merely cheap"

# --- 3. narrow writer, wide reader: must REUSE, cannot be free ---------------
valkey-cli -p 6399 flushall >/dev/null
jread
JK=$(valkey-cli -p 6399 --scan --pattern 'c1/*' | wc -l | tr -d ' ')
[ "$JK" -gt 0 ]; ck $? "armed: the JVM client populated the tier ($JK chunks)"
pyread
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
valkey-cli -p 6399 flushall >/dev/null
B=$(st gets); jread; A=$(st gets)
[ $((A-B)) -gt 0 ]; ck $? "negative control: a COLD JVM read DOES hit the origin ($((A-B)) GETs)"
valkey-cli -p 6399 flushall >/dev/null
pyread
CM=$(pyc chunk_misses)
[ "${CM:-0}" -gt 0 ]; ck $? "negative control: a COLD Python read DOES miss ($CM misses)"

echo "--- $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
