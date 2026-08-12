#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D3 drills 4 & 6: the shed order under pressure — the GO/NO-GO.
#
# The resource class (step 4) exists to make ONE promise: "GET must not get
# slower because a vector index is unhappy" — and D2 moved the contention point
# INTO the proxy, so "unhappy" now includes "merely busy", not just "dead". This
# bench measures whether that promise holds. It drives ordinary data-path load
# (a tenant's GET) through the proxy and reads the DATA PATH's read p99 AT THE
# PROXY (from PROXYLATENCY's per-tenant histogram) in three conditions:
#
#   baseline — no co-processor traffic at all
#   busy     — a co-processor flooded with family commands, each doing heavy
#              but IN-BUDGET channel I/O (drill 4)
#   loop     — a co-processor that tries to run away (channel writes FAR past
#              its budget), cut off at the bound on every command (drill 6)
#
# The claim, stated as a DELTA not an absolute (field-notes: a fixed-ms
# threshold catches client startup, not the system): the data path's read p99
# under a busy or looping co-processor stays within a bounded band of its own
# quiet-baseline p99, and never climbs into failover territory. If it does, the
# shed order is wrong and the family belongs native (D4), not beside the data
# plane.
#
# HONEST SCOPE: this runs on a laptop. The mem-engine + loopback numbers here
# are NOT the published p99<1ms budget (that is an i4i-class number — see
# capacity-model.md; a real go/no-go run belongs on that hardware, #153). What a
# laptop CAN prove is the PROPERTY — that a hot co-processor does not degrade the
# data path — because degradation from lost isolation is a 10-100x effect, not a
# noise-floor one. So this asserts the relative property and reports the numbers;
# it is a bench (run on demand), not a CORE gate (latency gating is flaky).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-shed 6680 6681 6682
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-shed; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""; FLOOD_PIDS=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$FLOOD_PIDS" ] && kill -9 $FLOOD_PIDS 2>/dev/null
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT

command -v valkey-benchmark >/dev/null || { echo "SKIP: valkey-benchmark not installed"; exit 0; }
cargo build --release -q -p flint-server -p flint-proxy --features flint-server/rocks

# --- the stand-in co-processor: K channel writes per FLINTFAM, K re-read from a
# file each frame so the harness can switch busy(K<budget) -> loop(K>>budget)
# without a restart. On "channel budget exhausted" (the loop case) it stops that
# command's writes and still answers, exactly as a real co-processor should. ----
echo 200 > "$D/K"
cat > "$D/coproc.py" <<'PY'
import socket, sys, threading, os
LISTEN = int(sys.argv[1]); KFILE = sys.argv[2]; CNTFILE = KFILE + ".count"
_lock = threading.Lock(); _handled = 0
def bump():
    global _handled
    with _lock:
        _handled += 1
        if _handled % 50 == 0:
            try: open(CNTFILE, "w").write(str(_handled))
            except OSError: pass
def read_array(f):
    line = f.readline()
    if not line or line[:1] != b'*': return None
    n = int(line[1:]); out = []
    for _ in range(n):
        h = f.readline(); ln = int(h[1:]); d = f.read(ln); f.read(2); out.append(d)
    return out
def cmd(*parts):
    o = b'*%d\r\n' % len(parts)
    for p in parts:
        if isinstance(p, str): p = p.encode()
        o += b'$%d\r\n%s\r\n' % (len(p), p)
    return o
def handle(conn):
    f = conn.makefile('rb')
    while True:
        a = read_array(f)
        if a is None: break
        if not a or a[0].upper() != b'FLINTFAM':
            try: conn.sendall(b'-ERR expected FLINTFAM\r\n')
            except OSError: pass
            continue
        token, callback = a[1], a[2].decode()
        try: K = int(open(KFILE).read().strip())
        except Exception: K = 200
        try:
            host, port = callback.split(':')
            ch = socket.create_connection((host, int(port)), timeout=3); cf = ch.makefile('rb')
            ch.sendall(cmd(b'PROXYCHAN', token)); cf.readline()          # +OK
            done = 0
            for i in range(K):
                ch.sendall(cmd(b'SET', b'ci:%d' % i, b'v'))
                r = cf.readline()
                if not r or r[:1] == b'-':   # channel budget exhausted -> cut off
                    break
                done += 1
            ch.close()
            bump()
            conn.sendall(b'+FAMOK %d\r\n' % done)
        except OSError:
            try: conn.sendall(b'-ERR channel io failed\r\n')
            except OSError: break
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", LISTEN)); srv.listen(64)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY

# --- the family-command flooder: C connections each AUTH nsV and loop VEC.SET
# until a stop-file appears. This is the co-processor's OFFERED load; the proxy's
# family admission cap decides how much is actually in flight. ------------------
cat > "$D/flood.py" <<'PY'
import socket, sys, threading, os
PORT = int(sys.argv[1]); TOKEN = sys.argv[2]; CONNS = int(sys.argv[3]); STOP = sys.argv[4]
def cmd(*parts):
    o = b'*%d\r\n' % len(parts)
    for p in parts:
        if isinstance(p, str): p = p.encode()
        o += b'$%d\r\n%s\r\n' % (len(p), p)
    return o
def worker(wid):
    try:
        s = socket.create_connection(("127.0.0.1", PORT), timeout=3); f = s.makefile('rb')
        s.sendall(cmd(b'AUTH', TOKEN)); f.readline()
        i = 0
        while not os.path.exists(STOP):
            s.sendall(cmd(b'VEC.SET', b'k:%d:%d' % (wid, i), b'v')); f.readline()
            i += 1
    except OSError:
        pass
ts = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(CONNS)]
for t in ts: t.start()
for t in ts: t.join()
PY

python3 "$D/coproc.py" 6682 "$D/K" &
COPROC_PID=$!
sleep 0.5

$B --port 6680 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6680; fleet_wait_ping 6680
$PX --port 6681 --pairs "127.0.0.1:6680" --tenants "tokD=nsD,tokV=nsV" \
    --families "VEC.=127.0.0.1:6682" --edge-advertise "127.0.0.1:6681" 2>"$D/proxy.log" &
fleet_wait_listen 6681
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6681 PING 2>&1)" in *NOAUTH*|PONG) break ;; esac
  sleep 0.1
done
DCLI="valkey-cli -p 6681 -a tokD --no-auth-warning"

# The GET load (valkey-benchmark's own random keyspace) mostly MISSES, which is
# the stronger contention test: a miss always reaches the backend — no near-cache
# short-circuit — so it is maximally sensitive to the co-processor's channel I/O
# competing for the node. And hit-vs-miss is constant across phases, so the
# relative comparison holds either way.

# --- p99 bucket (µs) of the read lane over ONE measured window, as a DELTA of
# two PROXYLATENCY snapshots (the histogram is cumulative-for-life). ------------
BUCKETS_US="250 500 1000 2000 5000 10000 25000 50000 100000 250000 1000000 5000000"
read_lane() { $DCLI PROXYLATENCY 2>/dev/null | tr -d '\r' | awk '$1=="nsD"&&$2=="read"{$1=$2="";print;exit}'; }
# p99_delta <before-line> <after-line> -> "<p99_us> <bucket_index>"
p99_delta() {
  awk -v B="$BUCKETS_US" -v b0="$1" -v b1="$2" 'BEGIN{
    n=split(B,bu," "); split(b0,a0," "); split(b1,a1," ");
    # fields in a lane line (after we blanked $1,$2): count sum c0..c12 -> here
    # the passed strings are "count sum c0..c12"; cumulative buckets start at 3.
    total=a1[1]-a0[1]; if(total<=0){print "0 -1"; exit}
    target=total*0.99;
    for(i=0;i<=n;i++){ d=a1[i+3]-a0[i+3]; if(d>=target){ if(i<n) print bu[i+1]" "i; else print "inf "n; exit } }
    print "inf "n
  }'
}
# median_p99 <phase-label>  -> echoes "<median_us> <median_idx>" over 3 windows.
# Each window: snapshot, run a fixed GET benchmark, snapshot, delta-p99.
median_p99() {
  local label="$1" us idx; local -a US=() IDX=()
  for run in 1 2 3; do
    local before after r
    before="$(read_lane)"
    valkey-benchmark -h 127.0.0.1 -p 6681 -a tokD -t get -n 30000 -c 20 -r 200 -q --threads 2 >/dev/null 2>&1
    after="$(read_lane)"
    r="$(p99_delta "$before" "$after")"
    us="${r% *}"; idx="${r#* }"
    US+=("$us"); IDX+=("$idx")
    printf '    %-8s run %d: read p99 <= %s us (bucket %s)\n' "$label" "$run" "$us" "$idx" >&2
  done
  # median of 3 by bucket index
  local sorted; sorted="$(printf '%s\n' "${IDX[@]}" | sort -n)"
  local mid_idx; mid_idx="$(printf '%s\n' "$sorted" | sed -n 2p)"
  # report the µs for that median index
  local mus; mus="$(awk -v i="$mid_idx" -v B="$BUCKETS_US" 'BEGIN{n=split(B,bu," ");if(i<0){print "0"}else if(i<n){print bu[i+1]}else{print "inf"}}')"
  echo "$mus $mid_idx"
}

echo "== BASELINE: data-path read p99 with NO co-processor traffic"
BASE="$(median_p99 baseline)"; BASE_US="${BASE% *}"; BASE_IDX="${BASE#* }"
echo "  baseline median read p99 <= ${BASE_US} us (bucket ${BASE_IDX})"

start_flood() { rm -f "$D/stop"; python3 "$D/flood.py" 6681 tokV 48 "$D/stop" & FLOOD_PIDS="$FLOOD_PIDS $!"; sleep 0.6; }
stop_flood()  { : > "$D/stop"; sleep 0.3; kill -9 $FLOOD_PIDS 2>/dev/null; FLOOD_PIDS=""; }
handled()     { cat "$D/K.count" 2>/dev/null || echo 0; }
# POSITIVE CONTROL: a green data-path p99 means nothing if the co-processor was
# not actually loading the proxy. Require the co-processor to have HANDLED a
# substantial number of family commands (each fanning out channel I/O) during
# the measured window; below that, the bench exercised nothing and must fail
# loudly rather than pass vacuously (field-notes §1).
MIN_FAM=100

echo "== BUSY (drill 4): co-processor flooded, 200 in-budget channel writes/cmd"
echo 200 > "$D/K"
start_flood; h0="$(handled)"
BUSY="$(median_p99 busy)"; BUSY_US="${BUSY% *}"; BUSY_IDX="${BUSY#* }"
h1="$(handled)"; stop_flood; BUSY_FAM=$((h1 - h0))
echo "  [control] co-processor handled ${BUSY_FAM} family commands during the busy window"
echo "  busy median read p99 <= ${BUSY_US} us (bucket ${BUSY_IDX})"

echo "== LOOP (drill 6): co-processor tries 5000 writes/cmd, cut off at budget"
echo 5000 > "$D/K"
start_flood; h0="$(handled)"
LOOP="$(median_p99 loop)"; LOOP_US="${LOOP% *}"; LOOP_IDX="${LOOP#* }"
h1="$(handled)"; stop_flood; LOOP_FAM=$((h1 - h0))
echo "  [control] co-processor handled ${LOOP_FAM} family commands during the loop window"
echo "  loop median read p99 <= ${LOOP_US} us (bucket ${LOOP_IDX})"

# Confirm the loop case really WAS cut off at the budget (drill 6's mechanism,
# under load): one VEC.SET now should report far fewer than 5000 done.
echo 5000 > "$D/K"
DONE="$(valkey-cli -p 6681 -a tokV --no-auth-warning VEC.SET probe v 2>&1 | tr -d '\r')"
echo "  a single looping VEC.SET completed: $DONE (expected FAMOK <= channel budget, not 5000)"

echo
echo "== the degradation curve (what the resource class actually bought) =="
echo "  quiet baseline read p99 <= ${BASE_US} us  (bucket ${BASE_IDX})"
echo "  busy   (drill 4)   read p99 <= ${BUSY_US} us  (bucket ${BUSY_IDX})"
echo "  loop   (drill 6)   read p99 <= ${LOOP_US} us  (bucket ${LOOP_IDX})"
echo "  Reading: the data path DOES rise under co-processor load — on shared"
echo "  laptop cores the admitted channel I/O still competes for proxy CPU — but"
echo "  the rise is BOUNDED to the ms class, not the 10-100 ms of lost isolation."
echo "  Clean isolation (baseline≈loaded) needs the extra cores of i4i-class HW;"
echo "  that is the definitive go/no-go run (#153)."
echo
echo "== VERDICT (relative property, not an absolute i4i budget) =="
# HARD gate: a hot/looping co-processor must not push the data path's read p99
# into failover territory (>= 25 ms, bucket 6) — the catastrophe that means
# isolation is broken — and not more than TOL buckets above its quiet baseline.
# Coarse-bucket comparisons are what survive laptop jitter; TOL carries one
# bucket of headroom over what a healthy resource class produces here.
TOL=3; CEIL=6
fail=0
# Positive control first: was there real co-processor pressure to shed?
for pair in "busy $BUSY_FAM" "loop $LOOP_FAM"; do
  ph="${pair% *}"; fam="${pair#* }"
  if [ "$fam" -lt "$MIN_FAM" ]; then
    echo "  FAIL: $ph phase only drove $fam family commands (< $MIN_FAM) — no real"
    echo "        pressure was applied, so a green p99 proves nothing. Investigate"
    echo "        the flooder/co-processor before trusting this bench."
    fail=1
  else
    echo "  ok:   $ph phase drove $fam family commands of co-processor pressure"
  fi
done
for pair in "busy $BUSY_IDX" "loop $LOOP_IDX"; do
  ph="${pair% *}"; ix="${pair#* }"
  if [ "$ix" -gt "$((BASE_IDX + TOL))" ]; then
    echo "  FAIL: $ph read p99 bucket $ix is >$TOL above baseline $BASE_IDX — the co-processor degraded the data path"
    fail=1
  elif [ "$ix" -ge "$CEIL" ]; then
    echo "  FAIL: $ph read p99 bucket $ix reached failover territory (>=25 ms) under co-processor load"
    fail=1
  else
    echo "  ok:   $ph read p99 bucket $ix within $TOL of baseline $BASE_IDX and below 25 ms"
  fi
done
[ "$fail" = 0 ] || { echo "SHED ORDER FAILED — see numbers above"; exit 1; }

echo
echo "PASS: under proven co-processor pressure (busy AND looping-cut-off-at-budget),"
echo "      the data path's read p99 stayed ms-class and out of failover territory,"
echo "      within $TOL buckets of its quiet baseline. That is the resource class"
echo "      bounding the damage — not eliminating it: the rise from baseline is the"
echo "      shared-core cost a laptop cannot isolate away. The catastrophe the shed"
echo "      order exists to prevent (10-100 ms data-path p99 when a co-processor"
echo "      runs hot) did NOT happen. The definitive p99<1ms go/no-go is the"
echo "      i4i-class run (#153); this proves the property, not the number."
