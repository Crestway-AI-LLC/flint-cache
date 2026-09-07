#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0029's gating measurement: do reads degrade behind STALLED writes?
#
# WHY THIS EXISTS AND WHY IT IS NOT rw_isolation. That drill (ADR-0005 D1)
# already samples read latency while another client pipelines a write storm,
# now with `--workers 1` so the two provably share one backend connection, and
# reads stay flat: p99 0.217ms quiet, 0.295ms under 27k writes. A shared FIFO
# is not enough to hurt a read, because a FIFO drains as fast as its slowest
# member and every write in that storm is sub-millisecond.
#
# ADR-0026 measured the regime where a write is NOT sub-millisecond: RocksDB's
# L0 write stall, where `writes_delayed_soft` runs at thousands/s, all
# connections sit pinned in flight, and neither side is CPU-bound -- both are
# waiting. THAT is where head-of-line blocking behind your own connection
# should show, and nothing measured it.
#
# The difference between the two drills is one LSM configuration, and it is
# the whole experiment: shrink the level base so 400 MB behaves like the
# hundreds of gigabytes a fleet node holds (ingest_saturation's trick).
#
# WHAT IT ASSERTS, AND WHAT IT ONLY REPORTS. It asserts its CONTROLS -- that
# the reader and the writer share one backend FIFO, and that a stall actually
# happened -- and it REPORTS the latency comparison. The number is what
# decides whether ADR-0029 is accepted or withdrawn; inventing a threshold
# before the first measurement would be picking the answer.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-rstall" 6965 6968
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-rstall; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features rocks -p flint-proxy \
  || { echo "FAIL: build"; exit 1; }

# THE SHRUNKEN LSM IS THE EXPERIMENT (ingest_saturation's trick): reach the
# stall regime by shrinking the structure rather than growing the dataset.
export FLINT_LEVEL_BASE_MB=8
export FLINT_WRITE_BUFFER_MB=4
export FLINT_STATS_DUMP_SEC=5

$B --port 6965 --engine rocks --data-dir "$D/m" 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen 6965
sleep 0.7
# --workers 1: the reader and the writer must land on the same worker to share
# a backend connection at all (ADR-0021 pins a client to the worker that
# accepted it, round-robin). Asserted below rather than assumed.
$PX --port 6968 --workers 1 --pairs "127.0.0.1:6965" 2>"${FLEET_SCOPE}proxy.log" &
fleet_wait_listen 6968
sleep 1.0
cli_ok valkey-cli -p 6968 SET readkey readval

STALL_BUDGET_S="${STALL_BUDGET_S:-90}"
python3 - "$STALL_BUDGET_S" <<'PY'
import os, socket, statistics, subprocess, sys, threading, time

BUDGET = float(sys.argv[1])

def resp(args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
    return out

def read_reply(s, buf=b""):
    while True:
        i = buf.find(b"\r\n")
        if i >= 0:
            head = buf[:i]
            if head.startswith(b"$") and head != b"$-1":
                need = int(head[1:]) + i + 4
                while len(buf) < need:
                    buf += s.recv(65536)
                return buf[need:]
            return buf[i + 2:]
        buf += s.recv(65536)

def conn(port=6968):
    s = socket.create_connection(("127.0.0.1", port), timeout=30)
    s.settimeout(30)
    return s

def info():
    """FLINTINFO from the NODE. The stall is a property of the store, and the
    proxy rejects FLINT* admin commands (the tenant boundary), so this is the
    only place it can be read."""
    out = subprocess.run(["valkey-cli", "-p", "6965", "FLINTINFO"],
                         capture_output=True, text=True, timeout=10).stdout
    d = {}
    for line in out.replace("\r", "").split("\n"):
        if ":" in line:
            k, v = line.split(":", 1)
            d[k] = v
    return d

def sample_reads(n, out, port=6968):
    s = conn(port)
    buf = b""
    for _ in range(n):
        t0 = time.perf_counter()
        s.sendall(resp(["GET", "readkey"]))
        buf = read_reply(s, buf)
        out.append((time.perf_counter() - t0) * 1000.0)
    s.close()

def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100.0))]

def report(tag, xs):
    print(f"  {tag:<22} n={len(xs):<5} p50 {statistics.median(xs):8.3f}ms  "
          f"p99 {pct(xs,99):8.3f}ms  p99.9 {pct(xs,99.9):9.3f}ms  max {max(xs):9.3f}ms")

# ---- baseline, quiet node -------------------------------------------------
base = []
sample_reads(400, base)
print("== read latency")
report("quiet", base)

# ---- the write storm, sized to stall --------------------------------------
stop = threading.Event()
wrote = [0]
peak = {"delayed": 0, "l0": 0, "stopped": 0}

def storm():
    s = conn()
    buf = b""
    v = os.urandom(5000)          # incompressible: a constant fill would be
    i = 0                          # squashed and the LSM would never fill
    while not stop.is_set():
        for _ in range(16):
            s.sendall(resp(["SET", f"stall:{i:012d}", v]))
            i += 1
        for _ in range(16):
            buf = read_reply(s, buf)
        wrote[0] = i
    s.close()

t = threading.Thread(target=storm, daemon=True)
t.start()

# Wait for the STALL, not for a timer. The measurement is meaningless until
# RocksDB is actually applying back-pressure, and how long that takes is a
# property of the machine.
t0 = time.time()
stalled = False
while time.time() - t0 < BUDGET:
    time.sleep(1.0)
    d = info()
    dl = int(d.get("writes_delayed_soft", 0) or 0)
    l0 = int(d.get("l0_files", 0) or 0)
    st = int(d.get("write_stopped", 0) or 0)
    peak["delayed"] = max(peak["delayed"], dl)
    peak["l0"] = max(peak["l0"], l0)
    peak["stopped"] = max(peak["stopped"], st)
    if dl > 0 or st > 0:
        stalled = True
        break

# ---- the precondition, where the condition can exist ----------------------
# AFTER ADR-0029 THIS EXPECTS TWO, AND THE INVERSION IS THE POINT. Before the
# lane split, one backend connection meant the reader was queued behind the
# writer -- which is what this drill measured, and it found reads waiting
# 555ms behind a stalled write. With the split there is no shared FIFO to
# queue in: the reader is on the read lane, the writer on the write lane, both
# on the one worker. So `pool_lanes == 2` is now the precondition that makes
# the numbers below mean anything, and a 1 would mean the separation is not in
# effect and the measurement is of the old world.
probe = conn()
probe.sendall(resp(["PROXYSTATS"]))
raw = b""
while b"pool_lanes:" not in raw:
    raw += probe.recv(65536)
probe.close()
lanes = next((l.split(":", 1)[1] for l in raw.decode(errors="replace").split("\r\n")
              if l.startswith("pool_lanes:")), None)

# ---- reads DURING the stall ----------------------------------------------
# 3000, NOT 400. The first run of this drill showed p50 and p99 essentially
# unmoved and ONE read of 21.8ms against a quiet max of 0.133ms -- which is
# what head-of-line blocking behind a stalled write should look like, and is
# also what one descheduled thread looks like. At n=400 the p99.9 IS that
# single sample. A tail claim needs enough samples that the tail is a
# population rather than an anecdote.
under = []
sample_reads(3000, under)
stop.set()
t.join(timeout=10)
report("under stalled writes", under)
print(f"  writes landed={wrote[0]}  peak writes_delayed_soft={peak['delayed']}  "
      f"peak l0_files={peak['l0']}  write_stopped={peak['stopped']}  "
      f"pool_lanes={lanes}")

fail = []
# CONTROL 1: two lanes, or the separation under test is not in effect.
if lanes != "2":
    fail.append(f"pool_lanes={lanes!r}, want 2 — the reader and the writer are "
                "not on separate lanes, so ADR-0029's separation is not in "
                "effect and this measures the world before it")
# CONTROL 2: the stall has to have HAPPENED. Without this a flat read latency
# is indistinguishable from a storm that never reached back-pressure, and the
# drill would report the reassuring answer for the wrong reason.
if not stalled:
    fail.append(f"no write stall within {BUDGET:.0f}s (peak writes_delayed_soft="
                f"{peak['delayed']}, peak l0_files={peak['l0']}) — the regime "
                "under test was never entered")

if fail:
    for f in fail:
        print(f"FAIL: {f}")
    sys.exit(1)

# EXCURSIONS, counted, because a percentile on a small tail is one sample
# wearing a statistic's clothes. The threshold is the quiet run's own maximum,
# so it is a measured baseline rather than a number chosen here.
qmax = max(base)
over = [x for x in under if x > qmax]
print(f"  reads slower than the quiet MAX ({qmax:.3f}ms): {len(over)} of "
      f"{len(under)} ({100.0*len(over)/len(under):.2f}%)"
      + (f", worst {max(over):.3f}ms" if over else ""))

b99, u99 = pct(base, 99), pct(under, 99)
ratio = (u99 / b99) if b99 > 0 else float("inf")
print(f"  read p99 under a stall is {ratio:.1f}x the quiet baseline "
      f"({b99:.3f}ms -> {u99:.3f}ms)")
print("MEASURED: controls hold (lanes separated, stall confirmed). Before "
      "ADR-0029 this ran on ONE shared FIFO and read p99.9 was 364.780ms "
      "with a 555.385ms worst case; the tail is what the split removes.")
PY
[ $? -eq 0 ] || exit 1
echo "PASS: read-under-stall measured with the lanes separated and both"
echo "      controls armed — see the tail above (ADR-0029)"
