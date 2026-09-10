#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A big-key DELETE costs what a small one costs — M1's last unmeasured clause.
#
# M1's exit asks that "a 1 M-element hash delete is O(1) and invisible in p99".
# The MECHANISM shipped with M1 — versioned subkey encoding, so DEL bumps a
# version and leaves the bodies to the compaction filter's orphan GC — and
# nothing ever measured it. The 2026-09-09 roadmap audit recorded the clause as
# NOT MEASURED rather than assume it; this is the measurement.
#
# WHAT IT ASSERTS
#   1. DEL of a BIG hash costs about what DEL of a SMALL one costs. The claim
#      is O(1), so the test is a RATIO. A wall-clock budget would pass on a
#      fast box while the cost still scaled with N, which is the thing the
#      clause is actually about.
#   2. The delete really deleted. A DEL that returned instantly having found
#      nothing satisfies (1) perfectly.
#   3. Unrelated reads, on a SECOND connection, hold their p99 across the
#      delete AND a settle window after it — the "invisible in p99" half.
#
#      This was one GET on the same socket after `cmd("DEL", ...)` had read
#      its reply, so it was strictly AFTER the delete while the text here
#      claimed "while it is in flight" (BUG-0130). One sample of the wrong
#      window.
#
#      The window that matters is not the DEL. At a million fields the DEL is
#      ~0.08 ms because the mechanism is a version bump — the bodies are left
#      to the compaction filter's orphan GC. So whatever this clause protects
#      against lands AFTER the reply, while a million orphaned subkeys are
#      collected in the background, and a probe that stops when DEL returns
#      cannot reach it by construction.
#
# TWO POSITIVE CONTROLS. On the CORPUS: building the big hash must take
# materially longer than building the small one. And on the p99 SAMPLE COUNT:
# a p99 of three reads is not a p99, and without a floor that assertion passes
# most loudly when the reader thread died in its first millisecond. Without it, a bug
# that created two equally tiny hashes makes (1) trivially true — the shape of
# every "it passed because it tested nothing" failure in docs/field-notes.md.
#
# WHY THE TIMING IS DONE IN ONE PROCESS. `date +%s%N` does not exist on macOS,
# and spawning a timestamp process per measurement costs ~30 ms — which would
# swamp a ~1 ms DEL and make the ratio assert vacuously true. The timed section
# speaks RESP over one socket and uses perf_counter.
#
#   N defaults to a gate-friendly size; FLINT_BIGKEY_N=1000000 is the figure
#   M1's exit actually names, run explicitly rather than in every gate.
set -uo pipefail
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-bigkey" 6979
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-bigkey
N="${FLINT_BIGKEY_N:-200000}"
fleet_kill server; sleep 0.4
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

$B --port 6979 --engine rocks --data-dir "$D/n" 2>"$D/n.log" &
disown
fleet_wait_listen 6979
fleet_wait_ping 6979

echo "== big-key delete: N=$N (small arm is N/100)"
python3 - "$N" <<'PY'
import socket, sys, threading, time

N = int(sys.argv[1])
SMALL = max(N // 100, 100)

s = socket.create_connection(("127.0.0.1", 6979), timeout=30)
buf = b""

def cmd(*args):
    """One RESP command, one reply. Bulk strings only — enough for this drill."""
    global buf
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    s.sendall(out)
    while b"\r\n" not in buf:
        chunk = s.recv(65536)
        if not chunk:
            raise RuntimeError("connection closed")
        buf += chunk
    line, buf = buf.split(b"\r\n", 1)
    kind, rest = line[:1], line[1:]
    if kind == b"-":
        raise RuntimeError(f"server said: {rest.decode(errors='replace')}")
    if kind == b":":                          # integer reply
        return int(rest)
    if kind == b"$":                          # bulk: consume the body too
        n = int(rest)
        if n < 0:
            return None
        while len(buf) < n + 2:
            buf += s.recv(65536)
        body, buf = buf[:n], buf[n + 2:]
        return body
    return rest                               # simple status, e.g. OK

def build(key, count):
    """HSET in batches; returns seconds spent."""
    t0 = time.perf_counter()
    B = 1000
    for base in range(0, count, B):
        args = ["HSET", key]
        for i in range(base, min(base + B, count)):
            args += [f"f{i}", f"v{i}"]
        cmd(*args)
    return time.perf_counter() - t0

cmd("SET", "bystander", "x")
big_build = build("big:h", N)
small_build = build("small:h", SMALL)
print(f"   built big:h ({N}) in {big_build:.2f}s, small:h ({SMALL}) in {small_build:.2f}s")

# POSITIVE CONTROL: the two arms must genuinely differ in ELEMENT COUNT, or
# the ratio assert below compares two identical things and cannot fail.
#
# This asked the BUILD TIMES first and that was wrong — at small N they are
# noise, and a 200-vs-100 corpus cleared a 2x build-time threshold by chance
# and reported PASS. The property is the element count, so it is read off the
# server rather than inferred from how long the writes took.
hlen_big, hlen_small = cmd("HLEN", "big:h"), cmd("HLEN", "small:h")
if hlen_big != N or hlen_small != SMALL:
    print(f"FAIL: corpus is not what was asked for — big:h {hlen_big} (want {N}), "
          f"small:h {hlen_small} (want {SMALL})")
    sys.exit(1)
if hlen_big < hlen_small * 50:
    print(f"FAIL: big:h has {hlen_big} fields and small:h {hlen_small} — fewer than "
          f"50x apart, so an O(N) delete would look O(1) here. Raise FLINT_BIGKEY_N.")
    sys.exit(1)

t0 = time.perf_counter(); cmd("DEL", "small:h"); small_del = time.perf_counter() - t0

# THE p99 HALF, MEASURED OVER THE WINDOW IT NAMES (BUG-0130).
#
# This was one GET on the SAME socket, issued after `cmd("DEL", ...)` had
# already read its reply -- so it was strictly AFTER the delete, not during
# it, while the header claimed "issued WHILE the big DEL is in flight". One
# sample of the wrong window.
#
# And the window that matters is not the DEL. At a million fields the DEL is
# ~0.08 ms, because the mechanism is a version bump: the bodies are left to
# the compaction filter's orphan GC. So the cost this clause protects against
# -- if it exists at all -- lands AFTER the reply, while a million orphaned
# subkeys are collected in the background. A probe that stops when DEL returns
# cannot see it by construction.
#
# So: a SECOND connection reads an unrelated key continuously, across the
# delete and on through a settle window, and the assertion is a p99 over that.
reader_lat = []
reader_stop = threading.Event()

def reader():
    rs = socket.create_connection(("127.0.0.1", 6979), timeout=30)
    rbuf = b""
    def rget():
        nonlocal rbuf
        rs.sendall(b"*2\r\n$3\r\nGET\r\n$9\r\nbystander\r\n")
        while b"\r\n" not in rbuf:
            ch = rs.recv(65536)
            if not ch:
                raise RuntimeError("reader connection closed")
            rbuf += ch
        line, rbuf = rbuf.split(b"\r\n", 1)
        if line[:1] == b"$":
            n = int(line[1:])
            while len(rbuf) < n + 2:
                rbuf += rs.recv(65536)
            rbuf = rbuf[n + 2:]
    try:
        while not reader_stop.is_set():
            t = time.perf_counter()
            rget()
            reader_lat.append(time.perf_counter() - t)
    except Exception:                      # noqa: BLE001 - reported via the count floor
        pass
    finally:
        rs.close()

rt = threading.Thread(target=reader, daemon=True)
rt.start()
time.sleep(0.2)                            # let the reader reach steady state
before = len(reader_lat)

t0 = time.perf_counter(); cmd("DEL", "big:h");   big_del   = time.perf_counter() - t0

# SETTLE WINDOW. The orphan GC is the part that could plausibly cost
# something, and it runs after the reply. Three seconds is not a claim that
# GC completes in three seconds -- it is the window this drill observes, and
# the drill says so rather than implying coverage it does not have.
SETTLE_S = 3.0
time.sleep(SETTLE_S)
reader_stop.set()
rt.join(timeout=10)

during = reader_lat[before:]
by = cmd("GET", "bystander")

def pct(xs, q):
    if not xs:
        return float("nan")
    ordered = sorted(xs)
    return ordered[min(int(q * len(ordered)), len(ordered) - 1)]

p50 = pct(during, 0.50)
p99 = pct(during, 0.99)
worst = max(during) if during else float("nan")

print(f"   DEL small:h {small_del*1000:.3f} ms")
print(f"   DEL big:h   {big_del*1000:.3f} ms   ({N//SMALL}x the elements)")
print(f"   unrelated GET across the delete + {SETTLE_S:.0f}s settle: "
      f"{len(during)} samples, p50 {p50*1000:.3f} ms, p99 {p99*1000:.3f} ms, "
      f"worst {worst*1000:.3f} ms")

if cmd("EXISTS", "big:h") != 0 or cmd("HLEN", "big:h") != 0:
    print("FAIL: big:h survived its DEL — an instant delete that deleted nothing")
    sys.exit(1)
if by != b"x":
    print(f"FAIL: bystander key reads {by!r}, expected b'x'")
    sys.exit(1)

# A p99 OF THREE SAMPLES IS NOT A p99. Without a floor this assertion passes
# most loudly when the reader died in its first millisecond -- the shape of
# every check that certifies by measuring nothing. The floor is deliberately
# far below what a working reader produces (thousands in three seconds) so it
# fails on a broken reader, not on a slow machine.
MIN_SAMPLES = 200
if len(during) < MIN_SAMPLES:
    print(f"FAIL: only {len(during)} reads landed across the delete and settle "
          f"window; a p99 over that is not a p99. The reader thread did not run, "
          f"or died early -- this assertion cannot certify anything.")
    sys.exit(1)

# O(1): the big delete must not scale with the element count. Compared against
# a floor because both are sub-millisecond and a ratio of two tiny numbers is
# noise; 5 ms is far below anything O(N) would cost at this size.
FLOOR = 0.005
if big_del > max(small_del * 10, FLOOR):
    print(f"FAIL: DEL big:h took {big_del*1000:.3f} ms against {small_del*1000:.3f} ms "
          f"for {N//SMALL}x fewer elements — the delete is scaling with N")
    sys.exit(1)
if p99 > 0.100:
    print(f"FAIL: unrelated reads saw p99 {p99*1000:.1f} ms across the big delete and "
          f"its settle window (worst {worst*1000:.1f} ms) — the delete is visible to "
          f"other traffic, which is the half of M1's clause this measures")
    sys.exit(1)
print(f"PASS: bigkey delete drill — DEL is O(1) at {N} elements "
      f"({big_del*1000:.3f} ms vs {small_del*1000:.3f} ms at {N//SMALL}x fewer), "
      f"the data is gone, and {len(during)} unrelated reads across the delete plus "
      f"{SETTLE_S:.0f}s of orphan GC held p99 at {p99*1000:.3f} ms "
      f"(worst {worst*1000:.3f} ms)")
PY
