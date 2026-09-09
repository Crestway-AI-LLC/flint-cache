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
#   3. An unrelated GET issued WHILE the big DEL is in flight comes back
#      inside a bound — the "invisible in p99" half, at one sample.
#
# THE POSITIVE CONTROL IS ON THE CORPUS, not the delete: building the big hash
# must take materially longer than building the small one. Without it, a bug
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
import socket, sys, time

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
t0 = time.perf_counter(); cmd("DEL", "big:h");   big_del   = time.perf_counter() - t0
# Serving during/right after the big delete, on an unrelated key.
t0 = time.perf_counter(); by = cmd("GET", "bystander"); bystander = time.perf_counter() - t0

print(f"   DEL small:h {small_del*1000:.3f} ms")
print(f"   DEL big:h   {big_del*1000:.3f} ms   ({N//SMALL}x the elements)")
print(f"   GET bystander after the big delete: {bystander*1000:.3f} ms")

if cmd("EXISTS", "big:h") != 0 or cmd("HLEN", "big:h") != 0:
    print("FAIL: big:h survived its DEL — an instant delete that deleted nothing")
    sys.exit(1)
if by != b"x":
    print(f"FAIL: bystander key reads {by!r}, expected b'x'")
    sys.exit(1)

# O(1): the big delete must not scale with the element count. Compared against
# a floor because both are sub-millisecond and a ratio of two tiny numbers is
# noise; 5 ms is far below anything O(N) would cost at this size.
FLOOR = 0.005
if big_del > max(small_del * 10, FLOOR):
    print(f"FAIL: DEL big:h took {big_del*1000:.3f} ms against {small_del*1000:.3f} ms "
          f"for {N//SMALL}x fewer elements — the delete is scaling with N")
    sys.exit(1)
if bystander > 0.100:
    print(f"FAIL: an unrelated GET took {bystander*1000:.1f} ms during the big delete")
    sys.exit(1)
print(f"PASS: bigkey delete drill — DEL is O(1) at {N} elements "
      f"({big_del*1000:.3f} ms vs {small_del*1000:.3f} ms at {N//SMALL}x fewer), "
      f"the data is gone, and an unrelated read stayed at {bystander*1000:.3f} ms")
PY
