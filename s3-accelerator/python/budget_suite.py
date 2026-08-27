# SPDX-License-Identifier: Apache-2.0
"""The tier budget bounds a COMMAND, not an instalment of one.

Nothing in the gate could catch this, because every instrument for tier
sickness made the FIRST byte late -- `slow_tier.py` delays the client->tier
direction, which is one delay per request. A tier that answers promptly and
then delivers slowly had no instrument at all, and that is what a *loaded* tier
looks like.

Measured before the fix: an 8 MiB warm read through a tier throttled to 1 MB/s
-- about eight seconds -- was served FROM THE TIER with `tier_failures=0`,
because redis-py applies `socket_timeout` per `recv()` and a large reply is
~128 of them. The JVM, whose budget is `orTimeout()` over the whole command,
degraded on the identical read. D12.9's rule -- never slower than no cache --
held on one path only.

Both failure shapes are asserted here, and so is the healthy path, because a
client that had simply stopped using the tier would pass the two degradation
checks on its own.
"""

from __future__ import annotations

import json
import socket
import sys
import threading
import time
import urllib.request

import redis as redis_lib

sys.path.insert(0, ".")
import flint_accel
from flint_accel.conn import DeadlineSocket

OK = [True]


def check(cond, label):
    OK[0] &= bool(cond)
    print(f"[{'ok' if cond else 'FAIL'}] {label}")


# --------------------------------------------------------------- mechanics
# No tier and no origin: these hold the socket wrapper itself, because two of
# its properties are invisible end-to-end. recv_into is hiredis's door and
# recv is the pure-python parser's, so a fix that covered one would silently
# not apply to any customer who installed hiredis for speed. And a deadline
# that shrinks a socket it does not own must hand it back.
def _dribbler(chunk, gap):
    """A sender that answers instantly and then delivers slowly, forever.

    The shape slow_tier.py cannot make: every instalment lands well inside the
    budget and only the command exceeds it.
    """
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)

    def run():
        try:
            c, _ = srv.accept()
            while True:
                c.sendall(b"x" * chunk)
                time.sleep(gap)
        except OSError:
            pass
        finally:
            srv.close()

    threading.Thread(target=run, daemon=True).start()
    return srv.getsockname()[1]


def mechanics():
    BUDGET, CH, GAP = 0.2, 4096, 0.02      # 20 ms an instalment, 200 ms budget
    for door in ("recv", "recv_into"):
        raw = socket.create_connection(("127.0.0.1", _dribbler(CH, GAP)), timeout=5)
        raw.settimeout(BUDGET)
        ds = DeadlineSocket(raw, BUDGET)
        ds.arm()
        buf, got, t0, err = bytearray(CH), 0, time.monotonic(), None
        try:
            for _ in range(1000):
                got += ds.recv_into(buf) if door == "recv_into" else len(ds.recv(CH))
        except (socket.timeout, OSError) as e:
            err = e
        el = time.monotonic() - t0
        check(err is not None and BUDGET * 0.8 <= el < BUDGET * 3,
              f"{door}: a reply that never ends is cut off at the budget "
              f"({el:.2f}s for {got} bytes, {type(err).__name__ if err else 'no error'})")
        raw.close()

    # Control: unarmed, the SAME slow sender must be read without complaint.
    # Without it the two checks above pass against a wrapper that cannot read.
    raw = socket.create_connection(("127.0.0.1", _dribbler(CH, GAP)), timeout=5)
    raw.settimeout(BUDGET)
    ds = DeadlineSocket(raw, BUDGET)
    got = sum(len(ds.recv(CH)) for _ in range(5))
    check(got == CH * 5,
          f"negative control -- UNARMED, the same slow sender delivers all {got} "
          "bytes, so it is the deadline doing this and not a broken read")

    # The deadline must not outlive the command. It once did: _apply used
    # min(current, remaining), remaining falls toward zero within a command,
    # and nothing put the socket back -- so one 8-instalment command capped a
    # 50 ms budget at 20.5 ms for the life of the connection, which is
    # D12.36's eager passthrough reintroduced by D12.9's fix.
    ds.arm()
    for _ in range(4):
        ds.recv(CH)                        # several instalments, budget draining
    ds.disarm()
    check(raw.gettimeout() == BUDGET,
          f"a multi-instalment command leaves the socket as it found it "
          f"({raw.gettimeout()} vs {BUDGET}) -- the budget must not ratchet down")
    raw.close()


def main():
    ep = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9000"
    tier = sys.argv[2] if len(sys.argv) > 2 else "redis://127.0.0.1:9399"
    narrow = sys.argv[3] if len(sys.argv) > 3 else "redis://127.0.0.1:9397"
    slow = sys.argv[4] if len(sys.argv) > 4 else "redis://127.0.0.1:9398"
    key = "s3://bucket/data/000002.bin"
    rc = redis_lib.Redis.from_url(tier)

    mechanics()

    def stats(path="/__stats"):
        with urllib.request.urlopen(ep + path) as r:
            return json.loads(r.read())

    def read(tier_uri, nbytes, budget=None):
        """One warm read, and who actually served it."""
        stats("/__reset")
        kw = dict(anon=False, key="p", secret="p", tier_uri=tier_uri,
                  client_kwargs={"endpoint_url": ep, "region_name": "us-east-1"})
        if budget is not None:
            kw["tier_budget_s"] = budget
        fs = flint_accel.FlintS3FileSystem(skip_instance_cache=True, **kw)
        with fs.open(key, "rb") as fh:
            fh.seek(0)
            got = len(fh.read(nbytes))
        # D17.5.1: the fill lands off the read path, so a read that returns has
        # not necessarily populated the tier yet. Every caller here reads and
        # then asks who served it, so the fill has to have settled first --
        # otherwise the warm control races the cold read that armed it.
        fs.drain(timeout=30)
        return got, stats()["gets"], dict(fs.counters)

    N = 1024 * 1024
    rc.flushall()
    read(tier, N)                      # cold: fill the tier through a sane path

    # The control comes FIRST: every assertion below is about a read that could
    # have been served from the tier, and one that could not would pass them all.
    got, gets, c = read(tier, N)
    check(got == N and gets == 0 and c["chunk_hits"] > 0,
          f"control -- a warm read is served by the TIER ({c['chunk_hits']} chunk "
          f"hits, {gets} origin GETs), so the tier holds these bytes")

    # 1. LATE TO START. Caught before this suite existed, asserted so a fix to
    #    the other shape cannot quietly trade it away.
    got, gets, c = read(slow, N)
    check(got == N, f"a tier late to FIRST BYTE still returns the right bytes ({got})")
    check(gets > 0 and c["degraded"] > 0 and c["tier_failures"] == 0,
          f"and degrades to the origin ({gets} GETs, degraded={c['degraded']}) "
          f"rather than waiting -- and WITHOUT moving tier_failures "
          f"({c['tier_failures']}), because slow is not broken")

    # 2. SLOW TO FINISH. The shape that had no instrument. Every instalment
    #    arrives well inside the 50 ms budget; only the command exceeds it.
    got, gets, c = read(narrow, N)
    check(got == N, f"a tier slow to FINISH still returns the right bytes ({got})")
    check(gets > 0 and c["degraded"] > 0 and c["tier_failures"] == 0,
          f"and degrades to the origin ({gets} GETs, degraded={c['degraded']}, "
          f"tier_failures={c['tier_failures']}) -- the budget bounds the COMMAND, "
          "not the recv()")

    # 3. The negative control that makes check 2 mean something. Same throttled
    #    proxy, same read, a budget large enough to cover it: if THIS degrades
    #    too, the proxy is breaking the connection and check 2 is passing for
    #    the wrong reason.
    got, gets, c = read(narrow, N, budget=30.0)
    check(got == N and gets == 0 and c["chunk_hits"] > 0,
          f"negative control -- the SAME throttled tier serves the read when the "
          f"budget is 30 s ({c['chunk_hits']} chunk hits, {gets} origin GETs), so "
          "it is the budget that degrades it and not a broken proxy")

    # Control for the split itself. Without it, "tier_failures == 0" above is
    # satisfied just as well by a counter that can no longer move at all.
    got, gets, c = read("redis://127.0.0.1:1", N)     # nothing listens there
    check(got == N and gets > 0 and c["tier_failures"] > 0,
          f"negative control -- a tier that is BROKEN rather than slow still "
          f"reaches tier_failures ({c['tier_failures']}), so the two checks above "
          "are a distinction and not a dead counter")

    print("\nBUDGET SUITE " + ("PASSED" if OK[0] else "FAILED"))
    return 0 if OK[0] else 1


if __name__ == "__main__":
    sys.exit(main())
