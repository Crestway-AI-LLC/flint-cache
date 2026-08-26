# SPDX-License-Identifier: Apache-2.0
"""The budget must bound the COMMAND, not each socket read of its reply.

`slow_tier.py` makes the tier late to START. This checks the other failure --
a tier that answers promptly and then dribbles, which is what a loaded tier
looks like. A client whose budget reaches redis-py as socket_timeout passes
that condition happily, because CPython applies socket_timeout per recv() and
no single gap between instalments is ever long enough to trip it.

Measured before the fix: an 8 MiB warm read through a 1 MB/s tier was served
FROM the tier in 9.9 s, with tier_failures=0 and degraded=0, against a 50 ms
budget -- 6.8x slower than the same read with no cache at all, and silent.

Assertions are on COUNTERS, not the clock. A wall-clock threshold on a gate
that runs 26 stages against a shared machine measures the machine.
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")
import flint_accel

OK = [True]


def check(cond, label):
    OK[0] &= bool(cond)
    print(f"[{'ok' if cond else 'FAIL'}] {label}")


def main():
    origin = sys.argv[1]
    healthy = sys.argv[2]
    narrow = sys.argv[3]
    base = dict(anon=False, key="p", secret="p",
                client_kwargs={"endpoint_url": origin, "region_name": "us-east-1"})
    KEY, N = "s3://bucket/data/000001.bin", 8 * 1024 * 1024

    def read(tier):
        f = flint_accel.FlintS3FileSystem(skip_instance_cache=True, tier_uri=tier, **base)
        with f.open(KEY, "rb") as h:
            h.seek(0)
            got = h.read(N)
        return len(got), dict(f.counters)

    # Warm over the healthy path, so what follows is a WARM read: the bug only
    # exists where the tier would otherwise have served the whole thing.
    read(healthy)
    n_warm, c_warm = read(healthy)
    check(n_warm == N, f"armed: the healthy tier returns the object ({n_warm} bytes)")
    check(c_warm["chunk_hits"] > 0 and c_warm["origin_gets"] == 0,
          f"armed: and serves it FROM the tier ({c_warm['chunk_hits']} hits, "
          f"{c_warm['origin_gets']} origin GETs) -- so the tier is genuinely warm")

    # The same warm read through a tier that answers at once and then dribbles.
    n_narrow, c_narrow = read(narrow)
    check(n_narrow == N, f"a narrow tier still returns CORRECT bytes ({n_narrow})")
    check(c_narrow["degraded"] > 0,
          f"THE BUDGET BOUNDS THE COMMAND: degraded={c_narrow['degraded']} "
          "-- a tier too slow to finish is a miss, not a wait")
    check(c_narrow["tier_failures"] == 0,
          f"and a SLOW tier is not counted as a broken one "
          f"(tier_failures={c_narrow['tier_failures']}) -- the two are different "
          "operator problems and must not share a counter")
    check(c_narrow["origin_gets"] > 0,
          f"and the read went to the ORIGIN instead ({c_narrow['origin_gets']} GETs) "
          "-- 'never slower than no cache' holds on this path too")

    # The control the assertion above needs to mean anything. "tier_failures
    # == 0 while slow" is satisfied just as well by a counter that can no
    # longer move at all, and nothing above would notice. So: a tier that is
    # BROKEN rather than slow must still reach it. A refused connection is the
    # cleanest broken there is.
    import socket as _s
    _p = _s.socket(); _p.bind(("127.0.0.1", 0)); _dead = _p.getsockname()[1]; _p.close()
    n_dead, c_dead = read(f"redis://127.0.0.1:{_dead}")
    check(n_dead == N, f"a DEAD tier still returns correct bytes ({n_dead})")
    check(c_dead["tier_failures"] > 0,
          f"armed: a BROKEN tier does reach tier_failures ({c_dead['tier_failures']}) "
          "-- without this, 'slow is not broken' would pass against a dead counter")
    return 0 if OK[0] else 1


if __name__ == "__main__":
    rc = main()
    print("NARROW-TIER CHECK", "PASSED" if rc == 0 else "FAILED")
    sys.exit(rc)
