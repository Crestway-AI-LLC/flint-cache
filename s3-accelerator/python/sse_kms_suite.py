# SPDX-License-Identifier: Apache-2.0
"""SSE-KMS on the Python path, held to the JVM path's rule (ADR-0023 D13.3).

The tier is SHARED between the two clients by design (the cross-language
drill asserts it), so a guarantee that differs by language is not a guarantee:
a Python reader caching KMS plaintext that the JVM reader refuses to cache
leaves exactly the hole the JVM rule was written to close.

Every "it did not cache" below is paired with proof that the same client DOES
cache a plain object, because a client that cached nothing at all would pass
every bypass assertion on its own.
"""

from __future__ import annotations

import hashlib
import sys

import redis as redis_lib

sys.path.insert(0, ".")
import flint_accel

OK = [True]


def check(cond, label):
    OK[0] &= bool(cond)
    print(f"[{'ok' if cond else 'FAIL'}] {label}")


def expect(key, off, n, gen=0):
    return bytes(hashlib.md5(f"{key}:{gen}:{(off + i) // 16}".encode()).digest()[(off + i) % 16]
                 for i in range(n))


def fs_for(endpoint, tier, cache_kms=False):
    return flint_accel.FlintS3FileSystem(
        skip_instance_cache=True, anon=False, key="p", secret="p",
        client_kwargs={"endpoint_url": endpoint, "region_name": "us-east-1"},
        tier_uri=tier, cache_sse_kms=cache_kms)


def read(f, key, off, n):
    with f.open(f"s3://bucket/{key}", "rb") as h:
        h.seek(off)
        return h.read(n)


def main():
    kms_ep = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9530"
    plain_ep = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:9531"
    tier = sys.argv[3] if len(sys.argv) > 3 else "redis://127.0.0.1:9399"
    rc = redis_lib.Redis.from_url(tier)
    KEY, LEN = "data/000001.bin", 200_000

    # --- 1. default: a KMS object must not touch the tier --------------------
    rc.flushall()
    f = fs_for(kms_ep, tier)
    got = read(f, KEY, 0, LEN)
    check(got == expect(KEY, 0, LEN), "KMS object still reads CORRECTLY (bypass is not a failure)")
    n = len(rc.keys("*"))
    check(n == 0, f"and NOTHING reached the tier -- no chunks, no metadata ({n} keys)")
    c = f.counters
    check(c["kms_bypassed"] > 0,
          f"armed: it was the KMS path that bypassed ({c['kms_bypassed']}), not some other bypass")
    check(c["kms_undetectable"] == 0, "armed: detection actually worked -- 0 undetectable")

    # --- 2. the control that carries check 1 ---------------------------------
    rc.flushall()
    f2 = fs_for(plain_ep, tier)
    got2 = read(f2, KEY, 0, LEN)
    check(got2 == expect(KEY, 0, LEN), "control: a PLAIN object reads correctly")
    n2 = len(rc.keys("c1/*"))
    check(n2 > 0,
          f"control: and the same client DOES cache it ({n2} chunks) -- so check 1 "
          "measured the KMS rule, not a broken cache")
    check(f2.counters["kms_bypassed"] == 0, "control: and nothing was KMS-bypassed")

    # --- 3. opt-in -----------------------------------------------------------
    rc.flushall()
    f3 = fs_for(kms_ep, tier, cache_kms=True)
    got3 = read(f3, KEY, 0, LEN)
    check(got3 == expect(KEY, 0, LEN), "opt-in: the KMS object reads correctly")
    n3 = len(rc.keys("c1/*"))
    check(n3 > 0, f"OPT-IN WORKS: cache_sse_kms=True caches the KMS object ({n3} chunks)")
    check(f3.counters["kms_bypassed"] == 0, "armed: and nothing was bypassed once opted in")

    # --- 4. and the two clients agree on the KEY, not merely on the policy ---
    # The whole point of a shared tier is that the JVM client can read what
    # this one wrote. A Python-only bypass rule that used a different key
    # prefix would pass every check above and still be useless.
    sample = rc.keys("c1/*")[0].decode()
    check(sample.startswith("c1/") and sample.count("/") == 2,
          f"opt-in wrote the SHARED key shape the JVM client reads ({sample})")

    print("SSE-KMS PYTHON SUITE PASSED" if OK[0] else "SSE-KMS PYTHON SUITE FAILED")
    return 0 if OK[0] else 1


if __name__ == "__main__":
    sys.exit(main())
