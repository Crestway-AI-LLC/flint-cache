# SPDX-License-Identifier: Apache-2.0
"""The Python path, held to the same standard as the JVM one."""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import urllib.request

import fsspec
import redis as redis_lib

sys.path.insert(0, ".")
import flint_accel

OK = [True]


def check(cond, label):
    OK[0] &= bool(cond)
    print(f"[{'ok' if cond else 'FAIL'}] {label}")


def expect(key, off, n, gen=0):
    out = bytearray()
    for i in range(n):
        a = off + i
        out.append(hashlib.md5(f"{key}:{gen}:{a // 16}".encode()).digest()[a % 16])
    return bytes(out)


def stats(ep, path="/__stats"):
    with urllib.request.urlopen(ep + path) as r:
        return json.loads(r.read())


def main():
    ep = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9000"
    tier = sys.argv[2] if len(sys.argv) > 2 else "redis://127.0.0.1:9399"
    key = "data/000002.bin"
    rc = redis_lib.Redis.from_url(tier)

    so = dict(
        anon=False, key="p", secret="p",
        client_kwargs={"endpoint_url": ep, "region_name": "us-east-1"},
        tier_uri=tier,
    )

    def fs():
        # a fresh instance each time: fsspec caches instances, and a cached one
        # would serve the second read from ITS buffer rather than from the
        # tier -- the same trap AAL's in-process cache set on the JVM side.
        return flint_accel.FlintS3FileSystem(skip_instance_cache=True, **so)

    def read(f, off, n):
        with f.open(f"s3://bucket/{key}", "rb") as fh:
            fh.seek(off)
            return fh.read(n)

    rc.flushall()
    stats(ep, "/__reset")

    f1 = fs()
    got = read(f1, 100_000, 8192)
    check(got == expect(key, 100_000, 8192), "cold read verifies against the oracle")
    check(f1.counters["chunk_misses"] > 0, "cold read actually missed the tier")

    g0 = stats(ep)["gets"]
    f2 = fs()
    got2 = read(f2, 100_000, 8192)
    check(got2 == got, "warm read through a FRESH filesystem matches")
    check(stats(ep)["gets"] == g0,
          f"warm read cost 0 extra origin GETs ({g0} -> {stats(ep)['gets']})")
    check(f2.counters["chunk_hits"] > 0, "and it was the TIER that served it")

    # cross-pattern sharing (D12.8): different offsets, same chunk grid
    f3 = fs()
    before = f3.counters
    read(f3, 103_000, 4096)
    check(f3.counters["chunk_hits"] > 0,
          "a different offset reuses the first reader's chunks")

    # negative control -- a cold OBJECT does reach the origin.
    #
    # The first version read a cold OFFSET of the same object and failed,
    # correctly: fsspec's block cache had already pulled a 5 MB block on the
    # first read, so offset 700_000 was long since in the tier. The control
    # was right and the test was wrong -- which is the only reason the block
    # behaviour was noticed at all.
    g1 = stats(ep)["gets"]
    with fs().open("s3://bucket/data/000005.bin", "rb") as fh:
        fh.seek(4096)
        fh.read(4096)
    check(stats(ep)["gets"] > g1,
          f"negative control -- a cold OBJECT reaches the origin ({g1} -> {stats(ep)['gets']})")

    # single-flight under a genuine race
    rc.flushall(); stats(ep, "/__reset")
    shared = fs()
    barrier = threading.Barrier(12)
    results = []

    def worker(i):
        barrier.wait()
        results.append(read(shared, 2_000_000 + (i % 3) * 4096, 4096))

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    [t.start() for t in ts]; [t.join(60) for t in ts]
    check(len(results) == 12 and all(r for r in results),
          "12 concurrent readers all returned bytes")
    check(stats(ep)["gets"] < 12,
          f"12 concurrent cold readers cost {stats(ep)['gets']} origin GETs")
    check(shared.counters["joined"] > 0,
          "armed-check: readers genuinely JOINED an in-flight fetch")

    # D13 -- SSE-C bypasses the tier entirely
    rc.flushall()
    enc = flint_accel.FlintS3FileSystem(
        skip_instance_cache=True,
        s3_additional_kwargs={"SSECustomerAlgorithm": "AES256",
                              "SSECustomerKey": "0" * 32},
        **so)
    try:
        read(enc, 300_000, 4096)
    except Exception:
        pass                      # the fixture may refuse; what matters is the tier
    check(rc.dbsize() == 0, f"SSE-C wrote NOTHING to the tier ({rc.dbsize()} keys)")
    rc.flushall()
    read(fs(), 300_000, 4096)
    check(rc.dbsize() > 0,
          f"negative control -- without SSE-C the tier IS populated ({rc.dbsize()} keys)")

    # how much does fsspec's block cache pull for a small read?
    rc.flushall(); stats(ep, "/__reset")
    with fs().open("s3://bucket/data/000006.bin", "rb") as fh:
        fh.seek(1_000_000)
        fh.read(4096)
    amp = stats(ep)["bytes_served"]
    print(f"[--] fsspec pulled {amp} bytes for a 4096-byte read "
          f"({amp // 4096}x), landing {rc.dbsize()} chunks in the tier")

    # adoption is one line and reversible
    flint_accel.install()
    check(fsspec.get_filesystem_class("s3").__name__ == "FlintS3FileSystem",
          "install() re-registers s3:// with no path changes")
    flint_accel.uninstall()
    check(fsspec.get_filesystem_class("s3").__name__ == "S3FileSystem",
          "uninstall() puts back what was there")

    # ---- install() must actually apply what it is given --------------------
    # It did not, for as long as install() existed: the defaults were stored on
    # the class and never read, so every argument was discarded and the
    # built-ins used instead. Invisible because every check above constructs
    # FlintS3FileSystem DIRECTLY with explicit keywords, while the README tells
    # users to call install(). The tested path and the documented path were
    # different paths, and only the tested one worked.
    flint_accel.install(tier_uri="redis://127.0.0.1:9399", cache_sse_kms=True,
                        anon=False, key="p", secret="p",
                        client_kwargs={"endpoint_url": ep, "region_name": "us-east-1"})
    inst = fsspec.filesystem("s3", skip_instance_cache=True)
    check(inst._tier_uri == "redis://127.0.0.1:9399",
          f"install() applies the tier endpoint it was given ({inst._tier_uri})")
    check(inst._cache_kms is True, "install() applies cache_sse_kms")
    check(inst.key == "p",
          "install() also passes s3fs options through, not just Flint ones")
    over = fsspec.filesystem("s3", skip_instance_cache=True,
                             tier_uri="redis://127.0.0.1:9400")
    check(over._tier_uri == "redis://127.0.0.1:9400",
          "an explicit argument OVERRIDES install()'s default")
    flint_accel.install()
    cleared = fsspec.filesystem("s3", skip_instance_cache=True)
    check(cleared._tier_uri == "redis://127.0.0.1:6379",
          "negative control: install() with no defaults CLEARS the previous ones "
          f"({cleared._tier_uri}) -- settings must not outlive the call")
    flint_accel.uninstall()   # leave the process as we found it

    print("\nPYTHON SUITE " + ("PASSED" if OK[0] else "FAILED"))
    return 0 if OK[0] else 1


if __name__ == "__main__":
    sys.exit(main())
