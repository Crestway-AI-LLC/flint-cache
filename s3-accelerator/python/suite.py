# SPDX-License-Identifier: Apache-2.0
"""The Python path, held to the same standard as the JVM one."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import threading
import urllib.request

import fsspec
import redis as redis_lib

sys.path.insert(0, ".")
import flint_accel
from flint_accel.tier import BLOCK_BYTES, CHUNK

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

    # D16 -- a write through this client must FORGET the object's metadata
    #
    # The cached entry carries the object LENGTH, and that makes staleness
    # worse than stale: a path rewritten SHORTER leaves a reader believing the
    # object ends where the old one did, so reads stop early with EOF instead
    # of returning old bytes. The JVM side hit exactly this and Hadoop's
    # contract suite caught it, because it rewrites one path repeatedly.
    # Nothing on this side did, because every suite we wrote read objects it
    # never wrote.
    #
    # A TTL is not an answer here. It bounds how long an entry survives a
    # change made ELSEWHERE, which is the D3 contract; it says nothing about
    # this process contradicting itself, and that window is the only one a
    # caller can actually observe.
    rc.flushall(); stats(ep, "/__reset")
    KW = "s3://bucket/scratch/rewrite.bin"
    LONG, SHORT = b"L" * 40_000, b"S" * 900
    w = fs()
    w.pipe_file(KW, LONG)
    # A FRESH reader. Reading back through the instance that just wrote hits
    # s3fs's own dircache, so _info delegates and never reaches the tier --
    # which is correct, and would have made the next check unarmed.
    # open()/read(), not cat_file(): cat_file is a one-shot ranged GET that
    # never resolves details, so it does not go through _info and would leave
    # the tier empty -- making every check below pass on 0 == 0.
    def _whole(path):
        with fs().open(path, "rb") as h:
            return h.read()
    check(_whole(KW) == LONG, "armed: the origin stores and returns what we wrote")
    mk = list(rc.scan_iter(match=b"m1/*"))
    check(len(mk) > 0, f"armed: reading it cached the metadata ({len(mk)} m1/ entries)")

    w.pipe_file(KW, SHORT)
    check(len(list(rc.scan_iter(match=b"m1/*"))) == 0,
          "a write through this client DROPS the cached metadata")
    got_short = _whole(KW)
    check(got_short == SHORT,
          f"and the rewritten object reads back WHOLE ({len(got_short)} bytes, want {len(SHORT)})"
          " -- not truncated to the cached length")

    # D17 -- an object above the cap is read, but never cached
    #
    # The bound is on the KEYSPACE as much as the bytes: an object of size S
    # occupies S/CHUNK keys, so a single very large object can cost more in
    # per-key overhead than its data is worth to anyone.
    rc.flushall(); stats(ep, "/__reset")
    BIG = "s3://bucket/scratch/oversize.bin"
    payload = b"O" * 200_000
    fs().pipe_file(BIG, payload)

    small = flint_accel.FlintS3FileSystem(skip_instance_cache=True,
                                          max_object_bytes=100_000, **so)
    with small.open(BIG, "rb") as h:
        h.seek(0); over = h.read(len(payload))
    check(over == payload, "an oversize object still READS correctly")
    check(len([k for k in rc.scan_iter(match=b"c2/*")]) == 0,
          "and wrote NO chunks to the tier")
    check(small.counters["oversize_bypassed"] > 0,
          f"counted, not silent ({small.counters['oversize_bypassed']} bypassed)"
          " -- 'too large' must not look like 'the cache broke'")
    # The control matters more than the check: without it, a client that
    # cached nothing at all for any reason would pass the two checks above.
    rc.flushall()
    with fs().open(BIG, "rb") as h:
        h.seek(0); h.read(len(payload))
    check(len([k for k in rc.scan_iter(match=b"c2/*")]) > 0,
          "negative control -- UNDER the cap the same object IS cached")

    # D19 -- the block size is 256 KiB, a measured point on a trade between
    # read amplification and warm-path tier cost. NOT our CHUNK, which this
    # comment used to say and which the measurement rejects: 64 KiB is worse
    # than 256 KiB on both tier axes for sequential work.
    K19 = "s3://bucket/data/000005.bin"
    size19 = fs().info(K19)["size"]

    def sparse_cost(fsys, n=16, sz=64 * 1024):
        """Origin bytes for n scattered 64 KiB reads. Counts, not durations --
        a duration on a shared machine is not a measurement."""
        rc.flushall(); stats(ep, "/__reset")
        step = size19 // n
        with fsys.open(K19, "rb") as h:
            for i in range(n):
                h.seek(i * step); h.read(sz)
        return stats(ep)["bytes_served"], n * sz

    tuned, asked = sparse_cost(fs())
    wide, _ = sparse_cost(flint_accel.FlintS3FileSystem(
        skip_instance_cache=True, default_block_size=5 * 1024 * 1024, **so))
    # Pin the DECISION, not a ratio. 256 KiB is a chosen point on a trade with
    # no free point (ADR-0023 D19), so the value itself is the thing a
    # regression would move -- and an exact check cannot drift the way a
    # threshold tuned to whatever was measured that day does.
    check(fs().default_block_size == BLOCK_BYTES,
          f"the block size default is the chosen {BLOCK_BYTES} B ({BLOCK_BYTES // CHUNK} chunks)")
    check(tuned < wide,
          f"our default block pulls LESS than fsspec's ({tuned} < {wide} bytes)")
    # Bound DERIVED, not tuned, and now derived from the RIGHT mechanism. It
    # read 2 x block / read = 8x, on the belief that fsspec fetches a block per
    # miss and one ahead. It does not: s3fs defaults to ReadAheadCache, which
    # fetches the REQUEST plus one block, so the ceiling is (read + block) /
    # read = 5.0x -- which is exactly what a 64 KiB read against a 256 KiB
    # block measures, to the byte. The old bound was correct but slack by 60%,
    # and slack in a derived bound is room for a regression to hide in.
    read19 = 64 * 1024
    bound = (read19 + BLOCK_BYTES) / read19
    check(tuned <= asked * bound,
          f"a selective read drags <= {bound:.1f}x what it asked for ({tuned/asked:.1f}x)")
    # Without this the two checks above would both pass against a client that
    # had simply stopped reading anything.
    check(wide > asked * 5,
          f"negative control -- fsspec's own 5 MiB default drags {wide/asked:.1f}x")

    # ---- the axis that was MISSING, and is why this default was first set
    # wrong: tier bytes on the WARM path. Every check above counts the S3 side
    # of the cache -- the side the product sells, and the side we had tooling
    # for. A block small enough to look free there is paid for here, because a
    # fetch that straddles the chunk grid costs a whole extra chunk off the
    # tier. Instrumented now, so the next change to BLOCK_BYTES cannot be
    # measured on one side only.
    def warm_tier_cost(fsys, n=16, sz=64 * 1024):
        """(cold origin bytes, warm tier bytes, warm tier trips, warm origin
        gets) for the same selective read run twice."""
        step = size19 // n

        def sweep():
            with fsys.open(K19, "rb") as h:
                for i in range(n):
                    h.seek(i * step); h.read(sz)

        rc.flushall(); stats(ep, "/__reset")
        sweep()
        cold = stats(ep)["bytes_served"]
        stats(ep, "/__reset"); rc.config_resetstat()
        sweep()
        i = rc.info("all")
        trips = sum(v["calls"] for k, v in i.items()
                    if k.startswith("cmdstat_") and k[len("cmdstat_"):] in ("mget", "set"))
        return cold, i["total_net_output_bytes"], trips, stats(ep)["gets"]

    # A FRESH filesystem for the warm pass: fsspec caches instances, and a
    # cached one serves the second sweep out of its own buffer, which measures
    # nothing.
    cold_b, warm_b, warm_trips, warm_gets = warm_tier_cost(fs())
    check(warm_gets == 0,
          f"control -- the warm sweep reached the origin {warm_gets} times, so what "
          "follows is the TIER's cost")
    check(warm_trips > 0, f"control -- and it cost {warm_trips} tier round trips")
    # D17's premise, on the byte axis: the cache changes where bytes come from,
    # it does not reduce them. Within the D14 seal and RESP framing.
    check(abs(warm_b - cold_b) <= 0.01 * cold_b,
          f"a warm read moves the SAME bytes as the cold one ({warm_b} off the tier "
          f"vs {cold_b} off the origin, +{warm_b - cold_b} of seal and framing)")
    # No separate ceiling check on the tier side: it would have to model the
    # 4-byte seal and RESP framing to be exact, and the equality above already
    # pins the tier to an origin figure that IS checked against the ceiling.
    #
    # Without a control, everything above passes against any block size at all
    # -- which is exactly how a 64 KiB default once looked free. The control is
    # derived rather than tuned: a wrong block blows the ceiling ours meets.
    w_cold, w_warm, _, _ = warm_tier_cost(flint_accel.FlintS3FileSystem(
        skip_instance_cache=True, default_block_size=5 * 1024 * 1024, **so))
    check(w_warm > asked * bound,
          f"negative control -- a 5 MiB block costs the TIER {w_warm/asked:.1f}x, "
          f"past the {bound:.1f}x ceiling ours meets, so this axis can fail")

    # The fill costs one round trip per BATCH, not one per chunk
    #
    # MEASURED before batching: 16 SET round trips per MiB filled -- one per
    # 64 KiB chunk -- against a single MGET to read the same run back. At the
    # D17 cap that is 8,192 sequential round trips for one object, while the
    # JVM pipelines the identical writes.
    #
    # Asserted as a RATIO per MiB rather than an absolute, so the check does
    # not have to be retuned when the object size or the chunk size moves.
    rc.flushall(); rc.config_resetstat()
    with fs().open("s3://bucket/data/000003.bin", "rb") as h:
        h.seek(0)
        filled = len(h.read(4 * 1024 * 1024))
    _cs = rc.info("commandstats") or {}
    _calls = {k.replace("cmdstat_", ""): v["calls"] for k, v in _cs.items()}
    writes = _calls.get("set", 0) + _calls.get("mset", 0)
    mib = max(filled / (1024 * 1024), 1)
    check(writes > 0, f"armed: filling {mib:.0f} MiB does write to the tier ({writes} round trips)")
    # 4 per MiB is generous against a measured 1.0 and would still catch a
    # regression to per-chunk writes, which is 16.
    check(writes / mib <= 4,
          f"the fill batches: {writes / mib:.1f} write round trips per MiB "
          "(per-chunk writes would be 16)")
    check(_calls.get("set", 0) == 0,
          f"and does it with MSET, not per-chunk SET ({_calls.get('set', 0)} SETs)")

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

    # Every chunk of one object must share ONE hash tag, because Flint routes a
    # multi-key command by its first key alone and neither MGET nor MSET carries
    # a CROSSSLOT guard. Without colocation a chunk on another pair answers nil
    # in its correct position -- a miss, so never wrong bytes, but a cache that
    # silently stops working the moment a second pair exists.
    #
    # A ONE-PAIR TIER CANNOT SHOW THIS. valkey has no slots and the playground
    # is a single pair, so both the suite and production would stay green while
    # the property was broken. What is asserted here is therefore the KEY SHAPE,
    # which is the part that lives on this side of the wire; Flint's own
    # flint-slot tests own the other half.
    rc.flushall()
    with fs().open(K19, "rb") as h:
        h.seek(0); h.read(300_000)
    tags = {re.search(rb"\{(.*?)\}", k).group(1)
            for k in rc.scan_iter(match=b"c2/*")}
    check(len(tags) == 1,
          f"every chunk of one object shares one hash tag ({len(tags)} distinct)")
    # Negative control: the tag must actually DISCRIMINATE. A key shape that
    # gave every object the same tag would pass the check above and put the
    # entire cache on one pair.
    with fs().open(BIG, "rb") as h:
        h.seek(0); h.read(4096)
    tags2 = {re.search(rb"\{(.*?)\}", k).group(1)
             for k in rc.scan_iter(match=b"c2/*")}
    check(len(tags2) > 1,
          f"negative control -- a DIFFERENT object gets a different tag ({len(tags2)} distinct)")

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
