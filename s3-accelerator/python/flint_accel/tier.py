# SPDX-License-Identifier: Apache-2.0
"""The cached read path, ported from the JVM client.

Every decision here has a measurement behind it in ADR-0023, and the port is
deliberate rather than a rewrite -- the JVM side spent a long time discovering
that chunking is load-bearing, that metadata caching carries most of the
request saving, and that a tier failure must degrade rather than hang. None of
that is worth rediscovering in a second language.

  D3   two-level keys   metadata under a TTL, data addressed by ETag
  D4   chunking         absolute 64 KiB grid, fetched ON the grid
  D5   single-flight    per CHUNK, not per request
  D13  SSE-C            bypass entirely; the tier never sees plaintext
"""

from __future__ import annotations

import threading
import time
import zlib
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutureTimeout

CHUNK = 64 * 1024
#: What fsspec's block cache fetches per miss. NOT the chunk size, and not
#: fsspec's own 5 MiB either.
#:
#: MEASURED on all FOUR axes, 64 MiB object, ReadAheadCache (what s3fs actually
#: defaults to), counts exact. The earlier table carried one axis per side --
#: origin bytes cold, tier round trips warm -- which is the shape of the trade
#: only if those are the only axes. They are not:
#:
#:   selective, 32 x 64 KiB reads (2 MiB asked)
#:     block    origin bytes   tier trips   TIER BYTES
#:     5 MiB      27.3x            11         27.3x
#:     1 MiB      17.0x            32         17.0x
#:     256 KiB     5.0x            32          5.0x
#:     64 KiB      2.0x            32          2.0x
#:
#:   small-sequential, 64 MiB walked in 4 KiB reads
#:     block    origin bytes   tier trips   TIER BYTES
#:     5 MiB       1.00x           13          1.01x
#:     1 MiB       1.00x           64          1.06x
#:     256 KiB     1.00x          253          1.23x
#:     64 KiB      1.00x          964          1.88x
#:
#:   large sequential: 8 trips and 1.00x at EVERY value, on both axes.
#:
#: The fourth axis is the one that had never been counted, and it does not
#: behave like the other three. A small block amplifies tier BYTES on the warm
#: path -- 64 KiB moves 1.88x what was asked, against 1.00x from the origin on
#: the identical cold read -- because every fetch straddles the chunk grid and
#: costs one chunk of spill. So 64 KiB is not the cheap end of a trade; it is
#: worse than 256 KiB on BOTH tier axes for sequential work and better only on
#: bytes for selective work.
#:
#: The trade that survives is 256 KiB against 1 MiB, and it has a break-even.
#: Per MiB asked, 256 KiB costs +2.95 tier round trips and +0.18 MB on
#: small-sequential, and saves 12.59 MB on selective. With f the fraction of
#: bytes read selectively, 256 KiB wins when
#:
#:     f x 12.59 MB / tier_BW  >  (1 - f) x (2.95 x tier_RTT + 0.18 MB / tier_BW)
#:
#: which at 0.2 ms RTT is f > 6% at 1 GB/s and f > 33% at 10 GB/s per
#: connection. 64 KiB would need f > 48%. The analytics reads this cache is for
#: -- Parquet footers, Iceberg manifests, column projection -- sit far above 6%,
#: so 256 KiB is the measured choice and not merely the middle one. tier_RTT and
#: tier_BW are the two numbers M0 owes this formula.
BLOCK_BYTES = 256 * 1024
#: Objects larger than this are read straight from the origin and never
#: chunk-cached.
#:
#: 512 MiB, and it is the number we are USING rather than a number anything
#: determined -- nothing here distinguishes it from its neighbours. What the
#: measurement below did was retire a wrong reason, correct a second by 165x,
#: and show that a cap denominated in object bytes cannot have a best value at
#: all, because object bytes is not what the tier spends.
#:
#: The premise holds. A warm read moves the same bytes as a cold one, measured
#: to within 0.02% on every pattern and every block size: 5,244,131 B off the
#: tier against 5,242,880 B off the origin for the identical selective read,
#: the difference being the D14 seal and RESP framing.
#:
#: What does NOT follow is a threshold. time_saved = bytes x (1/S3 - 1/tier) is
#: linear in bytes with a sign that does not depend on bytes, so it says either
#: every size pays or no size pays -- it cannot produce a size at which caching
#: stops being worth it. Measured, both sides scale linearly and neither
#: parallelism is a function of object size: a 64 MiB sequential read costs 8
#: origin GETs and 8 tier round trips whatever its size, at peak origin
#: concurrency 1 on this path (3 on the JVM's, where AAL prefetches). So the
#: payoff argument sets whether to cache sequential reads AT ALL -- tier_BW
#: against 1x a connection here, 3x on the JVM -- and never sets a cap.
#:
#: Two costs ARE size-dependent, and they are what the cap actually defends:
#:
#:   FILL. _fill writes one chunk per SET, synchronously, so a cold read of an
#:   object of size S costs S/CHUNK blocking tier round trips before it
#:   returns: 1,024 measured for 64 MiB, 8,192 for an object at this cap. The
#:   JVM has no equivalent -- Lettuce pipelines the same writes -- so this is a
#:   Python-path cost, and pipelining the SETs would remove most of the reason
#:   for a cap on this path at all.
#:
#:   OCCUPANCY. A cached object costs 1.2522x its own bytes of tier memory,
#:   measured at fleet scale on jemalloc 5.3.0: a 64 KiB chunk plus its 4-byte
#:   seal needs 82,008 B, because 65,540 B lands one byte past the allocator's
#:   64 KiB size class and takes the 80 KiB one. Not the ~100 B/key this
#:   comment used to claim -- 16,468 B/key, 165x more -- and it is the CHUNK
#:   size that lands badly, not the seal, which is free. See ADR-0023 D17.
#:
#: Neither derives a value. Fill cost gives a band (~320-640 MiB across
#: plausible tier RTTs, not a point) and occupancy gives a number only once
#: someone fixes a policy for what share of a node one object may take. At 512
#: MiB those read as ~8k blocking fills on the cold read and 641 MiB of tier for
#: one object -- defensible, not derived. The one justification that ever
#: pointed at THIS number is workload fit: 128-512 MB Parquet and Iceberg files.
#:
#: The cap rations the wrong quantity, and that is now measured rather than
#: suspected. What the tier spends is CHUNKS TOUCHED, not object bytes: a
#: Parquet-shaped read -- footer plus two column chunks -- touches 26 chunks and
#: occupies 2.03 MiB on an 8 MiB object and on a 64 MiB object alike, while a
#: full read of the same 64 MiB object occupies 80 MiB. An occupancy bound would
#: admit the first and still refuse the second, which is what D18's admission
#: class is shaped like and what this cap is a crude stand-in for.
MAX_OBJECT_BYTES = 512 * 1024 * 1024
#: Any tier COMMAND slower than this is a miss -- and that is now what the code
#: does, which it did not used to be.
#:
#: Reached as redis-py's socket_timeout, this bounded each recv() rather than
#: the command, and redis-py reads a large reply in many recvs. Measured: an
#: 8 MiB warm read through a tier throttled to 1 MB/s -- about 8 seconds -- was
#: served from the tier with tier_failures=0 and degraded=0, because no single
#: recv ever waited 50 ms. The JVM degraded on the identical read, its budget
#: being orTimeout() over the whole command, so D12.9's "never slower than no
#: cache" held on one path only. conn.bounded_client closes that: the same read
#: now degrades on both, and tools/narrow_tier.py + budget_suite.py hold it
#: there.
#:
#: The operational consequence is worth knowing before tuning this. Bounding
#: the command means a warm read whose reply is R bytes needs R/budget of tier
#: bandwidth or it degrades to the origin. Large sequential reads fetch up to
#: ~8 MiB per mget (measured), so they need ~1.3 Gbit/s of effective tier
#: throughput to stay cached at 50 ms. That is the guarantee working -- below
#: that the tier really is slower than S3 for those reads (D17) -- but it means
#: this number and the read size set a bandwidth floor together, not alone.
TIER_BUDGET_S = 0.05
META_TTL_S = 60


class Counters:
    """Every claim this product makes is a counter (D8)."""

    __slots__ = ("chunk_hits", "chunk_misses", "meta_hits", "meta_misses",
                 "origin_gets", "origin_bytes", "tier_failures", "degraded",
                 "claimed", "joined", "bypassed", "integrity_failures",
                 "kms_bypassed", "kms_undetectable", "oversize_bypassed")

    def __init__(self):
        for s in self.__slots__:
            setattr(self, s, 0)

    def as_dict(self):
        return {s: getattr(self, s) for s in self.__slots__}


class FlintTier:
    """Chunk cache over a Redis-protocol backend.

    `origin` supplies the bytes on a miss and is the caller's own S3 client,
    so the tier never holds a credential.
    """

    def __init__(self, redis_client, origin, chunk=CHUNK,
                 budget_s=TIER_BUDGET_S, meta_ttl_s=META_TTL_S, bypass=False,
                 cache_kms=False, max_object_bytes=MAX_OBJECT_BYTES):
        self.r = redis_client
        self.origin = origin
        self.chunk = chunk
        self.budget_s = budget_s
        self.meta_ttl_s = meta_ttl_s
        self.bypass = bypass
        # Default FALSE and it must stay false: a default that silently caches
        # KMS plaintext is the version of this that ends a security review.
        self.cache_kms = cache_kms
        self.max_object_bytes = max_object_bytes
        self._kms = {}
        self.c = Counters()
        self._inflight = {}
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- tier
    #: One pool for every tier call in the process. Shared rather than
    #: per-instance because fsspec hands out many filesystems and a pool each
    #: would be thousands of threads. When it saturates, submit() queues and the
    #: caller's own budget still expires -- which degrades to the origin, the
    #: correct answer for a tier too slow to keep up.
    _POOL = None
    _POOL_LOCK = threading.Lock()

    @classmethod
    def _pool(cls):
        if cls._POOL is None:
            with cls._POOL_LOCK:
                if cls._POOL is None:
                    cls._POOL = ThreadPoolExecutor(
                        max_workers=32, thread_name_prefix="flint-tier")
        return cls._POOL

    def _guard(self, fn, *a, **kw):
        """Any tier failure is a miss, never an error, and any tier call slower
        than the budget is a failure.

        S3 is authoritative and always reachable, so every tier interaction is
        an OPTIMISATION and has to be written as one. The JVM version learned
        this the hard way: a dead tier made the client HANG rather than fail,
        because the driver queued commands while disconnected.

        The call runs on a worker so the BUDGET BOUNDS THE COMMAND. redis-py's
        socket_timeout cannot: CPython applies it per recv(), so it bounds the
        gap between instalments of a reply rather than the reply, and a tier
        that answers promptly and then dribbles slips straight through it. This
        mirrors the JVM's orTimeout(), including its limitation -- neither can
        cancel the I/O already in flight, so the worker is abandoned rather than
        killed. It always finishes, because socket_timeout still bounds each of
        its reads, and the connection returns to the pool intact.
        """
        pool = type(self)._pool()
        try:
            fut = pool.submit(fn, *a, **kw)
        except RuntimeError:                     # interpreter shutting down
            return None
        try:
            return fut.result(timeout=self.budget_s)
        except _FutureTimeout:
            # Budget exceeded, not an error: the tier is up and answering, just
            # too slowly to be worth waiting for. Counted as degraded, which is
            # what the JVM calls the same condition, so one number means one
            # thing across both clients.
            self.c.degraded += 1
            return None
        except Exception:
            self.c.tier_failures += 1
            return None

    @staticmethod
    def _norm(etag):
        return (etag or "").strip('"')

    # ------------------------------------------------------------- D14 seal
    # A cached chunk is stored as a 4-byte CRC32 over the chunk's IDENTITY --
    # object ETag and chunk index -- followed by its bytes. Byte-identical to
    # the JVM client's seal, because the two share one tier and a seal only one
    # of them can compute would silently split the cache in half.
    #
    # zlib.crc32 and java.util.zip.CRC32 are the same polynomial and agree
    # exactly; CRC32C, which the JVM side used first, has no Python
    # standard-library equivalent. The cross-language drill asserts the
    # agreement on a fixed vector rather than trusting this comment.
    SEAL = 4

    @classmethod
    def _seal_of(cls, etag, idx, body):
        c = zlib.crc32(cls._norm(etag).encode())
        c = zlib.crc32(idx.to_bytes(8, "little"), c)
        return zlib.crc32(body, c) & 0xFFFFFFFF

    @classmethod
    def _seal(cls, etag, idx, body):
        return cls._seal_of(etag, idx, body).to_bytes(cls.SEAL, "little") + body

    def _unseal(self, etag, idx, sealed):
        """The payload, or None if this value is not what it claims to be."""
        if not sealed or len(sealed) < self.SEAL:
            if sealed:
                self.c.integrity_failures += 1
            return None
        want = int.from_bytes(sealed[:self.SEAL], "little")
        if want != self._seal_of(etag, idx, sealed[self.SEAL:]):
            self.c.integrity_failures += 1
            return None
        return sealed[self.SEAL:]

    def _ck(self, etag, idx):
        # Versioned prefix: a value-format change without a key change gives a
        # mixed fleet where new clients reject every value old clients wrote --
        # 100% miss and a stampede onto the origin.
        return f"c1/{self._norm(etag)}/{idx}".encode()

    def _mk(self, bucket, key):
        """Byte-identical to the JVM client's metadata key.

        It was not, and nothing noticed: the JVM wrote m1/s3://bucket/key while
        this wrote m/bucket/key -- different prefix AND different path form, so
        the two never shared a metadata entry. The chunk keyspace was unified
        and verified by a drill; the metadata keyspace was never checked, which
        is what "we share one tier" quietly meant.
        """
        return f"m1/s3://{bucket}/{key}".encode()

    # ------------------------------------------------------- metadata entries
    # Value format matches the JVM exactly: length|etag|kms, where the third
    # field is "1" when S3 decrypted the object with KMS. Carrying the answer
    # WITH the metadata is what stops the SSE-KMS rule depending on which
    # client happened to populate the entry first.
    def meta_get(self, bucket, key):
        """(length, etag, kms) from the tier, or None."""
        if self.bypass:
            return None
        raw = self._guard(self.r.get, self._mk(bucket, key))
        if not raw:
            return None
        try:
            length, etag, kms = raw.decode().split("|", 2)
            self.c.meta_hits += 1
            return int(length), etag, kms == "1"
        except ValueError:
            return None

    def meta_del(self, bucket, key):
        """Forget one object's metadata, because the caller just changed it."""
        if self.bypass:
            self._kms.pop((bucket, key), None)
            return
        self._guard(self.r.delete, self._mk(bucket, key))
        self._kms.pop((bucket, key), None)

    def meta_put(self, bucket, key, length, etag, kms):
        """Cache metadata. Refuses when the SSE state is UNKNOWN.

        Writing an unverified "not encrypted" is worse than writing nothing,
        because a client that CAN check would then read and trust it -- the
        same rule the JVM side arrived at, and the reason a client with no way
        to probe caches no metadata at all.
        """
        # Bypass is checked HERE and not only at the call sites, because a
        # caller that forgets is exactly how this broke: `bypass` is set for
        # SSE-C (D13), the chunk path honoured it, and _info wrote metadata for
        # an SSE-C object anyway -- length and etag of bytes the tier is not
        # allowed to know about. The JVM has this ordering right, testing
        # bypass in the first line of headObject.
        if self.bypass:
            return
        if kms is None:
            self.c.kms_undetectable += 1
            return
        if kms and not self.cache_kms:
            return                       # bypass means bypass: no metadata either
        self._guard(self.r.setex, self._mk(bucket, key), self.meta_ttl_s,
                    f"{length}|{etag}|{'1' if kms else '0'}".encode())

    # ------------------------------------------------------------ metadata
    def head(self, bucket, key):
        """D3 level 2. Not an optimisation: without it, short-lived readers
        pay a HEAD per open and HEADs cost the same as GETs."""
        if self.bypass:
            self.c.bypassed += 1
            return self.origin.head(bucket, key)
        mk = self._mk(bucket, key)
        raw = self._guard(self.r.get, mk)
        if raw:
            try:
                length, etag, _kms = raw.decode().split("|", 2)
                self.c.meta_hits += 1
                return {"length": int(length), "etag": etag}
            except ValueError:
                pass
        self.c.meta_misses += 1
        m = self.origin.head(bucket, key)
        # Third field required now. This path has no SSE answer to hand, so it
        # defers to meta_put's refusal rather than inventing one.
        self.meta_put(bucket, key, m["length"], m["etag"],
                      self._is_kms(bucket, key) if hasattr(self.origin, "sse") else None)
        return m

    # ------------------------------------------------------------- SSE-KMS
    def _is_kms(self, bucket, key):
        """Whether S3 decrypted this object with KMS on the way out.

        ADR-0023 D13.3. The check lives HERE and not in head(), because the
        read path does not call head() at all -- s3fs already has the etag and
        size in the file's details and passes them straight in, so a check
        wired into the metadata path would simply never run on the path that
        matters. That is the same shape as the D12.12 fill guard whose
        condition never held.

        s3fs.info() cannot answer this: it whitelists a fixed set of fields and
        the encryption headers are not among them, exactly as AAL's
        ObjectMetadata drops them on the JVM side. Both clients have to issue
        their own HEAD.
        """
        k = (bucket, key)
        hit = self._kms.get(k)
        if hit is not None:
            return hit
        probe = getattr(self.origin, "sse", None)
        if probe is None:
            self.c.kms_undetectable += 1
            return False
        try:
            v = (probe(bucket, key) or "").lower() == "aws:kms"
        except Exception:
            # A failed probe must not fail the read. Counted, because "checked
            # and it is not KMS" and "could not check" must not look alike --
            # this number is the exact size of the hole in the guarantee.
            self.c.kms_undetectable += 1
            return False
        if len(self._kms) > 8192:
            self._kms.clear()
        self._kms[k] = v
        return v

    # ---------------------------------------------------------------- read
    def read(self, bucket, key, start, length, etag=None, size=None):
        """Bytes [start, start+length) via the chunk grid."""
        if self.bypass:
            self.c.bypassed += 1
            self.c.origin_gets += 1
            return self.origin.get(bucket, key, start, start + length - 1)

        # SSE-KMS bypasses the cache unless the customer opted in. The tier is
        # SHARED with the JVM client by design, so a gap here would let a
        # Python reader cache plaintext the JVM reader refuses to cache -- the
        # guarantee is only as strong as its weakest client.
        # Capacity admission. Checked BEFORE the KMS probe so an oversize
        # object costs no HEAD to reject, and counted rather than silent --
        # "not cached because too large" and "not cached because something
        # broke" must never look alike in the counters.
        #
        # size is None when the caller could not tell us; we cache in that
        # case rather than guess, because refusing on an unknown is how a cap
        # quietly turns into "cache nothing".
        if size is not None and size > self.max_object_bytes:
            self.c.oversize_bypassed += 1
            self.c.origin_gets += 1
            return self.origin.get(bucket, key, start, start + length - 1)

        if not self.cache_kms and self._is_kms(bucket, key):
            self.c.kms_bypassed += 1
            self.c.origin_gets += 1
            return self.origin.get(bucket, key, start, start + length - 1)

        if etag is None or size is None:
            m = self.head(bucket, key)
            etag, size = m["etag"], m["length"]
        end = min(start + length, size) - 1
        if end < start:
            return b""

        first, last = start // self.chunk, end // self.chunk
        idxs = list(range(first, last + 1))
        keys = [self._ck(etag, i) for i in idxs]

        vals = self._guard(self.r.mget, keys)
        if vals is None:
            self.c.degraded += 1
            self.c.origin_gets += 1
            return self.origin.get(bucket, key, start, end)

        have = {}
        missing = []
        for i, v in zip(idxs, vals):
            body = self._unseal(etag, i, v)
            if body is not None:
                self.c.chunk_hits += 1
                have[i] = body
            else:
                self.c.chunk_misses += 1
                missing.append(i)

        for run in self._runs(missing):
            self._fill(bucket, key, etag, size, run, have)

        return self._assemble(have, start, end)

    def _fill(self, bucket, key, etag, size, run, have):
        """Fetch a contiguous run ON THE GRID.

        Not by slicing whatever range the caller asked for: the JVM version
        did that, guarded on grid alignment, and the guard almost never passed
        -- so the cache was silently never populated while every correctness
        test stayed green.
        """
        lo, hi = run[0] * self.chunk, min((run[-1] + 1) * self.chunk, size) - 1
        # D5: claim the run, or wait for whoever holds it.
        token = (self._norm(etag), run[0], run[-1])
        with self._lock:
            ev = self._inflight.get(token)
            leader = ev is None
            if leader:
                ev = self._inflight[token] = threading.Event()
                self.c.claimed += 1
            else:
                self.c.joined += 1
        if not leader:
            ev.wait(timeout=self.budget_s * 40)
            got = self._guard(self.r.mget, [self._ck(etag, i) for i in run])
            opened = ([self._unseal(etag, i, v) for i, v in zip(run, got)]
                      if got else None)
            if opened and all(b is not None for b in opened):
                for i, b in zip(run, opened):
                    have[i] = b
                    self.c.chunk_hits += 1
                return
            # leader failed or timed out: fall through and fetch it ourselves
        try:
            self.c.origin_gets += 1
            blob = self.origin.get(bucket, key, lo, hi)
            self.c.origin_bytes += len(blob)
            for n, i in enumerate(run):
                piece = blob[n * self.chunk:(n + 1) * self.chunk]
                if piece:
                    have[i] = piece
                    self._guard(self.r.set, self._ck(etag, i),
                                self._seal(etag, i, piece))
        finally:
            if leader:
                with self._lock:
                    self._inflight.pop(token, None)
                ev.set()

    def _assemble(self, have, start, end):
        out = bytearray()
        for i in sorted(have):
            cs = i * self.chunk
            piece = have[i]
            lo = max(0, start - cs)
            hi = min(len(piece), end - cs + 1)
            if hi > lo:
                out += piece[lo:hi]
        return bytes(out)

    @staticmethod
    def _runs(idxs):
        runs, cur = [], []
        for i in sorted(idxs):
            if cur and i == cur[-1] + 1:
                cur.append(i)
            else:
                if cur:
                    runs.append(cur)
                cur = [i]
        if cur:
            runs.append(cur)
        return runs
