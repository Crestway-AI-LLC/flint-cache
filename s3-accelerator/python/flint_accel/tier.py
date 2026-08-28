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

import concurrent.futures
import threading
import time
import zlib

import redis as redis_lib

#: The chunk grid. A power of two, and it costs 1.25x tier memory to be one.
#:
#: A chunk is stored as CHUNK + 4 bytes of D14 seal. At 65,536 that is 65,540,
#: which lands one byte past jemalloc's 64 KiB size class, so the tier takes
#: the 80 KiB class and charges 1.2522x the bytes cached (ADR-0023 D17.2).
#:
#: 65,408 -- 64 KiB minus 128 -- was implemented to escape that, MEASURED, and
#: WITHDRAWN. It does cut tier memory 19.5% on a full-object read. But
#: application read offsets are themselves powers of two (this suite's are
#: 4 MiB apart; Parquet row groups and page boundaries are no different), so a
#: grid that is not a power of two no longer divides them, and every selective
#: read straddles an extra chunk. Measured on the suite's 16 x 64 KiB pattern:
#: origin bytes went 5.00x -> 5.99x of what was asked, +19.8%, to buy a ~4%
#: memory saving on that same pattern. The suite's derived ceiling caught it.
#:
#: The tension is structural, not a tuning miss. The grid must divide the
#: application's alignment to avoid the extra chunk; every value that divides a
#: power of two IS a power of two; and every power of two plus the seal and the
#: tier's ~53 B per-value header crosses a size class. So the 1.25x is the price
#: of alignment, and the crossover between the two costs is a function of read
#: size -- the +1 chunk is 20% of a 5-chunk fetch and 0.3% of a 1024-chunk one.
#: That is D4's unsettled question and it wants M2's sweep, not a constant
#: chosen here.
CHUNK = 65536
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
#:
#: Written as 4 CHUNKs rather than as 256 KiB, which is the same number today
#: and would not have been under the 65,408 grid. fsspec anchors blocks at
#: multiples of the block size, so a block starts on a chunk boundary only if
#: CHUNK divides BLOCK_BYTES; at 262,144 over that grid every block straddled a
#: FIFTH chunk. Spelling the dependency is what keeps the two constants from
#: drifting apart silently the next time either moves.
BLOCK_BYTES = 4 * CHUNK
#: Not decoration. The two constants above are chosen independently -- one
#: against the tier's allocator, one against the origin's RTT -- and nothing
#: else in the module notices when a later edit to either breaks the nesting.
#: The symptom is a silent 25% read amplification, not an error.
assert BLOCK_BYTES % CHUNK == 0, (
    f"BLOCK_BYTES ({BLOCK_BYTES}) must be a whole multiple of CHUNK ({CHUNK}): "
    "fsspec aligns blocks to multiples of the block size, so a block that is "
    "not a whole number of chunks straddles an extra chunk on every read")
#: How much of a fill goes in one round trip. Bounded by BYTES because the
#: budget bounds a command: B bytes needs B/budget of bandwidth to land inside
#: it, so an unbounded batch would degrade on a tier that reads fine.
FILL_BATCH_BYTES = 1024 * 1024
# RETIRED by D17.5.1: 0 means no object-size cap, and this comment is
# deliberately short because the reasoning that used to live here described the
# cap as live. It was 512 MiB.
#
# The measurements that retired it are ADR-0023 D17.2-D17.4 and are not
# repeated here: the payoff argument is linear in bytes with a sign that does
# not depend on bytes, so it cannot produce a size at which caching stops being
# worth it; the keyspace argument is a fixed fraction of bytes at any size
# (1.2522x, measured, not the ~100 B/key this file once claimed); and what the
# tier actually spends is CHUNKS TOUCHED, which an object-size cap does not
# measure.
#
# It had to GO rather than be superseded: it is checked first and silently
# defeated its replacement -- a 1 GiB shard refused whole before the part gate
# saw one 256 KiB request, measured as tier keys 0 -> 1 on a run that should
# have written 16,385. Still honoured when set explicitly, because it may
# already be set in the field.
MAX_OBJECT_BYTES = 0
# ADR-0023 D17.5. The cap that decides the payoff is on the PART a reader asks
# for, not the object it belongs to: the part sets how many first-byte
# latencies a read pays. 65 MiB, deliberately loose -- real part sizes cluster
# at 1-8 MiB when a reader chunks and at 883-2048 MiB when it streams a whole
# object, so anything from ~10 MB to ~500 MB separates them identically.
# MUST MATCH the JVM client's DEFAULT_MAX_PART_BYTES: the two share one cache,
# and a gap here would let one of them store what the other refuses.
MAX_PART_BYTES = 65 * 1024 * 1024

# D17.5.1. The tier write is an optimisation for FUTURE reads and does not
# belong on the latency path of the current one. Measured before this: a 1 GiB
# shard cost 23% MORE with the cache than without it on the populating pass --
# ~1,024 blocking MSETs on the critical path of a read whose bytes were already
# in hand. The JVM path has never had this because lettuce pipelines the same
# writes asynchronously.
FILL_ASYNC = True
FILL_WORKERS = 4
# Outstanding fills allowed before we stop queueing. When it is reached the
# write happens INLINE rather than being dropped: that is backpressure, and it
# keeps D5's single-flight honest. Dropping would leave followers to miss and
# refetch from origin, which is the one thing single-flight exists to prevent.
FILL_MAX_INFLIGHT = 16
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
                 "kms_bypassed", "kms_undetectable", "oversize_bypassed",
                 "oversize_part_bypassed", "fill_async", "fill_inline",
                 # ADR-0026: what the chunk grid costs on the wire. `wanted` is
                 # the byte range the caller asked this tier for; `moved` is
                 # what the grid made us fetch after rounding both ends out to
                 # chunk boundaries. A range-capable protocol would move
                 # `wanted`; the difference is the grid's bill, and it is a
                 # counter rather than an argument because that is the standard
                 # every other claim here is held to (D8).
                 "range_bytes_wanted", "chunk_bytes_moved",
                 # Tier round trips, counted CLIENT-SIDE. The suite used to get
                 # these from the server's INFO commandstats, which Flint does
                 # not implement -- and which was measuring the wrong thing
                 # anyway: the claim under test is that THIS CLIENT batches its
                 # fills, so the client is where it should be counted. Portable
                 # across any tier, and direct rather than inferred.
                 "tier_ops", "tier_reads", "tier_writes",
                 # The tier said "I am FULL", not "I am broken". Flint sheds
                 # writes with -QUOTA and KEEPS SERVING READS, so folding that
                 # into tier_failures conflates a healthy full tier with a sick
                 # one -- the same conflation oversize_bypassed exists to
                 # prevent, and the one _guard's docstring already forbids for
                 # slow-vs-broken.
                 "tier_full")

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
                 cache_kms=False, max_object_bytes=MAX_OBJECT_BYTES,
                 max_part_bytes=MAX_PART_BYTES):
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
        self.max_part_bytes = max_part_bytes
        self._fill_pool = None
        self._fill_slots = threading.Semaphore(FILL_MAX_INFLIGHT)
        if FILL_ASYNC:
            self._fill_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=FILL_WORKERS, thread_name_prefix="flint-fill")
        self._kms = {}
        self.c = Counters()
        self._inflight = {}
        self._fill_futs = set()
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- tier
    def _guard(self, fn, *a, **kw):
        """Any tier failure is a miss, never an error, and any tier call slower
        than the budget is a miss too.

        S3 is authoritative and always reachable, so every tier interaction is
        an OPTIMISATION and has to be written as one. The JVM version learned
        this the hard way: a dead tier made the client HANG rather than fail,
        because the driver queued commands while disconnected.

        The BUDGET BOUNDS THE COMMAND, and it is conn.DeadlineSocket that makes
        that true -- redis-py's socket_timeout alone cannot, since CPython
        applies it per recv() and a tier that answers promptly then dribbles
        never has a single gap long enough to trip it. The call itself stays
        synchronous: bounding the actual I/O stops the doomed transfer, where
        abandoning the call on a worker would leave it pulling the whole reply
        and go on loading a tier that is slow BECAUSE it is loaded.

        Slow and broken are counted APART, and this used to conflate them: a
        tier that was merely too slow incremented `tier_failures`, which is the
        same mistake the oversize counter exists to avoid -- "not cached
        because too large" and "not cached because something broke" must not
        look alike, and neither must "too slow" and "broke".

        A timeout increments nothing HERE on purpose. The two counters sit at
        different levels: `tier_failures` is per CALL, `degraded` is per READ,
        and one degraded read may contain several failed calls. The read path
        already counts `degraded` whenever the tier did not answer -- which is
        where the JVM counts it too, on the passthrough -- so counting a
        timeout here as well would count one event twice at two levels. A
        broken tier still lands in `tier_failures` via the clause below; what
        this clause removes is a slow tier being reported as a broken one.
        """
        # Bucketed by operation, because the two checks that consume this want
        # different halves: "does the fill batch" is about writes, "what does a
        # warm read cost" is about reads. A single total would let a read-heavy
        # phase mask a write regression and vice versa.
        self.c.tier_ops += 1
        _op = getattr(fn, "__name__", "")
        if _op in ("get", "mget"):
            self.c.tier_reads += 1
        elif _op in ("set", "setex", "mset", "delete"):
            self.c.tier_writes += 1
        try:
            return fn(*a, **kw)
        except redis_lib.TimeoutError:
            return None
        except redis_lib.ResponseError as e:
            # -QUOTA is an ANSWER, not a fault: the server is alive, reachable
            # and still serving reads, and it is telling us the namespace is
            # full. A full never-evict tier is a deliberate configuration, so
            # reporting it as breakage would send an operator looking for a
            # broken tier that is working exactly as they configured it.
            if str(e).startswith("QUOTA"):
                self.c.tier_full += 1
                return None
            self.c.tier_failures += 1
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
        # THE BRACES ARE A HASH TAG, and they are load-bearing on a multi-pair
        # fleet. Flint routes a multi-key command by its FIRST key alone
        # (flint-proxy route_key) and neither MGET nor MSET carries the
        # CROSSSLOT guard that SINTER/SUNION/SDIFF do. So a chunk whose slot the
        # receiving node does not own answers nil IN ITS CORRECT POSITION, and a
        # fill writes it to a node that does not own it.
        #
        # `{etag}` makes every chunk of one object hash to one slot
        # (flint-slot::hash_tag, Redis-compatible), so a run is always
        # single-slot and both commands are correct on any topology.
        #
        # WHAT THIS COSTS: one object now lives entirely on one pair, so a very
        # hot object no longer spreads across the fleet. Accepted -- load still
        # spreads across objects, and a 512 MiB object is at most ~8k chunks on
        # one pair.
        #
        # The accelerator was never AT RISK of wrong bytes here: a phantom nil
        # reads as a miss and the origin is authoritative, and D14's seal binds
        # every value to its own etag and index. What was at risk was the cache
        # silently not working.
        # Versioned prefix: a value-format change without a key change gives a
        # mixed fleet where new clients reject every value old clients wrote --
        # 100% miss and a stampede onto the origin.
        return f"c2/{{{self._norm(etag)}}}/{idx}".encode()

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
        if (self.max_object_bytes > 0
                and size is not None and size > self.max_object_bytes):
            self.c.oversize_bypassed += 1
            self.c.origin_gets += 1
            return self.origin.get(bucket, key, start, start + length - 1)

        # D17.5: admission on the PART. Unlike the object cap above this needs
        # no size from the caller -- the request's own length is always known,
        # so there is no unknown to fall back on and no way for it to turn
        # quietly into "cache nothing".
        if self.max_part_bytes > 0 and length > self.max_part_bytes:
            self.c.oversize_part_bypassed += 1
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
        # Counted BEFORE any hit/miss split: the question is what the grid
        # forces onto the wire, which is the same whether the chunks are in the
        # tier or have to be filled from the origin.
        self.c.range_bytes_wanted += end - start + 1
        self.c.chunk_bytes_moved += min((last + 1) * self.chunk, size) - first * self.chunk
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

        # D17.5, write side. The read path already refuses an oversize REQUEST,
        # and every run is derived from a request, so the cap holds
        # transitively today. Transitively is not enforced: a caller reaching
        # the fill path by another route would publish a part the read path
        # would have refused, and nothing would say so. The rule is "no part
        # over the cap enters the tier", so it is checked where parts enter.
        #
        # No claim is taken, matching the read path's passthrough. A claim
        # whose fill will never land makes followers wait the full budget and
        # then refetch anyway -- strictly worse than each fetching at once.
        if self.max_part_bytes > 0 and hi - lo + 1 > self.max_part_bytes:
            self.c.oversize_part_bypassed += 1
            self.c.origin_gets += 1
            blob = self.origin.get(bucket, key, lo, hi)
            self.c.origin_bytes += len(blob)
            for n, i in enumerate(run):
                piece = blob[n * self.chunk:(n + 1) * self.chunk]
                if piece:
                    have[i] = piece
            return

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
            # One round trip per BATCH, not per chunk. MEASURED before: 16 SET
            # round trips per MiB filled -- one per 64 KiB -- against a single
            # MGET to read the same run back. At the D17 cap that is 8,192
            # sequential round trips for one object, and the JVM pipelines the
            # identical writes through lettuce's async API.
            #
            # Batched by BYTES rather than by count, because the budget now
            # bounds a command (D12.9) and a command of B bytes needs B/budget
            # of bandwidth to finish inside it. 1 MiB at a 50 ms budget asks
            # ~160 Mbit/s. That is 8x below what reads already require -- an
            # 8 MiB mget wants ~1.3 Gbit/s at the same budget -- so this cannot
            # make the cache degrade on any tier that was serving reads.
            batches, batch, batch_bytes = [], {}, 0
            for n, i in enumerate(run):
                piece = blob[n * self.chunk:(n + 1) * self.chunk]
                if not piece:
                    continue
                have[i] = piece          # the CALLER's bytes are ready here
                sealed = self._seal(etag, i, piece)
                batch[self._ck(etag, i)] = sealed
                batch_bytes += len(sealed)
                if batch_bytes >= FILL_BATCH_BYTES:
                    batches.append(batch)
                    batch, batch_bytes = {}, 0
            if batch:
                batches.append(batch)
            # D17.5.1: hand the writes off and return. `have` is already
            # populated, so the read owes the tier nothing.
            handed_off = leader and self._hand_off(batches, token, ev)
        finally:
            # Only when the writes were NOT handed off -- otherwise the worker
            # owns the event, and setting it here would release followers
            # before the chunks they are waiting for exist.
            if leader and not handed_off:
                with self._lock:
                    self._inflight.pop(token, None)
                ev.set()

    def drain(self, timeout=None):
        """Block until the fills already handed off have landed in the tier.

        The fill is asynchronous on purpose (D17.5.1): a cold read must not pay
        to populate the cache. The cost of that is that "the tier holds these
        bytes" becomes true EVENTUALLY rather than on return, so anything that
        ASSERTS tier state -- a test, a flush, a snapshot, a shutdown -- needs
        a barrier. A sleep is not a barrier; it is a guess that passes on a
        quiet machine and fails on a loaded one.

        Returns the number of fills still outstanding: 0 means drained.
        """
        with self._lock:
            futs = list(self._fill_futs)
        if not futs:
            return 0
        _, not_done = concurrent.futures.wait(futs, timeout=timeout)
        return len(not_done)

    def close(self):
        """Drain outstanding fills, then stop accepting more.

        wait=True on purpose. A queued fill holds a single-flight event that
        followers are blocked on, so abandoning it strands them for the full
        wait budget -- and the writes are cheap and nearly done by definition,
        since the pool is bounded.
        """
        pool, self._fill_pool = self._fill_pool, None
        if pool is not None:
            pool.shutdown(wait=True)

    def _hand_off(self, batches, token, ev):
        """Write the run to the tier off the read path. True if a worker took it.

        THE LEADER RETURNS EARLY; FOLLOWERS DO NOT. D5 releases followers when
        the leader has filled, and they then read the run back with one mget.
        If the event were set before the writes landed they would miss and each
        refetch from origin -- which is the exact duplication single-flight
        exists to prevent, arriving at the moment the tier is slowest. So the
        worker sets the event when the writes are done, and only the leader's
        own latency is spared.

        On saturation the write happens INLINE rather than being dropped. That
        is backpressure: it bounds how much can be in flight, and it keeps
        single-flight honest under exactly the load that would otherwise break
        it. A dropped write would be cheaper and would quietly undo D5.
        """
        if not batches:
            return False
        if not self._fill_pool or not self._fill_slots.acquire(blocking=False):
            # INLINE, not dropped. An earlier draft of this method returned
            # False here without writing anything, which would have turned
            # saturation into "silently stop caching" -- the failure mode that
            # looks exactly like a working cache with a poor hit rate.
            for b in batches:
                self._guard(self.r.mset, b)
            self.c.fill_inline += 1
            return False

        def work():
            try:
                for b in batches:
                    self._guard(self.r.mset, b)
                self.c.fill_async += 1
            finally:
                self._fill_slots.release()
                with self._lock:
                    self._inflight.pop(token, None)
                ev.set()

        try:
            fut = self._fill_pool.submit(work)
            # Registered before the done-callback, so a fill that finishes
            # between the two is still discarded rather than left in the set
            # forever -- add_done_callback fires inline on a done future.
            with self._lock:
                self._fill_futs.add(fut)
            fut.add_done_callback(self._fill_done)
        except RuntimeError:          # pool shut down under us
            self._fill_slots.release()
            for b in batches:         # same rule: refused is not dropped
                self._guard(self.r.mset, b)
            self.c.fill_inline += 1
            return False
        return True

    def _fill_done(self, fut):
        with self._lock:
            self._fill_futs.discard(fut)

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
