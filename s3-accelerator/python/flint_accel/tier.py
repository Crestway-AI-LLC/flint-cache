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

CHUNK = 64 * 1024
TIER_BUDGET_S = 0.05          # any tier call slower than this is a miss
META_TTL_S = 60


class Counters:
    """Every claim this product makes is a counter (D8)."""

    __slots__ = ("chunk_hits", "chunk_misses", "meta_hits", "meta_misses",
                 "origin_gets", "origin_bytes", "tier_failures", "degraded",
                 "claimed", "joined", "bypassed", "integrity_failures",
                 "kms_bypassed", "kms_undetectable")

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
                 cache_kms=False):
        self.r = redis_client
        self.origin = origin
        self.chunk = chunk
        self.budget_s = budget_s
        self.meta_ttl_s = meta_ttl_s
        self.bypass = bypass
        # Default FALSE and it must stay false: a default that silently caches
        # KMS plaintext is the version of this that ends a security review.
        self.cache_kms = cache_kms
        self._kms = {}
        self.c = Counters()
        self._inflight = {}
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- tier
    def _guard(self, fn, *a, **kw):
        """Any tier failure is a miss, never an error.

        S3 is authoritative and always reachable, so every tier interaction is
        an OPTIMISATION and has to be written as one. The JVM version learned
        this the hard way: a dead tier made the client HANG rather than fail,
        because the driver queued commands while disconnected. The timeout is
        the backstop for slow-rather-than-broken; the except is for the rest.
        """
        try:
            return fn(*a, **kw)
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
