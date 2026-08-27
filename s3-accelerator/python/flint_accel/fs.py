# SPDX-License-Identifier: Apache-2.0
"""fsspec / s3fs integration.

Adoption is one line and changes no paths:

    import flint_accel
    flint_accel.install(tier_uri="redis://flint:6379")

which re-registers the ``s3`` protocol so every existing ``s3://`` path routes
through the tier. That mirrors ``fs.s3a.impl`` on the JVM side and avoids the
two-endpoint problem ADR-0023 D1 rejected: a user who has to rewrite paths has
not adopted a cache, they have adopted a migration.

The seam is ``AbstractBufferedFile._fetch_range(start, end)`` -- fsspec's own
hook for ranged reads, and unlike the three JVM seams it is a single door.
Everything above it (buffering, readahead, the file API) is inherited, and the
origin on a miss is ``super()._fetch_range``, so s3fs keeps using the caller's
own credentials exactly as ADR-0023 D1 requires.
"""

from __future__ import annotations

import fsspec
from s3fs import S3FileSystem, S3File

from .conn import bounded_client
from .tier import (FlintTier, CHUNK, TIER_BUDGET_S, META_TTL_S,
                   MAX_OBJECT_BYTES, MAX_PART_BYTES, BLOCK_BYTES)


class _OriginAdapter:
    """The tier's view of 'where bytes come from on a miss': s3fs itself."""

    def __init__(self, f: "FlintS3File"):
        self.f = f

    def head(self, bucket, key):
        info = self.f.fs.info(f"{bucket}/{key}")
        etag = (info.get("ETag") or info.get("etag") or "").strip('"')
        return {"length": int(info["size"]), "etag": etag}

    def get(self, bucket, key, lo, hi):        # hi INCLUSIVE, as S3 ranges are
        return S3File._fetch_range(self.f, lo, hi + 1)

    def sse(self, bucket, key):
        """The server-side-encryption header, which s3fs.info() throws away.

        s3fs whitelists the fields it surfaces and the encryption headers are
        not among them -- the same shape as AAL's ObjectMetadata dropping them
        on the JVM side -- so the raw HEAD has to be issued directly. This is
        the documented way to reach s3fs's client from sync code.
        """
        import fsspec.asyn
        r = fsspec.asyn.sync(self.f.fs.loop, self.f.fs._call_s3,
                             "head_object", Bucket=bucket, Key=key)
        return r.get("ServerSideEncryption")


class FlintS3File(S3File):
    """s3fs's file, with ranged reads served from the tier."""

    def _fetch_range(self, start, end):        # end EXCLUSIVE, per fsspec
        tier = self.fs._flint_tier(_OriginAdapter(self))
        etag = (self.details.get("ETag") or self.details.get("etag") or "").strip('"')
        size = self.details.get("size")
        return tier.read(self.bucket, self.key, start, end - start,
                         etag=etag or None, size=size)


class FlintS3FileSystem(S3FileSystem):
    """``S3FileSystem`` whose reads go through Flint.

    Subclassed rather than wrapped, so writes, listings, deletes, credentials
    and every other s3fs behaviour are inherited untouched -- the same argument
    that decided the JVM side.
    """

    protocol = ("s3", "s3a", "flint")

    #: Settings install() may supply, and the value used if nobody supplies one.
    #: Precedence is explicit argument > install() default > built-in.
    _FLINT_OPTS = {
        "tier_uri": "redis://127.0.0.1:6379",
        "chunk": CHUNK,
        "tier_budget_s": TIER_BUDGET_S,
        "meta_ttl_s": META_TTL_S,
        "cache_sse_kms": False,
        "max_object_bytes": MAX_OBJECT_BYTES,
        # D17.5. Listed here or it is silently dropped: this map is an
        # enumeration, and a key missing from it does not fail -- it just never
        # reaches the client, which is how flint.cache.sse-kms was unusable for
        # a while. The JVM side takes it as flint.max.part.bytes.
        "max_part_bytes": MAX_PART_BYTES,
    }

    #: Set by install(**defaults). Read HERE -- it previously was not.
    _flint_defaults = {}

    def __init__(self, *args, **kw):
        """
        install() sets _flint_defaults; this reads it.

        It did not, for as long as install() has existed: the attribute was
        assigned and never consumed, so every argument passed to install() --
        the tier endpoint included -- was silently discarded and the built-in
        defaults used instead. Nothing caught it because every suite
        constructed this class DIRECTLY with explicit keywords, while the
        README told users to call install(). The tested path and the documented
        path were different paths, and only the tested one worked.
        """
        inst = dict(type(self)._flint_defaults or {})
        opts = {}
        for k, built_in in self._FLINT_OPTS.items():
            if k in kw:
                opts[k] = kw.pop(k)          # explicit argument wins
            elif k in inst:
                opts[k] = inst[k]            # then whatever install() was given
            else:
                opts[k] = built_in
        # Anything else install() carried is an s3fs storage option -- keys,
        # endpoint, client_kwargs -- and must reach s3fs, not be dropped here.
        for k, v in inst.items():
            if k not in self._FLINT_OPTS:
                kw.setdefault(k, v)

        # fsspec's block cache decides how much s3fs drags from the origin to
        # serve a read, and this default is a TRADE. Smaller blocks cut read
        # amplification and raise warm-path tier round trips AND warm-path tier
        # bytes -- four axes, measured, in BLOCK_BYTES.
        #
        # It was briefly set to the chunk size on the strength of the
        # amplification number alone -- 31x down to 2.0x -- which looked free
        # because every axis instrumented at the time lived on the S3 side of
        # the cache. The cost was on the tier side, unmeasured: 64 MiB walked in
        # 4 KiB reads costs 964 tier round trips at 64 KiB against 253 at
        # 256 KiB, and moves 1.88x the bytes asked against 1.23x.
        #
        # It must also be a whole number of CHUNKs, which is a fifth axis the
        # four above cannot see. fsspec anchors blocks at multiples of the block
        # size, so a block that is not a whole number of chunks starts mid-chunk
        # and drags an extra one -- off the origin as well as the tier, because
        # a miss is fetched on chunk boundaries. It holds trivially while both
        # are powers of two, which is exactly why it needs asserting: tier.py
        # does it at import, so a grid change cannot break it quietly.
        #
        # setdefault, so an explicit argument still wins and so does anything
        # install() was handed. An override that breaks the nesting costs 25%
        # amplification silently; that is a documented consequence of tuning it,
        # not a bug to defend against here.
        kw.setdefault("default_block_size", BLOCK_BYTES)

        super().__init__(*args, **kw)
        self._tier_uri = opts["tier_uri"]
        self._chunk = opts["chunk"]
        self._budget = opts["tier_budget_s"]
        self._meta_ttl = opts["meta_ttl_s"]
        self._max_object = opts["max_object_bytes"]
        self._redis = None
        self._tier_obj = None
        # D13: SSE-C means the tier never sees the bytes. s3fs carries the
        # customer key in s3_additional_kwargs; if it is there, we are out.
        extra = (kw.get("s3_additional_kwargs") or {})
        self._sse_c = any(k.startswith("SSECustomer") for k in extra)
        # D13.3: SSE-KMS bypasses the cache unless the customer opts in, having
        # decided that losing the KMS grant as the access gate and losing the
        # CloudTrail decrypt record are acceptable for their data. Same default
        # and same flag name as the JVM client, because the two share one tier
        # and a guarantee that differs by language is not a guarantee.
        self._cache_kms = bool(opts["cache_sse_kms"])

    def _flint_tier(self, origin):
        if self._redis is None:
            # NOT Redis.from_url(socket_timeout=...): that budget is applied
            # per recv(), so a large reply gets a fresh one per instalment and
            # the command is unbounded. bounded_client bounds the command, the
            # way the JVM's orTimeout does. See conn.py.
            self._redis = bounded_client(self._tier_uri, self._budget)
        if self._tier_obj is None:
            self._tier_obj = FlintTier(self._redis, origin, chunk=self._chunk,
                                       budget_s=self._budget,
                                       meta_ttl_s=self._meta_ttl,
                                       bypass=self._sse_c,
                                       cache_kms=self._cache_kms,
                                       max_object_bytes=self._max_object)
        else:
            self._tier_obj.origin = origin      # per-file origin, shared cache
        return self._tier_obj

    def _meta_tier(self):
        """The tier, for metadata only -- no per-file origin to attach."""
        if self._redis is None or self._tier_obj is None:
            try:
                self._flint_tier(None)
            except Exception:
                return None
        return self._tier_obj

    async def _info(self, path, bucket=None, key=None, refresh=False, version_id=None):
        """Serve object metadata from the tier.

        MEASURED BEFORE WRITING THIS: six opens over three distinct objects cost
        nine origin HEADs, and cost nine again against a fully warm tier, with
        ZERO metadata keys written. The Python path's metadata caching did
        nothing whatsoever -- FlintTier.head() is never reached, because s3fs
        resolves details through _info long before our read path runs. D3 says
        metadata caching carries most of the request saving; this side captured
        none of it.

        On a miss we issue head_object OURSELVES rather than delegating. s3fs
        makes the identical call and then discards the encryption headers, so
        doing it here costs nothing extra and yields the SSE state the cache
        entry needs (D13.3) from the same round trip -- the same reasoning that
        put ourHead() on the JVM side.
        """
        if refresh or version_id is not None:
            return await super()._info(path, bucket, key, refresh, version_id)
        try:
            b, k, path_version_id = self.split_path(self._strip_protocol(path))
        except Exception:
            return await super()._info(path, bucket, key, refresh, version_id)
        if not b or not k:
            # buckets and pure prefixes are a listing question, not a HEAD
            return await super()._info(path, bucket, key, refresh, version_id)
        if path_version_id is not None:
            # A version pinned in the path is a DIFFERENT object from the one
            # our key addresses. Dropping it -- which this did -- serves the
            # current version's metadata for an explicit version request.
            return await super()._info(path, bucket, key, refresh, version_id)

        # s3fs answers from its own directory cache BEFORE it ever heads, and
        # that answer is authoritative about existence: a path absent from a
        # cached listing is absent. Serving a tier entry ahead of it made
        # deleted objects reappear -- three fsspec copy tests failed on exactly
        # that, each asserting a nested file did NOT exist after a
        # non-recursive copy. Hand the whole case back rather than
        # reimplementing rules we would only get subtly wrong.
        if self._ls_from_cache("/".join((b, k))) is not None:
            return await super()._info(path, bucket, key, refresh, version_id)

        t = self._meta_tier()
        if t is None:
            return await super()._info(path, bucket, key, refresh, version_id)

        hit = t.meta_get(b, k)
        if hit is not None:
            length, etag, kms = hit
            # Hand the SSE answer to the read path. Without this it re-probes
            # with a raw HEAD per object per process (_OriginAdapter.sse), so a
            # fully warm read still cost one origin HEAD -- the JVM avoids it
            # the same way, by trusting the entry's third field.
            t._kms[(b, k)] = kms
            # LastModified is NOT carried: the entry's three fields are shared
            # byte-for-byte with the JVM, whose parser reads the third field as
            # everything after the second separator, so a fourth field would
            # silently read as "not KMS". "" is what s3fs itself returns when
            # S3 omits the header.
            return {"ETag": etag, "LastModified": "", "size": length,
                    "name": f"{b}/{k}", "type": "file",
                    "StorageClass": "STANDARD", "VersionId": None,
                    "ContentType": None}

        try:
            out = await self._call_s3("head_object", Bucket=b, Key=k, **self.req_kw)
        except Exception:
            # Directories, 404s, permission shapes -- anything that is not a
            # plain object head. s3fs knows how to interpret all of those and we
            # do not, so hand it back rather than guessing.
            return await super()._info(path, bucket, key, refresh, version_id)

        kms = (out.get("ServerSideEncryption") or "").lower() == "aws:kms"
        t.meta_put(b, k, out["ContentLength"], (out.get("ETag") or ""), kms)
        t._kms[(b, k)] = kms
        return {"ETag": out.get("ETag", ""), "LastModified": out.get("LastModified", ""),
                "size": out["ContentLength"], "name": f"{b}/{k}", "type": "file",
                "StorageClass": out.get("StorageClass", "STANDARD"),
                "VersionId": out.get("VersionId"),
                "ContentType": out.get("ContentType")}

    def invalidate_cache(self, path=None):
        """Drop our tier metadata wherever s3fs drops its own.

        s3fs funnels every mutation -- put, rm, copy, touch -- through here,
        which makes it the one seam that sees all of them. Our entry has to die
        with s3fs's: without this, ``exists()`` kept answering True for an
        object THIS PROCESS had just deleted, and two fsspec copy tests failed
        asserting a nested file was not there after a non-recursive copy.

        A TTL is not an answer to that. It bounds how long a stale entry can
        outlive a change made ELSEWHERE, which is the documented D3 contract;
        it says nothing about a process contradicting itself, and that window
        is precisely the one a caller can observe.

        ``path=None`` means "forget everything", and s3fs implements it by
        clearing its own dict. We deliberately do NOT do the equivalent: the
        tier is SHARED, so flushing it on one client's invalidate would throw
        away work every other reader paid for. The local memo is cleared and
        the shared entries are left to their TTL.
        """
        super().invalidate_cache(path)
        if path is None:
            # "Forget everything" clears the LOCAL memo only. s3fs implements
            # it by clearing its own dict; the tier is SHARED, so doing the
            # equivalent would throw away work every other reader paid for.
            if self._tier_obj is not None:
                self._tier_obj._kms.clear()
            return
        # Lazily, because an instance that has only ever WRITTEN has no tier
        # object yet -- and it is exactly the instance whose writes must
        # invalidate. Guarding on self._tier_obj meant a writer silently
        # skipped invalidation and left a stale entry for every reader sharing
        # the tier. The entry carries the object LENGTH, so that is not merely
        # stale: a path rewritten shorter makes readers stop at EOF early.
        t = self._meta_tier()
        if t is None:
            return
        try:
            b, k, _v = self.split_path(self._strip_protocol(path))
        except Exception:
            return
        if b and k:
            t.meta_del(b, k)

    def _open(self, path, mode="rb", block_size=None, **kw):
        if "r" not in mode:
            return super()._open(path, mode=mode, block_size=block_size, **kw)
        kw.pop("autocommit", None)
        return FlintS3File(self, path, mode=mode,
                           block_size=block_size or self.default_block_size, **kw)

    @property
    def counters(self):
        return self._tier_obj.c.as_dict() if self._tier_obj else None
