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
import redis as redis_lib
from s3fs import S3FileSystem, S3File

from .tier import FlintTier, CHUNK, TIER_BUDGET_S, META_TTL_S


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

        super().__init__(*args, **kw)
        self._tier_uri = opts["tier_uri"]
        self._chunk = opts["chunk"]
        self._budget = opts["tier_budget_s"]
        self._meta_ttl = opts["meta_ttl_s"]
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
            self._redis = redis_lib.Redis.from_url(
                self._tier_uri, socket_timeout=self._budget,
                socket_connect_timeout=self._budget)
        if self._tier_obj is None:
            self._tier_obj = FlintTier(self._redis, origin, chunk=self._chunk,
                                       budget_s=self._budget,
                                       meta_ttl_s=self._meta_ttl,
                                       bypass=self._sse_c,
                                       cache_kms=self._cache_kms)
        else:
            self._tier_obj.origin = origin      # per-file origin, shared cache
        return self._tier_obj

    def _open(self, path, mode="rb", block_size=None, **kw):
        if "r" not in mode:
            return super()._open(path, mode=mode, block_size=block_size, **kw)
        kw.pop("autocommit", None)
        return FlintS3File(self, path, mode=mode,
                           block_size=block_size or self.default_block_size, **kw)

    @property
    def counters(self):
        return self._tier_obj.c.as_dict() if self._tier_obj else None
