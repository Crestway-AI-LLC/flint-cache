#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A counting S3 endpoint: the instrument the economic asserts need.

The accelerator's correctness can be inherited (Hadoop's contract suite,
Iceberg's FileIO reference). Its VALUE cannot. Nothing in any inherited
suite notices that 64 workers missing the same chunk should produce one S3
GET rather than 64, because a cache that fetched 64 times would still return
the right bytes. A no-op cache passes every correctness test ever written.

So the claims this product is sold on -- miss deduplication, a warm pass
costing nothing, two clients sharing one dataset's worth of transfer -- are
only checkable against something that counts. This is that something.

Two design commitments, both learned the hard way:

1. **Bodies are self-describing.** The byte at absolute offset `i` of object
   `k` is derived from `(k, i)`, so a chunk delivered at the wrong offset, or
   from the wrong object, is detectable as WRONG rather than merely
   different. Borrowed from `flint-chaos`'s oracle, which caught a
   cross-key bleed that a checksum-only ledger had missed.

2. **The counters must be shown to arm.** An assertion of "exactly 1 GET"
   passes trivially against an instrument that never counts. `--self-test`
   therefore proves each counter moves before any test relies on it, and
   the suite includes a control that fails if counting is disabled.

Not a general S3 implementation and never will be. It serves synthetic
objects over the verbs a look-aside cache actually issues.
"""

from __future__ import annotations

import argparse
import datetime
import email.utils as eut
import hashlib
import json
import re
import sys
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

BLOCK = 16  # bytes per self-describing block

# A fixed instant, but the HTTP form is DERIVED rather than typed out. An
# earlier version hardcoded "Fri, 22 Aug 2026" -- 2026-08-22 is a Saturday --
# and the AWS SDK's strict RFC-1123 parser rejected the whole response with
# "DayOfWeek 6 differs from DayOfWeek 5". A hand-written constant can disagree
# with itself; a derived one cannot.
_LM = datetime.datetime(2026, 8, 22, 0, 0, 0, tzinfo=datetime.timezone.utc)
LAST_MODIFIED_HTTP = eut.format_datetime(_LM, usegmt=True)
LAST_MODIFIED_ISO = _LM.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def block_bytes(key: str, index: int, gen: int = 0) -> bytes:
    """The 16 bytes at block `index` of `key` at generation `gen`.

    `gen` exists so an object can be MUTATED. Bumping it changes every byte
    and therefore the ETag, which is what a cache keyed by content has to
    cope with. Without a way to mutate, the entire staleness contract is
    untestable.
    """
    return hashlib.md5(f"{key}:{gen}:{index}".encode()).digest()


def body_range(key: str, size: int, start: int, end: int, gen: int = 0) -> bytes:
    """Bytes [start, end) of `key`, generated rather than stored.

    end is exclusive and clamped to `size`. Generating per block means a
    100 GB object costs nothing to "store" -- only the range actually
    requested is ever materialised.
    """
    end = min(end, size)
    if start >= end:
        return b""
    first, last = start // BLOCK, (end - 1) // BLOCK
    buf = b"".join(block_bytes(key, b, gen) for b in range(first, last + 1))
    off = start - first * BLOCK
    return buf[off:off + (end - start)]


def verify(key: str, start: int, data: bytes, gen: int = 0) -> bool:
    """True if `data` really is the bytes at [start, start+len) of `key`.

    This is the check a client-side test uses to catch reassembly bugs: a
    chunk placed at the wrong offset fails here even though its length and
    its checksum-in-isolation are both fine.
    """
    return data == body_range(key, start + len(data), start, start + len(data), gen)


class Corpus:
    """Synthetic objects, plus anything a client writes.

    Writes exist so Hadoop's contract suite can run: those 45 read tests
    create their own fixtures, and a read-only fixture cannot host them.
    Written objects live in memory and take precedence over the synthetic
    ones, so the existing suites are unaffected.
    """

    def __init__(self, count: int, size: int, prefix: str = "data/",
                 multipart_parts: int = 0):
        self.count, self.size, self.prefix = count, size, prefix
        # Multipart ETags are NOT an MD5 of the object. S3 reports
        # "<md5-of-concatenated-part-md5s>-<partcount>", and anything Spark
        # writes at size arrives that way. D3 content-addresses cache entries
        # BY ETag, so that string flows straight into our key derivation and
        # had never once been exercised: every fixture object was single-part.
        self.multipart_parts = multipart_parts
        self._etags: dict[str, str] = {}
        self._gens: dict[str, int] = {}
        self._written: dict[str, bytes] = {}
        self._lock = threading.Lock()

    # ---- written objects -------------------------------------------------
    def put(self, key: str, body: bytes) -> str:
        with self._lock:
            self._written[key] = body
            tag = hashlib.md5(body).hexdigest()
            self._etags[key] = tag
            return tag

    def delete(self, key: str) -> bool:
        with self._lock:
            self._etags.pop(key, None)
            return self._written.pop(key, None) is not None

    def written(self, key: str):
        return self._written.get(key)

    def get_bytes(self, key: str):
        """The object's full body, written or synthetic, or None if absent.

        CopyObject needs the whole object, not a range, and the source may be
        either kind.
        """
        b = self.written(key)
        if b is not None:
            return b
        if self.has(key):
            # end is EXCLUSIVE here; self.size - 1 would copy one byte short.
            return body_range(key, self.size, 0, self.size, self.gen(key))
        return None

    def all_keys(self) -> list:
        with self._lock:
            w = sorted(self._written)
        return w + self.keys()

    def gen(self, key: str) -> int:
        with self._lock:
            return self._gens.get(key, 0)

    def mutate(self, key: str) -> int:
        """Bump the object's generation: new bytes, new ETag, same key."""
        with self._lock:
            g = self._gens.get(key, 0) + 1
            self._gens[key] = g
            self._etags.pop(key, None)
            return g

    def keys(self) -> list[str]:
        return [f"{self.prefix}{i:06d}.bin" for i in range(self.count)]

    def size_of(self, key: str):
        b = self.written(key)
        if b is not None:
            return len(b)
        return self.size if self.has(key) else None

    def _tag(self, md5hex: str) -> str:
        """Dress the ETag as multipart when asked. Same object, same bytes,
        different ETag STRING -- which is the only variable under test."""
        return f"{md5hex}-{self.multipart_parts}" if self.multipart_parts else md5hex

    def has(self, key: str) -> bool:
        if key in self._written:
            return True
        m = re.fullmatch(re.escape(self.prefix) + r"(\d{6})\.bin", key)
        return bool(m) and int(m.group(1)) < self.count

    def etag(self, key: str) -> str:
        """MD5 of the whole object, as S3 reports for a single-part upload.

        Cached: D3 keys cache entries by ETag, so a test that reads one
        object a thousand times must not pay a thousand full hashes.
        """
        with self._lock:
            if key in self._etags:
                return self._etags[key]
        b = self.written(key)
        if b is not None:
            return self._tag(hashlib.md5(b).hexdigest()[:32])
        g = self.gen(key)
        h = hashlib.md5()
        for b in range((self.size + BLOCK - 1) // BLOCK):
            h.update(block_bytes(key, b, g))
        tag = self._tag(h.hexdigest()[:32])
        with self._lock:
            self._etags[key] = tag
        return tag


class Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.enabled = True          # flipped off only by the arming control
        self.reset()

    def reset(self):
        with self.lock:
            self.verbs = defaultdict(int)
            self.per_key = defaultdict(int)
            self.bytes_served = 0
            self.ranged = 0
            self.ssec = 0
            self.concurrent_peak = 0
            self._in_flight = 0

    def hit(self, verb: str, key: str = "", nbytes: int = 0, ranged: bool = False,
            ssec: bool = False):
        if not self.enabled:
            return
        with self.lock:
            if ssec:
                self.ssec += 1
            self.verbs[verb] += 1
            if key:
                self.per_key[key] += 1
            self.bytes_served += nbytes
            if ranged:
                self.ranged += 1

    def enter(self):
        with self.lock:
            self._in_flight += 1
            self.concurrent_peak = max(self.concurrent_peak, self._in_flight)

    def leave(self):
        with self.lock:
            self._in_flight -= 1

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "counting_enabled": self.enabled,
                "gets": self.verbs.get("GET", 0),
                "heads": self.verbs.get("HEAD", 0),
                "lists": self.verbs.get("LIST", 0),
                "ranged_gets": self.ranged,
                "ssec_requests": self.ssec,
                "bytes_served": self.bytes_served,
                "distinct_keys": len(self.per_key),
                "concurrent_peak": self.concurrent_peak,
                "per_key": dict(self.per_key),
            }


RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    corpus: Corpus
    counters: Counters
    delay_s: float = 0.0
    require_ssec: bool = False
    sse_kms: bool = False

    def log_message(self, *a):
        pass  # the counters are the record; stderr noise drowns the tests

    # -- helpers ---------------------------------------------------------
    def _send(self, code, body=b"", headers=None, head_only=False):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and not head_only:
            self.wfile.write(body)

    def _parse_range(self, size):
        """Return (start, end_exclusive, is_ranged). Absent header = whole."""
        hdr = self.headers.get("Range")
        if not hdr:
            return 0, size, False
        m = RANGE_RE.fullmatch(hdr.strip())
        if not m:
            return 0, size, False
        lo, hi = m.group(1), m.group(2)
        if lo == "":                       # suffix range: bytes=-N
            n = int(hi or 0)
            return max(0, size - n), size, True
        start = int(lo)
        end = (int(hi) + 1) if hi else size
        return start, min(end, size), True

    # -- verbs -----------------------------------------------------------
    def _control(self):
        """Control endpoints answer to ANY verb, deliberately.

        /__reset used to live only in do_GET, so `curl -X POST /__reset` got a
        501 that a `-s` swallowed: the reset silently did nothing and every
        absolute count taken afterwards was cumulative. A test instrument whose
        control plane depends on guessing the right verb will eventually be
        driven with the wrong one, and the failure is silent by construction.

        Prefer DELTAS over resets in callers regardless -- a delta is correct
        even when the reset is not -- but the instrument should not be laying
        this trap in the first place.
        """
        u = urlparse(self.path)
        if not u.path.startswith("/__"):
            return False
        q = parse_qs(u.query)
        if u.path == "/__stats":
            self._send(200, json.dumps(self.counters.snapshot()).encode(),
                       {"Content-Type": "application/json"})
        elif u.path == "/__reset":
            self.counters.reset()
            self._send(200, b"{}", {"Content-Type": "application/json"})
        elif u.path == "/__mutate":
            k = (q.get("key") or [""])[0]
            g = self.corpus.mutate(k)
            self._send(200, json.dumps({"key": k, "generation": g,
                                        "etag": self.corpus.etag(k)}).encode(),
                       {"Content-Type": "application/json"})
        else:
            self._send(404, b"{}", {"Content-Type": "application/json"})
        return True

    def do_GET(self):
        if self._control():
            return
        self.counters.enter()
        try:
            u = urlparse(self.path)
            q = parse_qs(u.query)
            if u.path == "/__stats":
                self._send(200, json.dumps(self.counters.snapshot()).encode(),
                           {"Content-Type": "application/json"})
                return
            if u.path == "/__mutate":
                k = (q.get("key") or [""])[0]
                g = self.corpus.mutate(k)
                self._send(200, json.dumps({"key": k, "generation": g,
                                            "etag": self.corpus.etag(k)}).encode(),
                           {"Content-Type": "application/json"})
                return
            if u.path == "/__reset":
                self.counters.reset()
                self._send(200, b"{}", {"Content-Type": "application/json"})
                return
            if q.get("list-type") == ["2"] or "prefix" in q and self._is_bucket(u.path):
                self._list(q)
                return
            self._object(u.path, head_only=False)
        finally:
            self.counters.leave()

    def _read_body(self) -> bytes:
        """Read a PUT body, decoding aws-chunked if that is what arrived.

        The AWS SDK streams uploads as `aws-chunked`: each chunk prefixed with
        a hex length and a chunk-signature, terminated by a zero-length chunk.
        An earlier version stored that framing verbatim, so a 10-byte write
        became a 182-byte object beginning "a;chunk-si..." -- and EVERY
        contract test that writes then reads failed. The failures looked like
        read-path bugs in our own stream, and were reported as such; they were
        this.
        """
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        enc = (self.headers.get("Content-Encoding") or "").lower()
        decoded_len = self.headers.get("x-amz-decoded-content-length")
        if "aws-chunked" not in enc and decoded_len is None:
            return raw
        out, i = bytearray(), 0
        while i < len(raw):
            j = raw.find(b"\r\n", i)
            if j < 0:
                break
            header = raw[i:j]
            size_hex = header.split(b";", 1)[0].strip()
            try:
                size = int(size_hex, 16)
            except ValueError:
                return raw          # not what we thought; store it verbatim
            i = j + 2
            if size == 0:
                break
            out += raw[i:i + size]
            i += size + 2           # payload plus its trailing CRLF
        if decoded_len is not None and len(out) != int(decoded_len):
            # Say so rather than silently storing a wrong-length object: a
            # corrupt fixture reads as a bug in whatever is under test.
            print(f"WARN decoded {len(out)} bytes, header said {decoded_len}",
                  file=sys.stderr)
        return bytes(out)

    def do_PUT(self):
        if self._control():
            return
        self.counters.enter()
        try:
            key = self._key_of(urlparse(self.path).path)

            # CopyObject. A PUT carrying x-amz-copy-source is a server-side
            # copy, and its body is EMPTY -- so treating it as an ordinary
            # write silently stores a zero-byte object at the destination.
            #
            # This is not a corner case. S3A implements rename() as COPY plus
            # DELETE, and Iceberg's HadoopTableOperations commits by writing a
            # UUID-named temp metadata file and renaming it into place. Without
            # copy, every Iceberg table's v1.metadata.json arrived empty and
            # the failure surfaced two layers away as a Jackson
            # "No content to map due to end-of-input" -- reading like a bug in
            # the metadata parser, or in our stream, rather than in the fixture.
            src = self.headers.get("x-amz-copy-source")
            if src:
                src = unquote(src).lstrip("/")
                if "?" in src:
                    src = src.split("?", 1)[0]
                # strip the leading bucket component
                src_key = src.split("/", 1)[1] if "/" in src else src
                data = self.corpus.get_bytes(src_key)
                if data is None:
                    self._send(404, b"<Error><Code>NoSuchKey</Code></Error>",
                               {"Content-Type": "application/xml"})
                    return
                tag = self.corpus.put(key, data)
                self.counters.hit("COPY", key)
                self._send(200, (f'<?xml version="1.0" encoding="UTF-8"?>'
                                 f'<CopyObjectResult><ETag>&quot;{tag}&quot;</ETag>'
                                 f'<LastModified>2026-08-22T00:00:00.000Z</LastModified>'
                                 f'</CopyObjectResult>').encode(),
                           {"Content-Type": "application/xml"})
                return

            body = self._read_body()
            tag = self.corpus.put(key, body)
            self.counters.hit("PUT", key)
            self._send(200, b"", {"ETag": f'"{tag}"'})
        finally:
            self.counters.leave()

    def do_DELETE(self):
        if self._control():
            return
        self.counters.enter()
        try:
            key = self._key_of(urlparse(self.path).path)
            self.corpus.delete(key)
            self.counters.hit("DELETE", key)
            self._send(204, b"")
        finally:
            self.counters.leave()

    def do_POST(self):
        if self._control():
            return

        # Bulk DeleteObjects: POST /<bucket>?delete with an XML body of keys.
        #
        # Worth implementing rather than routing around. S3A's recursive
        # delete uses it, so Iceberg DROP TABLE goes through here -- and
        # without it the drop SILENTLY did nothing, which surfaced as
        # "table already exists" on the next run and reads like state
        # corruption rather than a missing verb. It was also what made the
        # fsspec contract suite unusable against this fixture.
        u = urlparse(self.path)
        if "delete" in parse_qs(u.query) or u.query == "delete":
            self.counters.enter()
            try:
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                keys = re.findall(rb"<Key>([^<]*)</Key>", body)
                out = ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "<DeleteResult>"]
                for raw in keys:
                    k = unquote(raw.decode())
                    self.corpus.delete(k)
                    self.counters.hit("DELETE", k)
                    out.append(f"<Deleted><Key>{k}</Key></Deleted>")
                out.append("</DeleteResult>")
                self._send(200, "".join(out).encode(),
                           {"Content-Type": "application/xml"})
            finally:
                self.counters.leave()
            return

        # S3A probes multipart; refuse clearly rather than hanging.
        self._send(501, b"<Error><Code>NotImplemented</Code></Error>",
                   {"Content-Type": "application/xml"})

    def do_HEAD(self):
        if self._control():
            return
        self.counters.enter()
        try:
            self._object(urlparse(self.path).path, head_only=True)
        finally:
            self.counters.leave()

    def _is_bucket(self, path):
        return path.count("/") <= 1 or path.rstrip("/").count("/") == 1

    def _key_of(self, path):
        # /bucket/some/key  ->  some/key
        parts = unquote(path).lstrip("/").split("/", 1)
        return parts[1] if len(parts) == 2 else ""

    def _list(self, q):
        prefix = (q.get("prefix") or [""])[0]
        keys = [k for k in self.corpus.all_keys() if k.startswith(prefix)]
        maxk = int((q.get("max-keys") or ["1000"])[0])
        shown = keys[:maxk]
        self.counters.hit("LIST")
        rows = "".join(
            f"<Contents><Key>{k}</Key>"
            f"<Size>{self.corpus.size_of(k) or 0}</Size>"
            f'<ETag>&quot;{self.corpus.etag(k)}&quot;</ETag>'
            f"<LastModified>{LAST_MODIFIED_ISO}</LastModified></Contents>"
            for k in shown)
        xml = ('<?xml version="1.0" encoding="UTF-8"?>'
               '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
               f"<Name>bucket</Name><Prefix>{prefix}</Prefix>"
               f"<KeyCount>{len(shown)}</KeyCount><MaxKeys>{maxk}</MaxKeys>"
               f"<IsTruncated>{'true' if len(keys) > maxk else 'false'}</IsTruncated>"
               f"{rows}</ListBucketResult>")
        self._send(200, xml.encode(), {"Content-Type": "application/xml"})

    def _object(self, path, head_only):
        key = self._key_of(path)
        size = self.corpus.size_of(key)
        if size is None:
            self.counters.hit("MISS404", key)
            self._send(404, b"<Error><Code>NoSuchKey</Code></Error>",
                       {"Content-Type": "application/xml"}, head_only)
            return
        # SSE-C: the caller supplies the key per request and S3 returns
        # PLAINTEXT. Simulated by simply honouring the header -- the point of
        # the fixture is to exercise the code path that must NOT cache, not to
        # encrypt anything.
        ssec = self.headers.get("x-amz-server-side-encryption-customer-algorithm") is not None
        if self.require_ssec and not ssec:
            # Real S3 refuses to serve an SSE-C object without the key. The
            # first version of this fixture served it anyway, which let a
            # client that had DROPPED the key look perfectly healthy.
            self.counters.hit("SSEC_DENIED", key)
            self._send(400, b"<Error><Code>InvalidRequest</Code>"
                            b"<Message>requires SSE-C key</Message></Error>",
                       {"Content-Type": "application/xml"}, head_only)
            return
        start, end, ranged = self._parse_range(size)
        if self.delay_s:
            time.sleep(self.delay_s)   # widen the window concurrency tests need
        stored = self.corpus.written(key)
        if stored is not None:
            data = b"" if head_only else stored[start:end]
        else:
            data = b"" if head_only else body_range(key, size, start, end,
                                                    self.corpus.gen(key))
        # A HEAD transfers NO body, so it contributes nothing to bytes_served.
        # An earlier version credited it with (end - start) -- the size it
        # advertises -- which would have inflated the "two clients move 1x the
        # dataset" assert by one full object per metadata probe. Caught by this
        # file's own self-test, which is the entire argument for having one.
        n = 0 if head_only else len(data)
        self.counters.hit("HEAD" if head_only else "GET", key, n, ranged, ssec)
        headers = {
            "ETag": f'"{self.corpus.etag(key)}"',
            # S3 decrypts SSE-KMS server-side, so the BODY is identical either
            # way. The header is the whole difference, and the only thing a
            # client can base a caching policy on.
            **({"x-amz-server-side-encryption": "aws:kms",
                "x-amz-server-side-encryption-aws-kms-key-id":
                    "arn:aws:kms:us-east-1:000000000000:key/test"}
               if self.sse_kms else {}),
            "Accept-Ranges": "bytes",
            "Content-Type": "application/octet-stream",
            "Last-Modified": LAST_MODIFIED_HTTP,
        }
        if ranged:
            headers["Content-Range"] = f"bytes {start}-{end - 1}/{size}"
        if head_only:
            self.send_response(200)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(size))
            self.end_headers()
            return
        self._send(206 if ranged else 200, data, headers)


def serve(corpus, counters, port=0, delay_s=0.0, extra=None):
    attrs = {"corpus": corpus, "counters": counters, "delay_s": delay_s}
    attrs.update(extra or {})
    handler = type("H", (Handler,), attrs)
    httpd = ThreadingHTTPServer(("127.0.0.1", port), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd, httpd.server_address[1]


# ---------------------------------------------------------------- self-test

def _get(port, path, headers=None):
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("GET", path, headers=headers or {})
    r = c.getresponse()
    body, status, hdrs = r.read(), r.status, dict(r.getheaders())
    c.close()
    return status, body, hdrs


def _verb(port, method, path):
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request(method, path)
    r = c.getresponse()
    body, status = r.read(), r.status
    c.close()
    return status, body


def _stats(port):
    return json.loads(_get(port, "/__stats")[1])


def self_test() -> int:
    ok = True

    def check(cond, label):
        nonlocal ok
        ok &= bool(cond)
        print(f"[{'ok' if cond else 'FAIL'}] {label}")

    # --- the oracle, with both negative controls ---
    k, size = "data/000001.bin", 4096
    mid = body_range(k, size, 1024, 1088)
    check(verify(k, 1024, mid), "self-describing bytes verify at their own offset")
    check(not verify(k, 1088, mid),
          "negative control -- same bytes REJECTED at the wrong offset")
    check(not verify("data/000002.bin", 1024, mid),
          "negative control -- same bytes REJECTED against the wrong key")

    corpus = Corpus(count=8, size=size)
    counters = Counters()
    httpd, port = serve(corpus, counters)

    # --- the control plane answers to ANY verb ---
    # /__reset used to be reachable only by GET, so POST got a 501 that a
    # `curl -s` swallowed: the reset silently did nothing and every absolute
    # count taken afterwards was cumulative. Three false failures in a drill
    # came from that before it was found.
    for m in ("GET", "POST", "PUT", "DELETE", "HEAD"):
        st_, _b = _verb(port, m, "/__reset")
        check(st_ == 200, f"/__reset answers {m} with 200 (got {st_})")
    st_, _b = _verb(port, "POST", "/__nosuch")
    check(st_ == 404, f"negative control -- an unknown /__ path is 404, not 200 (got {st_})")
    _get(port, f"/bucket/{k}")
    check(_stats(port)["gets"] == 1, "armed: a real object request is still counted after that")

    # --- CopyObject ---
    # S3A implements rename() as COPY + DELETE, and Iceberg commits a table by
    # writing a UUID-named temp metadata file and renaming it into place.
    # Without copy support the destination arrived EMPTY, and the failure
    # surfaced two layers away as a Jackson "No content to map" -- reading like
    # a metadata-parser or read-path bug rather than a fixture gap.
    import http.client as _hc
    body = b"copy-me-" + b"x" * 500
    c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("PUT", "/bucket/src.txt", body=body,
              headers={"Content-Length": str(len(body))})
    c.getresponse().read(); c.close()
    c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("PUT", "/bucket/dst.txt",
              headers={"Content-Length": "0", "x-amz-copy-source": "bucket/src.txt"})
    r = c.getresponse(); cp_status, cp_body = r.status, r.read(); c.close()
    check(cp_status == 200, f"CopyObject returns 200 (got {cp_status})")
    check(b"CopyObjectResult" in cp_body, "CopyObject returns a CopyObjectResult")
    st_, got, _h = _get(port, "/bucket/dst.txt")
    check(got == body,
          f"the COPY carried the bytes ({len(got)} of {len(body)}) -- not an empty object")
    # A copy of a SYNTHETIC object exercises the other branch, and the length
    # is the off-by-one guard: body_range's end is exclusive.
    c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("PUT", "/bucket/syn.bin",
              headers={"Content-Length": "0", "x-amz-copy-source": f"bucket/{k}"})
    c.getresponse().read(); c.close()
    st_, syn, _h = _get(port, "/bucket/syn.bin")
    check(len(syn) == size, f"copying a synthetic object copies ALL {size} bytes (got {len(syn)})")
    check(verify(k, 0, syn), "and the copied synthetic bytes verify against the oracle")
    # --- bulk DeleteObjects ---
    for nm in ("bulk1.txt", "bulk2.txt"):
        c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
        c.request("PUT", f"/bucket/{nm}", body=b"x" * 10,
                  headers={"Content-Length": "10"})
        c.getresponse().read(); c.close()
    del_body = (b"<Delete><Object><Key>bulk1.txt</Key></Object>"
                b"<Object><Key>bulk2.txt</Key></Object></Delete>")
    c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("POST", "/bucket?delete", body=del_body,
              headers={"Content-Length": str(len(del_body))})
    r = c.getresponse(); bd_status, bd_body = r.status, r.read(); c.close()
    check(bd_status == 200, f"bulk DeleteObjects returns 200 (got {bd_status})")
    check(bd_body.count(b"<Deleted>") == 2, "and reports BOTH keys deleted")
    gone = [_get(port, f"/bucket/{nm}")[0] for nm in ("bulk1.txt", "bulk2.txt")]
    check(gone == [404, 404], f"the objects are really gone (got {gone})")
    # A bulk delete that quietly deleted nothing would also return 200, so the
    # 404s above are the check and this is its positive control.
    c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("PUT", "/bucket/keepme.txt", body=b"y" * 10,
              headers={"Content-Length": "10"})
    c.getresponse().read(); c.close()
    check(_get(port, "/bucket/keepme.txt")[0] == 200,
          "positive control: an object NOT in the delete list survives")

    c = _hc.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("PUT", "/bucket/nope.txt",
              headers={"Content-Length": "0", "x-amz-copy-source": "bucket/does-not-exist"})
    r = c.getresponse(); miss = r.status; r.read(); c.close()
    check(miss == 404, f"negative control -- copying a MISSING source is 404, not a silent empty object (got {miss})")

    # --- ranged reads land where they claim ---
    st, body, hdrs = _get(port, f"/bucket/{k}", {"Range": "bytes=1024-1087"})
    check(st == 206 and len(body) == 64, f"ranged GET returns 206 + 64 bytes (got {st}, {len(body)})")
    check(hdrs.get("Content-Range") == f"bytes 1024-1087/{size}",
          f"Content-Range is exact ({hdrs.get('Content-Range')})")
    check(verify(k, 1024, body), "ranged body verifies against the oracle at offset 1024")

    # --- the arming control: counters must be shown to move ---
    _get(port, "/__reset")
    for _ in range(5):
        _get(port, f"/bucket/{k}")
    s = _stats(port)
    check(s["gets"] == 5, f"GET counter armed -- 5 requests counted as {s['gets']}")
    check(s["per_key"].get(k) == 5, "per-key counter attributes all 5 to the right key")
    check(s["bytes_served"] == 5 * size,
          f"bytes counted ({s['bytes_served']} == 5 x {size})")

    counters.enabled = False
    _get(port, "/__reset")
    for _ in range(5):
        _get(port, f"/bucket/{k}")
    s_off = _stats(port)
    check(s_off["gets"] == 0,
          "negative control -- with counting disabled the SAME traffic reads 0, "
          "so the assertion above was measuring something")
    counters.enabled = True

    # --- verbs are distinguished ---
    _get(port, "/__reset")
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("HEAD", f"/bucket/{k}"); hr = c.getresponse(); hr.read(); c.close()
    _get(port, "/bucket?list-type=2&prefix=data/")
    s = _stats(port)
    check(s["heads"] == 1 and s["lists"] == 1 and s["gets"] == 0,
          f"HEAD and LIST counted apart from GET ({s['heads']}/{s['lists']}/{s['gets']})")
    check(hr.getheader("Content-Length") == str(size),
          "HEAD reports full size while serving no body")
    check(s["bytes_served"] == 0, "a HEAD serves no bytes")

    # --- ETag is stable and is the whole-object MD5 ---
    h = hashlib.md5()
    for b in range((size + BLOCK - 1) // BLOCK):
        h.update(block_bytes(k, b))
    check(corpus.etag(k) == h.hexdigest()[:32] == corpus.etag(k),
          "ETag is the whole-object MD5 and is stable across calls")

    # --- concurrency really overlaps (the 64-worker assert depends on it) ---
    slow_counters = Counters()
    slow_httpd, slow_port = serve(Corpus(4, 4096), slow_counters, delay_s=0.25)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda _: _get(slow_port, "/bucket/data/000000.bin"), range(8)))
    peak = _stats(slow_port)["concurrent_peak"]
    check(peak > 1, f"requests genuinely overlap under the delay knob (peak {peak})")

    fast_counters = Counters()
    fast_httpd, fast_port = serve(Corpus(4, 4096), fast_counters, delay_s=0.0)
    for _ in range(8):
        _get(fast_port, "/bucket/data/000000.bin")
    check(_stats(fast_port)["concurrent_peak"] == 1,
          "negative control -- serial traffic shows no overlap")

    for h_ in (httpd, slow_httpd, fast_httpd):
        h_.shutdown()
    # --- mutation: new generation must change bytes AND etag ---
    mk = "data/000002.bin"
    e0 = corpus.etag(mk)
    b0 = body_range(mk, size, 0, 64, corpus.gen(mk))
    corpus.mutate(mk)
    e1 = corpus.etag(mk)
    b1 = body_range(mk, size, 0, 64, corpus.gen(mk))
    check(e0 != e1, f"mutation changes the ETag ({e0[:8]} -> {e1[:8]})")
    check(b0 != b1, "mutation changes the bytes")
    check(verify(mk, 0, b1, corpus.gen(mk)), "post-mutation bytes verify at the new generation")
    check(not verify(mk, 0, b0, corpus.gen(mk)),
          "negative control -- OLD bytes are rejected at the NEW generation")
    check(verify(mk, 0, b0, 0), "old bytes still verify at their OWN generation")

    print("\nCOUNTING-S3 SELF-TEST", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--objects", type=int, default=1000)
    ap.add_argument("--object-bytes", type=int, default=8 * 1024 * 1024)
    ap.add_argument("--require-ssec", action="store_true",
                    help="refuse any read that omits the SSE-C key, as S3 does")
    ap.add_argument("--sse-kms", action="store_true",
                    help="report x-amz-server-side-encryption: aws:kms, as a "
                         "KMS-protected bucket does. S3 decrypts server-side, "
                         "so the BODY is plaintext either way -- the header is "
                         "the only difference, and the only thing a client can "
                         "key a policy on.")
    ap.add_argument("--multipart-parts", type=int, default=0,
                    help="report multipart-style ETags (<hash>-<N>), as S3 does "
                         "for anything Spark uploads at size")
    ap.add_argument("--delay-ms", type=float, default=0.0,
                    help="per-request delay; widens the window concurrency tests need")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    corpus = Corpus(a.objects, a.object_bytes, multipart_parts=a.multipart_parts)
    counters = Counters()
    handler_extra = {"require_ssec": a.require_ssec, "sse_kms": a.sse_kms}
    httpd, port = serve(corpus, counters, a.port, a.delay_ms / 1000.0, handler_extra)
    print(f"counting-s3 on http://127.0.0.1:{port}  "
          f"({a.objects} x {a.object_bytes} B)   stats: /__stats  reset: /__reset")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        httpd.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
