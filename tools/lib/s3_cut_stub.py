#!/usr/bin/env python3
# SPDX-License-Identifier: Elastic-2.0
"""A path-style S3 stand-in over plain http that spoils ONE download.

    s3_cut_stub.py PORT ROOT BUCKET PREFIX MATCH NTH MODE

Serves `GET /BUCKET/PREFIX/<key>` from `ROOT/<key>` and answers
`GET /BUCKET?list-type=2&prefix=...` with every file under ROOT. Signatures
are not checked: this stands in for the transport, not for S3's auth.

The NTH GET of the object whose key contains MATCH is spoiled, by MODE:
  cut   send the full Content-Length header, half the body, and close: a
        connection lost part way through a download
  flip  send the whole length with one byte changed: a second read that
        does not return what the first one did

flint-backup restore reads each object twice, once to verify it and once to
copy it, so NTH=2 spoils the copy of a set that verified (BUG-0281).
"""
import http.server
import os
import socketserver
import sys
import urllib.parse

port, root, bucket, prefix, match = sys.argv[1:6]
nth, mode = int(sys.argv[6]), sys.argv[7]
gets = {}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("s3stub: " + (fmt % args) + "\n")

    def reply(self, code, body):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        parts = urllib.parse.unquote(url.path).lstrip("/").split("/", 1)
        if parts[0] != bucket:
            return self.reply(404, b"")
        if len(parts) == 1 or parts[1] == "":
            want = urllib.parse.parse_qs(url.query).get("prefix", [""])[0]
            keys = []
            for d, _, files in os.walk(root):
                for f in files:
                    rel = os.path.relpath(os.path.join(d, f), root)
                    full = f"{prefix}/{rel}" if prefix else rel
                    if full.startswith(want):
                        keys.append(full)
            body = "<ListBucketResult>"
            body += "".join(f"<Contents><Key>{k}</Key></Contents>" for k in sorted(keys))
            body += "<IsTruncated>false</IsTruncated></ListBucketResult>"
            return self.reply(200, body.encode())
        key = parts[1]
        if prefix:
            if not key.startswith(prefix + "/"):
                return self.reply(404, b"")
            key = key[len(prefix) + 1:]
        path = os.path.join(root, key)
        if not os.path.isfile(path):
            return self.reply(404, b"<Error><Code>NoSuchKey</Code></Error>")
        with open(path, "rb") as f:
            data = f.read()
        n = gets[key] = gets.get(key, 0) + 1
        if match in key and n == nth and mode == "flip":
            sys.stderr.write(f"s3stub: FLIP GET #{n} of {key}: one byte changed\n")
            spoiled = bytearray(data)
            spoiled[len(spoiled) // 2] ^= 0xFF
            return self.reply(200, bytes(spoiled))
        if match in key and n == nth and mode == "cut":
            sys.stderr.write(f"s3stub: CUT GET #{n} of {key}: {len(data) // 2} of {len(data)} bytes\n")
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data[: len(data) // 2])
            self.wfile.flush()
            self.close_connection = True
            self.connection.shutdown(2)
            return
        self.reply(200, data)


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


Server(("127.0.0.1", int(port)), Handler).serve_forever()
