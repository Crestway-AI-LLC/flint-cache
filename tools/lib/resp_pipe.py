#!/usr/bin/env python3
# SPDX-License-Identifier: Elastic-2.0
"""Send commands as ONE pipeline and print each reply on its own line.

    resp_pipe.py PORT [--auth TOKEN] -- 'SET k v' 'GET k' ...

valkey-cli reading commands from stdin waits for each reply before sending
the next, so it never reaches the proxy's pipelined path (`prefetch_run`).
This sends every command in a single write. Arguments are split on spaces.

`--auth` is sent first, ON ITS OWN, and its reply awaited: the proxy stages
nothing from a batch that arrives unauthenticated, so an AUTH in the same
write would turn the whole pipeline back into one command at a time.

A reply prints as its text: a bulk's bytes, `(nil)` for a null bulk,
`(nil-array)` for a null array, `(error) <text>`, an integer's digits, and an
array as its elements in brackets.
"""
import socket
import sys


def encode(args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def read(f):
    line = f.readline()
    if not line:
        raise SystemExit("connection closed before every reply arrived")
    kind, rest = line[:1], line[1:-2].decode()
    if kind in (b"+", b":"):
        return rest
    if kind == b"-":
        return "(error) " + rest
    if kind == b"$":
        n = int(rest)
        return "(nil)" if n < 0 else f.read(n + 2)[:-2].decode()
    if kind == b"*":
        n = int(rest)
        if n < 0:
            return "(nil-array)"
        return "[" + " ".join(read(f) for _ in range(n)) + "]"
    raise SystemExit("unexpected reply: %r" % line)


def main(argv):
    port = int(argv[1])
    rest = argv[2:]
    auth = None
    if rest[:1] == ["--auth"]:
        auth, rest = rest[1], rest[2:]
    if rest[:1] == ["--"]:
        rest = rest[1:]
    cmds = [[a.encode() for a in c.split(" ")] for c in rest]
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    f = s.makefile("rb")
    if auth is not None:
        s.sendall(encode([b"AUTH", auth.encode()]))
        ok = read(f)
        if ok != "OK":
            raise SystemExit("AUTH answered: " + ok)
    s.sendall(b"".join(encode(c) for c in cmds))
    for _ in cmds:
        print(read(f))


if __name__ == "__main__":
    main(sys.argv)
