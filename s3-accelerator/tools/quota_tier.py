#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A tier that is FULL: it serves reads and refuses writes with -QUOTA.

Every tier this harness points a client at is either healthy or dead, and those
are precisely the two states that do NOT distinguish a full tier from a broken
one. Flint sheds writes on an over-quota namespace with

    -QUOTA storage quota exceeded; writes rejected until usage drops (reads still served)

and goes on answering reads, so a full never-evict tier -- the DEFAULT
configuration -- is a healthy tier the client had never been tested against.
Both clients now count that as `tier_full` rather than `tier_failures`, and this
is the fixture that holds them to it.

Speaks only the six commands the client is allowed to use (README, "What the
tier must implement"), and answers anything else with an error, so a client that
quietly grows a seventh fails here rather than in a customer's cluster.
"""
import argparse
import socket
import socketserver
import sys
import threading

QUOTA = (b"-QUOTA storage quota exceeded; writes rejected until usage drops "
         b"(reads still served)\r\n")
NIL = b"$-1\r\n"
WRITES = {b"SET", b"SETEX", b"PSETEX", b"MSET", b"DEL"}


def _read_cmd(f):
    """One RESP command, or None at EOF."""
    line = f.readline()
    if not line:
        return None
    if not line.startswith(b"*"):
        return line.strip().split()          # inline form, used by hand-probes
    out = []
    for _ in range(int(line[1:])):
        h = f.readline()
        if not h.startswith(b"$"):
            return None
        out.append(f.read(int(h[1:])))
        f.read(2)                            # CRLF
    return out


class _H(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            try:
                cmd = _read_cmd(self.rfile)
            except Exception:
                return
            if not cmd:
                return
            op = cmd[0].upper()
            if op == b"PING":
                self.wfile.write(b"+PONG\r\n")
            elif op == b"GET":
                self.wfile.write(NIL)        # a full tier that is also cold
            elif op == b"MGET":
                n = len(cmd) - 1
                self.wfile.write(b"*%d\r\n" % n + NIL * n)
            elif op in WRITES:
                self.wfile.write(QUOTA)      # the whole point
            elif op == b"QUIT":
                self.wfile.write(b"+OK\r\n")
                return
            else:
                self.wfile.write(b"-ERR unknown command\r\n")
            try:
                self.wfile.flush()
            except Exception:
                return


class _S(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def serve(port):
    """Start on 127.0.0.1:port in a daemon thread; returns the server."""
    srv = _S(("127.0.0.1", port), _H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def self_test():
    """Does the fixture do what its docstring claims?

    A fixture that silently answers +OK to writes would make the counter split
    it exists to test pass for the wrong reason, and nothing downstream could
    tell. So: prove the read is served AND the write is refused AND the refusal
    carries the QUOTA prefix the clients key on.
    """
    ok = True

    def check(c, label):
        nonlocal ok
        print(("[ok] " if c else "[FAIL] ") + label)
        ok = ok and c

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    serve(port)

    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    f = s.makefile("rwb")

    def send(*parts):
        f.write(b"*%d\r\n" % len(parts)
                + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts))
        f.flush()
        return f.readline()

    check(send(b"PING") == b"+PONG\r\n", "answers PING, so a client can connect")
    check(send(b"GET", b"c2/{x}/0") == b"$-1\r\n",
          "serves reads -- a full tier is not a dead tier")
    r = send(b"SET", b"c2/{x}/0", b"v")
    check(r.startswith(b"-QUOTA"), "refuses SET with -QUOTA (%r)" % r[:24])
    check(send(b"MSET", b"a", b"1", b"b", b"2").startswith(b"-QUOTA"),
          "refuses MSET with -QUOTA -- the fill path's write")
    check(send(b"FLUSHALL").startswith(b"-ERR"),
          "refuses a command outside the six, rather than answering +OK")
    # Armed: if the handler answered +OK to everything, the two checks above
    # would pass on the PING branch alone. A read must NOT look like a refusal.
    check(not send(b"GET", b"anything").startswith(b"-QUOTA"),
          "armed: reads and writes really do take different branches")
    s.close()
    print("QUOTA TIER SELF-TEST " + ("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=9316)
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    serve(a.listen)
    print("quota tier on %d" % a.listen, flush=True)
    threading.Event().wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
