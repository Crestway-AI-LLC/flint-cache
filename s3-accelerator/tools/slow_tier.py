#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A TCP proxy that makes the tier SICK rather than dead.

A dead tier is cheap and already handled: the connection refuses inline and the
client falls through to S3 immediately. The dangerous failure is the tier that
still answers, just slowly -- because a bounded client burns its whole budget
on every request and THEN goes to the origin, so the cache adds latency to
every read instead of removing it. A sick cache is worse than no cache, and
nothing in the suites could produce that condition.

  tools/slow_tier.py --listen 9398 --upstream 9399 --delay-ms 200

--drop makes it accept connections and never answer, which is the pathological
version: the client cannot even learn that anything is wrong except by timing
out.
"""
from __future__ import annotations

import argparse
import socket
import sys
import threading
import time

STOP = threading.Event()
_stats = {"conns": 0, "bytes": 0}


def _pump(src: socket.socket, dst: socket.socket, delay_s: float, drop: bool):
    try:
        while not STOP.is_set():
            b = src.recv(65536)
            if not b:
                break
            _stats["bytes"] += len(b)
            if drop:
                # Accepted, never answered. The client can only time out.
                while not STOP.is_set():
                    time.sleep(0.05)
                return
            if delay_s:
                time.sleep(delay_s)
            dst.sendall(b)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def _serve(listen: int, upstream: int, delay_s: float, drop: bool):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", listen))
    srv.listen(128)
    srv.settimeout(0.5)
    while not STOP.is_set():
        try:
            c, _ = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        _stats["conns"] += 1
        try:
            u = socket.create_connection(("127.0.0.1", upstream), timeout=5)
        except OSError:
            c.close()
            continue
        # Delay the CLIENT->TIER direction only. Delaying both would double the
        # figure and make the measured penalty unattributable to one hop.
        threading.Thread(target=_pump, args=(c, u, delay_s, drop), daemon=True).start()
        threading.Thread(target=_pump, args=(u, c, 0.0, False), daemon=True).start()
    srv.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=9398)
    ap.add_argument("--upstream", type=int, default=9399)
    ap.add_argument("--delay-ms", type=float, default=200.0)
    ap.add_argument("--drop", action="store_true",
                    help="accept and never answer")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    print(f"slow-tier: 127.0.0.1:{a.listen} -> 127.0.0.1:{a.upstream}  "
          f"{'DROP' if a.drop else f'+{a.delay_ms}ms'}", flush=True)
    try:
        _serve(a.listen, a.upstream, a.delay_ms / 1000.0, a.drop)
    except KeyboardInterrupt:
        STOP.set()
    return 0


def self_test() -> int:
    """An instrument that adds no delay would make a broken client look fine."""
    ok = True

    def check(c, label):
        nonlocal ok
        ok &= bool(c)
        print(f"[{'ok' if c else 'FAIL'}] {label}")

    # a trivial echo upstream, so the test needs no valkey
    up = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    up.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    up.bind(("127.0.0.1", 0))
    up_port = up.getsockname()[1]
    up.listen(8)

    def echo():
        while not STOP.is_set():
            try:
                c, _ = up.accept()
            except OSError:
                return
            threading.Thread(target=lambda s=c: _echo_one(s), daemon=True).start()

    def _echo_one(s):
        try:
            while True:
                b = s.recv(4096)
                if not b:
                    return
                s.sendall(b)
        except OSError:
            pass

    threading.Thread(target=echo, daemon=True).start()

    def round_trip(port, payload=b"ping"):
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        t0 = time.time()
        s.sendall(payload)
        s.recv(64)
        dt = (time.time() - t0) * 1000
        s.close()
        return dt

    # control: straight to the echo, no proxy
    direct = round_trip(up_port)
    check(direct < 50, f"control: the bare upstream answers fast ({direct:.0f} ms)")

    p1 = 6497
    threading.Thread(target=_serve, args=(p1, up_port, 0.200, False), daemon=True).start()
    time.sleep(0.5)
    slowed = round_trip(p1)
    check(slowed >= 190, f"the proxy really adds the delay ({slowed:.0f} ms, asked for 200)")
    check(slowed < 400, f"and only once, not per byte ({slowed:.0f} ms)")

    # negative control: a proxy with no delay must NOT slow things down, or
    # every measurement through it is confounded by the proxy itself.
    p2 = 6498
    threading.Thread(target=_serve, args=(p2, up_port, 0.0, False), daemon=True).start()
    time.sleep(0.5)
    passthru = round_trip(p2)
    check(passthru < 50,
          f"negative control: with --delay-ms 0 the proxy is transparent ({passthru:.0f} ms)")

    STOP.set()
    print("SLOW-TIER SELF-TEST " + ("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
