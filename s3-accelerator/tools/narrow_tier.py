#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A tier that is NARROW rather than slow: full latency, throttled bandwidth.

slow_tier.py makes the first byte late, which is the failure D12.36's budget
was written for. This makes every byte late by a little -- the tier answers
promptly and then delivers the reply slowly -- because that is what a loaded
tier or a saturated link actually looks like, and the two clients may not
agree about whether it counts against the 50 ms budget at all:

  python  socket_timeout, which CPython applies per recv() call
  jvm     orTimeout(tierBudgetMs), which applies to the whole command

If that is the difference, a tier at a tenth of its bandwidth degrades one
client and is invisible to the other, and "never slower than no cache" means
two different things by language.

  narrow_tier.py --listen 9397 --upstream 9399 --kbps 8000
"""
from __future__ import annotations

import argparse
import socket
import sys
import threading
import time

STOP = threading.Event()
CH = 16 * 1024                     # shaping granularity, well under one chunk


def _pump(src, dst, bps):
    """Forward src->dst at no more than bps, in CH-sized instalments.

    The delay is per INSTALMENT, so no single recv on the far side waits long;
    only the total does. That is precisely the case a per-recv timeout cannot
    see.
    """
    try:
        while not STOP.is_set():
            b = src.recv(CH)
            if not b:
                break
            if bps:
                time.sleep(len(b) * 8 / bps)
            dst.sendall(b)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def _serve(listen, upstream, bps):
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
        try:
            u = socket.create_connection(("127.0.0.1", upstream), timeout=5)
        except OSError:
            c.close()
            continue
        # Throttle the TIER->CLIENT direction: the reply is the big one, and
        # shaping the request would measure a different hop.
        threading.Thread(target=_pump, args=(c, u, 0), daemon=True).start()
        threading.Thread(target=_pump, args=(u, c, bps), daemon=True).start()
    srv.close()


def self_test() -> int:
    """A proxy that quietly throttled nothing would make an unbounded client
    look bounded.

    Every assertion here is a LOWER bound on elapsed time, deliberately.
    Contention can only make a transfer slower, so "it took at least the
    modelled time" survives a busy machine; "it took at most" would not be a
    measurement at all.
    """
    ok = True

    def check(c, label):
        nonlocal ok
        ok &= bool(c)
        print(f"[{'ok' if c else 'FAIL'}] {label}")

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
            threading.Thread(target=_echo_one, args=(c,), daemon=True).start()

    def _echo_one(s):
        try:
            while True:
                b = s.recv(65536)
                if not b:
                    return
                s.sendall(b)
        except OSError:
            pass

    threading.Thread(target=echo, daemon=True).start()

    def through(port, nbytes):
        s = socket.create_connection(("127.0.0.1", port), timeout=30)
        payload = b"x" * nbytes
        t0 = time.time()
        s.sendall(payload)
        got = 0
        while got < nbytes:
            b = s.recv(65536)
            if not b:
                break
            got += len(b)
        el = time.time() - t0
        s.close()
        return got, el

    N = 512 * 1024
    KBPS = 4000                       # 512 KiB at 4 Mbit/s ~ 1.05 s
    modelled = N * 8 / (KBPS * 1000)

    # _serve binds its own listener, so pick two free ports up front.
    def free_port():
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        p = s.getsockname()[1]
        s.close()
        return p

    slow_port, fast_port = free_port(), free_port()
    threading.Thread(target=_serve, args=(slow_port, up_port, KBPS * 1000),
                     daemon=True).start()
    threading.Thread(target=_serve, args=(fast_port, up_port, 0),
                     daemon=True).start()
    time.sleep(0.5)

    got, el = through(slow_port, N)
    check(got == N, f"throttled path delivers every byte ({got} of {N}) -- a "
                    "proxy that DROPPED data would also look slow")
    check(el >= modelled * 0.8,
          f"and takes at least the modelled {modelled:.2f}s ({el:.2f}s)")
    got2, el2 = through(fast_port, N)
    check(got2 == N, f"transparency control delivers every byte ({got2} of {N})")
    check(el2 < modelled * 0.5,
          f"negative control -- unthrottled is not slow ({el2:.2f}s vs "
          f"{modelled:.2f}s modelled), so the assertion above can fail")
    STOP.set()
    up.close()
    print("NARROW-TIER SELF-TEST " + ("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=9397)
    ap.add_argument("--upstream", type=int, default=9399)
    ap.add_argument("--kbps", type=float, default=8000)
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    print(f"narrow-tier: 127.0.0.1:{a.listen} -> 127.0.0.1:{a.upstream} "
          f"at {a.kbps} kbit/s", flush=True)
    try:
        _serve(a.listen, a.upstream, a.kbps * 1000)
    except KeyboardInterrupt:
        STOP.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())
