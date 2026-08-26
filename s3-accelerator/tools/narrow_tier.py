#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A TCP proxy that answers PROMPTLY and then delivers SLOWLY.

`slow_tier.py` makes the tier late to START -- it sleeps once per request in
the client->tier direction, so the whole penalty lands before the first byte
comes back. That is one of the two ways a tier can be slow, and it is the one
every instrument here already produced.

The other way is a tier that replies immediately and then dribbles the body,
which is what a LOADED tier actually looks like: the connection is alive, the
first byte is fast, and the reply takes seconds to arrive. Nothing could make
that condition, so nothing tested against it -- and a client whose budget is
enforced per socket read rather than per command passes it happily, because no
single read ever waits long enough to trip.

  tools/narrow_tier.py --listen 9397 --upstream 9399 --rate-bps 1048576

--rate-bps throttles the TIER->CLIENT direction only. Delaying both directions
would make the measured penalty unattributable to one hop, which is the same
reasoning slow_tier.py records for delaying only one of them.

A 64 KiB chunk through a 1 MB/s throttle takes ~64 ms to deliver; an 8 MiB read
takes ~8 s. Both are far past a 50 ms budget, and neither has a single gap
between bytes longer than one chunk of the throttle window.
"""
from __future__ import annotations

import argparse
import socket
import sys
import threading
import time

STOP = threading.Event()
_stats = {"conns": 0, "bytes": 0}

#: The throttle is applied in slices rather than by sleeping per byte. A slice
#: small enough that no single send looks like a stall to the reader, and large
#: enough that the sleep granularity does not dominate.
SLICE = 4096


def _pump_fast(src: socket.socket, dst: socket.socket) -> None:
    """Unthrottled direction: the request reaches the tier at full speed."""
    try:
        while not STOP.is_set():
            b = src.recv(65536)
            if not b:
                break
            dst.sendall(b)
    except OSError:
        pass
    finally:
        _shutdown(src, dst)


def _pump_narrow(src: socket.socket, dst: socket.socket, rate_bps: float) -> None:
    """Throttled direction: the reply starts at once and arrives slowly."""
    try:
        while not STOP.is_set():
            b = src.recv(65536)
            if not b:
                break
            _stats["bytes"] += len(b)
            for i in range(0, len(b), SLICE):
                piece = b[i:i + SLICE]
                dst.sendall(piece)
                if rate_bps > 0:
                    time.sleep(len(piece) / rate_bps)
                if STOP.is_set():
                    return
    except OSError:
        pass
    finally:
        _shutdown(src, dst)


def _shutdown(*socks: socket.socket) -> None:
    for s in socks:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def _serve(listen: int, upstream: int, rate_bps: float) -> None:
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
        threading.Thread(target=_pump_fast, args=(c, u), daemon=True).start()
        threading.Thread(target=_pump_narrow, args=(u, c, rate_bps), daemon=True).start()
    srv.close()


def self_test() -> int:
    """An instrument that does not throttle would make a broken client look
    healthy, which is the whole failure this exists to expose. So it proves
    BOTH that it throttles and -- with --rate-bps 0 -- that it is transparent
    when told not to. A control that cannot fail is not a control."""
    import subprocess
    ok = True

    def echo_server(port: int, payload: int) -> threading.Thread:
        def run():
            s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.1", port)); s.listen(4); s.settimeout(8)
            try:
                c, _ = s.accept()
                c.recv(64)
                c.sendall(b"x" * payload)
                time.sleep(0.3)
                c.close()
            except OSError:
                pass
            finally:
                s.close()
        t = threading.Thread(target=run, daemon=True); t.start(); return t

    def probe(listen: int, up: int, rate: float, payload: int) -> float:
        echo_server(up, payload)
        th = threading.Thread(target=_serve, args=(listen, up, rate), daemon=True)
        th.start(); time.sleep(0.4)
        t0 = time.time()
        c = socket.create_connection(("127.0.0.1", listen), timeout=10)
        c.sendall(b"PING\r\n")
        got = 0
        c.settimeout(10)
        try:
            while got < payload:
                b = c.recv(65536)
                if not b:
                    break
                got += len(b)
        except OSError:
            pass
        dt = time.time() - t0
        c.close(); STOP.set(); time.sleep(0.3); STOP.clear()
        return dt

    slow = probe(9971, 9972, 262144, 262144)      # 256 KiB at 256 KiB/s ~ 1 s
    fast = probe(9973, 9974, 0, 262144)           # transparent
    print(f"[{'ok' if slow > 0.6 else 'FAIL'}] throttled: 256 KiB at 256 KiB/s took {slow:.2f}s (want > 0.6)")
    ok &= slow > 0.6
    print(f"[{'ok' if fast < 0.5 else 'FAIL'}] transparency control: --rate-bps 0 took {fast:.2f}s (want < 0.5)")
    ok &= fast < 0.5
    print("NARROW-TIER SELF-TEST", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=9397)
    ap.add_argument("--upstream", type=int, default=9399)
    ap.add_argument("--rate-bps", type=float, default=1048576.0,
                    help="tier->client bytes per second; 0 = transparent")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    print(f"narrow-tier: 127.0.0.1:{a.listen} -> 127.0.0.1:{a.upstream}  "
          f"{a.rate_bps:.0f} B/s reply rate", flush=True)
    try:
        _serve(a.listen, a.upstream, a.rate_bps)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
