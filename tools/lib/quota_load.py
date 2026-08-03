# SPDX-License-Identifier: Elastic-2.0
"""Offered-load generator for the quota drills.

Getting this right took three wrong versions, and each was wrong in a way
worth writing down, because the shape recurs in any rate-limit test.

1. SYNCHRONOUS (one request, wait for reply, repeat). The load OFFERED is
   1/RTT — about 200/s on a laptop, which is the quota itself. The test was
   measuring the client. Three consecutive runs failed three different ways:
   170/s, 445/s, and "never throttled" with 189 accepted and 0 shed.

2. UNBOUNDED PIPELINING (64 in flight, no pacing). This overshoots the other
   way: the proxy's own admission control and bounded buffers stop reading,
   `sendall` blocks, and the connection gets connection-level backpressure
   instead of per-command shedding. Accepted stayed correct (202/179/235/204
   against a 200 budget — the quota binds) but the THROTTLED count swung
   between 0 and 3 million depending on which regime the socket landed in. A
   flood tests admission control, not the quota.

3. PACED (this file). Offer a fixed multiple of the budget, in small batches,
   with a sleep to hold the rate. High enough that the bucket must shed, low
   enough never to reach the buffer-backpressure regime. The generator's rate
   is a property of the test, not of the machine.

The assertions built on this measure the SHAPE a token bucket guarantees —
accepted bounded above by the budget, not strangled below it, and something
actually shed — rather than a narrow band around a laptop-measured number.
"""

import socket
import time

OVERSUBSCRIBE = 3  # offer 3x the budget: must shed, must not flood
BATCH = 8  # in flight at once: amortizes RTT, stays under the buffer bound
WARMUP = 2.0  # the bucket starts FULL; drain the burst before measuring
WINDOW = 8.0  # long enough that one refill tick is not a big share of it


def _resp(args):
    return f"*{len(args)}\r\n".encode() + b"".join(
        f"${len(a)}\r\n{a}\r\n".encode() for a in args
    )


class _Conn:
    """One authenticated connection with a paced, pipelined SET generator."""

    def __init__(self, port, token, offered_per_sec):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.s.settimeout(10)
        self.s.sendall(_resp(["AUTH", token]))
        self.s.recv(64)
        self.set = _resp(["SET", "k", "v"])
        self.buf = b""
        self.interval = BATCH / offered_per_sec
        self.next_at = time.time()

    def pump(self):
        """Send BATCH SETs, read exactly BATCH single-line replies, then pace."""
        self.s.sendall(self.set * BATCH)
        ok = thr = got = 0
        while got < BATCH:
            while b"\r\n" not in self.buf:
                self.buf += self.s.recv(65536)
            line, self.buf = self.buf.split(b"\r\n", 1)
            got += 1
            if b"THROTTLED" in line:
                thr += 1
            else:
                ok += 1
        self.next_at += self.interval
        delay = self.next_at - time.time()
        if delay > 0:
            time.sleep(delay)
        else:
            self.next_at = time.time()  # fell behind; do not chase a backlog
        return ok, thr

    def run_until(self, deadline):
        ok = thr = 0
        while time.time() < deadline:
            o, t = self.pump()
            ok += o
            thr += t
        return ok, thr


def _warm_then_measure(ports, token, budget):
    """Offer 3x `budget` split across `ports`; return {port: (accepted, thr)}.

    Every connection warms and measures inside ONE shared wall-clock window,
    in its own thread. Two details that were bugs in earlier versions:

    * warming the connections in sequence means the second one's warmup
      deadline has already passed by the time its turn comes, so it measures
      with a full bucket while the first measured with an empty one;
    * summing per-thread accepted/elapsed with separately measured elapseds
      inflates the total whenever the threads do not finish together — which
      is how a healthy fleet once read 445/s against a 440 cap. Divide by the
      shared WINDOW, once.
    """
    import threading

    per_conn = budget * OVERSUBSCRIBE / len(ports)
    conns = {p: _Conn(p, token, per_conn) for p in ports}
    start = time.time() + WARMUP
    stop = start + WINDOW
    out = {}

    def go(port, conn):
        conn.run_until(start)  # drain the initial burst; discard the counts
        out[port] = conn.run_until(stop)

    ts = [threading.Thread(target=go, args=(p, c)) for p, c in conns.items()]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return out


def measure_share(ports, token, budget):
    """Return (accepted ops/s across all ports, total throttled)."""
    out = _warm_then_measure(ports, token, budget)
    return sum(o for o, _ in out.values()) / WINDOW, sum(t for _, t in out.values())


def measure_per_port(ports, token, budget):
    """Return (total accepted ops/s, {port: accepted ops/s}, total throttled)."""
    out = _warm_then_measure(ports, token, budget)
    return (
        sum(o for o, _ in out.values()) / WINDOW,
        {p: o / WINDOW for p, (o, _) in out.items()},
        sum(t for _, t in out.values()),
    )


def pin(port, token, budget, stop_flag):
    """Hold a tenant AT its quota until stop_flag[0] — the noisy-neighbour."""
    conn = _Conn(port, token, budget * OVERSUBSCRIBE)
    while not stop_flag[0]:
        conn.pump()
