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

3. PACED, BUT STILL SEND-AND-WAIT (batches of 8, sleep to hold the rate).
   Passed five times standalone and then failed inside the full drill suite:
   128 accepted/s per connection and ZERO throttled against a 200/s share.
   With N requests in flight, throughput is capped at N/RTT — 8/0.062s = 129/s
   — so on a machine where the round trip had grown to ~62ms the generator
   never reached its target and never approached the quota. Exactly failure 1
   again, merely 8x less severe, and it reported "the split is losing
   capacity": the instrument blaming the product.

4. PACED, SEND AND RECEIVE DECOUPLED (this file). A sender thread emits at the
   target rate without waiting for replies; a reader thread drains and
   classifies them. Offered load is then a property of the test rather than of
   the machine's latency, at a rate low enough never to reach the
   buffer-backpressure regime of failure 2.

   And the instrument reports on ITSELF: `measure` returns the offered rate it
   actually achieved, so a drill can distinguish "the quota did not bind" from
   "this machine could not generate enough load to find out". A test that
   cannot tell those apart will eventually report the second as the first.

The assertions built on this measure the SHAPE a token bucket guarantees —
accepted bounded above by the budget, not strangled below it, and something
actually shed — rather than a narrow band around a laptop-measured number.
"""

import socket
import threading
import time

OVERSUBSCRIBE = 3  # offer 3x the budget: must shed, must not flood
BATCH = 8  # per send, purely to amortize syscalls — NOT a window
WARMUP = 2.0  # the bucket starts FULL; drain the burst before measuring
WINDOW = 8.0  # long enough that one refill tick is not a big share of it


def _resp(args):
    return f"*{len(args)}\r\n".encode() + b"".join(
        f"${len(a)}\r\n{a}\r\n".encode() for a in args
    )


class _Conn:
    """One authenticated connection: a paced sender and an independent reader.

    They are separate threads on purpose. If the sender waits for each batch's
    replies, the offered rate collapses to BATCH/RTT and the test silently
    stops exercising the limit on any machine slower than the one it was
    written on.
    """

    def __init__(self, port, token, offered_per_sec):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.s.settimeout(10)
        self.s.sendall(_resp(["AUTH", token]))
        self.s.recv(64)
        self.set = _resp(["SET", "k", "v"])
        self.interval = BATCH / offered_per_sec
        self.counting = False
        self.sent = self.ok = self.thr = 0
        self._done = False

    def _read_loop(self):
        buf = b""
        while not self._done:
            try:
                chunk = self.s.recv(65536)
            except OSError:
                return
            if not chunk:
                return
            buf += chunk
            while b"\r\n" in buf:
                line, buf = buf.split(b"\r\n", 1)
                if self.counting:
                    if b"THROTTLED" in line:
                        self.thr += 1
                    else:
                        self.ok += 1

    def _send_loop(self, stop_at):
        next_at = time.time()
        while time.time() < stop_at:
            try:
                self.s.sendall(self.set * BATCH)
            except OSError:
                return
            if self.counting:
                self.sent += BATCH
            next_at += self.interval
            delay = next_at - time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                next_at = time.time()  # fell behind; do not chase a backlog

    def run(self, start_counting_at, stop_at):
        reader = threading.Thread(target=self._read_loop, daemon=True)
        reader.start()
        flip = threading.Timer(max(0.0, start_counting_at - time.time()),
                               lambda: setattr(self, "counting", True))
        flip.start()
        self._send_loop(stop_at)
        self.counting = False
        flip.cancel()
        time.sleep(0.3)  # let the last replies land before the reader stops
        self._done = True
        try:
            self.s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        reader.join(timeout=2)


def _warm_then_measure(ports, token, budget):
    """Offer 3x `budget` split across `ports`. Returns the _Conn objects.

    Every connection warms and measures inside ONE shared wall-clock window.
    Two details that were bugs in earlier versions:

    * warming the connections in sequence means the second one's warmup
      deadline has already passed by the time its turn comes, so it measures
      with a full bucket while the first measured with an empty one;
    * summing per-thread accepted/elapsed with separately measured elapseds
      inflates the total whenever the threads do not finish together — which
      is how a healthy fleet once read 445/s against a 440 cap. Divide by the
      shared WINDOW, once.
    """
    per_conn = budget * OVERSUBSCRIBE / len(ports)
    conns = {p: _Conn(p, token, per_conn) for p in ports}
    start = time.time() + WARMUP
    stop = start + WINDOW
    ts = [threading.Thread(target=c.run, args=(start, stop)) for c in conns.values()]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return conns


class Load:
    """What the run achieved. `offered` and `replied` are the instrument's own
    positive controls: if either falls short, nothing about the quota was
    tested and the accepted rate is not a verdict."""

    def __init__(self, conns):
        self.accepted = sum(c.ok for c in conns.values()) / WINDOW
        self.throttled = sum(c.thr for c in conns.values())
        self.offered = sum(c.sent for c in conns.values()) / WINDOW
        self.replied = sum(c.ok + c.thr for c in conns.values()) / WINDOW
        self.per_port = {p: c.ok / WINDOW for p, c in conns.items()}

    def require_saturating(self, budget):
        # (1) did we ASK for enough?
        if self.offered < budget * 1.5:
            raise AssertionError(
                f"the load generator only offered {self.offered:.0f} ops/s against a "
                f"{budget}/s budget, so the quota was never actually pressed. This is a "
                f"HARNESS limit on this machine (send-side stall or an overloaded box), "
                f"not a product failure — do not read the accepted rate below as a verdict."
            )
        # (2) did the answers KEEP UP? Checking only (1) was not enough: a full
        # gate run offered its 600/s correctly while just 1,900 of 4,800 sends
        # were answered inside the window, so `accepted` measured a reply
        # BACKLOG and the drill failed the quota for "strangling" at 137/s.
        # A limiter that sheds answers cheaply, so replies trailing sends this
        # badly means the box is the bottleneck, not the bucket.
        if self.replied < self.offered * 0.6:
            raise AssertionError(
                f"only {self.replied:.0f} of {self.offered:.0f} offered ops/s came back inside "
                f"the window — replies are backlogged, so the accepted rate is measuring a "
                f"queue rather than the limit. HARNESS/machine limit, not a product failure."
            )


def measure(ports, token, budget):
    """Offer 3x `budget` across `ports` and report what happened."""
    return Load(_warm_then_measure(ports, token, budget))


def pin(port, token, budget, stop_flag):
    """Hold a tenant AT its quota until stop_flag[0] — the noisy neighbour."""
    conn = _Conn(port, token, budget * OVERSUBSCRIBE)
    reader = threading.Thread(target=conn._read_loop, daemon=True)
    reader.start()
    while not stop_flag[0]:
        try:
            conn.s.sendall(conn.set * BATCH)
        except OSError:
            break
        time.sleep(conn.interval)
    conn._done = True
