# SPDX-License-Identifier: Apache-2.0
"""The tier connection, with a budget that bounds a COMMAND.

`TIER_BUDGET_S` reaches redis-py as `socket_timeout`, and CPython applies a
socket timeout **per operation**. redis-py's read loop calls `recv()` until the
declared reply length has arrived, so an 8 MiB `mget` reply is ~128 recvs, each
granted a fresh 50 ms. The command as a whole was unbounded.

MEASURED before this module existed: an 8 MiB warm read through a tier throttled
to 1 MB/s -- about eight seconds -- was served **from the tier** with
`tier_failures=0` and `degraded=0`. The identical read on the identical throttle
degrades to the origin on the JVM path, whose budget is `orTimeout()` over the
whole command. Same tier, same read, opposite verdicts, and D12.9's rule --
never slower than no cache -- held on one path only.

Latency to first byte was always caught (200 ms of it degrades, controlled).
What was not caught is a tier that answers promptly and then delivers slowly,
which is what a *loaded* tier looks like, and is exactly the case the cap in
D17 was being asked about.

The fix is a socket whose instalments shrink toward one deadline:

  * `recv` / `recv_into` share a deadline armed once per `read_response`, so a
    reply of any size is bounded by the budget rather than each instalment.
  * `sendall` carries its own deadline over however many `send` syscalls it
    takes, because a 64 KiB chunk write has the same defect in the other
    direction.

**This is one budget per direction, not one per command**, so a command slow in
both directions may take up to 2x the budget. The JVM's single `orTimeout`
covers both together. Stated rather than hidden: closing that last gap would
mean reimplementing `send_packed_command`, whose health-check recursion sends
and reads inside the send path, and a subtly wrong reimplementation of the
driver is a worse trade than a bounded factor of two.
"""

from __future__ import annotations

import socket
import time

import redis as redis_lib


class DeadlineSocket:
    """A socket whose blocking operations shrink toward a single deadline."""

    def __init__(self, sock, budget_s):
        self._sock = sock
        self._budget = budget_s
        self._deadline = None
        #: What the driver left on the socket -- redis-py's _connect() sets
        #: socket_timeout as the last thing it does before returning. Captured
        #: so an armed operation can hand it back instead of leaving the socket
        #: tighter than it found it.
        self._base = sock.gettimeout()

    # ------------------------------------------------------------- deadline
    def arm(self):
        self._deadline = time.monotonic() + self._budget

    def disarm(self):
        self._deadline = None
        self._restore()

    def _apply(self, deadline):
        """Give the next instalment whatever is left, or raise if nothing is.

        Set from the DEADLINE alone, never min()'d against the socket's current
        timeout. That version ratcheted: within one multi-instalment command
        `left` shrinks toward zero, so the command ended with the socket holding
        a few milliseconds, and the next command's min(stale, fresh budget)
        could never climb back out. MEASURED before it shipped -- one 8-recv
        command left a 50 ms budget capped at 20.5 ms for the life of the
        connection, and every later command inherited it. A cache that degrades
        at 20 ms when it was configured for 50 is the eager-passthrough failure
        D12.36 already had, reintroduced by its own fix.

        Nothing is needed here to protect a caller's tighter timeout:
        can_read() polls with 0, and it does that OUTSIDE read_response, where
        no deadline is armed and this returns immediately.
        """
        if deadline is None:
            return
        left = deadline - time.monotonic()
        if left <= 0:
            raise socket.timeout("flint tier budget exhausted")
        self._sock.settimeout(left)

    def _restore(self):
        try:
            self._sock.settimeout(self._base)
        except OSError:
            pass          # torn down mid-command; there is nothing to restore

    # ------------------------------------------------------------------ i/o
    def recv(self, *a, **kw):
        self._apply(self._deadline)
        return self._sock.recv(*a, **kw)

    def recv_into(self, *a, **kw):
        # Not reached with the pure-python parser, which uses recv(). It is
        # reached when the customer has hiredis installed, and a fix that
        # silently did not apply to half the installs would be worse than none.
        self._apply(self._deadline)
        return self._sock.recv_into(*a, **kw)

    def sendall(self, data, *flags):
        """One budget for the whole write, however many syscalls it takes.

        Self-contained rather than sharing read_response's deadline: the send
        path is entered recursively by redis-py's health check, so a deadline
        armed by a caller would be disarmed by the PING nested inside it.

        On expiry this raises socket.timeout out of send_packed_command, which
        disconnects rather than returning a connection with half a command on
        it -- checked, not assumed: redis-py catches socket.timeout there and
        calls disconnect() before re-raising.
        """
        if self._budget is None:
            return self._sock.sendall(data, *flags)
        deadline = time.monotonic() + self._budget
        view = memoryview(data)
        try:
            while len(view):
                self._apply(deadline)
                view = view[self._sock.send(view, *flags):]
        finally:
            # Hand the socket back as it was found, on the failure path too --
            # see _apply on why a shrunken timeout must not outlive the command.
            self._restore()

    def __getattr__(self, name):
        return getattr(self._sock, name)


class _Bounded:
    """Mixin over whichever connection class the URL scheme selected."""

    flint_budget_s = None

    def _connect(self):
        return DeadlineSocket(super()._connect(), self.flint_budget_s)

    def read_response(self, *a, **kw):
        """Arm once for the whole reply, not once per instalment.

        On expiry redis-py's own handling applies: it disconnects the
        connection rather than returning it to the pool, which is required
        here -- a command abandoned mid-reply leaves unread bytes on the wire,
        and reusing that connection would serve one key's chunks as another's.
        """
        sock = self._sock
        armed = isinstance(sock, DeadlineSocket)
        if armed:
            sock.arm()
        try:
            return super().read_response(*a, **kw)
        finally:
            if armed:
                sock.disarm()


def bounded_client(uri, budget_s):
    """A redis client whose budget bounds each command, not each recv().

    The connection class is taken from the pool the URL produced rather than
    passed in, so `rediss://` still gets TLS and `unix://` still gets a domain
    socket. Naming a class here would have silently downgraded both.
    """
    pool = redis_lib.ConnectionPool.from_url(
        uri, socket_timeout=budget_s, socket_connect_timeout=budget_s)
    base = pool.connection_class
    pool.connection_class = type(
        "FlintBounded" + base.__name__, (_Bounded, base),
        {"flint_budget_s": budget_s})
    return redis_lib.Redis(connection_pool=pool)
