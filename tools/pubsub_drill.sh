#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Pub/sub across pairs and proxies (ADR-0052 D5), and what a seat restart
# does to it.
#
# Every pair's master holds every subscription of every proxy, and a
# PUBLISH runs on one pair. So the property under test is that a message
# reaches every client holding its channel, whichever pair the channel
# hashes to and whichever proxy each client came through, exactly once.
# Sixteen channels cover both pairs here with near certainty (each lands on
# either with even odds); the drill counts how many each pair got, and
# refuses to pass on a run where one pair got none.
#
# The restart is the reason this is a drill and not a unit test. A seat that
# restarts has forgotten every subscription, and nothing tells the proxies:
# their links see a closed connection, dial again, and register what their
# clients still hold. Until they do, messages on that pair are lost (at most
# once, as Redis). After they do, every channel must deliver again, to the
# same client connections, which never noticed.
#
# Requires: a release build with --features rocks.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-pubsub- 6522 6523 6524 6525
fleet_guard
fleet_kill proxy; fleet_kill server; sleep 0.4

B=./target/release/flint-server
PX=./target/release/flint-proxy
ADIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-pubsub-a.XXXXXX)
BDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-pubsub-b.XXXXXX)

cleanup() {
  fleet_kill proxy; fleet_kill server
  rm -rf "$ADIR" "$BDIR"
}
trap cleanup EXIT

start_seat() {
  $B --port "$1" --engine rocks --data-dir "$2" 2>>"${FLEET_SCOPE}server-$1.log" &
  fleet_wait_listen "$1"
  fleet_wait_ping "$1"
}
start_seat 6522 "$ADIR"
start_seat 6523 "$BDIR"
for p in 6524 6525; do
  $PX --port $p --pairs "127.0.0.1:6522;127.0.0.1:6523" 2>"${FLEET_SCOPE}proxy-$p.log" &
  fleet_wait_listen $p
done

# Phase 1 runs with every seat up; phase 2 after seat 6523 is restarted by
# the shell between the two calls, the subscribers held open across it by a
# fifo-driven client.
run() { python3 -I - "$@" <<'PY'
import os, socket, sys, time

class C:
    def __init__(s, port, proto=2):
        s.k = socket.create_connection(("127.0.0.1", port)); s.buf = b""
        if proto == 3: s.call(["HELLO", "3"])
    def send(s, args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        s.k.sendall(out)
    def call(s, args):
        s.send(args); return s.read()
    def fill(s, dl):
        t = dl - time.time()
        if t <= 0: raise TimeoutError
        s.k.settimeout(t)
        try: d = s.k.recv(65536)
        except socket.timeout: raise TimeoutError
        if not d: raise EOFError
        s.buf += d
    def line(s, dl):
        while b"\r\n" not in s.buf: s.fill(dl)
        i = s.buf.index(b"\r\n"); l = s.buf[:i]; s.buf = s.buf[i + 2:]; return l
    def read(s, t=3.0):
        return s._read(time.time() + t)
    def _read(s, dl):
        l = s.line(dl); t, r = l[:1], l[1:]
        if t in (b"+", b"-"): return (t.decode(), r)
        if t == b":": return int(r)
        if t == b"$":
            n = int(r)
            if n < 0: return None
            while len(s.buf) < n + 2: s.fill(dl)
            d = s.buf[:n]; s.buf = s.buf[n + 2:]; return d
        if t in (b"*", b">", b"~"):
            return [s._read(dl) for _ in range(int(r))]
        if t == b"%":
            return [(s._read(dl), s._read(dl)) for _ in range(int(r))]
        if t == b"_": return None
        if t == b"#": return r == b"t"
        raise ValueError(l)
    def messages(s, t=0.3):
        out = []
        while True:
            try: out.append(s.read(t))
            except TimeoutError: return out

def fail(msg):
    print("FAIL: " + msg); sys.exit(1)

CH = ["chan:%d" % i for i in range(16)]

def slot(key):
    crc = 0
    for b in key.encode():
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
    return crc % 16384

def subscribe_all(c, chans, pattern):
    c.send(["SUBSCRIBE"] + chans)
    for ch in chans:
        r = c.read()
        if r[0] not in (b"subscribe",) or r[1] != ch.encode():
            fail("bad SUBSCRIBE confirmation %r" % (r,))
    c.send(["PSUBSCRIBE", pattern]); c.read()

phase = sys.argv[1]
s1 = C(6524); s2 = C(6525, 3); pub = C(6525)
subscribe_all(s1, CH, "chan:1*")
subscribe_all(s2, CH, "nothing:*")
on_b = sum(1 for ch in CH if slot(ch) >= 8192)
if on_b in (0, len(CH)):
    fail("all 16 channels on one pair (%d on the second): the drill proves nothing" % on_b)

def round_trip(label, want_receivers=True):
    got1, got2 = {}, {}
    for ch in CH:
        n = pub.call(["PUBLISH", ch, "m-" + ch])
        # s1 holds the channel, and chan:1* also matches chan:1 and
        # chan:10..15; s2 holds the channel.
        expect = 2 + (1 if ch.startswith("chan:1") else 0)
        if want_receivers and n != expect:
            fail("%s: PUBLISH %s reached %r clients, expected %d" % (label, ch, n, expect))
    for m in s1.messages():
        key = (m[0], m[-2])
        got1[key] = got1.get(key, 0) + 1
    for m in s2.messages():
        got2[m[-2]] = got2.get(m[-2], 0) + 1
    for ch in CH:
        c = ch.encode()
        if got1.get((b"message", c)) != 1: fail("%s: s1 got %r of %s" % (label, got1.get((b"message", c)), ch))
        want_p = 1 if ch.startswith("chan:1") else None
        if got1.get((b"pmessage", c)) != want_p: fail("%s: s1 pattern copies of %s: %r" % (label, ch, got1.get((b"pmessage", c))))
        if got2.get(c) != 1: fail("%s: s2 (RESP3, other proxy) got %r of %s" % (label, got2.get(c), ch))
    print("  [%s] 16 channels, %d on the second pair: each message once to each holder, across both proxies" % (label, on_b))

if phase == "steady":
    round_trip("steady")
    # A subscribe is answered once every master holds it, so a PUBLISH sent
    # the moment the confirmation arrives, through the OTHER proxy, reaches
    # the new subscriber. Answering first would lose some of these.
    for i in range(200):
        ch = "fresh:%d" % i
        s1.send(["SUBSCRIBE", ch]); s1.read()
        n = pub.call(["PUBLISH", ch, "now"])
        if n != 1: fail("PUBLISH right after SUBSCRIBE's confirmation reached %r clients (%s)" % (n, ch))
        m = s1.read()
        if m[-1] != b"now": fail("expected the message on %s, got %r" % (ch, m))
        s1.send(["UNSUBSCRIBE", ch]); s1.read()
    print("  [subscribe] 200 fresh channels, each published to the instant it was confirmed: none lost")
    # A transaction's PUBLISH goes out with its write, and a refused one's
    # never does.
    pub.call(["MULTI"]); pub.call(["SETEX", "chan:3", "60", "v"])
    pub.call(["PUBLISH", "chan:3", "committed"]); r = pub.call(["EXEC"])
    if not isinstance(r, list) or r[1] != 2: fail("transaction EXEC answered %r" % (r,))
    m = s1.messages()
    if [x[-1] for x in m] != [b"committed"]: fail("transaction publish delivered %r" % (m,))
    s2.messages()
    pub.call(["MULTI"]); pub.call(["PUBLISH", "chan:3", "never"]); pub.call(["NOSUCHCOMMAND"])
    r = pub.call(["EXEC"])
    if s1.messages(): fail("an aborted transaction's publish was delivered")
    # A channel is not a key: in a transaction pinned to one pair by its
    # key, a PUBLISH on a channel of the other pair runs there, and still
    # reaches every holder.
    kp = slot("x") >= 8192
    other = next(ch for ch in CH if (slot(ch) >= 8192) != kp)
    pub.call(["MULTI"]); pub.call(["SET", "{x}k", "v"])
    pub.call(["PUBLISH", other, "cross"]); r = pub.call(["EXEC"])
    want_n = 2 + (1 if other.startswith("chan:1") else 0)
    if not isinstance(r, list) or r[1] != want_n: fail("cross-pair transaction EXEC answered %r" % (r,))
    if b"cross" not in [x[-1] for x in s1.messages()] or [x[-1] for x in s2.messages()] != [b"cross"]:
        fail("a transaction's PUBLISH on another pair's channel was not delivered")
    pub.call(["DEL", "{x}k"])
    # A script keeps none of its writes when it fails, and so none of its
    # publishes: the subscriber must not hear of a write that did not land.
    r = pub.call(["EVAL", "redis.call('SET', KEYS[1], 'x'); redis.call('PUBLISH', KEYS[1], 'held'); error('boom')", "1", "chan:3"])
    if r[0] != "-": fail("the failing script answered %r" % (r,))
    if s1.messages() or s2.messages(): fail("a failed script's publish was delivered")
    r = pub.call(["EVAL", "redis.call('PUBLISH', KEYS[1], 'sent'); return 1", "1", "chan:3"])
    if [x[-1] for x in s1.messages()] != [b"sent"]: fail("a script's publish was not delivered")
    s2.messages()
    print("  [transaction] delivered after EXEC, on the channel's pair or another, and after a script; not after EXECABORT or a failed script")
    # PUBSUB answers for every proxy's clients, from any pair.
    r = pub.call(["PUBSUB", "NUMSUB", "chan:0", "chan:9"])
    if r != [b"chan:0", 2, b"chan:9", 2]: fail("PUBSUB NUMSUB answered %r" % (r,))
    r = pub.call(["PUBSUB", "NUMPAT"])
    if r != 2: fail("PUBSUB NUMPAT answered %r" % (r,))
    # A client that stops reading is cut off past 32 MiB, and the others
    # keep their messages.
    slow = C(6524); slow.call(["SUBSCRIBE", "flood"]); slow.k.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    fast = C(6525); fast.call(["SUBSCRIBE", "flood"])
    blob = "x" * (1024 * 1024)
    for i in range(48):
        pub.call(["PUBLISH", "flood", blob])
        while True:
            try: fast.read(0.05)
            except TimeoutError: break
    deadline = time.time() + 10
    while time.time() < deadline and pub.call(["PUBSUB", "NUMSUB", "flood"])[1] != 1:
        time.sleep(0.1)
    if pub.call(["PUBSUB", "NUMSUB", "flood"])[1] != 1:
        fail("a subscriber 48 MiB behind is still subscribed")
    print("  [slow client] cut off past its 32 MiB, the reading one kept")
    # The connections end; nothing of theirs stays registered.
    for c in (s1, s2, slow, fast): c.k.close()
    deadline = time.time() + 5
    while time.time() < deadline and pub.call(["PUBSUB", "NUMSUB", "chan:0"])[1] != 0:
        time.sleep(0.05)
    if pub.call(["PUBSUB", "NUMSUB", "chan:0"])[1] != 0: fail("a closed client's subscription outlived it")
    print("  [disconnect] a closed client's subscriptions are gone")
elif phase == "restart":
    round_trip("before restart")
    print("  restarting seat 6523", flush=True)
    # The shell restarts the seat when it reads this line.
    with open(sys.argv[2], "w") as f: f.write("go\n")
    with open(sys.argv[3]) as f: f.read()
    # Lost until the links register again; then every channel delivers.
    t0 = time.time()
    while time.time() - t0 < 10:
        ok = all(pub.call(["PUBLISH", ch, "probe"]) == 2 + (1 if ch.startswith("chan:1") else 0) for ch in CH)
        s1.messages(0.1); s2.messages(0.1)
        if ok: break
        time.sleep(0.05)
    else:
        fail("subscriptions on the restarted seat did not come back within 10 s")
    print("  [restart] links registered again %.2f s after the seat was back" % (time.time() - t0))
    round_trip("after restart")
PY
}

echo "== pubsub drill: two pairs, two proxies"
run steady || exit 1

GO=$(mktemp -u $FLINT_DRILL_ROOT/flint-pubsub-go.XXXXXX); mkfifo "$GO"
BACK=$(mktemp -u $FLINT_DRILL_ROOT/flint-pubsub-back.XXXXXX); mkfifo "$BACK"
run restart "$GO" "$BACK" &
DRIVER=$!
read -r _ < "$GO"
fleet_signal_port 6523 9
sleep 0.3
start_seat 6523 "$BDIR"
echo back > "$BACK"
wait $DRIVER || exit 1
rm -f "$GO" "$BACK"

echo "PASS: every message once to every holder, across pairs, proxies, a transaction and a seat restart"
