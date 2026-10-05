#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0201: a master at its connection cap is FULL, not dead, and the
# controller must not fail it over.
#
# Over --max-conns the node used to drop a new connection unanswered. The
# connections it held kept serving, so clients on pooled connections saw
# nothing, but every probe that opens a NEW connection saw a reset. On the
# playground the controller's FLINTINFO and PING read a serving master that
# way for 41 ticks, and it promoted the replica: a fence split and refused
# writes (ops OPS-0348, OPS-0349).
#
# Here a real fleet (CP, one pair, proxy, controller, mutual TLS) has its
# master's cap lowered and filled by held connections. Then, every one on a
# connection of its own, as a probe opens it:
#   - PING answers PONG and FLINTINFO answers as the master, with
#     active_conns at max_conns, which is how a full node says why;
#   - anything else answers Redis's "-ERR max number of clients reached";
#   - for well past the controller's confirm ticks and its slow-promote
#     window, the master stays master;
#   - the edge keeps serving on the proxy's pooled connections;
#   - BUG-0206: a replica restarted while its master is full re-attaches,
#     through the connections held past the cap for a replica's handshake
#     (FLINTSYNC/FLINTFULLSYNC), and the master stays full.
# Unfixed, the first PING is a reset and the controller promotes the replica.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-conncap 6513 6514 6515 6516
fleet_guard
D=$FLINT_DRILL_ROOT/flint-conncap; INV=$D/cluster.flint
CTL=./target/release/flintctl
HOLD_PID=""
fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  [ -n "$HOLD_PID" ] && kill "$HOLD_PID" 2>/dev/null
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:6516
pair 127.0.0.1:6513,127.0.0.1:6514
proxy 127.0.0.1:6515
controller on
poll-ms 150
confirm 3
EOF

echo "== bootstrap (CP, one pair, proxy, controller, mutual TLS)"
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -20 "$D/bootstrap.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" verify >"$D/verify.log" 2>&1 \
  || { echo "FAIL: fleet did not verify"; tail -15 "$D/verify.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" tenant add cap tok-cap cap 1 >/dev/null 2>&1 \
  || { echo "FAIL: could not create the tenant"; exit 1; }

# One command on a connection of its own, over the fleet's internal mutual
# TLS, printed as the reply's text. And a holder that opens connections until
# the node will take no more, then keeps them. Python, not valkey-cli: a probe
# here must send exactly one command, and must say a reset is a reset.
cat > "$D/conncap.py" <<'PY'
import socket, ssl, sys, time

def connect(port, certs):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.load_verify_locations(certs + "/ca.crt")
    ctx.load_cert_chain(certs + "/int.crt", certs + "/int.key")
    raw = socket.create_connection(("127.0.0.1", port), timeout=3)
    return ctx.wrap_socket(raw)

def command(s, args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    s.sendall(out)

def reply(s):
    # A simple string or integer as its text, an error with its "-", so the
    # two can never be mistaken for each other.
    buf = b""
    while True:
        if buf[:1] in (b"+", b":") and b"\r\n" in buf:
            return buf.split(b"\r\n", 1)[0][1:].decode()
        if buf[:1] == b"-" and b"\r\n" in buf:
            return buf.split(b"\r\n", 1)[0].decode()
        if buf[:1] == b"$" and b"\r\n" in buf:
            head, rest = buf.split(b"\r\n", 1)
            n = int(head[1:])
            if n < 0:
                return "(nil)"
            if len(rest) >= n:
                return rest[:n].decode()
        chunk = s.recv(65536)
        if not chunk:
            return "(closed)"
        buf += chunk

mode, port, certs = sys.argv[1], int(sys.argv[2]), sys.argv[3]
if mode == "probe":
    try:
        s = connect(port, certs)
        command(s, sys.argv[4:])
        print(reply(s))
    except Exception as e:
        print("(error) %s: %s" % (type(e).__name__, e))
elif mode == "hold":
    # Fill the cap, report, then keep taking any slot that frees (the
    # controller's own probes come and go), so the node stays full and a
    # probe meets the over-cap path rather than a slot someone left.
    def take():
        try:
            s = connect(port, certs)
            command(s, ["ECHO", "held"])
            r = reply(s)
        except Exception as e:
            return None, "(error) %s" % e
        return (s, r) if r == "held" else (None, r)
    held, last = [], "(none refused)"
    for _ in range(int(sys.argv[5])):
        s, r = take()
        if s is None:
            last = r
            break
        held.append(s)
    with open(sys.argv[4], "w") as f:
        f.write("%d %s\n" % (len(held), last))
    while True:
        s, _ = take()
        if s is not None:
            held.append(s)
        time.sleep(0.05)
PY

C="$D/state/certs"
probe() { python3 "$D/conncap.py" probe "$1" "$C" "${@:2}"; }
# The master's address and epoch: the rest of its row (seq_lag) moves with
# every write.
masters() { $CTL -f "$INV" status 2>/dev/null | awk '$1=="pair" && $4=="master" {print $3, $6}'; }
edge_set() { valkey-cli -h 127.0.0.1 -p 6515 -a tok-cap --no-auth-warning SET "cap:$1" ok 2>&1 | tr -d '\r' | head -1; }

M=""; REP=""
for p in 6513 6514; do
  case "$(probe $p FLINTINFO)" in *role:master*) M=$p ;; *role:replica*) REP=$p ;; esac
done
[ -n "$M" ] && [ -n "$REP" ] || { echo "FAIL: want a master and a replica among 6513/6514 (master '$M', replica '$REP')"; exit 1; }
role_of() { probe "$1" FLINTINFO | sed -n 's/^role:\([a-z]*\).*/\1/p'; }
epoch_of() { probe "$1" FLINTINFO | sed -n 's/^role_epoch:\([^[:space:]]*\).*/\1/p'; }
EPOCH="$(epoch_of $M)"
BEFORE="$(masters)"
[ -n "$BEFORE" ] || { echo "FAIL: flintctl status shows no master"; exit 1; }
# Warm every proxy worker's pool first: each client connection lands on the
# next worker, and one that first dials the master after it is full is a
# failed dial (as it should be), which is not what this drill is about.
for i in $(seq 1 32); do
  [ "$(edge_set "warm$i")" = OK ] || { echo "FAIL: the edge is not writable before the cap"; exit 1; }
done
ACTIVE=$(probe $M FLINTINFO | sed -n 's/^active_conns:\([0-9]*\).*/\1/p')
[ -n "$ACTIVE" ] || { echo "FAIL: FLINTINFO has no active_conns"; exit 1; }
CAP=$((ACTIVE + 6))
R="$(probe $M FLINTCONFIG max-conns $CAP)"
[ "$R" = OK ] || { echo "FAIL: FLINTCONFIG max-conns: $R"; exit 1; }
echo "  master $M, $ACTIVE connections, cap lowered to $CAP"

echo "== fill the master's cap with held connections"
python3 "$D/conncap.py" hold $M "$C" "$D/held" 64 & HOLD_PID=$!
for _ in $(seq 1 100); do [ -s "$D/held" ] && break; sleep 0.1; done
[ -s "$D/held" ] || { echo "FAIL: the holder never reported"; exit 1; }
read -r HELD LAST < "$D/held"
[ "$HELD" -ge 1 ] || { echo "FAIL: held no connection at all ($LAST)"; exit 1; }
echo "  holding $HELD; the next connection got: $LAST"

echo "== a probe on a new connection finds the master full, not dead"
R="$(probe $M PING)"
[ "$R" = PONG ] || { echo "FAIL (BUG-0201): PING over the cap answered [$R], not PONG"; exit 1; }
INFO="$(probe $M FLINTINFO)"
case "$INFO" in *role:master*) : ;; *) echo "FAIL (BUG-0201): FLINTINFO over the cap: [$(printf '%s' "$INFO" | head -2)]"; exit 1 ;; esac
A=$(printf '%s\n' "$INFO" | sed -n 's/^active_conns:\([0-9]*\).*/\1/p')
X=$(printf '%s\n' "$INFO" | sed -n 's/^max_conns:\([0-9]*\).*/\1/p')
[ -n "$A" ] && [ -n "$X" ] && [ "$A" -ge "$X" ] \
  || { echo "FAIL: FLINTINFO over the cap should show active_conns ($A) at max_conns ($X)"; exit 1; }
R="$(probe $M SET cap:direct v)"
case "$R" in "-ERR max number of clients reached") : ;; *) echo "FAIL: a data command over the cap answered [$R]"; exit 1 ;; esac
echo "  PING -> PONG, FLINTINFO -> role:master with active_conns $A of $X, SET -> $R"

echo "== the controller holds the full master, well past its confirm ticks and slow window"
# confirm 3 at 150 ms is 0.45 s; the slow-promote window is 4 s by default.
# The replica's own role is the direct question: it has room, so it answers
# truly whatever the master's state, and promoting it is the failure.
for s in $(seq 1 10); do
  RR="$(role_of $REP)"
  [ "$RR" = replica ] || {
    echo "FAIL (BUG-0201): the controller promoted the replica $REP of a full, serving master, at ${s}s (role:${RR:-?})"
    grep -rh "PROMOTED\|no master" "$D/state" 2>/dev/null | tail -3 | sed 's/^/    /'
    exit 1
  }
  [ "$(edge_set "s$s")" = OK ] || { echo "FAIL: the edge stopped serving while the master was full (${s}s)"; exit 1; }
  sleep 1
done
[ "$(role_of $M)" = master ] && [ "$(epoch_of $M)" = "$EPOCH" ] \
  || { echo "FAIL: after 10 s full, $M is role:$(role_of $M) epoch $(epoch_of $M), not master at $EPOCH"; exit 1; }
[ "$(masters)" = "$BEFORE" ] || { echo "FAIL: flintctl status: [$(masters)] against [$BEFORE] before"; exit 1; }
echo "  10 s: $REP still the replica, $M still master at epoch $EPOCH, edge writable throughout"

echo "== BUG-0206: a replica restarted while the master is full re-attaches"
# Its link is a connection like any other, and it opens one with FLINTSYNC or
# FLINTFULLSYNC. Over the cap that got the max-clients error, so a link that
# dropped while the master was full stayed down until it had room.
field_of() { probe "$1" FLINTINFO | sed -n "s/^$2:\([0-9]*\).*/\1/p"; }
RA0="$(field_of $M conns_reserve_admitted)"
[ -n "$RA0" ] || { echo "FAIL: FLINTINFO has no conns_reserve_admitted"; exit 1; }
$CTL -f "$INV" restart-node "127.0.0.1:$REP" >"$D/restart.log" 2>&1 \
  || { tail -8 "$D/restart.log" | sed 's/^/  | /'; echo "FAIL (BUG-0206): the replica could not rejoin its master while the master was full"; exit 1; }
LIVE=""
for _ in $(seq 1 75); do
  [ "$(field_of $M live_replicas)" = 1 ] && { LIVE=1; break; }
  sleep 0.2
done
[ -n "$LIVE" ] || { echo "FAIL (BUG-0206): after the restart the master reports live_replicas $(field_of $M live_replicas)"; exit 1; }
RA1="$(field_of $M conns_reserve_admitted)"
[ "${RA1:-0}" -gt "$RA0" ] \
  || { echo "FAIL: the replica re-attached without the reserve ($RA0 -> $RA1): the master was not full, so this proved nothing"; exit 1; }
A=$(field_of $M active_conns); X=$(field_of $M max_conns)
[ "$A" -ge "$X" ] || { echo "FAIL: the master is no longer full ($A of $X); the holder let go"; exit 1; }
R="$(probe $M SET cap:direct2 v)"
case "$R" in "-ERR max number of clients reached") : ;; *) echo "FAIL: a data command in the reserve answered [$R], not the max-clients error"; exit 1 ;; esac
echo "  $REP restarted and re-attached through the reserve ($RA0 -> $RA1 admitted); the master stayed full ($A of $X) and still refuses data commands"

echo "== release the held connections: the master takes new ones again"
kill "$HOLD_PID" 2>/dev/null; wait "$HOLD_PID" 2>/dev/null; HOLD_PID=""
OK=""
for _ in $(seq 1 50); do
  R="$(probe $M ECHO back)"
  [ "$R" = back ] && { OK=1; break; }
  sleep 0.2
done
[ -n "$OK" ] || { echo "FAIL: the master still refuses after the holder left: [$R]"; exit 1; }
probe $M FLINTCONFIG max-conns 2048 >/dev/null

echo "PASS: a master at its connection cap answers PING and FLINTINFO on a new connection,"
echo "      refuses everything else with Redis's max-clients error, and the controller"
echo "      keeps it as master while the edge serves on (BUG-0201); a replica restarted"
echo "      meanwhile re-attaches through the reserve held for its handshake (BUG-0206)."
