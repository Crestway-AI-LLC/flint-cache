#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0175: does a REWOUND replica ack only in the new master's sequence space,
# and does the master refuse to record an ack from the old one?
#
# WHY THIS EXISTS. At the rc.76 playground roll the ops agent read
# `flint_node_seq_lag` 3,042,520 -- the new master's rewind-attach offset
# (274,668,873 - 271,627,120) plus ~770 writes. Two replica paths put the OLD
# master's cursor on the wire after a rewind: the FLINTSYNC-OK handler acked
# the `cursor` it had asked with, and the 500 ms heartbeat fires on a clock,
# before the OK is read. The master recorded either as the connection's first
# ack, so `seq_lag` and the WAL-headroom SHED gate read the whole offset as
# lag until the next one. Offsets on the playground have reached 21,384,102;
# a roll's just-booted master sheds above 3,568,800.
#
# ARM 1 (the replica): rejoin a rewound ex-master under live load, with the
# new master holding its FLINTSYNC-OK 1500 ms (FLINT_TEST_DELAY_SYNC_OK_MS) so
# the heartbeat path is exercised EVERY run, and a shed threshold set BELOW the
# measured offset. The master must count zero acks below the cursor it served
# and shed nothing. Without the fix both replica paths send one.
#
# ARM 2 (the master, and the negative control): a raw client speaks the
# pre-fix replica's handshake -- FLINTSYNC with the old epoch, then ACK of the
# old-space cursor -- and never acks again. The master must COUNT it (so the
# arm-1 zero is a witness that can go red) and must NOT shed: recorded, that
# ack would pin the headroom gate at the offset for the 2 s liveness window
# while the load writes.
#
# THE OFFSET IS MEASURED, NOT ASSUMED. A replica's own sequence space runs
# ahead of its upstream's as it applies (the durable cursor rows cost
# sequences the stream does not carry). The drill reads it before the kill,
# sets the threshold to half of it, and then checks the attach's logged offset
# really exceeds that threshold -- a fixture whose offset sat under the gate
# would pass with or without either fix.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rewind-ack 6468 6469
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-rewind-ack; rm -rf "$D"; mkdir -p "$D"
fleet_kill server
sleep 0.3
LOADPID=""
cleanup() {
  [ -n "$LOADPID" ] && kill "$LOADPID" 2>/dev/null
  fleet_kill server; rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $*"; exit 1; }

# The pre-fix replica's handshake, as a file rather than a heredoc inside
# $( ): macOS bash 3.2 mis-parses the latter far from the cause.
cat > "$D/rawsync.py" <<'RAWPY'
import socket, sys, time
port, cur = int(sys.argv[1]), sys.argv[2]
def resp(*a):
    return ("*%d\r\n" % len(a) + "".join("$%d\r\n%s\r\n" % (len(x), x) for x in a)).encode()
s = socket.create_connection(("127.0.0.1", port), timeout=5)
s.sendall(resp("FLINTSYNC", cur, "0", "1"))  # the old epoch, (0,1)
# NOT PIPELINED. Sent back to back, both frames usually arrive in one read; the
# connection's command reader consumes that buffer, dispatches FLINTSYNC, and
# the ACK never reaches the stream's ack drain -- the first run of arm 2 passed
# only when the bytes happened to split, and a mutant exposed it. A pre-fix
# replica's heartbeat came >= 500 ms after its FLINTSYNC; 300 ms lands it, like
# that one, while the master holds its OK (FLINT_TEST_DELAY_SYNC_OK_MS=1500).
time.sleep(0.3)
s.sendall(resp("ACK", cur))                  # the cursor it asked with: OLD space
s.settimeout(0.5)
got, end = b"", time.time() + 4.5            # past the 1500 ms delayed OK and 2 s liveness
while time.time() < end:
    try:
        d = s.recv(65536)
        if not d:
            break
        got += d
    except socket.timeout:
        pass
s.close()
print("OK accepted" if b"FLINTSYNC-OK" in got else "NOT ACCEPTED: " + got[:200].decode(errors="replace"))
RAWPY

cargo build --release -q -p flint-server --features rocks || fail "build"
fleet_warm "$B"

info() {  # info <port> <field>
  valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr '\r' '\n' | sed -n "s/^$2://p"
}
wait_seq() {  # wait until node $1's last_applied reaches $2 (budget: 20s)
  for _ in $(seq 1 200); do
    LA=$(info "$1" last_applied)
    [ -n "$LA" ] && [ "$LA" -ge "$2" ] && return 0
    sleep 0.1
  done
  return 1
}

echo "== pair up: A(6468) master, B(6469) replica (B delays its FLINTSYNC-OK 1500 ms once it serves)"
$B --port 6468 --engine rocks --data-dir "$D/a" >"$D/a1.log" 2>&1 &
fleet_wait_ready 6468
FLINT_TEST_DELAY_SYNC_OK_MS=1500 \
  $B --port 6469 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6468 >"$D/b1.log" 2>&1 &
fleet_wait_ready 6469

# THE OFFSET THAT MATTERS IS THE ONE AT THE SNAPSHOT. The rewound copy
# attaches at its snapshot's cursor, and B maps THAT position -- so the offset
# B had accumulated when the snapshot was taken is what an old-space ack would
# be wrong by, not the larger one at B's tip. The first cut of this drill
# measured the tip (2002) and its own fixture check caught the attach at 302.
# So: build the offset first, snapshot, then add only a little.
for i in $(seq 1 6000); do printf 'SET pre:%d v%d\n' "$i" "$i"; done | valkey-cli -p 6468 >/dev/null
TIP=$(info 6468 latest_seq)
wait_seq 6469 "$TIP" || fail "B never caught up to A before the snapshot"
BLATEST=$(info 6469 latest_seq); BAPPLIED=$(info 6469 last_applied)
case "$BLATEST$BAPPLIED" in ''|*[!0-9]*) fail "B's latest_seq/last_applied unreadable ('$BLATEST'/'$BAPPLIED')" ;; esac
OFFSET_SNAP=$((BLATEST - BAPPLIED))
SNAP_OUT=$(valkey-cli -p 6468 FLINTSNAPSHOT "$D/snaps-a")
case "$SNAP_OUT" in OK\ snap-*-e0.1) ;; *)
  fail "master snapshot id not epoch-labeled: '$SNAP_OUT' -- no snapshot is rewind-eligible"
esac
for i in $(seq 6001 6300); do printf 'SET pre:%d v%d\n' "$i" "$i"; done | valkey-cli -p 6468 >/dev/null
TIP=$(info 6468 latest_seq)
wait_seq 6469 "$TIP" || fail "B never caught up to A before the kill"

# The threshold must sit BELOW the offset (or a recorded old-space ack could
# not trip it) and well ABOVE the legitimate catch-up backlog -- the 300
# post-snapshot writes plus the paced load while A rewinds -- or the drill
# would count honest lag as the defect.
[ "$OFFSET_SNAP" -ge 2000 ] || fail "B's own sequence space is only $OFFSET_SNAP ahead of A's at the
      snapshot -- too little room between honest catch-up lag and the offset"
THRESH=$((OFFSET_SNAP / 2))
echo "  at the snapshot B ran $OFFSET_SNAP sequences ahead of the stream it applies; shed threshold $THRESH"

echo "== arm 1: kill A, promote B, rejoin A by rewind under load -- no old-space ack may reach B"
kill -9 "$(pgrep -f "flint-server --port 6468" | head -1)" 2>/dev/null
sleep 0.3
valkey-cli -p 6469 FLINTPROMOTE 0 2 | grep -q "OK promoted" || fail "promote B"
valkey-cli -p 6469 FLINTCONFIG wal-headroom-seq "$THRESH" >/dev/null
[ "$(info 6469 wal_headroom_shed_seq)" = "$THRESH" ] \
  || fail "B's shed threshold did not take ($(info 6469 wal_headroom_shed_seq), wanted $THRESH)"
ABC0=$(info 6469 acks_below_cursor); SHED0=$(info 6469 writes_shed_headroom)
case "$ABC0" in ''|*[!0-9]*) fail "acks_below_cursor is not in FLINTINFO ('$ABC0') -- every zero below would be vacuous" ;; esac
case "$SHED0" in ''|*[!0-9]*) fail "writes_shed_headroom unreadable ('$SHED0')" ;; esac

# LIVE LOAD through both arms: a shed is only observable as a refused write.
# PACED, so the honest backlog while A rewinds stays far under the threshold;
# a few hundred writes a second still lands many inside every window tested.
( i=0; while :; do valkey-cli -p 6469 SET "live:$((i+=1))" "L$i" >/dev/null 2>&1; sleep 0.005; done ) &
LOADPID=$!

echo "drill: superseded copy rejoining" > "$D/a/NEEDS_RESEED"
$B --port 6468 --engine rocks --data-dir "$D/a" --replica-of 127.0.0.1:6469 \
   --rewind-snaps "$D/snaps-a" >"$D/a2.log" 2>&1 &
fleet_wait_ready 6468
fleet_wait_log "$D/a2.log" "rewound to" 30 \
  || { sed 's/^/    /' "$D/a2.log"; fail "A did not rewind -- this drill needs a rewind attach"; }
fleet_wait_log "$D/a2.log" "adopted the master's translated cursor" 30 \
  || { sed 's/^/    /' "$D/a2.log"; fail "A never adopted a translated cursor"; }

ATTACH=$(grep -o 'rewind attach: upstream cursor [0-9]* (epoch ([0-9,]*)) maps to local seq [0-9]*' "$D/b1.log" | tail -1)
UP=$(printf '%s' "$ATTACH" | sed -n 's/.*upstream cursor \([0-9]*\) .*/\1/p')
OWN=$(printf '%s' "$ATTACH" | sed -n 's/.*local seq \([0-9]*\)$/\1/p')
[ -n "$UP" ] && [ -n "$OWN" ] || fail "B logged no rewind attach -- nothing was translated"
OFFSET=$((OWN - UP))
[ "$OFFSET" -gt "$THRESH" ] || fail "the attach's offset $OFFSET does not exceed the shed threshold
      $THRESH, so this fixture cannot tell a recorded old-space ack from none"
echo "  rewind attach: upstream $UP -> own $OWN, offset $OFFSET > threshold $THRESH"

# Several heartbeats under load, so a late heartbeat path would have fired.
sleep 2
ABC1=$(info 6469 acks_below_cursor); SHED1=$(info 6469 writes_shed_headroom)
[ "$ABC1" = "$ABC0" ] || fail "the rewound replica acked below the cursor B served it
      $((ABC1 - ABC0)) time(s) -- an old-space cursor reached the wire (BUG-0175)"
[ "$SHED1" = "$SHED0" ] || fail "B shed $((SHED1 - SHED0)) write(s) on WAL headroom during the rejoin"
echo "  0 acks below the served cursor, 0 writes shed across the rejoin"

echo "== arm 2: a replica that DOES ack in the old space is counted and not recorded"
# The pre-fix handshake, spoken directly: the old epoch, the old-space cursor,
# and an ACK of that cursor before the OK is read. It never acks again.
RAW=$(python3 "$D/rawsync.py" 6469 "$UP" 2>&1) || true
case "$RAW" in OK*) ;; *) fail "the raw handshake was not accepted: $RAW" ;; esac
ABC2=$(info 6469 acks_below_cursor); SHED2=$(info 6469 writes_shed_headroom)
[ "$ABC2" -gt "$ABC1" ] || { tail -12 "$D/b1.log" | sed 's/^/    b1: /'; echo "    raw: $RAW"
  fail "B did not count the old-space ack ($ABC1 -> $ABC2) -- the arm-1
      zero is then not evidence of anything"; }
[ "$SHED2" = "$SHED1" ] || { tail -12 "$D/b1.log" | sed 's/^/    b1: /'; }; [ "$SHED2" = "$SHED1" ] || fail "B RECORDED the old-space ack and shed $((SHED2 - SHED1)) write(s)
      while it pinned the headroom gate -- the master-side guard is not holding"
echo "  counted $((ABC2 - ABC1)) old-space ack(s), shed nothing"

# CONVERGE: the rejoin must still be a correct one.
kill "$LOADPID" 2>/dev/null; wait "$LOADPID" 2>/dev/null; LOADPID=""
BTIP=""
for _ in $(seq 1 50); do
  CUR=$(info 6469 latest_seq)
  [ -n "$CUR" ] && [ "$CUR" = "$BTIP" ] && break
  BTIP=$CUR; sleep 0.1
done
wait_seq 6468 "$BTIP" || fail "rewound A never converged to B's tip"
DA=$(valkey-cli -p 6468 DBSIZE); DB=$(valkey-cli -p 6469 DBSIZE)
[ "$DA" = "$DB" ] || fail "keyspaces diverge after the rejoin ($DA vs $DB)"
echo "  A converged to B: $DA keys each"

echo "PASS: rewind ack space -- the rewound replica acks only in the master's space, and an old-space ack is counted, not recorded"
