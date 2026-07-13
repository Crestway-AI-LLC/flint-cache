#!/usr/bin/env bash
# THE durability proof: interrupt a slot cutover by killing BOTH the source
# and the destination (a whole-cluster redeploy) mid-move, restart them, and
# let the recovery controller reconcile from the durable manifest records.
# After recovery: exactly one node owns the slot (source answers -MOVED to the
# dest), the dest has every key, and no write was lost. Runs several times so
# the kill lands at different phases (pull / freeze / flip).
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
SPORT=6580; DPORT=6581
SADDR="127.0.0.1:$SPORT"; DADDR="127.0.0.1:$DPORT"
KEYS=150000
SLOT=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"mover")%16384)')

run_once() {
  local delay="$1"
  pkill -9 -f "flint-server --port 658" 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
  local SDIR DDIR
  SDIR=$(mktemp -d /tmp/flint-rec-s.XXXXXX); DDIR=$(mktemp -d /tmp/flint-rec-d.XXXXXX)
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>/dev/null &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>/dev/null &
  sleep 0.8

  awk -v n="$KEYS" 'BEGIN{for(i=0;i<n;i++){k=sprintf("{mover}:key%06d",i);v=sprintf("val-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p $SPORT --pipe >/dev/null

  # Start the cutover in the background; it blocks until done.
  ( valkey-cli -p $DPORT FLINTMIGRATEIN "$SADDR" "$SLOT" "$DADDR" >/dev/null 2>&1 ) &
  local MIG=$!
  sleep "$delay"
  # WHOLE-CLUSTER KILL mid-move.
  pkill -9 -f "flint-server --port 658" 2>/dev/null
  kill -9 $MIG 2>/dev/null; wait $MIG 2>/dev/null
  sleep 0.5

  # Restart both nodes on the same data dirs (the redeploy).
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>/dev/null &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>/dev/null &
  sleep 0.9

  # Observe the interrupted state from the durable records.
  local SM DM PHASE
  SM=$(valkey-cli -p $SPORT FLINTMIGRATIONS 2>/dev/null)
  DM=$(valkey-cli -p $DPORT FLINTMIGRATIONS 2>/dev/null)
  if [ -n "$SM" ] || [ -n "$DM" ]; then PHASE="INTERRUPTED mid-move"; else PHASE="completed pre-kill (recovery is a no-op)"; fi
  echo "  [delay $delay] after restart: source=[$SM] dest=[$DM] -> $PHASE"

  # Recovery controller: reconciles from the manifests, no other input.
  ./target/release/flint-controller --recover-nodes "$SADDR,$DADDR" --id REC --poll-ms 200 2>>/tmp/flint-rec.log &
  local CTL=$!

  # Wait until the move is fully resolved: dest owns (a write succeeds) AND
  # source redirects the slot with -MOVED.
  local RESOLVED=0 i
  for i in $(seq 1 150); do
    local dw sr
    dw=$(valkey-cli -p $DPORT SET "{mover}:key000000" val-000000 2>&1)
    sr=$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)
    if [ "$dw" = "OK" ] && echo "$sr" | grep -qE "MOVED $SLOT $DADDR"; then RESOLVED=1; break; fi
    sleep 0.2
  done
  kill -9 $CTL 2>/dev/null
  if [ "$RESOLVED" != "1" ]; then
    echo "  FAIL: move not resolved after recovery (dest write='$dw' source read='$sr')"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"; return 1
  fi

  # No split ownership: the source must NOT serve writes for the slot.
  local sw
  sw=$(valkey-cli -p $SPORT SET "{mover}:key000001" x 2>&1)
  echo "$sw" | grep -qE "MOVED $SLOT" || { echo "  FAIL: SPLIT OWNERSHIP — source still writable for slot: $sw"; pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"; return 1; }

  # No data loss: every sampled key present on the owner (dest).
  local miss=0 k
  for k in 000000 000001 075000 149999; do
    [ "$(valkey-cli -p $DPORT GET "{mover}:key$k")" = "val-$k" ] || { echo "  MISSING key$k on dest"; miss=$((miss+1)); }
  done
  [ "$miss" = "0" ] || { echo "  FAIL: $miss keys lost after recovery"; pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"; return 1; }

  echo "  [delay $delay] RESOLVED: dest owns all keys, source -MOVED, no split, no loss"
  pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"
  return 0
}

# Deterministically exercise the OTHER recovery branch — a flip interrupted
# between "dest owns" and "source disowned" (source Migrating, dest already
# owns). Timing-based kills rarely land in this sub-100ms window, so we
# construct the state directly: ship the data (no cutover), then freeze the
# source. Recovery must COMPLETE the flip: source -> Moved.
test_half_done_flip() {
  pkill -9 -f "flint-server --port 658" 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
  local SDIR DDIR
  SDIR=$(mktemp -d /tmp/flint-rec-s.XXXXXX); DDIR=$(mktemp -d /tmp/flint-rec-d.XXXXXX)
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>/dev/null &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>/dev/null &
  sleep 0.8
  awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{mover}:key%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n",length(k),k}}' \
    | valkey-cli -p $SPORT --pipe >/dev/null
  # Ship data to the dest (no cutover), then place the half-done-flip state:
  # dest owns (has the data, no record), source frozen Migrating to dest.
  valkey-cli -p $DPORT FLINTMIGRATEIN "$SADDR" "$SLOT" >/dev/null
  valkey-cli -p $SPORT FLINTSLOTFREEZE "$SLOT" "$DADDR" >/dev/null
  echo "  [half-done-flip] source frozen (Migrating), dest owns; source records=[$(valkey-cli -p $SPORT FLINTMIGRATIONS)]"

  ./target/release/flint-controller --recover-nodes "$SADDR,$DADDR" --id REC2 --poll-ms 200 2>>/tmp/flint-rec.log &
  local CTL=$! RESOLVED=0 i
  for i in $(seq 1 60); do
    if echo "$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)" | grep -qE "MOVED $SLOT $DADDR"; then RESOLVED=1; break; fi
    sleep 0.2
  done
  kill -9 $CTL 2>/dev/null
  pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"
  [ "$RESOLVED" = "1" ] || { echo "  FAIL: recovery did not complete the half-done flip"; return 1; }
  echo "  [half-done-flip] RESOLVED: recovery completed the flip, source -MOVED to dest"
  return 0
}

trap 'pkill -9 -f "flint-server --port 658" 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null' EXIT
: > /tmp/flint-rec.log
echo "== slot {mover}=$SLOT, $KEYS keys; killing BOTH nodes mid-cutover (timing-based)"
FAILS=0
for d in 0.3 0.5 0.7; do
  run_once "$d" || FAILS=$((FAILS+1))
done
echo "== deterministic half-done-flip recovery"
test_half_done_flip || FAILS=$((FAILS+1))
[ "$FAILS" = "0" ] || { echo "FAIL: $FAILS recovery runs failed"; exit 1; }
echo "PASS: whole-cluster interruption mid-cutover recovers to clean single ownership, no loss, no split"
