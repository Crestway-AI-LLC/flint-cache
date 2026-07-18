#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Managed-controller drill: the controller OWNS the full failover cycle —
# it bootstraps the pair, and on any kill it promotes and/or respawns a
# fresh replacement replica. The drill only KILLS nodes; it never promotes
# or spawns. Proves hands-free detect -> promote -> respawn -> reconverge.
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
D1=/tmp/flint-mng-1; D2=/tmp/flint-mng-2
rm -rf "$D1" "$D2" "$D1.log" "$D2.log"
P1=6460; P2=6470
cleanup() {
  pkill -9 -f "flint-server --port 64" 2>/dev/null
  pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$D1" "$D2"
}
trap cleanup EXIT

echo "== start the managed controller (it bootstraps the pair itself)"
./target/release/flint-controller --manage-slots "$P1:$D1,$P2:$D2" --id MNG \
  --poll-ms 150 --confirm 3 --lease-ttl-ms 3000 2>/tmp/flint-mng.log &
# Wait for the controller to bootstrap master + replica.
for i in $(seq 1 60); do
  [ "$(valkey-cli -p $P1 PING 2>/dev/null)" = "PONG" ] && [ "$(valkey-cli -p $P2 PING 2>/dev/null)" = "PONG" ] && break
  sleep 0.2
done
[ "$(valkey-cli -p $P1 PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: controller never bootstrapped"; cat /tmp/flint-mng.log; exit 1; }
echo "pair bootstrapped by the controller"

master_port() {
  for p in $P1 $P2; do
    if valkey-cli -p $p FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master"; then echo $p; return; fi
  done
}

echo "== load 15000 keys into the current master"
M=$(master_port)
awk 'BEGIN{for(i=0;i<15000;i++){k=sprintf("key:%07d",i);v=sprintf("value-%07d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p $M --pipe | tail -1
sleep 2.0   # let the replica converge and the controller observe it

for round in 1 2 3; do
  M=$(master_port)
  OTHER=$([ "$M" = "$P1" ] && echo $P2 || echo $P1)
  echo "== round $round: KILL master :$M (controller must promote :$OTHER AND respawn :$M)"
  pkill -9 -f "flint-server --port $M"

  # Wait for the survivor to become master (controller promoted it).
  PROMOTED=0
  for i in $(seq 1 60); do
    valkey-cli -p $OTHER FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master" && { PROMOTED=1; break; }
    sleep 0.2
  done
  [ "$PROMOTED" = "1" ] || { echo "FAIL: controller did not promote :$OTHER"; tail -12 /tmp/flint-mng.log; exit 1; }
  [ "$(valkey-cli -p $OTHER SET p$round ok 2>&1)" = "OK" ] || { echo "FAIL: promoted master not writable"; exit 1; }

  # Wait for the killed slot to be respawned as a fresh replica and reconverge.
  RECONVERGED=0
  for i in $(seq 1 100); do
    SL=$(valkey-cli -p $OTHER FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE "seq_lag:[a-z0-9]+")
    LR=$(valkey-cli -p $OTHER FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE "live_replicas:[0-9]+")
    [ "$SL" = "seq_lag:0" ] && [ "$LR" = "live_replicas:1" ] && { RECONVERGED=1; break; }
    sleep 0.2
  done
  [ "$RECONVERGED" = "1" ] || { echo "FAIL: controller did not respawn+reconverge a replica"; tail -12 /tmp/flint-mng.log; exit 1; }
  # Let the controller independently observe the converged pair before the
  # next kill — it promotes only survivors IT has confirmed caught up
  # (killing faster than any monitor polls is a degraded window, correctly
  # refused; that is not the scenario under test here).
  sleep 1.0

  # Data intact across the failover.
  [ "$(valkey-cli -p $OTHER GET key:0000000)" = "value-0000000" ] || { echo "FAIL: head lost"; exit 1; }
  [ "$(valkey-cli -p $OTHER GET key:0014999)" = "value-0014999" ] || { echo "FAIL: tail lost"; exit 1; }
  echo "  promoted :$OTHER, respawned :$M as fresh replica, reconverged, data intact"
done

echo "PASS: managed controller owns the full failover cycle hands-free (promote + respawn + reconverge)"
