#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0280: a zombie the controller fenced rejoined its new master at a
# cursor in its own old sequence space. The controller fences a zombie with
# FLINTDEMOTE at max_epoch + 1, which is ABOVE the new master's epoch, and the
# master read any cursor at an epoch not below its own as a position in its
# own WAL. The zombie kept the writes its successor never had and skipped as
# many of the successor's, while the master reported it live at seq_lag 0.
# Asserts, on real seats, for a zombie holding 200 writes its successor never
# had, demoted the way the controller does it:
#   - marked for re-seed, as FLINTDEMOTE and every flintctl rejoin path leave
#     it, it is refused, re-seeds, and holds exactly the master's keys
#   - unmarked (the marker write failed), it is refused on every reconnect,
#     saying why, and its copy is left as it was: none of the master's writes
#     is applied to it, and the master counts no live replica
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-zombie- 6592 6593
fleet_guard
fleet_kill server; sleep 0.4
B=./target/release/flint-server
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-zombie-d.XXXXXX)
cleanup() {
  fleet_kill server
  rm -rf "$D"
}
trap cleanup EXIT
info() { valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$2://p"; }
put() { # port prefix from to
  python3 - "$2" "$3" "$4" <<'PY' | valkey-cli -p "$1" --pipe | tail -1
import sys
p, a, b = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(a, b):
    k = f"{p}:{i}"; v = f"v-{p}-{i}"
    sys.stdout.write(f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")
PY
}
keys() { valkey-cli -p "$1" --scan --count 1000 2>/dev/null | tr -d '\r' | sort; }
gone() { for _ in $(seq 1 50); do valkey-cli -p "$1" PING >/dev/null 2>&1 || return 0; sleep 0.1; done; echo "FAIL: port $1 still answers after its seat was killed"; exit 1; }
M=6592; S=6593

# The zombie at $M and its successor at $S, as a failover leaves them. $1 is
# the case's directory.
zombie() {
  local d=$1 mp sp ac
  mkdir -p "$d"
  $B --port $M --engine rocks --data-dir "$d/z" 2>>"$d/z.log" & mp=$!
  fleet_wait_listen $M; fleet_wait_ping $M
  put $M base 0 1000 >/dev/null
  $B --port $S --engine rocks --data-dir "$d/s" --replica-of 127.0.0.1:$M 2>>"$d/s.log" & sp=$!
  fleet_wait_listen $S
  for _ in $(seq 1 200); do
    [ "$(valkey-cli -p $S DBSIZE 2>/dev/null)" = 1000 ] && [ "$(info $M seq_lag)" = 0 ] && break
    sleep 0.1
  done
  [ "$(info $M seq_lag)" = 0 ] || { echo "FAIL: the replica never caught up before the cut"; exit 1; }
  # The successor is cut off; the old master goes on taking writes.
  kill -9 $sp; wait $sp 2>/dev/null; gone $S
  put $M zomb 0 200 >/dev/null
  # The successor is promoted above it and goes on past it.
  $B --port $S --engine rocks --data-dir "$d/s" 2>>"$d/s.log" &
  fleet_wait_listen $S
  ac=$(info $M role_epoch | tr -c '0-9\n' ' ' | awk '{print $NF}')
  case "$(valkey-cli -p $S FLINTPROMOTE 0 $((ac + 1)) | tr -d '\r')" in
    "OK promoted"*) ;; *) echo "FAIL: the successor was not promoted"; exit 1 ;;
  esac
  put $S new 0 2000 >/dev/null
  # Fenced as the controller fences a zombie: max_epoch + 1.
  case "$(valkey-cli -p $M FLINTDEMOTE 0 $((ac + 2)) | tr -d '\r')" in
    "OK demoted"*) ;; *) echo "FAIL: the zombie was not demoted"; exit 1 ;;
  esac
  [ -f "$d/z/NEEDS_RESEED" ] || { echo "FAIL: FLINTDEMOTE left no re-seed marker"; exit 1; }
  kill -9 $mp; wait $mp 2>/dev/null; gone $M
  echo "  zombie demoted at (0,$((ac + 2))) above its successor's (0,$((ac + 1))): 200 writes only it has, 2000 only the successor has"
}

echo "== marked, as every rejoin path leaves it: refused, re-seeded, equal to its master"
C=$D/marked; zombie "$C"
$B --port $M --engine rocks --data-dir "$C/z" --replica-of 127.0.0.1:$S 2>"$C/rejoin.log" &
fleet_wait_listen $M
for _ in $(seq 1 300); do
  [ "$(info $M loading)" = 0 ] && [ "$(info $S seq_lag)" = 0 ] && [ "$(info $S live_replicas)" = 1 ] && break
  sleep 0.1
done
keys $M >"$C/z.keys"; keys $S >"$C/s.keys"
ONLY_Z=$(comm -23 "$C/z.keys" "$C/s.keys" | grep -c .)
ONLY_S=$(comm -13 "$C/z.keys" "$C/s.keys" | grep -c .)
if [ "$ONLY_Z" != 0 ] || [ "$ONLY_S" != 0 ]; then
  echo "FAIL (BUG-0280): the rejoined zombie holds $ONLY_Z key(s) its master does not, and lacks $ONLY_S of the master's"
  sed 's/^/    /' "$C/rejoin.log" | grep -vE 'budget|headroom|disk guard' | tail -12; exit 1
fi
grep -q "is newer than this master's" "$C/rejoin.log" \
  || { echo "FAIL (BUG-0280): the rejoin does not say why it was refused:"; tail -12 "$C/rejoin.log" | sed 's/^/    /'; exit 1; }
grep -q "NEEDS_RESEED present" "$C/rejoin.log" \
  || { echo "FAIL (BUG-0280): the refused zombie did not re-seed:"; tail -12 "$C/rejoin.log" | sed 's/^/    /'; exit 1; }
for i in 0 1000 1999; do
  [ "$(valkey-cli -p $M GET "new:$i")" = "v-new-$i" ] || { echo "FAIL (BUG-0280): new:$i is not on the rejoined copy"; exit 1; }
done
echo "  refused, re-seeded: $(valkey-cli -p $M DBSIZE) keys, the master's exactly; no zomb: key left"
fleet_kill server; sleep 0.4; gone $M; gone $S

echo "== unmarked (the marker write failed): refused on every reconnect, the copy left as it was"
C=$D/unmarked; zombie "$C"
rm -f "$C/z/NEEDS_RESEED"
$B --port $M --engine rocks --data-dir "$C/z" --replica-of 127.0.0.1:$S 2>"$C/rejoin.log" & ZP=$!
fleet_wait_listen $M
for _ in $(seq 1 100); do
  [ "$(grep -c "is newer than this master's" "$C/rejoin.log")" -ge 2 ] && break
  sleep 0.1
done
[ "$(grep -c "is newer than this master's" "$C/rejoin.log")" -ge 2 ] \
  || { echo "FAIL (BUG-0280): no refusal, twice, in the unmarked zombie's log:"; tail -12 "$C/rejoin.log" | sed 's/^/    /'; exit 1; }
kill -0 $ZP 2>/dev/null || { echo "FAIL: the refused zombie exited"; exit 1; }
NEW=$(keys $M | grep -c '^new:')
ZOMB=$(keys $M | grep -c '^zomb:')
[ "$NEW" = 0 ] || { echo "FAIL (BUG-0280): $NEW of the master's writes were applied to the unmarked zombie at the wrong cursor"; exit 1; }
[ "$ZOMB" = 200 ] || { echo "FAIL: the refused zombie's copy changed: $ZOMB of its 200 own writes left"; exit 1; }
[ "$(info $S live_replicas)" = 0 ] || { echo "FAIL (BUG-0280): the master counts the refused zombie as a live replica"; exit 1; }
echo "  refused $(grep -c "is newer than this master's" "$C/rejoin.log") times, still up; its copy unchanged (200 own writes, none of the master's); live_replicas 0"

echo "PASS: a zombie fenced above its new master is never resumed at its own cursor: marked, it re-seeds"
echo "      and holds exactly the master's keys; unmarked, it is refused and left as it was (BUG-0280)."
