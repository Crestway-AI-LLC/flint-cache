#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Config-file drill: operator tunables live in the inventory (a config file
# flintctl reads at launch), so changing them is edit + restart — no
# rebuild, no redeploy. This bootstraps with NON-DEFAULT values and proves
# each reaches the running process (FLINTINFO exposes the node knobs), then
# CHANGES a value, restarts, and proves the new value took — the whole point.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-cfg-state 7211 7212 7213 7214
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-cfg-state
INV=$FLINT_DRILL_ROOT/flint-cfg.flint
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

# NON-DEFAULT tunables in the config file (defaults: wal 500, soft 500,
# hard 1000, min-replicas 0, max-conns big).
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7214
pair 127.0.0.1:7211,127.0.0.1:7212
proxy 127.0.0.1:7213
controller on
wal-fsync-ms 250
lag-soft-ms 300
lag-hard-ms 2000
min-replicas 1
max-conns 4096
EOF

echo "== bootstrap with a non-default config file"
./target/release/flintctl -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }

# flintctl's own mesh client can't easily be scripted here; read FLINTINFO
# straight off the master over the mesh via the controller's cert set.
info() { # field -> value, off node 7211 (the master) using the mesh CA
  ./target/release/flintctl -f "$INV" status >/dev/null 2>&1
  python3 - "$1" <<'PY'
import socket, ssl, sys, os
field = sys.argv[1]
d = os.path.expanduser(os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-cfg-state/certs")
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key")
ctx.check_hostname = False
s = ctx.wrap_socket(socket.create_connection(("127.0.0.1", 7211), timeout=5),
                    server_hostname="flint-internal")
s.sendall(b"*1\r\n$9\r\nFLINTINFO\r\n")
buf = b""
while b"\r\n\r\n" not in buf and len(buf) < 65536:
    c = s.recv(4096)
    if not c: break
    buf += c
for line in buf.split(b"\r\n"):
    if line.startswith(field.encode()+b":"):
        print(line.split(b":",1)[1].decode()); break
PY
}

echo "== assert the config reached the running node (FLINTINFO)"
check() { got=$(info "$1"); [ "$got" = "$2" ] || { echo "FAIL: $1=$got (want $2)"; exit 1; }; echo "  $1 = $got"; }
check wal_fsync_ms 250
check lag_soft_ms 300
check lag_hard_ms 2000
check min_replicas_to_write 1
check max_conns 4096

echo "== HOT RELOAD: change values in the config, 'flintctl reload', NO restart"
# Capture the master's pid so we can prove it never restarted.
PID_BEFORE=$(cat "$STATE/pids/node-7211.pid")
sed -i.bak 's/wal-fsync-ms 250/wal-fsync-ms 1000/; s/lag-hard-ms 2000/lag-hard-ms 3000/; s/max-conns 4096/max-conns 8192/' "$INV"
# ADR-0028 obligation 2: DECLARE THE BYTES THAT CHANGED, not the text predicted
# to change. The three checks below assert the server reports 1000/3000/8192 --
# which is the right assertion and passes just as well if the inventory ALREADY
# said those values and the sed matched nothing. A default moving to any one of
# them turns this stage into a reload of an unchanged file that certifies hot
# reload works. `sed -i.bak` has already written the pre-mutation copy; nothing
# was reading it.
cmp -s "$INV" "$INV.bak" && {
  echo "FAIL: the config edit changed nothing — $INV is byte-identical to its"
  echo "      pre-edit copy, so the reload below would prove nothing. Either a"
  echo "      pattern drifted or the inventory already carried these values."
  exit 1; }
./target/release/flintctl -f "$INV" reload 2>&1 | sed 's/^/  /'
check wal_fsync_ms 1000
check lag_hard_ms 3000
check max_conns 8192
PID_AFTER=$(cat "$STATE/pids/node-7211.pid")
[ "$PID_BEFORE" = "$PID_AFTER" ] || { echo "FAIL: node restarted (pid $PID_BEFORE -> $PID_AFTER)"; exit 1; }
echo "  applied live via FLINTCONFIG; node pid unchanged ($PID_AFTER) — no restart"

echo "== FLINTCONFIG dump reflects the live values"
# The certs dir rides argv: the -c body is single-quoted, so a shell
# variable inside it is a literal dollar string, not a path (the #180
# scratch-root conversion put one there and this check failed on the
# unexpanded name for every gate run after c539a0d).
DUMP=$(python3 -c '
import socket, ssl, os, sys
d=sys.argv[1]
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7211),timeout=5),server_hostname="flint-internal")
s.sendall(b"*1\r\n$11\r\nFLINTCONFIG\r\n")
print(s.recv(65536).decode(errors="replace"))
' "$FLINT_DRILL_ROOT/flint-cfg-state/certs")
echo "$DUMP" | grep -q 'wal-fsync-ms:1000' || { echo "FAIL: dump missing wal-fsync-ms:1000"; echo "$DUMP"; exit 1; }
echo "  FLINTCONFIG dump: hot knobs reported live"

echo "PASS: operator tunables are config-file driven AND hot-reloadable (flintctl reload, no restart, no rebuild)"
