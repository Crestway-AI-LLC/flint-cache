#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# One command to a real Flint cluster on a laptop.
#
# Not a single node: a control plane, a replicated PAIR, a routing proxy and
# the failover controller — the shape the product is actually about. A single
# node would demo a key-value store; a pair lets you kill the master and watch
# the controller promote the replica, which is the thing worth evaluating.
#
#   tools/quickstart.sh          bring it up (builds first if needed)
#   tools/quickstart.sh status   roles, lag, liveness
#   tools/quickstart.sh failover kill this cluster's master, watch the promote
#   tools/quickstart.sh down     stop it, keep the data
#   tools/quickstart.sh purge    stop it and delete the data
#
# Env:
#   FLINT_QS_DIR   where state and the inventory live (default .flint-quickstart)
#   FLINT_BINS     use these prebuilt binaries instead of building from source
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$(pwd)

QS="${FLINT_QS_DIR:-$REPO/.flint-quickstart}"
INV="$QS/cluster.flint"
CP_PORT=7500; M_PORT=7001; R_PORT=7002; PX_PORT=7379
TENANT=demo; TOKEN=demo-token
CMD="${1:-up}"

die() { echo; echo "quickstart: $*" >&2; exit 1; }
say() { echo; echo "== $*"; }

# Is something LISTENING on this port? bash's /dev/tcp connects without
# needing lsof/ss/netstat, which are not all present on a minimal container.
port_busy() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- 2>/dev/null; return 0; }
  return 1
}

# Any Redis-protocol CLI will do; the drills use valkey-cli by name, but for
# the quickstart redis-cli is equally fine and far more likely to be installed.
CLI=""
for c in valkey-cli redis-cli; do
  command -v "$c" >/dev/null 2>&1 && { CLI=$c; break; }
done

BINS="${FLINT_BINS:-$REPO/target/release}"

preflight() {
  if [ -z "${FLINT_BINS:-}" ]; then
    command -v cargo >/dev/null 2>&1 \
      || die "no cargo on PATH. Install Rust 1.85 or newer (https://rustup.rs), or point FLINT_BINS at prebuilt binaries."
    local v maj min
    v=$(rustc --version 2>/dev/null | awk '{print $2}')
    maj=${v%%.*}; min=${v#*.}; min=${min%%.*}
    if [ "${maj:-0}" -le 1 ] && [ "${min:-0}" -lt 85 ]; then
      die "Rust $v is too old — this workspace is edition 2024, which needs 1.85 or newer.
      rustup update stable    (or install from https://rustup.rs)"
    fi
    # RocksDB is compiled from source, so a C++ compiler is genuinely required.
    # libclang is required too but cannot be probed portably — if the build
    # fails we name it there rather than guessing about it here.
    command -v clang >/dev/null 2>&1 || command -v c++ >/dev/null 2>&1 || command -v g++ >/dev/null 2>&1 \
      || die "no C++ compiler on PATH — RocksDB is built from source.
      Debian/Ubuntu:  sudo apt-get install build-essential clang libclang-dev
      RHEL/Amazon:    sudo dnf install gcc-c++ clang clang-devel
      macOS:          xcode-select --install"
  else
    [ -x "$BINS/flintctl" ] || die "FLINT_BINS=$BINS has no flintctl in it"
  fi

  local busy=""
  for p in $CP_PORT $M_PORT $R_PORT $PX_PORT; do
    port_busy "$p" && busy="$busy $p"
  done
  # A already-running quickstart is the common case, and telling someone to
  # "free the ports" when their own cluster is what holds them is a bad
  # message. Distinguish the two.
  if [ -n "$busy" ]; then
    if [ -f "$QS/pids" ] || [ -d "$QS/state/pids" ]; then
      die "ports$busy are in use, and $QS exists — this quickstart looks like it is already up.
      tools/quickstart.sh status     see it
      tools/quickstart.sh down       stop it"
    fi
    die "ports$busy are already in use by something else. Stop it, or run with a clean set of ports."
  fi
}

build() {
  [ -n "${FLINT_BINS:-}" ] && return 0
  say "building (first run compiles RocksDB — expect ~10 minutes and a couple of GB in target/)"
  if ! cargo build --release --features flint-server/rocks; then
    die "the build failed. If it stopped inside the rocksdb or bindgen crate, the usual cause is a
      missing libclang:
      Debian/Ubuntu:  sudo apt-get install build-essential clang libclang-dev
      RHEL/Amazon:    sudo dnf install gcc-c++ clang clang-devel
      macOS:          xcode-select --install"
  fi
}

write_inventory() {
  mkdir -p "$QS"
  [ -f "$INV" ] && return 0
  cat > "$INV" <<EOF
# Written by tools/quickstart.sh — a THROWAWAY laptop cluster.
statedir $QS/state
bins $BINS
tls on
# 'disposable on' is what lets a from-source build (which reports no release
# version, no manifest, no checksums) mutate this fleet at all. Never put it
# in an inventory whose data you would miss — see docs/self-hosting.md for
# the stamped-build path a real deployment uses instead.
disposable on
cp 127.0.0.1:$CP_PORT
pair 127.0.0.1:$M_PORT,127.0.0.1:$R_PORT
proxy 127.0.0.1:$PX_PORT
controller on
min-replicas 1
EOF
}

up() {
  local t0 fresh
  t0=$(date +%s)
  preflight
  build
  write_inventory
  # bootstrap ONCE, start forever after. bootstrap mints the CA and REGISTERS
  # the topology; running it a second time would append duplicate pairs to the
  # registry. This is the same rule a production boot unit follows.
  if [ -e "$QS/state/cp-state" ]; then
    fresh=0
    say "existing state found — starting (not re-bootstrapping)"
    "$BINS/flintctl" -f "$INV" start
  else
    fresh=1
    say "bootstrapping: internal CA, control plane, a replicated pair, proxy, controller"
    "$BINS/flintctl" -f "$INV" bootstrap
  fi

  if [ "$fresh" = 1 ]; then
    say "adding tenant '$TENANT'"
    "$BINS/flintctl" -f "$INV" tenant add "$TENANT" "$TOKEN" "$TENANT" 1
    sleep 2
  fi

  if [ -n "$CLI" ]; then
    say "proving it serves"
    local got
    "$CLI" -p $PX_PORT -a "$TOKEN" --no-auth-warning SET quickstart "it works" >/dev/null 2>&1 \
      || die "the cluster came up but the write through the proxy failed.
      tools/quickstart.sh status     what flintctl thinks
      $QS/state/logs/                the process logs"
    got=$("$CLI" -p $PX_PORT -a "$TOKEN" --no-auth-warning GET quickstart 2>/dev/null | tr -d '\r')
    [ "$got" = "it works" ] || die "read back '$got', expected 'it works'"
    echo "  wrote and read a key through the proxy"
  else
    echo "  (no valkey-cli or redis-cli found — skipping the write test)"
  fi

  # Paths under the repo print relative — an absolute path here wraps the
  # terminal and buries the command it is trying to show.
  local qs_rel=${QS#"$REPO"/} inv_rel=${INV#"$REPO"/} bins_rel=${BINS#"$REPO"/}
  cat <<EOF

Flint is up in $(( $(date +%s) - t0 ))s. Connect with any Redis client:

    ${CLI:-valkey-cli} -p $PX_PORT -a $TOKEN SET hello world
    ${CLI:-valkey-cli} -p $PX_PORT -a $TOKEN GET hello

  control plane   127.0.0.1:$CP_PORT
  pair 0          127.0.0.1:$M_PORT (master) + 127.0.0.1:$R_PORT (replica)
  proxy / edge    127.0.0.1:$PX_PORT      tenant '$TENANT', token '$TOKEN'
  state + logs    $qs_rel/

Worth doing next, in rough order of how much it tells you:

    tools/quickstart.sh status     roles, epochs, lag, liveness
    tools/quickstart.sh failover   kill THIS cluster's master and watch the
                                   controller promote the replica — a replicated
                                   witness key proves the data came through
    tools/failover_drill.sh        the self-contained proof of the same thing,
                                   plus epoch fencing (own throwaway topology)
    tools/gates.sh                 the whole release gate: checks, conformance,
                                   20 drills, chaos
    $bins_rel/flintctl -f $inv_rel verify --probe $TENANT:$TOKEN
                                   reconcile every view of the cluster, with a
                                   live write/read through the data plane

    tools/quickstart.sh down       stop it, keep the data
    tools/quickstart.sh purge      stop it and delete the data

This cluster is DISPOSABLE on purpose — see $inv_rel.
For a deployment you intend to keep, docs/self-hosting.md.
EOF
}

# The wow moment: SIGKILL this cluster's own master and watch the controller
# do its job, with a witness key proving no acked write was lost. Distinct
# from tools/failover_drill.sh, which proves the epoch-fencing machinery on a
# throwaway topology of its own — this one acts on the cluster you are looking
# at, which is what "kill the master" naturally reads as.
failover() {
  [ -f "$INV" ] || die "nothing here yet — run tools/quickstart.sh"
  local out mport rport pidfile
  out=$("$BINS/flintctl" -f "$INV" status)
  # status prints:  pair 0    127.0.0.1:7001  master  ...
  # No `exit` in these awk programs: quitting early closes the pipe while
  # flintctl is still printing, and its stdout EPIPE panic (exit 101) then
  # reads as a flintctl failure. Read everything, print once.
  mport=$(echo "$out" | awk '!done && $1=="pair" && $4=="master" {split($3,a,":"); print a[2]; done=1}')
  [ -n "$mport" ] || die "no master found — is the cluster up?
      tools/quickstart.sh status"
  rport=$R_PORT; [ "$mport" = "$R_PORT" ] && rport=$M_PORT
  pidfile="$QS/state/pids/node-$mport.pid"
  [ -f "$pidfile" ] || die "no pidfile at $pidfile — was this cluster started by quickstart?"

  if [ -n "$CLI" ]; then
    # Retry THROTTLED for a while: right after a previous failover the
    # rejoining replica is still reseeding, min-replicas is unmet, and the
    # proxy correctly refuses writes until the pair is whole again.
    local reply=""
    for i in $(seq 1 120); do
      reply=$("$CLI" -p $PX_PORT -a "$TOKEN" --no-auth-warning SET witness "written before the kill" 2>&1 | tr -d '\r')
      [ "$reply" = "OK" ] && break
      case "$reply" in THROTTLED*) sleep 0.5 ;; *) break ;; esac
    done
    [ "$reply" = "OK" ] || die "could not write the witness key through the proxy (got '$reply') —
      fix that before killing anything. tools/quickstart.sh status"
    # Wait until the replica HAS the write. Replication is async with a bounded
    # loss window, so a write acked only by the master may legitimately die
    # with it — the first run of this script did exactly that. seq_lag 0 means
    # the replica is caught up, and from there survival is the contract.
    local lag=""
    for i in $(seq 1 40); do
      lag=$("$BINS/flintctl" -f "$INV" status 2>/dev/null \
        | awk '!done && $1=="pair" && $4=="master" {for(j=1;j<NF;j++) if($j=="seq_lag") print $(j+1); done=1}')
      [ "$lag" = "0" ] && break
      sleep 0.25
    done
    [ "$lag" = "0" ] || die "the replica never caught up (seq_lag $lag) — not killing a master
      whose replica is behind. tools/quickstart.sh status"
    echo "wrote witness key through the proxy, and the replica has it (seq_lag 0)"
  fi

  say "killing the master 127.0.0.1:$mport — SIGKILL, no goodbye"
  # Trust the pidfile only after checking it names a live flint-server: a
  # stale pidfile would make this drill "kill" a corpse and then report the
  # controller broken when nothing ever died.
  local pid
  pid=$(cat "$pidfile" 2>/dev/null || true)
  if [ -z "$pid" ] || ! ps -p "$pid" -o comm= 2>/dev/null | grep -q flint-server; then
    command -v lsof >/dev/null 2>&1 \
      && pid=$(lsof -ti "tcp:$mport" -s tcp:LISTEN 2>/dev/null | head -1)
  fi
  [ -n "$pid" ] || die "could not find the flint-server process on port $mport (stale pidfile, no lsof)"
  kill -9 "$pid"

  say "watching the controller detect, verify, promote and fence"
  local t0 promoted="" i
  t0=$(date +%s)
  for i in $(seq 1 60); do
    if "$BINS/flintctl" -f "$INV" status 2>/dev/null \
        | awk -v a="127.0.0.1:$rport" '$1=="pair" && $3==a && $4=="master" {found=1} END {exit !found}'; then
      promoted=1; break
    fi
    sleep 0.5
  done
  [ -n "$promoted" ] || die "127.0.0.1:$rport was not promoted within 30s.
      $QS/state/logs/controller.log is where the controller explains itself"
  echo "  127.0.0.1:$rport is master, ~$(( $(date +%s) - t0 ))s after the kill"

  if [ -n "$CLI" ]; then
    local got
    got=$("$CLI" -p $PX_PORT -a "$TOKEN" --no-auth-warning GET witness 2>/dev/null | tr -d '\r')
    [ "$got" = "written before the kill" ] \
      || die "witness key read back '$got' after the failover — that is a real problem, report it"
    echo "  witness key survived: the acked write is still there"
  fi

  say "restarting the killed node — it rejoins as the replica of the new master"
  "$BINS/flintctl" -f "$INV" start >/dev/null
  local rejoined=""
  for i in $(seq 1 60); do
    if "$BINS/flintctl" -f "$INV" status 2>/dev/null \
        | awk -v a="127.0.0.1:$mport" '$1=="pair" && $3==a && $4=="replica" {found=1} END {exit !found}'; then
      rejoined=1; break
    fi
    sleep 0.5
  done
  [ -n "$rejoined" ] || die "the ex-master did not come back as a replica within 30s.
      tools/quickstart.sh status, and $QS/state/logs/"

  echo
  "$BINS/flintctl" -f "$INV" status
  echo
  echo "Roles are now swapped. Run it again to fail back."
}

case "$CMD" in
  up)     up ;;
  failover) failover ;;
  status) [ -f "$INV" ] || die "nothing here yet — run tools/quickstart.sh"
          "$BINS/flintctl" -f "$INV" status ;;
  down)   [ -f "$INV" ] || die "nothing here yet — run tools/quickstart.sh"
          "$BINS/flintctl" -f "$INV" stop
          echo; echo "stopped. Data kept in $QS — 'tools/quickstart.sh' starts it again, 'purge' deletes it." ;;
  purge)  if [ -f "$INV" ]; then "$BINS/flintctl" -f "$INV" stop 2>/dev/null || true; fi
          rm -rf "$QS"
          echo "purged $QS" ;;
  *)      die "usage: tools/quickstart.sh [up|status|failover|down|purge]" ;;
esac
