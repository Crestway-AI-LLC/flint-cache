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
    tools/failover_drill.sh        kill a master, watch the controller promote
    tools/gates.sh                 the whole release gate: checks, conformance,
                                   20 drills, chaos
    $bins_rel/flintctl -f $inv_rel verify
                                   reconcile every view of the cluster

    tools/quickstart.sh down       stop it, keep the data
    tools/quickstart.sh purge      stop it and delete the data

This cluster is DISPOSABLE on purpose — see $inv_rel.
For a deployment you intend to keep, docs/self-hosting.md.
EOF
}

case "$CMD" in
  up)     up ;;
  status) [ -f "$INV" ] || die "nothing here yet — run tools/quickstart.sh"
          "$BINS/flintctl" -f "$INV" status ;;
  down)   [ -f "$INV" ] || die "nothing here yet — run tools/quickstart.sh"
          "$BINS/flintctl" -f "$INV" stop
          echo; echo "stopped. Data kept in $QS — 'tools/quickstart.sh' starts it again, 'purge' deletes it." ;;
  purge)  if [ -f "$INV" ]; then "$BINS/flintctl" -f "$INV" stop 2>/dev/null || true; fi
          rm -rf "$QS"
          echo "purged $QS" ;;
  *)      die "usage: tools/quickstart.sh [up|status|down|purge]" ;;
esac
