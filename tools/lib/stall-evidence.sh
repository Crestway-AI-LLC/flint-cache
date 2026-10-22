# SPDX-License-Identifier: Elastic-2.0
# What a stalled drill's fleet was doing, taken BEFORE the per-drill cap kills
# it (BUG-0270).
#
# WHY THIS EXISTS. On 2026-10-20 a coproc_vec_drill fleet stopped answering
# with every process busy: the proxy at 224% CPU, the server at 78%, the
# co-processor at 115%, three sockets in CLOSE_WAIT, and not even PING
# answered. Nothing was kept. `sample` printed nothing, lldb could not attach,
# and the drill's EXIT trap deleted its logs the moment it was stopped. One
# sighting with no evidence tells no more than none, so the cap now takes the
# evidence first: whatever the next stall is, it arrives with the process tree,
# per-thread state, stacks where a tool can attach, sockets, and log tails.
#
# BEST EFFORT, AND IT SAYS WHICH. A stack tool needs ptrace, which the gate
# box's Yama setting or macOS may refuse; each section prints what it could
# not take rather than nothing, so an absent stack never reads as an empty one.
# Bounded output: this lands in the drill's log, which the gate keeps.

# Every process descended from <pid>, one per line. A drill starts its seats
# with `&`, so they are its descendants. Portable: ps on both platforms.
_stall_descendants() {   # _stall_descendants <pid>
  ps -axo pid=,ppid= 2>/dev/null | awk -v root="$1" '
    { kids[$2] = kids[$2] " " $1 }
    END {
      n = split(root, q, " "); head = 1
      while (head <= n) {
        p = q[head++]
        m = split(kids[p], k, " ")
        for (i = 1; i <= m; i++) { print k[i]; q[++n] = k[i] }
      }
    }'
}

# Threads, then a stack where a tool can attach.
_stall_stacks() {   # _stall_stacks <pid>
  local p="$1" f
  # Per thread: CPU, state and (Linux) the kernel wait channel. No ptrace.
  # By platform, not by trying: macOS `ps -L` lists ps's keywords instead.
  if [ -d "/proc/$p/task" ]; then
    ps -L -o tid=,pcpu=,stat=,wchan:24=,comm= -p "$p" 2>/dev/null | head -48 | sed 's/^/     /'
  else
    ps -M -p "$p" 2>/dev/null | head -48 | sed 's/^/     /'
  fi
  f=$(mktemp "${TMPDIR:-/tmp}/flint-stall-stack.XXXXXX") || return 0
  if command -v eu-stack >/dev/null 2>&1 && eu-stack -p "$p" >"$f" 2>&1 && [ -s "$f" ]; then
    echo "     stacks (eu-stack):"
  elif command -v gdb >/dev/null 2>&1 \
       && gdb -p "$p" -batch -nx -ex 'thread apply all bt' >"$f" 2>&1 \
       && grep -q '^#' "$f"; then
    echo "     stacks (gdb):"
  elif command -v sample >/dev/null 2>&1 && sample "$p" 1 -file "$f" >/dev/null 2>&1 && [ -s "$f" ]; then
    echo "     stacks (sample, 1 s):"
  else
    echo "     no stack tool could attach (eu-stack, gdb, sample): thread states above only"
    rm -f "$f"; return 0
  fi
  # sample's list of loaded images is long and says nothing about the stall.
  awk '/^Binary Images:/ { exit } { print }' "$f" | head -160 | sed 's/^/       /'
  rm -f "$f"
}

_stall_sockets() {   # _stall_sockets <pid>
  local p="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -a -p "$p" -i 2>/dev/null | head -40 | sed 's/^/     /'
  elif command -v ss >/dev/null 2>&1; then
    ss -tanp 2>/dev/null | grep "pid=$p," | head -40 | sed 's/^/     /'
  else
    echo "     neither lsof nor ss: sockets not listed"
  fi
}

stall_evidence() {   # stall_evidence <drill-name> <drill-pid>   (prints; never fails)
  local d="$1" dpid="$2" root pids p args scopes scope f n=0 logs=0
  root="${FLINT_DRILL_ROOT:-/tmp}"
  echo "== stall evidence for $d, taken before the cap's kill (BUG-0270)"
  pids=$(_stall_descendants "$dpid" | tr '\n' ' ')
  echo "-- the drill's process tree (pid ppid stat elapsed %cpu args):"
  # One comma-separated list: the form both platforms' ps accept.
  ps -o pid=,ppid=,stat=,etime=,pcpu=,args= -p "$(echo "$dpid" $pids | tr ' ' ',')" 2>/dev/null \
    | cut -c1-200 | sed 's/^/     /'
  for p in $pids; do
    args=$(ps -o args= -p "$p" 2>/dev/null) || continue
    # The shells are the drill's own plumbing; the daemons are what stalls.
    case "${args%% *}" in
      bash|*/bash|sh|*/sh|zsh|*/zsh|sleep|*/sleep|tail|*/tail) continue ;;
    esac
    n=$((n + 1)); [ "$n" -le 8 ] || { echo "-- (more processes; first 8 shown)"; break; }
    echo "-- pid $p: $(echo "$args" | cut -c1-160)"
    echo "   threads:"
    _stall_stacks "$p"
    echo "   sockets:"
    _stall_sockets "$p"
  done
  # The fleet's logs, by the scope its fleet_init declares: the drill's EXIT
  # trap removes them once the cap stops it, so this is their last reading.
  scopes=$(grep -h '^fleet_init' "tools/${d}_drill.sh" 2>/dev/null | awk '{print $2}')
  for scope in $scopes; do
    scope=${scope//\$\{FLINT_DRILL_ROOT\}/$root}
    scope=${scope//\$FLINT_DRILL_ROOT/$root}
    [ -n "$scope" ] || continue
    for f in "$scope"*.log "$scope"/*.log; do
      [ -f "$f" ] || continue
      logs=$((logs + 1)); [ "$logs" -le 12 ] || break 2
      echo "-- last 40 lines of $f"
      tail -40 "$f" 2>/dev/null | cut -c1-240 | sed 's/^/     /'
    done
  done
  [ "$logs" -gt 0 ] || echo "-- no fleet logs found under its fleet_init scope(s): ${scopes:-none declared}"
  echo "== end of stall evidence for $d"
  return 0
}
