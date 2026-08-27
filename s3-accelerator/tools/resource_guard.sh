#!/usr/bin/env bash
# Resource floors for the test path. SOURCE this, do not execute it.
#
# WHY THIS EXISTS. The gate bounded TIME -- SUITE_TIMEOUT, run_bounded -- and
# nothing else. Every other way a run can go wrong was unbounded, and all three
# of these have actually happened here:
#
#   - THREADS. BUG-0057: a client per FileSystem read, +4 threads each, until
#     the JVM could not create another Netty event loop group. We crashed
#     ourselves, in our own product, and the first sign was a stack trace.
#   - MEMORY. The TPC-DS generator materialised 24 tables in an in-memory
#     DuckDB and reached 25.6 GB of 30 GB before it was stopped. The fix moved
#     it to /tmp, which on Amazon Linux 2023 is a 16 GB tmpfs, so it was still
#     in RAM -- the same failure through a different number.
#   - DISK. A cargo target/ reached 68 GB on a laptop whose root volume has
#     ~53 GiB free. A build does not fail politely when the disk fills; it takes
#     the editor, the browser and anything else writing a file down with it.
#
# The point is not to make failures impossible. It is to make them arrive as a
# refusal BEFORE the run, naming the resource and the number, instead of as an
# OOM kill, a corrupt target dir, or an unrelated suite failing three steps
# later. A limit that reports the wrong cause is barely better than no limit.

GUARD_MIN_DISK_GB=${GUARD_MIN_DISK_GB:-12}
GUARD_MIN_MEM_GB=${GUARD_MIN_MEM_GB:-2}
GUARD_JVM_MAX_HEAP=${GUARD_JVM_MAX_HEAP:-2g}
GUARD_MIN_FDS=${GUARD_MIN_FDS:-4096}
# Set 0 where the run starts no JVM, so the summary does not claim a heap cap
# that is not in force. A status line that reports something untrue is worse
# than a missing one: it is read, believed, and never checked.
GUARD_REPORT_HEAP=${GUARD_REPORT_HEAP:-1}

guard_free_disk_gb() { # path
  df -Pk "${1:-.}" 2>/dev/null | awk 'NR==2 {printf "%d", $4/1048576}'
}

guard_free_mem_gb() {
  if [ -r /proc/meminfo ]; then
    awk '/^MemAvailable:/ {printf "%d", $2/1048576; found=1}
         END {if (!found) print -1}' /proc/meminfo
  elif command -v vm_stat >/dev/null 2>&1; then
    # macOS. Free alone understates what is available, because inactive and
    # purgeable pages are reclaimable; counting only free would refuse to start
    # on a perfectly healthy machine, and a guard that cries wolf gets disabled.
    vm_stat | awk '
      /page size of/ { for (i=1;i<=NF;i++) if ($i ~ /^[0-9]+$/) ps=$i }
      /Pages free/ { gsub(/\./,""); free=$3 }
      /Pages inactive/ { gsub(/\./,""); inact=$3 }
      /Pages purgeable/ { gsub(/\./,""); purge=$3 }
      END { if (!ps) ps=4096; printf "%d", (free+inact+purge)*ps/1073741824 }'
  else
    echo -1
  fi
}

# Refuse early, name the resource, and say what to do about it.
#
# TAKES EVERY PATH THE RUN WRITES TO, and judges the tightest. Checking one path
# -- or worse, `.` -- is a guard pointed away from the hazard: build output and
# scratch are commonly on different volumes here (the cargo target/ dirs in this
# repo are symlinks to an external SSD), so a single check can refuse on a full
# source disk nothing touches while the volume actually being filled runs out.
# flint/tools/gates.sh learned this first; the reasoning is its, and this is the
# same rule applied to a gate that had no disk check at all.
guard_check() { # label, path [path...]
  local label="${1:-run}"; shift
  [ $# -gt 0 ] || set -- .
  local bad=0 mem fds worst="" worst_gb=""

  for path in "$@"; do
    [ -e "$path" ] || continue
    local d; d=$(guard_free_disk_gb "$path")
    [ -n "$d" ] || continue
    if [ -z "$worst_gb" ] || [ "$d" -lt "$worst_gb" ] 2>/dev/null; then
      worst_gb=$d; worst=$path
    fi
  done
  local disk="${worst_gb:-}"
  mem=$(guard_free_mem_gb)
  fds=$(ulimit -n 2>/dev/null || echo 0)
  [ "$fds" = "unlimited" ] && fds=$GUARD_MIN_FDS

  if [ -n "$disk" ] && [ "$disk" -lt "$GUARD_MIN_DISK_GB" ] 2>/dev/null; then
    echo "REFUSING $label: ${disk} GB free on the volume holding $worst, floor is ${GUARD_MIN_DISK_GB} GB." >&2
    df -h "$worst" 2>/dev/null | sed 's/^/  /' >&2
    echo "  A build that fills the disk does not fail politely. Clear target/ dirs" >&2
    echo "  (cargo clean, mvn clean) or raise GUARD_MIN_DISK_GB if you mean it." >&2
    bad=1
  fi
  # MEMORY WARNS, DISK REFUSES, and the difference is deliberate. Memory is
  # reclaimable -- inactive and compressed pages come back under pressure, and
  # every "available memory" number on macOS disagrees with every other one
  # (this machine reports 4 GB by free+inactive+purgeable and 55% by
  # memory_pressure, both defensible). Refusing on a figure that soft would stop
  # healthy runs, and a guard that stops healthy runs gets commented out, which
  # costs more than it ever saved. Disk does not reclaim itself, and a full
  # volume takes down whatever else is writing a file.
  if [ "$mem" -ge 0 ] 2>/dev/null && [ "$mem" -lt "$GUARD_MIN_MEM_GB" ] 2>/dev/null; then
    echo "WARNING: only ${mem} GB memory available (floor ${GUARD_MIN_MEM_GB} GB)." >&2
    echo "  Suites run real JVMs and a real tier. Under pressure they produce" >&2
    echo "  timeouts that read as product bugs -- if this run fails oddly, look" >&2
    echo "  here first. Continuing, because memory is reclaimable." >&2
  fi
  if [ "$fds" -lt "$GUARD_MIN_FDS" ] 2>/dev/null; then
    echo "NOTE: ulimit -n is $fds, below the ${GUARD_MIN_FDS} these suites expect." >&2
    echo "  Raising it for this shell only." >&2
    ulimit -n "$GUARD_MIN_FDS" 2>/dev/null \
      || echo "  Could not raise it; connection-heavy suites may fail spuriously." >&2
  fi
  [ "$bad" = 0 ] || return 3
  local heapnote=""
  [ "$GUARD_REPORT_HEAP" = 1 ] && heapnote=", JVM heap capped at $GUARD_JVM_MAX_HEAP"
  echo "   limits ok: ${disk} GB disk (tightest of $#: $worst), ${mem} GB memory, $(ulimit -n) fds${heapnote}"
}

# What every JVM the gate starts should carry. A suite that leaks now dies as
# an OutOfMemoryError naming itself, in seconds, instead of pushing the machine
# into swap and taking everything else with it.
guard_java_opts() {
  echo "-Xmx${GUARD_JVM_MAX_HEAP} -XX:+ExitOnOutOfMemoryError"
}

# Disk is the one that does not recover on its own: a run that leaves the volume
# near full makes the NEXT run fail for reasons that have nothing to do with it.
guard_report_after() { # label, path, before-gb
  local label="$1" path="$2" before="$3"
  local after; after=$(guard_free_disk_gb "$path")
  local used=$((before - after))
  if [ "$used" -ge 5 ] 2>/dev/null; then
    echo "   NOTE: $label consumed ${used} GB of disk (${before} -> ${after} GB free)"
  fi
}
