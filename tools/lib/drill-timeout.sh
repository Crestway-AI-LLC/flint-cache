# SPDX-License-Identifier: Elastic-2.0
# One wall-clock cap for one drill — ONE implementation, two callers.
#
# WHY THIS EXISTS. On 2026-09-12 tenant_quota's isolation arm asserted, and
# the assertion skipped the statement that stops its load thread. The thread
# was not a daemon, so CPython's shutdown joined it forever: the drill printed
# a traceback and then hung. gates.sh replays the parallel batch only after
# xargs returns, so 129 finished drills reported NOTHING for 58 minutes and
# the job died at GitHub's 60-minute cap — recorded as "cancelled", a word
# that reads as somebody's decision rather than a defect. BUG-0134.
#
# The drill's own bug is fixed and assert_tools_threads_are_daemons now
# refuses a non-daemon thread anywhere in tools/. Neither of those can see a
# drill wedged in a syscall, a fleet that never comes up, or a `wait` on a
# process that will not exit. This is the layer that does not need to know
# why: after the cap the drill is killed, the batch reports, and the run says
# which drill stopped making progress.
#
# THE CAP IS NOT A TUNING KNOB. The slowest core drill is ns_escape at
# 383-387s, measured across four consecutive green CI runs on a 4-vCPU runner
# at 4-way parallelism. 900s is 2.3x that. A drill that legitimately needs
# longer should raise FLINT_DRILL_TIMEOUT_S deliberately, rather than have the
# cap quietly sized up to whatever the slowest drill has become.
: "${FLINT_DRILL_TIMEOUT_S:=900}"

# NO `timeout`, ON PURPOSE. GNU coreutils has it, this Mac has neither it nor
# gtimeout, and the gate runs on both. A cap that exists on only one of the
# two platforms is the rc.15 bug shape — wait_port_free bound an address macOS
# permits and Linux refuses, so every local drill passed while the first real
# roll failed. The poll below is bash 3.2, which is what /bin/bash is here.
capped_drill() {   # capped_drill <drill-name>   (stdout/stderr are the caller's)
  local d="$1" tmo="$FLINT_DRILL_TIMEOUT_S" rc=0 dpid wpid mark i

  # A unique name, then removed: its EXISTENCE is the watchdog's one bit of
  # signal back to us. Exit status cannot carry it -- the drill's own rc is
  # what we must return when the cap did not fire.
  mark=$(mktemp "${TMPDIR:-/tmp}/flint-drill-cap.XXXXXX") || return 125
  rm -f "$mark"

  bash "tools/${d}_drill.sh" &
  dpid=$!

  (
    i=0
    while [ "$i" -lt "$tmo" ]; do
      kill -0 "$dpid" 2>/dev/null || exit 0
      sleep 1
      i=$((i + 1))
    done
    kill -0 "$dpid" 2>/dev/null || exit 0
    : > "$mark"
    # step_report greps ^FAIL first, so this is the line the summary shows.
    printf 'FAIL: killed at the %ss per-drill cap -- it stopped making progress.\n' "$tmo"
    printf '      Everything it managed to print is above. Raise the cap with\n'
    printf '      FLINT_DRILL_TIMEOUT_S if this drill is legitimately slower;\n'
    printf '      seats it left behind are named by the leak check below.\n'
    kill -TERM "$dpid" 2>/dev/null
    # Its EXIT trap gets a chance to tear the fleet down before SIGKILL.
    i=0
    while [ "$i" -lt 10 ] && kill -0 "$dpid" 2>/dev/null; do
      sleep 1
      i=$((i + 1))
    done
    kill -KILL "$dpid" 2>/dev/null
  ) &
  wpid=$!

  # `wait 2>/dev/null`: the shell announces a job it had to SIGTERM
  # ("Terminated: 15 bash tools/x_drill.sh"), attributed to a line in THIS
  # file, which reads in the drill log like the cap itself broke.
  wait "$dpid" 2>/dev/null || rc=$?
  kill "$wpid" 2>/dev/null
  wait "$wpid" 2>/dev/null || true

  if [ -f "$mark" ]; then
    rm -f "$mark"
    return 124        # the conventional timeout status
  fi
  return "$rc"
}
