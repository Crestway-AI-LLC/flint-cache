#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
#
# A seat's name and its state directory have ONE spelling each.
#
# WHY THIS EXISTS, and it is not hypothetical. `cp_seat_name`'s own comment
# records the cost of two spellings: `roll_edge` used the literal "cp"
# against a three-seat fleet, `stop_seat` looked for `cp.pid`, found nothing,
# reported the process already gone, and then failed `wait_port_free` because
# the real `cp-n1` was alive and holding the port. The abort read "port still
# bound after the process was gone", which is exactly what it looks like when
# you kill the wrong thing (docs/bugs/0004, docs/bugs/0012).
#
# The response was a helper per seat kind — and only for the kind that had
# already gone wrong. As of 2026-09-14 the count was: `proxy-{port}` spelled
# in four places, two of them the spawn in `launch` and the stop in
# `roll_edge` (BUG-0141); `node-{port}` in nine and its data dir in eight
# more; and the CP's state dir recomputed inside `backup_args` with its own
# copy of the seat-count if/else. All of them agreed. That is what this kind
# of bug looks like right up until it does not.
#
# So: every seat name and state dir is built by exactly one function, and
# this refuses any other spelling. `coproc_seat` needs no rule — it has no
# fixed prefix to spell by hand and was single-sourced from the start.
#
# WHAT IT DOES NOT COVER, deliberately. The rules anchor on the opening quote
# (`"node-{`), so a name embedded in prose — `eprintln!("  node-{port} already
# up")` — is not matched. Those were converted to the helper by hand in the
# same change, but they are not enforced here, because a message that drifts
# is confusing and a pidfile that drifts is a stop aimed at the wrong process.
# Broadening the rule to catch prose would also make it match its own error
# text, which is the shape of a check that reports itself.
#
# A source assertion on purpose. The runtime version is "roll a fleet and see
# whether the stop finds the thing the start made", which is what the gate
# already does, and which passed for every one of the duplications above,
# because agreeing spellings agree until someone edits one.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${FLINT_CTL_MAIN:-$ROOT/crates/flint-ctl/src/main.rs}"
fail() { echo "SEAT NAMES DRILL FAILED: $*"; exit 1; }
[ -f "$SRC" ] || fail "no flint-ctl main.rs at $SRC"

scan() {  # scan <file> -> offences on stdout; rc 1 if any, 2 if it saw nothing
  python3 - "$1" <<'PY'
import re, sys

# pattern -> (functions allowed to spell it, what to use instead)
RULES = [
    (r'"node-\{',  {"node_seat_name"},  "node_seat_name(port)"),
    (r'/node-\{',  {"node_data_dir"},   "node_data_dir(statedir, port)"),
    (r'"cp-n\{',   {"cp_seat_name"},    "cp_seat_name(inv, i)"),
    # `/cp-state`, not `cp-state`: the CLI FLAG is `--cp-state`, and a flag
    # name is not a path. The first version of this rule flagged
    # `"--cp-state".into()` in backup_args, which is correct code.
    # cp_state_single / cp_state_raft OWN the two literals; cp_seat_state picks
    # between them by seat count, and `launch` needs both independently of the
    # count (BUG-0145), which is why the pair exists rather than one function.
    (r'/cp-state',  {"cp_seat_state", "cp_state_single", "cp_state_raft"},  "cp_seat_state(inv, i)"),
    (r'"proxy-\{', {"proxy_seat_name"}, "proxy_seat_name(inv, i)"),
]
FN = re.compile(r"^(?:pub )?(?:async )?fn ([A-Za-z_][A-Za-z_0-9]*)")

src = open(sys.argv[1], encoding="utf-8").read().split("\n")
end = len(src)
for i, l in enumerate(src):
    if l.strip().startswith("#[cfg(test)]"):
        end = i
        break

pats = [(re.compile(p), own, fix) for p, own, fix in RULES]
owner, bad, examined = "<top>", [], 0
for n in range(end):
    line = src[n]
    m = FN.match(line)
    if m:
        owner = m.group(1)
    # Prose names these strings constantly — the incident reports are in the
    # comments. Stripping from `//` can only hide a hit, never invent one.
    code = line.split("//", 1)[0]
    for pat, own, fix in pats:
        if not pat.search(code):
            continue
        examined += 1
        if owner in own:
            continue
        bad.append((n + 1, owner, fix, code.strip()[:70]))

if examined == 0:
    print("EXAMINED-NOTHING")
    raise SystemExit(2)
for n, o, fix, t in bad:
    print(f"{n}\t{o}\tuse {fix}\t{t}")
print(f"# examined {examined} spelling(s) of a seat name or state dir, to line {end}")
raise SystemExit(1 if bad else 0)
PY
}

OUT=$(scan "$SRC"); RC=$?
case "$RC" in
  0) ;;
  2) fail "the scan matched no seat-name spelling at all, not even inside the
  helpers that own them. It examined nothing, which passes for the wrong
  reason — has main.rs moved, or a helper been renamed?" ;;
  1)
    echo "$OUT" | grep -v '^#'
    fail "the above spell a seat name or state directory by hand.
  One spelling each, and the helper named on each line owns it. Two spellings
  agree until someone edits one, and then a stop looks for a pidfile the start
  never wrote — docs/bugs/0004, docs/bugs/0012." ;;
  *) fail "the scan did not run to a verdict (rc=$RC):
$OUT" ;;
esac
echo "  ok    $(echo "$OUT" | sed -n 's/^# //p')"

# POSITIVE CONTROLS, one per rule. Each is the shape that was actually found
# in the tree on 2026-09-14, injected into a function that must not have it.
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
control() {  # control <injected line> <label>
  awk -v inj="$1" '{print} /^fn status\(/ && !d {print "    " inj; d=1}' "$SRC" > "$T/m.rs"
  grep -qF "$1" "$T/m.rs" || fail "positive control for $2 could not be injected
  (no \`fn status(\` in $SRC), so the pass above is unverified."
  if scan "$T/m.rs" >/dev/null 2>&1; then
    fail "positive control for $2 PASSED: an injected \`$1\` was not caught."
  fi
  echo "  ok    an injected $2 spelling is caught"
}
control 'let _ = format!("node-{}", 1u16);'        "node name"
control 'let _ = format!("{}/node-{}", "d", 1u16);' "node data dir"
control 'let _ = format!("cp-n{}", 1);'            "cp name"
control 'let _ = format!("{}/cp-state", "d");'     "cp state dir"
control 'let _ = format!("proxy-{}", 1u16);'       "proxy name"
echo "SEAT NAMES DRILL PASSED"
