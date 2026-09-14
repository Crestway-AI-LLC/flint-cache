#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
#
# BUG-0139 — a CP seat's address may only reach the rest of the program
# through cp_dial (to dial it) or cp_runner (to place it).
#
# WHY THIS EXISTS. BUG-0138 found that `inv.cp` is a BIND address handed to
# seats as a DIAL target, added cp_dial, and converted the sites a grep for
# `inv.cp[0].clone()` and `inv.cp.join` returned. That is not the population.
# `for seat in &inv.cp` and `inv.cp.iter().enumerate()` match neither, and
# behind those were status, status --json, verify, launch's two passes,
# upgrade, roll_edge and the --control-plane list every proxy is given.
# BUG-0139 converted them; nothing stopped the next one being written the old
# way, and BUG-0139's own write-up said so.
#
# It is a source assertion on purpose. The runtime version is "stand up a
# fleet with the control plane on its own machine", which needs real hosts
# and is exactly the topology every drill lacks — the reason the defect
# survived two chaos runs built to catch what loopback hides.
#
# THE EXEMPT SET IS THE POINT. Three functions may touch an element of
# inv.cp: the two resolvers, and cp_seat_args, which BINDS and so must have
# the literal. Everything else goes through a resolver. Collection-level uses
# (.len(), .is_empty(), .push()) are not element access and are unrestricted.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${FLINT_CTL_MAIN:-$ROOT/crates/flint-ctl/src/main.rs}"
fail() { echo "CP DIAL SITES DRILL FAILED: $*"; exit 1; }
[ -f "$SRC" ] || fail "no flint-ctl main.rs at $SRC"

scan() {  # scan <file>  -> prints offending "line:fn:text", exit 1 if any
  python3 - "$1" <<'PY'
import re, sys

EXEMPT = {"cp_dial", "cp_dial_all", "cp_runner", "cp_seat_args"}
# Element access, not collection access. `.len()`, `.is_empty()` and
# `.push()` say nothing about an address and are deliberately not listed.
ELEM = re.compile(r"&inv\.cp\b(?!\.(?:len|is_empty|push)\b)|inv\.cp\s*\[|inv\.cp\.iter\(\)")
FN   = re.compile(r"^(?:pub )?(?:async )?fn ([A-Za-z_][A-Za-z_0-9]*)")

src = open(sys.argv[1], encoding="utf-8").read().split("\n")

# Tests may inspect the inventory freely: they assert ON this invariant, and
# a test fixture is not a dial site. Cut at the first test module.
end = len(src)
for i, l in enumerate(src):
    if l.strip().startswith("#[cfg(test)]"):
        end = i
        break

owner, bad, examined = "<top>", [], 0
for n in range(end):
    line = src[n]
    m = FN.match(line)
    if m:
        owner = m.group(1)
    # Prose discusses `inv.cp[0]` freely and must not be flagged. Stripping
    # from `//` can only ever HIDE a hit inside a string literal, never
    # invent one; no such literal exists and one would be pathological.
    code = line.split("//", 1)[0]
    if "inv.cp" not in code:
        continue
    examined += 1
    if not ELEM.search(code):
        continue
    if owner in EXEMPT:
        continue
    bad.append((n + 1, owner, line.strip()))

# A check that examined nothing passes for the wrong reason.
if examined == 0:
    print("EXAMINED-NOTHING")
    raise SystemExit(2)

for n, o, t in bad:
    print(f"{n}\t{o}\t{t[:100]}")
print(f"# examined {examined} line(s) mentioning inv.cp, up to line {end}")
raise SystemExit(1 if bad else 0)
PY
}

OUT=$(scan "$SRC"); RC=$?
case "$RC" in
  0) ;;
  2) fail "the scan found no mention of inv.cp at all — it examined nothing,
  which passes for the wrong reason. Has main.rs moved, or the field renamed?" ;;
  1)
    echo "$OUT" | grep -v '^#'
    fail "the above read an ELEMENT of inv.cp outside cp_dial/cp_runner/cp_seat_args.
  inv.cp is what the control plane BINDS. To DIAL a seat use cp_dial(inv, i);
  to decide which machine runs it use cp_runner(inv, i). Only cp_seat_args,
  which composes the seat's own bind arguments, may take the literal." ;;
  *) fail "the scan did not run to a verdict (rc=$RC):
$OUT" ;;
esac
echo "  ok    $(echo "$OUT" | sed -n 's/^# //p')"

# POSITIVE CONTROL. The check must be able to fail, and the shape it must
# catch is the one that got through BUG-0138: an iteration, not an index.
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
awk '{print} /^fn status\(/ && !d {print "    for seat in &inv.cp { let _ = seat; }"; d=1}' \
  "$SRC" > "$T/mutant.rs"
if ! grep -q "for seat in &inv.cp { let _ = seat; }" "$T/mutant.rs"; then
  fail "the positive control could not be injected — no `fn status(` in $SRC,
  so the control proved nothing and the pass above is unverified."
fi
if scan "$T/mutant.rs" >/dev/null 2>&1; then
  fail "the positive control PASSED: a bare \`for seat in &inv.cp\` inside status()
  was not flagged, so this drill cannot catch the shape BUG-0138 missed."
fi
echo "  ok    a bare 'for seat in &inv.cp' injected into status() is caught"
echo "CP DIAL SITES DRILL PASSED"
