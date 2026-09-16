#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
#
# BUG-0139/BUG-0141 — an inventory line that a seat BINDS may only reach the
# rest of the program through the helper that resolves it.
#
# WHY THIS EXISTS. `cp 0.0.0.0:7500` and `proxy 0.0.0.0:7379` are what those
# seats bind. They name no machine, so handing one onward — to another host
# as a dial target, or to a reader as the address that was tested — is wrong
# in a way nothing observes on a single box, because there a wildcard dial
# reaches the local seat and the report is about the right process anyway.
#
# THE CLASS HAS RECURRED THREE TIMES, which is why this is a gate and not a
# code review note:
#
#   BUG-0110  the proxy line was DIALLED verbatim; proxy_dial was added, and
#             the first 7-host run had already paid for it.
#   BUG-0138  the cp line was handed to every seat as --journal/--lease-cp.
#             cp_dial was added and the conversion was scoped by a grep for
#             `inv.cp[0].clone()`, which is not the population — BUG-0139
#             found thirteen more, most of them `for seat in &inv.cp`.
#   BUG-0141  BUG-0139 fixed the `status` proxy row, which reported the bind
#             line beside an up/DOWN decided against proxy_dial, and did not
#             look for the same shape in `verify` or `status --json`. Both
#             had it. The JSON one is worse: a consumer cannot see it.
#
# Each fix was correct and each was scoped by reading rather than by
# enumerating. This enumerates.
#
# It is a source assertion on purpose. The runtime version is "stand up a
# fleet with the control plane and a proxy each on their own machine", which
# needs real hosts and is exactly the topology the drills lack — the reason
# BUG-0138 survived two chaos runs built to catch what loopback hides.
#
# THE EXEMPT SETS ARE THE POINT. Only functions whose whole job is deriving
# something from the line may touch an element of it. Collection-level uses
# (.len(), .is_empty(), .push()) are not element access and are free.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${FLINT_CTL_MAIN:-$ROOT/crates/flint-ctl/src/main.rs}"
fail() { echo "BIND/DIAL SITES DRILL FAILED: $*"; exit 1; }
[ -f "$SRC" ] || fail "no flint-ctl main.rs at $SRC"

scan() {  # scan <file> -> "line<TAB>fn<TAB>text" per offence; rc 1 if any, 2 if it saw nothing
  python3 - "$1" <<'PY'
import re, sys

FIELDS = {
    "cp":      {"cp_dial", "cp_dial_all", "cp_runner", "cp_seat_args"},
    "proxies": {"proxy_dial", "proxy_runner", "proxy_args",
                "proxy_seat_name", "proxy_port",
                # The one message whose whole subject is the difference
                # between the two forms: "proxy <bind> (dialled at <dial>)".
                "proxy_down_help"},
    # BUG-0140, the third field. Unlike cp and proxies this one has no dial
    # helper and deliberately never will: a wildcard `coproc` address is
    # REFUSED at parse time, because no generator writes the key and the only
    # writer resolves the host itself. So the six exempt readers are, each for
    # its own reason: parse_inventory, because reading the raw literal IS the
    # refusal; coproc_args, which BINDS; families_arg, which dials an address
    # the refusal has already guaranteed names a machine; coproc_runner, which
    # resolves placement from it the way cp_runner and proxy_runner do; and
    # coproc_seat_name and coproc_family, which exist so that launch never
    # destructures the element itself -- the rule cp and proxies already
    # follow. A SEVENTH reader is the signal that the refusal is no longer
    # enough and the key is wanted after all.
    "coprocs": {"coproc_args", "families_arg", "coproc_runner",
                "parse_inventory", "coproc_seat_name", "coproc_family"},
}
FN = re.compile(r"^(?:pub )?(?:async )?fn ([A-Za-z_][A-Za-z_0-9]*)")

def elem(field):
    return re.compile(
        rf"&inv\.{field}\b(?!\.(?:len|is_empty|push)\b)"
        rf"|inv\.{field}\s*\["
        rf"|inv\.{field}\.iter\(\)"
    )

src = open(sys.argv[1], encoding="utf-8").read().split("\n")

# Tests may inspect the inventory freely: they assert ON this invariant, and
# a fixture is not a dial site. Cut at the first test module.
end = len(src)
for i, l in enumerate(src):
    if l.strip().startswith("#[cfg(test)]"):
        end = i
        break

pats = {f: elem(f) for f in FIELDS}
owner, bad, examined = "<top>", [], 0
for n in range(end):
    line = src[n]
    m = FN.match(line)
    if m:
        owner = m.group(1)
    # Prose discusses `inv.cp[0]` freely. Stripping from `//` can only hide a
    # hit inside a string literal, never invent one; no such literal exists.
    code = line.split("//", 1)[0]
    for field, exempt in FIELDS.items():
        if f"inv.{field}" not in code:
            continue
        examined += 1
        if not pats[field].search(code):
            continue
        if owner in exempt:
            continue
        bad.append((n + 1, field, owner, code.strip()[:90]))

if examined == 0:
    print("EXAMINED-NOTHING")
    raise SystemExit(2)

for n, f, o, t in bad:
    print(f"{n}\tinv.{f}\t{o}\t{t}")
print(f"# examined {examined} line(s) over {len(FIELDS)} field(s), up to line {end}")
raise SystemExit(1 if bad else 0)
PY
}

OUT=$(scan "$SRC"); RC=$?
case "$RC" in
  0) ;;
  2) fail "the scan found no mention of inv.cp or inv.proxies at all — it examined
  nothing, which passes for the wrong reason. Has main.rs moved, or a field been
  renamed?" ;;
  1)
    echo "$OUT" | grep -v '^#'
    fail "the above read an ELEMENT of a BIND line outside the helpers that resolve it.
  inv.cp, inv.proxies and inv.coprocs are what those seats bind, and a bind
  address names no machine. To DIAL a cp or a proxy use cp_dial/proxy_dial; to
  decide which machine runs it use cp_runner/proxy_runner; to REPORT it beside a
  probe result, name what was probed, which is the dial form. Only the seat's
  own bind arguments may take the literal.
  inv.coprocs has NO dial helper by design (BUG-0140): a wildcard coproc address
  is refused at parse time instead, so the literal is safe to dial and what this
  rule protects is the RULE -- a new reader means the refusal is no longer
  enough and a coproc-host key is wanted after all. Say so in the exempt set
  above, with the reason, rather than adding the reader quietly." ;;
  *) fail "the scan did not run to a verdict (rc=$RC):
$OUT" ;;
esac
echo "  ok    $(echo "$OUT" | sed -n 's/^# //p')"

# POSITIVE CONTROL, one per field. The shape that got through BUG-0138 is an
# ITERATION, not an index, so that is what is injected.
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
control() {  # control <fn-signature-prefix> <injected line> <label>
  awk -v inj="$2" '{print} $0 ~ "^fn " sig "\\(" && !d {print "    " inj; d=1}' \
    sig="$1" "$SRC" > "$T/mutant.rs"
  grep -qF "$2" "$T/mutant.rs" || fail "the positive control for $3 could not be
  injected — no \`fn $1(\` in $SRC, so it proved nothing and the pass is unverified."
  if scan "$T/mutant.rs" >/dev/null 2>&1; then
    fail "the positive control for $3 PASSED: an injected \`$2\` was not flagged,
  so this drill cannot catch the shape it exists for."
  fi
  echo "  ok    an injected '$2' is caught"
}
control status "for seat in &inv.cp { let _ = seat; }" "inv.cp"
control status "for p in &inv.proxies { let _ = p; }" "inv.proxies"
# BUG-0140's field gets its own, because a rule with no control is a rule that
# has not been shown to fail -- and this one was added last, when the exempt
# set was being tuned and an over-broad entry would have gone unnoticed.
control status "for c in &inv.coprocs { let _ = c; }" "inv.coprocs"
echo "BIND/DIAL SITES DRILL PASSED"
