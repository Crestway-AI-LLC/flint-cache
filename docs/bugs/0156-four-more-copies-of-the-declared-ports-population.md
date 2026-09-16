# BUG-0156: three gate checks carry their own copy of the declared-ports population, and BUG-0154 converted one of five consumers (FIXED 2026-09-15)

Status: **FIXED 2026-09-15**, found 2026-09-15 · Severity: **low** — every gap
below is latent; none of them is hiding a collision today, and that was measured
rather than hoped. It is filed because `tools/lib/drill-ports.sh` exists
specifically so this cannot happen, and says so in its first paragraph.

## The contract

> Two things need this answer and must never disagree: the gate's
> `assert_no_duplicate_drill_ports`, which refuses a collision, and
> `tools/next-free-ports.sh`, which suggests where to put a new drill. **A
> helper that suggested a port the gate then rejected would be worse than no
> helper**, so they read the same function rather than each carrying a copy.

Five things need that answer. Three carried a copy:
`assert_no_port_overlap`, `assert_no_cross_repo_ports` and
`assert_no_default_ports`, each doing `grep -h '^fleet_init' tools/*_drill.sh`.
`assert_no_scope_overlap` carried a fourth copy for the other half of ownership.

## Measured, not inferred

538 declared ports through the library; **535** through the blind scan. The
three missing are **6388, 6389 and 6390**, claimed by the conformance stage
inside `gates.sh` — which is exactly the case the library was extended for, and
its comment records why:

> conformance sat on 6397-6399, inside edge_reroute's 6398-6401, and no check
> could say so because nothing scanned the file the claim was in.

The scan differs in two more ways, both of which the library's comments record
as having cost something: it anchors at column 0 where the library allows
leading whitespace, and it does not join line continuations, after wrapped
argument lists silently dropped every port past the break.

## What each gap actually let through

Injected into a copy of the tree and run against both implementations:

| injected | before | after |
|---|---|---|
| indented `fleet_init … 7001` | **passes** | refused |
| a drill taking conformance port 6389 | **passes** | refused |
| indented duplicate port between two drills | **passes** | refused |
| indented duplicate scope dir | **passes** | refused |
| ops-repo port behind a continuation | **passes** | refused |
| continuation-wrapped duplicate scope | refused | refused |
| ops-repo port, plainly declared | refused | refused |

The last two are controls, and the first of them **corrected a claim in the
first draft of this file**: I had written that the scope check was blind to
continuations, and it is not — a scope is always field 2 of the FIRST physical
line, so only the ports hide behind the break. Its real gap is indentation
alone.

**No live collision.** The two repos share no port under either scan, and ops
declares none of 6388-6390.

## The honest half

`assert_no_cross_drill_kill_patterns` was converted to the library this
afternoon under [BUG-0154](0154-next-free-ports-suggests-ports-the-other-collision-check-rejects.md),
and that fix was scoped by the failure in front of me: one consumer of five. It
is the diagnosis BUG-0139 wrote down — *a population taken from a convenient
syntax rather than from the authority* — applied to my own fix, on the same
afternoon, in the same file. Writing the lesson down does not stop it; only
enumerating the consumers does.

## Fixed

One preprocessing step, in the library, that every `fleet_init` question now
goes through:

- **`_fleet_init_lines_in <file>`** — joins continuations, then anchors on
  `^[[:space:]]*fleet_init`. Both details are load-bearing and each was learned
  from a real miss; the comment says which.
- **`drill_fleet_init_lines [dir]`** — the same shape `grep -n` over the glob
  would print, so a check that wants to report WHERE can use it directly.
  `assert_no_default_ports` does.
- **`drill_declared_scopes [dir]`** — the other half of ownership, for
  `assert_no_scope_overlap`. **Drills only, and that difference is deliberate**:
  `gates.sh` declares `fleet_init "$CDIR"`, a variable with no literal to
  compare against a drill's literal, and the check that must resolve it
  (`assert_declared_scopes_cover_data_dirs`) already reads the lines itself.
- **`drill_declared_ports`** re-expressed on top, and **its output is
  byte-identical** before and after on both repos — 538 rows here, 160 in ops —
  so nothing that already read it changed.

Every converted check also got `_have_drill_files || return 0` and the library
refusal, because `gates_drill.sh` forges a tree holding only `tools/gates.sh`.
That is not hypothetical: the BUG-0154 fix shipped without it and the gates
drill caught it on the box.

## Deliberately NOT done

**`assert_no_port_overlap` is now provably redundant.**
`assert_no_duplicate_drill_ports` asks the same question on the same map and
names the drills rather than the files. It is converted rather than deleted:
removing a gate assertion is a call for whoever owns the suite, and two checks
that read one function can no longer disagree, which was the defect. Worth
deciding, not worth me deciding.
