# ADR-0028 — A verdict must name what it examined, and the naming must be refutable

**Status:** proposed, 2026-09-01.

## The observation

On 2026-09-01 five checks passed, across two sessions and two repos, and every
pass was about something nobody had asked about:

| # | check | passed about |
|---|---|---|
| 1 | the exec-bit drill | a stale copy of the binary, not the built one |
| 2 | a mutation control | a file the mutation missed by two spaces of indentation |
| 3 | the write-path guard audit | a property none of the three bugs had |
| 4 | the release gate | `/Volumes/FlintDev/wt/flint`, a worktree days stale, while `main` sat clean |
| 5 | a repo-identity guard | `flint-cache-ops`, because it CONTAINS `flint-cache` |

Provenance, since this ADR is about exactly that: (3) and (4) are mine and I
have the logs. (1), (2) and (5) were reported to me by the session that hit
them, and I have not reproduced them — I am relying on their account, which
they volunteered including the parts against their own interest. Recorded so
the count is readable as three-verified-plus-two-reported rather than five
established facts.

Different mechanisms — a stale artifact, a no-op edit, a wrong premise, a path
default, a substring. One shared property, and it is not that the checks were
weak:

**Every one of them answered "did it pass?" and none answered "on what?"**

The subject was either unstated, or stated as a log line that no verdict
depended on. Where it was printed, a human caught it — the gate printed the
tree, the drill printed the binary path, the guard printed `$want` and someone
saw the trailing `o`. **Zero of the five were caught by the check's own
result.** The detectors that worked were all incidental.

(5) is the one that makes this structural rather than a bad day: it is the
fix for (4), and it shipped with (4)'s defect in a new costume. A class that
reproduces itself inside its own remedy is not a run of carelessness.

## The decision

**A check must state what it examined in a form something else can refute, and
the verdict must depend on that statement.**

Three obligations follow, in the order they are worth implementing:

### 1. The gate declares its subject, and refuses a mismatch

`gates.sh` knows its tree. It must emit provenance as DATA — exact origin URL,
branch, SHA, dirty flag — and accept an EXPECTED identity from its caller,
failing before the first stage when the two disagree.

`run.sh` and `release.sh` then pass what they meant. This converts (4) from
visible to impossible: today `run.sh` resolves a tree and prints it; under this
it resolves a tree, DECLARES it, and the gate refuses when resolution and
declaration disagree. It is also the unimplemented half of OPS-0063's own
heading, "NAME THE TREE, AND REFUSE THE CONTRADICTION" — the naming half
shipped and the refusing half did not, and the naming half is what saved (4).

### 2. A mutation declares the bytes it changed

Not the string it predicted changing. `cmp -s` against the pre-mutation copy,
fail if identical — which catches every no-op mutation including the ones the
author did not anticipate. (2) asserted the specific text and so tested only
the failure its author already had in mind.

### 3. A matcher declares its match count and its match kind

**Measured 2026-09-02, so this stops being an abstraction.** Across 127 drills
there are **57 NEGATIVE-assertion sites** — places where the ABSENCE of a match
means PASS.

The distinction matters more than the count, and getting it wrong is the same
error this ADR is about. `grep -q X || fail` is safe: a matcher that finds
nothing FAILS, which is the correct direction. The hazard is the inverse,
`grep -q X && fail`, where a pattern that has drifted — a changed message, a
moved path, a renamed literal — finds nothing and passes **without having
looked**. (My first measurement counted the safe form. Corrected before it was
written down.)

**AMENDED after reading them: most are already paired, and 57 quoted as a
defect count would have libelled work that was done correctly.**
`token_hash_drill` follows every plaintext-absence check with a digest
PRESENCE check on the same file:

```sh
grep -q "super-secret-token" "$D/cp" && FAIL   # the plaintext must be absent
grep -q "$DIGEST"            "$D/cp" || FAIL   # the digest must be present
```

The second is what makes the first mean anything: it proves the file exists, is
readable and is greppable before the silence of the first is trusted. It also
catches literal drift, because `DIGEST` derives from the same literal — change
the token at the creation site alone and the digest check fails.

So 57 is the POPULATION, not the defect count. The useful number is how many
lack such a pair, and that cannot be grepped; it has to be read.

The first drill audited had exactly one, and it was the security assertion:

```sh
grep -q "legacy-plaintext-tok" "$D/cp-old" && FAIL   # nothing proved cp-old was read
```

certifying "the plaintext did not survive the rewrite" equally well against an
empty or truncated file. Now paired with the legacy tenant's digest, and
verified by mutation rather than by inspection: emptying `cp-old` makes it fail,
where before it passed.

**The fix is not to rewrite 57 sites.** It is to pair each negative assertion
with a positive control proving the matcher CAN match — the same shape
`unit_exec_bit_drill` already uses when it refuses if fewer than four units
resolve, and `roll_exec_bit_drill` when it requires a clean rev to still roll.
For the token case that means asserting the literal appears where it SHOULD
before asserting it is absent where it should not.

Sequenced after obligation 1 and deliberately not started during a release
window: these are gate drills, and changing them moves `main` under whoever is
cutting (OPS-0044).



Zero matches must fail. An implausible count must fail. Name comparisons are
exact, never substring — (5) is a substring, and so is every future one.

**This is prior art, not a new idea, and that is the argument for it.** The
discipline is already implemented, independently, under different phrasings.
Verified in this repo:

- `unit_exec_bit_drill.sh` refuses if fewer than four units resolve, *"because
  a matcher that finds nothing agrees with everything"*
- `agent_deploy_drill.sh` guards both mutation sites — `grep -q ... && bad
  "control setup failed: mentions remain"`
- `roll_lease_drill.sh` asserts its own control fired: *"the transcript is
  non-empty and both stubs recorded (positive control)"*

Reported from `flint-kv-ops` and not verified here: `rules_drill.sh` requires
that the file differ, that the mutation not be comment-only, and that the
mutant still compile — a non-compiling mutant producing no failures being its
own version of this bug.

Three independent reinventions under three spellings, in one repo, is the
evidence. People keep arriving at this rule locally, one drill at a time,
because nothing requires it globally — and a rule that must be rediscovered is
one the fifth drill's author will miss. This ADR proposes no new technique. It
promotes an existing local habit into a property the gate requires.

*(The count in this section was four when first drafted, on a colleague's
report. Checking it, one of the four — `meter_report_drill.sh` — turned out to
carry `sed -i` portability notes rather than a mutation guard, and a fifth I
grepped for matched nothing because my pattern guessed at phrasings I had not
read. An ADR about naming what you examined does not get to publish a count it
did not check.)*

## The worked example: BUG-0083

Filed the same night, and it is this ADR in miniature at the level of a single
function. `proxystats_field` (`crates/flint-ctl/src/main.rs:900`) reads a
proxy's build with one attempt and collapses every failure — connect refused,
TLS handshake, read timeout, `-NOAUTH`, and a proxy genuinely reporting no
build — into a single `None`. `roll_edge` treats `None` as fatal, after every
seat has already rolled.

When it fired on the public gate, the 12-line failure tail carried NO timeout,
NO connection error and NO `-NOAUTH`. The log could not name the cause because
the code discarded the class before anything could log it.

That is the property this ADR asks for, failing in the smallest possible unit:
**the reading stated a verdict in a form nothing could refute.** Asked which of
two fixes the evidence supported, the honest answer was that the evidence could
not support either — on that run or any future one. So the first fix is not
retry and not ordering; it is making the verdict carry its subject.

A useful corollary comes with it. Given a timing-window mechanism, a PASS is a
run in which the window did not open: greens bound the rate and cannot refute
the defect, while one red demonstrates it. Nine greens and one red reads at
first glance as "flaky, mostly fine", and that reading is exactly backwards.

## What this is not

**Not a demand for more assertions.** Every one of the five had assertions and
they all passed. Adding a sixth check of the same shape adds a sixth thing that
can be green about the wrong subject.

**Not provenance as logging.** Printing the subject is what we already do, and
it is why these were caught at all — but a log line is a detector that fires
only when a human reads line 1 of a 4,000-line log and knows which value is
correct. The decision is that the verdict DEPENDS on the declaration.

## Cost, and the honest objection

The objection is that a declaration can be wrong too — the caller can declare
the same wrong tree the resolver resolved. True, and it is a real limit: this
does not make a check correct, it makes a check's SUBJECT falsifiable by a
second party. That is worth having anyway, because in all five cases the two
parties existed and disagreed, and nothing compared them. (4)'s resolver said
`/Volumes/FlintDev/wt/flint` while its caller meant `main`; (1)'s drill said
one path while the build wrote another.

Second objection, and it constrains the design rather than merely costing
something: a fixture may legitimately use non-production identity.
`release_preflight_drill` overrides with `FLINT_OPS_GH=o/o` and
`FLINT_PUB_GH=p/p` precisely so its sandbox is not the production pair. So an
expected-identity mechanism MUST be overridable, or every drill with a fixture
becomes untestable.

**The override must be explicit, never a fallback.** A mechanism that quietly
accepts an unexpected identity when no expectation is configured has recreated
the hole it was built to close — that is exactly the shape of (4), where an
unstated default resolved to whatever was nearest. Absent expectation must
refuse, not proceed.

The related temptation, once such a guard refuses a fixture, is to weaken the
guard until the fixture passes — which converts the guard into a test of the
test. Name the sandbox origins to match the configured values instead.

## Sequencing

Obligation 1 first: one file, one caller pair, and it closes the case that
reached a release script. 2 and 3 are cheap and can follow. None of this blocks
rc.67.
