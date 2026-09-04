# ADR-0028 — A verdict must name what it examined, and the naming must be refutable

**Status:** **ACCEPTED 2026-09-04** (proposed 2026-09-01). The fourth
obligation was added on acceptance, on the evidence in the 2026-09-04
section below.

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

Four obligations follow, in the order they are worth implementing:

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

### 4. A failure names only what it observed

The first three govern what a check declares about its SUBJECT. This one governs
what it declares about a CAUSE, and it exists because six of the seven failures
in the 2026-09-04 section satisfied all three and still misled.

**Where a verdict cannot separate two causes, it says so and prints what it saw,
rather than choosing the more serious one.** Where it CAN separate them, the
separation is an assertion the message makes out loud, so that it can be wrong
in public.

Three shapes recur, and each has a fix that costs a line:

* **A product claim for a harness condition.** "no replication after bootstrap"
  when `bootstrap` itself had failed unseen. The fix is to check the step before
  asserting about its effect, and to print the step's own output when it fails.
* **A verdict from a control that never armed.** "the master shed NOTHING" reads
  as a broken gate; the client had offered no writes. A control reports whether
  it armed BEFORE it reports what it found, and an unarmed control is a failure
  of the harness, not of the product.
* **An assertion stronger than the wait that guards it.** "verify still red"
  after waiting on liveness and asserting streaming. Wait for the condition you
  are about to assert, not for a proxy that is reached earlier.

The obligation is on the MESSAGE, not on the check: a check that genuinely
cannot tell two causes apart satisfies this by saying which two, and is more
useful than one that guesses correctly most of the time.

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

**4 is already largely paid.** It was written from seven fixes that landed
before it was proposed, so the obligation describes work done rather than work
scheduled — the drills named in the table below now satisfy it. What remains is
that new failures are written to it, which is a review habit rather than a
task.

## 2026-09-04 — the same defect in the FAILURE direction, seven times in one day

The observation above is five checks that PASSED about the wrong subject. A day
of drill work produced seven that FAILED naming the wrong cause — a verdict
asserting a fault in the PRODUCT for a condition in the harness or the clock.

**Provenance, since this ADR is about that:** all seven are mine, each reproduced
locally and each with the commit that fixed it. No second-hand accounts in this
table.

| # | the verdict it printed | what was actually true | fix |
|---|---|---|---|
| 1 | "no replication after bootstrap" | `flintctl bootstrap` had failed and its output went to `/dev/null`; 23 drills, 14 of which did not even check the exit status | `b7b74a9` |
| 2 | "fleet B did not start", recorded in the gate's own exclusion list as a suspected PORT COLLISION | the inventory omitted `disposable on`, so bootstrap was REFUSED — the refusal discarded | `b7b74a9` |
| 3 | "master unchanged (7221 -> 7221)" | read one second after a failover, while the demoted master was legitimately booting as MASTER from its durable role | `bbed94c` |
| 4 | "the master shed NOTHING by the lag cause" | a positive control that never armed, because no write was offered inside the stall window | `4aa5c56` |
| 5 | "verify still red after the member came back" — **this one reddened the v0.1.0-rc.68 release gate** | waited on liveness (`nodes_live`), asserted streaming | `f9c194d` |
| 6 | "the reason is gone, not absent" | the log was one directory below where the reader globbed — inside the function written to stop cannot-look-being-reported-as-absent | `9e6af65` |
| 7 | *(nothing at all — a silent exit mid-run)* | a diagnostic under `set -euo pipefail` whose own `FLINTINFO` call failed, aborting the run it was diagnosing | `6fed4d0` |

**Not one was a product fault.** Every one was read as one until someone opened
the log.

### What this adds to the decision

The three obligations above govern what a check DECLARES ABOUT ITS SUBJECT. Six
of these seven satisfied all three and still misled, because the defect was in
the FAILURE MESSAGE: it named a cause the check had not established. #3 could
not distinguish a stale read from a stalled failover; #4 could not distinguish
an unarmed control from a silent gate; #5 could not distinguish "not yet
streaming" from "not streaming".

**This became obligation 4**, accepted 2026-09-04 and written up in the decision
section above rather than left here as an appendix.

The cost is the same objection the ADR already answers about the other three:
more words per failure, and a message that sometimes ends in "I cannot tell
which". The evidence for paying it is that a wrong attribution here cost a
release gate, an evening of drill-by-drill triage, and — twice — a hypothesis
recorded in the tree that was never true.

## 2026-09-04, later the same day — obligation 4 gets a mechanism

**OPS-0121 was found hours after this ADR was accepted**, live on both ops
boxes: the daily report explained an empty shadow lane with "the expected state
until a widowed master occurs (~1 per 36h)" while 1112 widowed masters sat in
the spool. The real reason was that 1400 of 1489 findings arrived past a
freshness ceiling.

So the obligation, written down and accepted, did not survive its own
acceptance day. That is not an argument against it — it is the evidence that
obligation 1 is the only one that stopped recurring, and the only one with a
thing that refuses (`FLINT_GATE_SUBJECT`).

**A helper is not the mechanism either.** Instance #6 in the table above is
`fleet_why_not_up` — written to stop cannot-look being reported as absent, and
it reported cannot-look as absent. A remedy that reproduces the defect rules
out "route everything through one function" as the whole answer.

What has actually caught this class, every time, is an **induced-failure
control**: break the thing on purpose and assert the message names what you
broke. #6 was proved by squatting a declared port. BUG-0090 was proved by
shrinking a 5 s write timeout to 1 ms and reproducing the failure
byte-identically. OPS-0121's drill mutates the fixture's skip reason and
requires the rendered output to follow it, because a second hardcoded sentence
would pass a weaker check.

**The mechanism is therefore a ratchet on those controls, not a rule about
messages.** `assert_induced_controls_have_not_regressed` counts drills carrying
one and fails only when the count DROPS — 42 of 132 here, 40 of 84 in the ops
repo, floors in `tools/induced-control-floor.txt`. It has no exclusion list, so
it does not incur BUG-0086's cost; it cannot redden a gate that is green today,
including another session's, because `n > floor` is a NOTE rather than a
failure; and its matcher is loose on purpose, since an inflated floor protects
more than it should while a tight one would quietly permit deletions.

**What it does not do**, stated so nobody reads more into it: it does not stop
the next such message from being written. It stops the control that would catch
one from being deleted, and it makes the population countable. The stronger
form — refusing any new explainer that ships without a control — was considered
and declined the same day: recognising an "explainer" needs a list, and a list
is the thing BUG-0086 warns about.

Filed as OPS-0122 in the ops repo, with the drill that pins all five ratchet
behaviours.
