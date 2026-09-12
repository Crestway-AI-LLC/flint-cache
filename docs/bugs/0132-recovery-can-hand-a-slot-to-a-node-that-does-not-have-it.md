# BUG-0132: slot recovery can hand ownership to a node that does not have the data (FIXED 2026-09-12 — confirmation pending)

Status: **FIXED 2026-09-12 — confirmation pending** · found 2026-09-10 ·
Severity: **the CI failures were the drill's, not the product's** — see
"2026-09-12 — the third occurrence, and the answer" at the end, which is the
section to read first. The hazard this file also documents (recovery completing
a flip on `reachable(dest)` rather than `has-the-data`) was real and was closed
by BUG-0133's durability barrier; what remained after that was a drill that
restarted a seat holding 150,000 keys and read "still loading" as "lost".

Two earlier readings in this file are superseded by that section and left in
place deliberately: "Why this is not 'a flaky drill'" was right to refuse the
hand-wave and wrong in its conclusion, and "ONLY SLOW HARDWARE SEES IT" was
circling the answer without reaching it.

## What CI shows

`slot_cutover_recovery` has failed **2 of the last 6 gate runs on `main`**, at
24.1s and 24.5s — near-identical durations, which is a timeout shape rather
than random noise. It failed on `e47bf84` (a documentation-only commit, so it
cannot have been caused by that change) and again on `bbca61f`.

The failing arm, from run 34564798603:

```
[delay 0.5] after restart: source=[] dest=[] -> completed pre-kill (recovery is a no-op)
  MISSING key000001 on dest
  MISSING key075000 on dest
  MISSING key149999 on dest
  FAIL: 3 keys lost after recovery
```

## Is this reachable outside the drill? Yes, behind one flag

`docs/roadmap.md` lists **"Slot migration (static placement; tenants capped
~50 GB)"** under *explicitly out of v0 scope*, which reads like this path is
dead. It is not — that line is about **tenant-facing placement**, and the same
roadmap describes the traffic rebalancer two hundred lines earlier as doing
*"cutover via epoch-fenced FLINTMIGRATEIN"*.

The ops agent issues the real command against live masters
(`crates/flint-agent/src/rebalance.rs`):

```rust
world::call_slow(&to_master, tls, &[b"FLINTMIGRATEIN", from_master.as_bytes(),
                 slot_s.as_bytes(), to_master.as_bytes(), ns.as_bytes()], ...)
```

— and on success commits ownership to the control plane, so proxies route from
the CP snapshot with the `-MOVED` bridge covering the gap. That is the same
sequence the drill interrupts.

**It is opt-in and off by default.** `Config::from_args` sets
`enabled = std::env::args().any(|a| a == "--traffic-rebalance")`, so an agent
started without that flag never plans a move.

So the exposure is: **something initiating a cutover, and a whole-cluster
restart while the move is in flight.** The drill's kill is exactly a redeploy.

### ANSWERED 2026-09-11 — measured on both ops boxes, not inferred

`--traffic-rebalance` is **absent on both**. The running agents carry 1022
characters of arguments and the flag is in neither, no rebalance-related flag
of any kind appears, nothing under `/etc/systemd/system/` or
`/var/lib/flint-phase1/` mentions it, and both are armed to exactly:

```
FLINT_ACT_ARGS=--tier2 AttachReplica,PromoteReplica
```

**With a positive control on the check**, because the whole claim is an
absence: the same `grep` against the same string finds `--control-plane`, so
it was capable of a hit. Without that the result would be worth nothing — the
lesson from this session's `\b` regex, applied to the check that answers this
file.

**So nothing on the current fleet initiates a slot cutover automatically.**
The rebalancer is the only automated producer and it is off.

**But an operator can still reach it by hand**: `flintctl migrate-slots` is a
shipped verb, and `FLINTMIGRATEIN` is reachable from `flint-ctl`,
`flint-controller` and the server directly. So the bug is **latent for the
automated path and live for a deliberate one** — a human migrating slots, with
a redeploy landing mid-move, hits exactly this.

That is why the severity moved to medium rather than being closed: the
trigger now requires an operator action rather than a background sweep, and
the consequence if it fires is unchanged.

## Why this is not "a flaky drill"

**The failure lands AFTER the drill's `RESOLVED` check passed.** That check is
not a formality — it loops for up to 30s requiring BOTH:

- `dest` accepts a write for the slot, and
- `source` answers `-MOVED <slot> <dest>` for a read.

So recovery did not fail to act. It completed, it moved ownership to `dest`,
and it made `source` redirect the whole slot there. Only then were three of
four sampled keys absent on `dest`.

**The data was on `source`.** The drill seeds all 150,000 keys to `SPORT`
before the cutover begins:

```
awk ... | valkey-cli -p $SPORT --pipe
```

So the bytes are on source's disk. After recovery, source `-MOVED`s the slot
to dest and dest has nothing. **Stranded, not destroyed** — and from a client's
position the difference is invisible: it follows the redirect and gets nothing
for a write that was acked.

## The question the mechanism turns on

At `delay 0.5` the drill reports `source=[] dest=[]` — **no migration records
on either node** — and the first version of this file asked what made
*recovery* conclude that dest owned the slot.

**That was the wrong question.** Reading `recover_migrations` answers it: with
no records anywhere it returns before touching either node, so recovery was not
involved. The flip was already committed by the cutover itself. The corrected
account is under "What recovery actually did at `delay 0.5`" below; the
question that remains is **why the ownership flip is durable when the
destination's copy of the data is not**.

Kept rather than deleted because the wrong question was load-bearing for a
while, and because one of its two branches survives in a different place: a
`migrating` record CAN drive recovery to hand the slot to a dest that never
took the data. That is the second hazard below, and it is not what CI hit.

**Do not tune the drill's delays.** Moving the timing moves which phase the
kill lands in, and the phase is the finding.

## The drill cannot tell "not started" from "completed", and says so out loud

Line 59:

```bash
if [ -n "$SM" ] || [ -n "$DM" ]; then PHASE="INTERRUPTED mid-move"; else PHASE="completed pre-kill (recovery is a no-op)"; fi
```

No migration records on either side is rendered as **completed pre-kill**. It
equally means **never started**. There is no third branch, and the verdict text
then asserts one of the two readings as fact.

This is the same rule as ops OPS-0037 and BUG-0131: *"could not measure" must
never be recorded as "measured nothing"*, and *absence and not-yet must not
render alike*. Here it matters twice over, because the classification is what a
reader uses to decide whether the subsequent `MISSING` lines are alarming.

**FIXED in this commit** — the line now asks the source who owns the slot
rather than inferring it from an absence, and distinguishes *completed pre-kill*
(source already `-MOVED`) from *NEVER STARTED* (source still serves it) from
*indeterminate*. That is what established the corrected account below: on the
failing CI run the source had already flipped, so the cutover completed and the
data did not follow.

## RETRACTED: "it also fails locally on different arms"

An earlier version of this file reported three local failures — `delay 0.3`
resolving BACKWARD, and the deterministic `half-done-flip` arm — and built a
design question on them about whether recovery must always roll forward.

**All of it was a missing binary.** The drill had no `cargo build` line and ran
`./target/release/flint-controller` directly; this worktree had only
`flint-server`. The recovery controller never started in any local run, and
`$FLINT_DRILL_ROOT/flint-rec.log` said so plainly:

```
tools/slot_cutover_recovery_drill.sh: line 63: ./target/release/flint-controller: No such file or directory
```

So `delay 0.3` was simply the un-recovered interrupted state — dest holds an
`importing` record and redirects to source, source still serves the key —
which is what "nothing recovered it" looks like, not a rollback. **With the
controller built, the drill passes completely here: all three timed arms and
the half-done-flip arm.** The CI failure does not reproduce on this machine.

The lesson is the one already in the ops KB from the other direction: a drill
that runs a binary it does not build reports the *product's* failure text for
an *environment* fault, and the real cause sits in a log nobody opens. Fixed
below.

## What recovery actually did at `delay 0.5`, which is sharper than the first reading

`recover_migrations` (`crates/flint-controller/src/main.rs`) opens by
collecting `importing`, `migrating` and `aborted` records from every
`--recover-nodes` target, and then:

```rust
if importing.is_empty() && migrating.is_empty() && aborted.is_empty() {
    return;
}
```

The failing CI run reports `source=[] dest=[]`. **So recovery did nothing at
all** — it returned before touching either node.

But the drill's `RESOLVED` gate requires `source` to answer
`-MOVED <slot> <dest>`, and a source only reaches that state through
`FLINTSLOTMOVED`, which is the last step of a *successful* cutover. Nothing
else wrote it, because recovery returned early.

**So the flip committed before the kill, and the destination still did not
have the keys.** That reframes the bug: it is not "recovery hands a slot to the
wrong node" but **the ownership flip is not ordered after the durability of the
data it transfers**. The kill lands after the flip is durable and before dest's
copy is.

**This is a hypothesis from reading the code and one CI log, not a
measurement.** It has not been reproduced, and nobody has yet seen a failing
run's data directories.

### A second hazard, in the same function, that this failure does NOT exercise

When a `migrating` record IS present and the dest has no `aborted` marker, the
flip is completed on one condition:

```rust
if reachable(dest) { ... FLINTSLOTMOVED ... }
```

**Reachable, not "has the data".** BUG-0025 already hardened the neighbouring
inference — its comment is explicit that *"the dest has no Importing record,
therefore it owns the slot"* was wrong because an abort writes the same absence,
and records the cost: *"an acked write on the source, absent on the dest, gone
from both within seconds with this loop logging success."* The `aborted` marker
fixes the case where a dest SAYS it gave up. A dest killed before it could
write that marker is indistinguishable from a healthy one, and this branch will
hand it the slot.

Not exercised by the CI failure above, which never reaches recovery at all.
Recorded because it is the same defect one branch over, and whoever fixes the
ordering should decide whether "reachable" is the right predicate here too.

## What the drill now does, so the next failure is answerable

The CI failure could not be diagnosed from two occurrences, and three of the
reasons were in the drill rather than the product:

- **It builds what it runs.** `cargo build` for `flint-server` and
  `flint-controller`, plus an `-x` check on each, so a missing binary fails
  saying so instead of failing in the product's words.
- **It keeps the evidence.** Every failure path used to `rm -rf` both data
  directories — destroying the one artifact that answers stranded-versus-lost,
  on exactly the runs where the question arises. Failures now print where the
  directories are and leave them.
- **It says WHERE the bytes are.** On a missing key it reports
  `source DBSIZE` and `dest DBSIZE` against the seeded count. Deliberately NOT
  a per-key `GET` on the source: past the split-ownership assertion the source
  answers `-MOVED` for every key in the slot, so that branch could never return
  a value and would be dead code reporting "lost" in all cases. `DBSIZE`
  answers through the redirect because it counts what the node HOLDS. Verified
  by forcing the path with a key that was never written: a healthy run reports
  `dest DBSIZE=150000`.
- **The phase classification has a third state.** `source=[] dest=[]` now asks
  the source who owns the slot instead of inferring it, and distinguishes
  *completed pre-kill* (source already `-MOVED`) from *NEVER STARTED* (source
  still serves) from *indeterminate*. Those were rendered identically, and the
  verdict text then asserted one of them.
- **Mode changed 100644 → 100755, and the claim that came with it was wrong.**
  This file originally reported the `100644` mode as a defect — "it cannot be
  executed directly, only via `bash`", "so it runs directly like its peers".
  **Most of its peers do not.** Counted afterwards:

  ```
  106  tools/*_drill.sh at 100644
   34  tools/*_drill.sh at 100755
  ```

  100644 is the norm here and 100755 is the exception, so there was no
  convention to restore. I sampled ONE neighbouring drill, found 100755, and
  generalised; the peer session sampled two and found a pair. Neither sample
  supported the conclusion either of us drew from it. The change is harmless
  and left in place — both modes run, since the gate invokes drills through
  `bash` — but it is not a fix and nothing should cite it as one.

  It is the same error as the `\b` regex in ops field-notes, one layer up: a
  check performed on a sample too small to fail. Two sessions made it about
  the same file within an hour.

## ONLY SLOW HARDWARE SEES IT, AND OUR DEFAULT GATE IS THE FAST BOX

Three verdicts on the same drill, the same commit range:

| where | shape | verdict | time |
|---|---|---|---|
| GitHub Actions runner | 2 vCPU | **FAIL** | 24.1s (`34548804753`) |
| GitHub Actions runner | 2 vCPU | **FAIL** | 24.5s (`34564798603`) |
| EC2 gate box, `c7i.xlarge` | 4 vCPU | PASS | 14.1s |
| this Mac | — | PASS, all three delays | — |

The failing runs take ~70% longer on a fraction of the cores. **That pattern is
evidence about the mechanism, not just about flakiness:** a defect in
`recover_migrations` — a wrong branch, a missing record — would fail the same
way on any machine, because it is a decision rather than a race. A window
between "the flip is committed" and "the transferred data is durable" is
exactly what widens on slower storage and narrows to nothing on faster.

So the hardware sensitivity is independent support for the ordering reading
above, arrived at from a different direction than the `delay 0.5` analysis.

**The operational consequence is larger than this bug.**
`packaging/aws/gate-box/run.sh` is the documented default for a core gate, and
it is the `c7i.xlarge`. It passes this while CI fails it twice. Every
timing-sensitive durability defect of this shape is invisible to the check we
actually gate on.

That is not a new lesson, which is the uncomfortable part. `docs/field-notes.md`
already carries *"The cheaper box created a condition the expensive one never
did"*: the 5-host rc.52 chaos run on `c5d.large` surfaced
`writes shed -THROTTLED (retried): 23`, a path `docs/slo.md` had recorded as
never having fired in any run. We learned that slower hardware finds different
bugs, wrote it down, and then standardised on the fast box.

**Reproducing it therefore needs the slow box, not the gate box:**

```
FLINT_GATE_TYPE=c5d.large \
FLINT_GATE_CMD='cargo build --release --workspace --features flint-server/rocks,flint-backup/rocks \
  && for i in 1 2 3 4 5 6; do echo "== round $i"; tools/slot_cutover_recovery_drill.sh; echo "rc=$?"; done' \
  packaging/aws/gate-box/run.sh
```

`c5d.large` is 2 vCPU, matching a runner's shape, and is the instance type that
produced the field-notes entry above.

*(This section from the ops session, which reached the same ordering conclusion
from the same two `[delay N]` lines independently. Two derivations recorded as
two rather than collapsed into one.)*

## 2026-09-11 — reproduction attempted on EC2 and FAILED TO REPRODUCE, 26 runs

Three controlled attempts on `c7i.large` (2 vCPU, the same core count as the
GitHub runner), driving the drill directly rather than through the gate:

| condition | runs | result |
|---|---|---|
| the drill alone | 12 | all pass |
| cores SATURATED — 4 busy loops on 2 vCPU, load average 4.71 | 6 | all pass |
| after the `conformance` stage, as the failing CI job does | 8 | all pass |
| **total** | **26** | **26 pass** |

**So two hypotheses are dead, and one of them was this file's.**

**"Only slow hardware sees it" is not supported.** It was a reasonable reading
of three data points — 2 vCPU fails twice, 4 vCPU passes, a laptop passes —
and a fourth point breaks it: a 2 vCPU box passes twelve times running. Core
count is not the discriminator.

**Nor is contention.** The obvious rescue of the hardware reading is that CI
runs the drill amid 140 others while the EC2 comparison ran it alone. Saturating
both cores to a load average of 4.71 did not produce it either.

**And `conformance` running first turned out to be a weak hypothesis once
measured rather than assumed** — the whole stage is sub-second per arm
(`PASS conformance rocks (RESP3) (0.4s)`), not the heavy prior workload the
job name suggests. Disk was unchanged either side, 23% before and after.

### What that leaves, and why the next step is to wait

The remaining differences between a passing EC2 box and the failing runner are
its **storage** and its **OS image** — neither of which is faithfully
reproducible on EC2, and guessing at them costs a box per guess with no better
prior than the three already spent.

**The drill is instrumented and CI keeps the output.** `.github/workflows/gate.yml`
uploads `/tmp/flint-gates` with 90-day retention, and the drill's stdout lands
in `drill-slot_cutover_recovery.log` inside it. So the next failure carries the
three-state phase, both `DBSIZE`s and both `FLINTMIGRATIONS` — which is the
evidence this file has been missing since it was opened.

**The kept data directories are NOT uploaded.** They sit in
`$FLINT_DRILL_ROOT/flint-rec-*`, outside the artifact path, so they die with
the runner. That is deliberate for now: the printed numbers should separate
"dest never received the data" from "dest received it and lost some", and only
if they do not is it worth paying artifact size on every failure.

**Decision (Jeff, 2026-09-11): let CI answer it.** The failure is 2-in-6, so it
should fire within a few pushes. Anyone tempted to reproduce this on EC2 first:
the table above is why not.

## 2026-09-11 — the drill was sampling phases by wall-clock, and now does not

The three arms killed after `sleep 0.3 / 0.5 / 0.7`, and the header claimed
that made the kill "land at different phases (pull / freeze / flip)". **A sleep
does not select a phase, it guesses at one.** Which phase 0.5s lands in is a
property of how fast the machine copies 150,000 rows — so one commit tested
different things on different hardware, and *the drill was itself the race*.
That fits every observation: 26 EC2 runs green, two GitHub runs red, and the
phase line reading differently on different machines with no product change
between them.

**Each arm now waits for the state it names**, with the source's outbound copy
throttled by `FLINTCONFIG migrate-rate-bytes` — a product knob, hot-reloadable
mid-copy, already used by the rebalancer for the same pacing — so the state is
reachable regardless of machine speed. An arm that never reaches its phase
**fails**, printing both records and `dest DBSIZE of KEYS`, because an arm that
did not interrupt what it claims certifies nothing (ops OPS-0037).

Five consecutive local runs now classify identically:

```
INTERRUPTED mid-move | completed pre-kill (source already -MOVED)
INTERRUPTED mid-move | completed pre-kill (source already -MOVED)
... x5
```

### The `freeze` arm was dropped, and why that is not a coverage loss

Racing for the frozen window caught the POST-FLIP state every time while
labelling itself `freeze` — an arm claiming a phase it never reached, which is
this file's own defect one level up. The window is genuinely near-zero *here*:
with no live writes there is no frozen tail to drain, so the source records
`migrating` and the flip follows within the same millisecond.

**The frozen state is already covered deterministically** by
`test_half_done_flip`, which CONSTRUCTS `source=Migrating` with the destination
holding the data via `FLINTSLOTFREEZE` rather than hoping a kill lands inside a
window that is not there. **Constructing a state beats racing for it whenever
the state can be constructed** — and the racing version was strictly worse,
because it also lied about what it had done.

### What this does and does not settle

**It removes the drill's contribution to the mystery.** From here a CI failure
names a reproducible phase instead of a wall-clock coincidence, which is the
difference between "2-in-6, unreproducible" and a state anyone can re-enter.

**It does not explain the two observed failures.** The `flip` arm now
deterministically produces exactly the state CI reported — `source=[] dest=[]`,
source already `-MOVED`, recovery a no-op — and passes, locally and on the gate
box. So either those failures were a state the old delays happened to reach and
these two do not, or there is a product defect that does not fire every time
even in this state. **This file stays OPEN on that question.**

Verified by mutation: with the `pull` phase made unobservable the drill fails
with *"never observed phase 'pull' within 60s"* and reports
`DBSIZE=150000 of 150000`, which names the reason rather than leaving it to be
inferred.

## What is NOT established

- **Whether any byte was lost.** Still unanswered, but no longer
  unanswerable: the drill now preserves both data directories on failure and
  prints both DBSIZE counts. The next occurrence settles it.
- **Whether this is new.** No bisect has been run. It failed on a
  documentation-only commit, so it predates today's changes, but how far back
  is unknown.
- **Whether the CI failure reproduces anywhere else.** It does not reproduce
  on this Mac at any of the three delays. The drill is timing-sensitive by
  construction, so that is weak evidence about the Linux runner rather than
  about the product.

## How it was found

Watching CI after landing an unrelated fix, rather than assuming the peer's
chaos fix would turn `main` green. It did turn chaos green; `main` stayed red
for a different reason, in a different job, that had already fired once and
been attributed to the wrong commit.

## 2026-09-12 — the third occurrence, and the answer

`gate` on `ded3de0`, `slot_cutover_recovery` FAIL at 17.2s, GitHub runner at
load 3.15 with three peer drills live. The phase rewrite did its job — the arm
named itself — and the diagnostic added with it printed the discriminator:

```
[killed in phase flip] after restart: source=[] dest=[] -> completed pre-kill
  MISSING key000001 on dest
  MISSING key075000 on dest
  MISSING key149999 on dest
  WHERE: source DBSIZE=0 dest DBSIZE=1 (seeded 150000 to source)
  FAIL: 3 keys lost after recovery
```

And the whole log carried **68,039 lines of `LOADING Flint is loading the
dataset in memory`**, every one of them before that verdict.

**That count is the answer.** A destination that had genuinely lost 149,999 of
150,000 keys would finish loading instantly — you cannot spend 68,039 replies
loading one key. The data was on the dest. The drill asked for it before the
seat had finished replaying its WAL, got `-LOADING` from every data command,
and reported an absence as a loss. `DBSIZE=1` is a partial count mid-load, not
a dest with one key.

**The cause is a fixed sleep, in this file's own drill, at the restart:**

```bash
$B --port $SPORT --engine rocks --data-dir "$SDIR" &
$B --port $DPORT --engine rocks --data-dir "$DDIR" &
fleet_wait_listen $SPORT $DPORT     # the socket accepts
sleep 0.8                           # ...and then we guess
```

`fleet_wait_listen` returns when the port accepts a connection, which happens
long before a seat with 150k keys is READY. `tools/lib/fleet.sh` already has
the right waiter — `fleet_wait_ready`, whose header says it exists for "a node
that refuses data commands with -LOADING" and which polls the seat's own
`loading:1` field with a 120-second loud deadline. This drill never called it.

**The uncomfortable part.** On 2026-09-11 this same drill was rewritten,
recorded in the section above, to stop sampling *kill* phases by wall-clock and
wait for the state it names. That rewrite fixed one side of the file's timing
dependence and left the other side untouched: the kill became observed, the
restart stayed a guess, three lines away. Same defect, same file, and it fired
the next day.

**Why only GitHub.** A runner needs more than 0.8s to replay 150k keys; the
gate box does that work several times faster and was ready before the drill
looked. Three CI failures, 26 EC2 reproduction attempts that could not fail —
and the reason was never the product's behaviour on slow hardware, it was the
*drill's* dependence on fast hardware. Jeff's framing (*correctness should be
orthogonal to underlying hardware*) turns out to have applied to the test.

**The fix.** `fleet_wait_ready` on both seats at all three restart sites, and
the fixed sleeps deleted. Verified against a stub seat rather than argued:

| the seat reports | wanted | got |
|---|---|---|
| `loading:1` | keeps waiting | still waiting at 4s (the old code proceeded at 0.8s) |
| `loading:0` | proceeds | returned rc=0 immediately |

**Confirmation is pending on a GitHub runner, and cannot come from the box.**
The box has never reproduced this and still cannot: it is the machine that is
fast enough to hide it. A green gate-box run proves only that the drill still
passes where it always passed. The next CI run on this drill is the test.
