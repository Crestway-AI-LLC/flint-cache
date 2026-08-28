# BUG-0065: a freshly promoted master self-fenced against its own pair's stale lease

*Filed as "…when the OTHER pair promoted". That title was hypothesis (b), which
the second occurrence eliminated; renamed 2026-08-28. The original wording is
kept here rather than erased, because it records that the cross-pair framing
came from a single sample where pair 1 happened to promote 79ms earlier —
coincidence read as mechanism.*

Status: FIXED in code, UNCONFIRMED against a live recurrence — cause narrowed
to (a) by the second occurrence on 2026-08-28 and the path that makes (a)
possible is now closed;
the instrument added on the first firing worked and the (a)/(b) question is
settled. What is unfixed is the cache-update path itself · Severity: high if real — a healthy
new master going read-only mid-roll is a availability fault on the exact path
a roll exercises · Found 2026-08-27 triaging the one red ops-CI run since
08-22 (run `33112175958`, `canary`, ops @ `7902cb2`, core @ `60cb7e4`).

## What the artifact shows

The canary drill's `--nodes-only` re-run, masters-last phase
(`drill-canary.log`, artifact `fleet-gate-logs`):

    pair 0: 127.0.0.1:6921 demoted + drained; 127.0.0.1:6920 promoted at (0,5)
    ...
    pair 1: 127.0.0.1:6922 demoted + drained; 127.0.0.1:6923 promoted at (0,6)
    started node-6922 (pid 9493)
    == UPGRADE ABORTED after pair 1 master roll: unexpected SelfFenced
       (expected only Detected for g1, at_ms >= ...763134; this event is at_ms ...763213)
    {"actor":"node:127.0.0.1:6920","kind":"SelfFenced","subject":"127.0.0.1:6920",
     "cause":"lease superseded: promotion on record at the CP"}

**Pair 0's freshly promoted master fenced itself 79ms after pair 1's
promotion was fenced at the CP.** The runner was quiet (`load 1.22`), and the
same tree family passed 57/0 on the gate box in the same hour — so this is a
race with a narrow window, not load and not a deterministic regression.

## What the code says, and why the verdict is stuck

The node side is a bystander: `CPLEASE` returned `-SUPERSEDED <addr>` and the
node did what a fence must. The question is why the CP said it.

`CPLEASE` answers from the in-memory `shared.leases.entries`, keyed by "any
entry whose member-vector CONTAINS the caller". `CPFENCE` updates the durable
state and that cache, keyed by member-vector EQUALITY. Read tonight; the
scoping looks correct, which is exactly why one occurrence cannot pick between:

- **(a) a stale own-pair record** — pair 0's entry still naming `6921` when
  `6920` renewed, some path having missed the cache update. The CP would have
  answered `SUPERSEDED 127.0.0.1:6921`.
- **(b) cross-pair aliasing** — `6920`'s renewal matching an entry updated by
  pair 1's fence, e.g. via duplicate entries under member-vectors that differ
  in ordering (equality-keyed writes + containment-keyed reads is a shape that
  tolerates duplicates silently). The CP would have answered
  `SUPERSEDED 127.0.0.1:6923`.

The successor address decides in one line. **And the journal row did not carry
it.** The node prints `lease superseded by {successor}` to stderr — per-seat,
dead with the box — and journaled a fixed string. The gate prints the journal.

## Fixed here: the instrument, not the bug

`superseded_cause()` — the journal row now reads
`lease superseded by <addr>: promotion on record at the CP`. The upgrade
journal gate matches on the KIND, so nothing changes in what aborts; what
changes is that the abort names the one fact that discriminates (a) from (b).
Unit-tested, and mutation-tested: reverting to the fixed string fails
`the_fence_row_names_who_superseded_it`.

Nothing parses the old cause text (grepped both repos), so the change is safe.

## Not established

- Which of (a)/(b) it is. One sample, no successor in the record — that is the
  gap this fixes forward, same shape as BUG-0014's instrument.
- The rate. One firing in the ops CI history since 08-22; the gate box has
  never produced it.
- Whether the drill's expectation is even wrong — if a legitimate mechanism
  lets a promotion supersede a sibling pair's lease, the product is what needs
  the fix, not the gate.

## Where to start on the next firing

1. Read the successor in the SelfFenced row. `6921` → chase the cache-update
   path on pair 0's own fence. `6923` → chase duplicate lease entries; dump
   `shared.leases.entries` and count rows containing `6920`.
2. `CPJOURNALREAD` around the abort for the fence ordering.
3. Do not re-run to make it green; pull the artifact (`gh run download <id>
   -n fleet-gate-logs`). BUG-0014 already paid for this lesson.


## Second occurrence, 2026-08-28 — the instrument paid, and it is (a)

ops CI run `33148455433`, `canary`, ops @ `82ddfcc`, core @ `cd1e3c8`. Artifact
pulled rather than re-run-to-green, per the note below. The row this file was
written to obtain:

```json
{"at_ms":1787899168878,"actor":"node:127.0.0.1:6920","kind":"SelfFenced",
 "subject":"127.0.0.1:6920",
 "cause":"lease superseded by 127.0.0.1:6921: promotion on record at the CP"}
```

**Successor is `127.0.0.1:6921`** — pair 0's own other member, and the very node
the roll had just demoted:

```
pair 0: 127.0.0.1:6921 demoted + drained; 127.0.0.1:6920 promoted at (0,5)
```

By the decision rule this file already set out, `6921` means **(a): a stale
own-pair record**. The CP told a freshly promoted master it was superseded by
the member it had just replaced.

A second, independent line agrees, and it is stronger than the first because it
does not depend on reading the successor correctly: **pair 1 never fenced at
all in this run.** Zero `pair 1: ... demoted` lines in the whole drill — the
abort came after pair 0's roll and before pair 1's. Hypothesis (b) needs
`6920`'s renewal to match an entry updated by pair 1's fence, and there was no
pair 1 fence to update one. (b) had no prerequisite in this run.

So the answer to *"why did the CP say it"* is now scoped to one path: pair 0's
own fence updating the durable state without the `shared.leases.entries` cache
following, so a containment-keyed read still finds the pre-promotion member.
The equality-keyed-write / containment-keyed-read asymmetry this file flagged
is the place to look; the duplicate-entry theory is not.

### Not caused by the core commit it landed on

Worth stating because the CI error line prints the core commit and reads like
an attribution. `cd1e3c8` is ADR-0025's reply streaming. It is not implicated:
`canary` passed on the gate box (91s) and locally on that same code (168,244
acked writes, 0 missing), the fenced path never touches reply encoding, and
this failure has a documented prior occurrence on a different core commit
(`60cb7e4`). The first-pass reasoning "15 consecutive green CI runs, so this is
new" was wrong for a familiar reason: the 15-run window did not reach back to
the first firing, so it was a search space too small to contain the event.

### One process note against this file's own instruction

Step 3 above says do not re-run. I did start one re-run, as a
deterministic-vs-intermittent signal, **after** pulling the artifact — and the
verdict above rests on the artifact and the prior report, not on the re-run's
outcome. Recording it because a rule worth writing down is worth noting when
you bend it.

### Rate

Two firings, 2026-08-27 and 2026-08-28, both in ops CI, both `canary`, both on
node `6920`. Never once on the gate box or locally. That the SAME node fences
in both is itself a datum — it is the pair whose master rolls FIRST.


## Fixed — one key, and the duplicate that let two keys disagree

The fence commits BEFORE the promotion and `controlled_failover` fails hard if
`CPFENCE` does not commit (`flint-ctl/src/main.rs:5011`), so the record for
`6920` existed. The renewal still read `6921`. That leaves one way for a
committed fence to be invisible to a renewal: **the two sides were not looking
at the same row.**

- `CPLEASE` found "the first row whose members CONTAIN the caller".
- `CPFENCE` updated "the row whose member vector EQUALS this pair".

Those agree only while there is exactly one row per pair — and `CPADDPAIR`
deduped with `!st.pairs.contains(&pair)`, vector equality, so `CPADDPAIR a,b`
and `CPADDPAIR b,a` registered as **two** pairs to the dedupe and **one** pair
to every containment check. Two rows, and the fence writes one while the
renewal reads the other.

Two changes:

1. **One key.** `lease_row_index(rows, addr)` is now the only way a lease row
   is resolved, used by the renewal read and by both of the fence's writes
   (durable and mirror). They cannot land on different rows whatever the table
   holds; a duplicate becomes merely stale instead of contradictory.
2. **The root.** `CPADDPAIR` sorts the member vector before the dedupe, so
   `a,b` and `b,a` are one pair and the existing `contains` check does what it
   already read as doing.

Five tests. The behavioural ones are weak by construction — the duplicate test
drives both sides through the same function, so it cannot fail while that stays
true, which is the point and also why it proves little alone. The one that
holds this shut is structural: `no_site_resolves_a_lease_row_by_member_vector_equality`
scans the production half of the source and fails if any site resolves a lease
row by member-vector comparison. Mutation-confirmed — reverting the fence to
`m == &members` fails it, and the unmutated tree passes. (It also caught itself
on the first run: scanning the WHOLE file matched the test's own pattern
literals, so it now scans only up to `#[cfg(test)]`.)

**Why UNCONFIRMED.** The artifact never contained the CP's `leases.entries`,
so duplicate rows were never directly observed — step 1's follow-up ("dump
shared.leases.entries and count rows containing 6920") was not runnable after
the fact. What is established is that the fence committed, the renewal
disagreed, and this asymmetry is the only remaining path between those two
facts. If canary aborts on `SelfFenced` again with this in place, the mechanism
is something else and this file should reopen rather than be trusted.
