# BUG-0065: a freshly promoted master self-fenced when the OTHER pair promoted

Status: OPEN — one occurrence, cause unknown, and the next one is now
diagnosable, which was the blocking gap · Severity: high if real — a healthy
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
