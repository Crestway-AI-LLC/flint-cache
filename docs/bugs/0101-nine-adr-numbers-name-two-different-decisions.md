# BUG-0101 — nine ADR numbers name two different decisions, and one citation lands on the wrong one

Status: OPEN — filed as a measured finding 2026-09-05; **the remedy is a
numbering decision and not mine to take** · Severity: low-to-medium, and the
severity is carried almost entirely by the one live case below.
Area: `docs/adr/` in both repositories.

## The invariant, stated by this repo's own README

> *They are numbered in one sequence on purpose. A decision does not become a
> different decision because of which repository it lands in, and renumbering
> per repository would make the two halves impossible to discuss together.*

That is the rule. It is currently broken nine times.

## Measured

| number | this repository | the managed-plane repository |
|---|---|---|
| 0016 | bloom-filter-type | agent-learning |
| 0018 | cp-held-leases | earning-unattended-action |
| 0019 | rewind-rejoin-promotion-fences | a-site-operations-journal-in-git |
| 0023 | slot-aligned-bulk-eviction | s3-accelerator-look-aside-library |
| 0024 | boot-decision-counters-that-outlive-the-process | distributing-secrets-the-fleet-consumes |
| 0025 | stream-collection-reads-instead-of-materialising-them | verify-the-recommendation-not-the-execution |
| 0026 | admission-control-on-write-stall | a-second-protocol-for-the-object-cache |
| 0027 | shared-stripe-locks-for-pure-writes | arming-is-a-declaration-not-a-hand-edit |
| 0028 | a-verdict-must-name-what-it-examined | the-shipping-path-is-unexercised-until-you-ship |

20 ADRs here, 28 there, nine numbers in both.

## One of them is not latent

Eight of the nine are ambiguous but currently harmless: each repository's code
cites its own. **0023 is not.**

`crates/flint-storage` cites `ADR-0023 D7` and `D7.1` **sixteen times** —
"reclaim must run above the shed", "D7.1 pair-agreement", "capacity-eviction
state". Those refer to the managed-plane ADR-0023, whose D7 is *"Flint must
gain an evictable namespace class. This is the blocking core requirement."*

This repository's ADR-0023 is `slot-aligned-bulk-eviction`. **It has no D7 —
it has no D-numbered decisions at all.** So a reader here follows the citation,
opens the ADR-0023 they have, and finds a document that is *also about
eviction*, is plausibly the right one, and does not contain the thing cited.

That is worse than a dangling reference. The README already answers dangling
ones, deliberately and well:

> *Where an ADR in that range decides something visible from here, the code
> comment at the call site states the decision itself, so nothing you need in
> order to read this repository depends on a document you cannot see.*

A reference that resolves to the WRONG document defeats that answer, because
the reader does not know to fall back on the call-site comment — they think
they found the source.

## What this is not

Not a dangling-citation complaint. Citing ADRs that live in the private
repository is stated policy and a good one. The defect is two documents sharing
one number under a rule that says numbers are global.

## The remedy is a decision, so it is recorded rather than taken

Three shapes, and each costs something different:

1. **Renumber one side.** Honours the stated invariant. Costs every citation of
   the renumbered ADRs — 53 for 0018 here, 60 there — and breaks links in
   commit messages and bug files that cannot be rewritten.
2. **Qualify the citations** (`managed-plane ADR-0023 D7`). Cheap, fixes the
   live case in sixteen places, and leaves the numbering rule broken while
   making it survivable.
3. **Retire the shared-sequence rule** and prefix per repository. Honest about
   what is actually happening, and makes every existing citation ambiguous
   until it is qualified.

Only option 2 addresses the live harm without a decision about the rule, and
even it is a convention change. **Not taken here.**

## A check is possible and is also not obviously right

The managed-plane gate has both trees checked out, so a collision check belongs
there. It would fail immediately on nine pre-existing cases and therefore need
a grandfather list — which is the exclusion-list shape BUG-0086 argues against.
Worth building AFTER a remedy, so it starts from zero and stays there, not
before.

## Found by

Auditing ADR statuses against how often each is cited in code, while looking
for proposals whose decisions had shipped. `ADR-0023` stood out because its
citation count (16) did not match a document that says "Nothing here is built"
— and the reason was that the citations were not about that document at all.
