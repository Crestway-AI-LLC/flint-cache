# BUG-0101 — nine ADR numbers name two different decisions, and one citation lands on the wrong one

Status: **CLOSED 2026-09-05 — the decision already existed and had not been
carried through.** The measurement below stands; the framing "the remedy is a
numbering decision and not mine to take" did not, because the decision was
taken on 2026-08-27 in the managed plane's ADR-0030 and this file did not know
it. Severity: low-to-medium, carried almost entirely by the one live case.
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

## Closed 2026-09-05 — re-litigating a settled question, and why that happened

**The remedy was decided nine days before this was filed.** The managed
plane's ADR-0030 (*"ADR numbers collide across the two repos"*) proposed a
range split, and its own revision of 2026-08-27 accepted a PREFIX instead:
`OPS-ADR-<n>`. Its reasoning is the reasoning this file arrives at
independently — the bug series already solved the identical problem with a
prefix, a prefix is self-describing where a range is a lookup, and a prefix
survives a third repository.

That third repository is not hypothetical, which this file did not know:
`ADR-0050`, `ADR-0051` and `ADR-0056` are cited from the managed plane and
belong to **`flint-kv`**. So the count is three sequences, not two.

**How a settled question came to be re-opened.** ADR-0030's Status line said
`proposed` for nine days after the acceptance was written into its own foot.
A reader checks the status line. That is the same defect this tree spent the
week fixing in the bug index — a state claimed in two places and maintained in
one — one artefact over, and it cost a full re-derivation of an existing
decision.

## What was actually done, 2026-09-05

- **ADR-0030's Status line** now records the acceptance.
- **This repository's `docs/adr/README.md`** stated the opposite rule — one
  shared sequence, on purpose — and now states the accepted one, with the
  nine-number table so a bare citation can be recognised.
- **The sixteen live citations are qualified**: `flint-storage` and
  `flint-server` now cite `OPS-ADR-0023 D7`, so the one reference that
  resolved to the wrong local document no longer does.

**The other eight collisions are left alone, and so are the 68 files here that
cite a managed-plane ADR by bare number.** ADR-0030 says adoption is
incremental — corrected as files are touched, renaming nothing — because a
sweep would invalidate commit messages, field notes and bug files that cannot
be rewritten. Eight of the nine are latent anyway: each repository's code
cites its own.

## The check this file proposed, and why it is still not built

The suggestion was a collision check in the managed-plane gate, deferred until
after a remedy so it could start from zero. **It still cannot start from
zero**, and now for a better-understood reason: adoption is deliberately
incremental, so 68 legitimate bare citations exist here by design and would
need exactly the grandfather list BUG-0086 argues against.

What *is* checkable without an exclusion list is narrower and worth stating
for whoever tries: a bare `ADR-<n>` whose number has no local file is
necessarily cross-repo. That is a real property — but with incremental
adoption it describes 68 files that are fine, so it is a lint to run when
touching a file, not a gate. Recorded rather than built.
