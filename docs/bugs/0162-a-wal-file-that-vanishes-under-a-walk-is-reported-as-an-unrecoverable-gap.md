# BUG-0162: a WAL file that vanishes under a walk is reported as an unrecoverable gap (FIXED)

Status: **FIXED 2026-09-17**, found in the 24-hour ops-agent review Jeff ordered
· Severity: **high** — no data was lost and the oracle never saw a wrong answer.
A replica killed itself three times in a week over a transient file system race,
and each time the pair ran single-copy until something restarted it.

## What the box said

Replica `:7001` on the playground, **twenty times** between 2026-08-06 and
2026-09-17 — the span of its log — the last at about 16:58Z on 2026-09-17:

    FATAL: WALGAP full sync required: IO error: No such file or directory:
    while stat a file for size: /var/lib/flint/node-7002/archive/163477.log:
    No such file or directory (archive holds 1437 segment(s), newest 0s old,
    oldest 43190s old) — this link can never resume. Marking for re-seed and
    exiting; the next start will full-sync from a checkpoint.

Four things in that line do not fit "the replica fell behind":

- **`while stat a file for size`.** That is a walk LISTING the WAL files and
  sizing each one, not a read of the segment the cursor needs.
- **`newest 0s old`.** The archive is being written continuously; the walk was
  not looking at a stale directory.
- **`oldest 43190s old`** — 12 hours, the retention TTL. The named segment was
  at the pruning edge, which is the file the pruner is about to delete.
- **The replica was not behind.** It had streamed healthily for the ~19 hours
  since the rc.73 roll; `verify` saw it, and so did a `status` minutes earlier.
  Its cursor at the failure was `255764912`, and after the restart it warm
  rejoined at `255824176` — about 59,000 sequences later, some twenty minutes
  at the soak's rate. The span it needed was retained. The walk failed on a
  neighbouring file.

So: RocksDB lists and stats every WAL file to build a walk; the archive pruner
deletes the oldest on a TTL; and when the two coincide, the walk fails on a file
that went away underneath it.

## Why that became fatal

`repl.rs` mapped **every** error from that call to `ReplError::WalGap`:

    .get_updates_since(last_applied)
    .map_err(|e| ReplError::WalGap(e.to_string()))?;

`WalGap` means *the sequences you want are not retained here*, and the replica
acts on it accordingly: mark for re-seed, log `this link can never resume`, and
exit. For a cursor the WAL genuinely cannot reach, that is right — BUG-0085's
control turns on this exact message meaning recycling, and says so: *"A MISSING
segment is the recycling this test needs. Any other error is a broken database,
and collapsing the two would let a real fault masquerade as the condition under
test."*

That blanket mapping came from **BUG-0082**, which fixed the opposite failure: a
real gap surfacing as `Storage`, which the master's admission check did not
match, so it fell through and answered OK. Making every error a `WalGap` closed
that hole and swallowed this race into the same bucket. A check whose identity
is weaker than the question, in the direction that had not bitten yet.

## The fix

The two cases are indistinguishable by message, so they are distinguished by
**asking again**. `retrying_walk` retries a walk while it reports a vanished
file — twice, 50ms apart, bounding the cost at 100ms and paying it only on the
failure itself.

This is safe in the direction that matters: **a retry can only turn a transient
failure into a success, never a real gap into a pass.** A file that vanished
under one walk is absent from the next listing; a cursor that is genuinely
unreachable stays unreachable and still ends as a gap, either here after the
retries or in the coverage checks that follow.

**All four walks, not the one that bit.** The first enumeration of call sites
was string-sorted and truncated at twelve, which dropped two of them, and four
of the remainder were mentions in comments rather than calls. Read properly,
production has four: `updates_since_budgeted`, the cursor scan, `batch_covering`
— all three mapping to `WalGap` — and `snap_to_batch_end`, which swallows the
error with `.ok()?` and quietly leaves a cursor unsnapped. All four go through
the helper.

## Verification

- **Five unit tests** driven by closures, so the retry is tested by counting
  how many times it asks: the playground's verbatim message is recognised and
  a corruption error and a real "not retained" message are not; a walk that
  works is asked once; **a file that vanishes once is asked again and
  succeeds**, which is the playground's case; a cursor that is really gone is
  tried `WAL_WALK_RETRIES + 1` times and still fails; and a corruption error is
  not retried at all.
- **The first gate run failed on this change's own doc comment.** The example
  error message was written as an indented block, and rustdoc compiles an
  indented block in a doc comment as Rust, so the doctest tried to parse
  `IO error: No such file or directory:` as a statement. There is no fenced doc
  block anywhere in these crates; the idiom is prose with inline code spans, and
  now it says why.
- **The existing gap tests are the regression that matters.** The
  deleted-oldest-segment test and `walgap_shortread` still require `WalGap`
  from a permanently missing segment. They now also prove the retries do not
  mask a real gap, because they pass through the retrying path.

## How many, and how the first count was wrong

Twenty, and the number was first reported as three. That three came from a
narrow grep piped through `tail -12`, which is the same truncated-listing
mistake that nearly scoped this fix to two of its four call sites. Counted
properly on the box:

- **20** FATALs carrying `while stat a file for size`, naming **20 distinct
  archive segments** — `055191` through `163477` — so each is its own
  occurrence and not one event logged repeatedly;
- the log spans 2026-08-06 to 2026-09-17, so this fired about **once every two
  days**, each time costing single-copy exposure until something restarted the
  replica;
- **0** since the restart at 2026-09-17T17:27Z.

**Four other FATALs in the same log are a different shape and are NOT addressed
here:** `WALGAP full sync required: sequence 90776415 is no longer in the WAL
(latest is 90776416)`. A cursor one behind the head being called unreachable is
its own question, and this change does not touch it.

## Not claimed

That every one of the twenty had the cursor evidence the last one had. Only
2026-09-17's was observed live, with `status` and `verify` minutes earlier and
the post-restart rejoin sequence to bound how far behind the replica really
was. The other nineteen are counted on the strength of the identical message
shape and a distinct segment each.

Nor that the race is now impossible — only that a walk which loses this race is
no longer a replica's death sentence. The underlying coincidence, a walk listing
~1,440 files while the pruner deletes the oldest, is unchanged. Whether the
master should be enumerating the whole archive to serve a cursor twenty minutes
old is a separate question, and a better one; it is not answered here.
