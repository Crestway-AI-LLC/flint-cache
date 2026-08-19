# BUG-0022: RocksDB tickers read zero in production because statistics are never enabled, and zero is indistinguishable from absent (PARTLY RETRACTED; the real half FIXED)

Status: **the central claim is RETRACTED; the narrower real defect is FIXED
2026-08-18.** Found the same day while preparing BUG-0013's confirming
measurement · Severity as filed: medium-high · Severity as it turned out:
**low-medium**, because the two counters this was filed about were live all
along

> **READ THIS FIRST.** The claim below — that the counters behind
> `write_stall()` are not running, and that BUG-0013's falsification criterion
> can therefore never fail to acquit — **is wrong**. `rocksdb.is-write-stopped`
> and `rocksdb.actual-delayed-write-rate` are DB **properties**, not statistics
> **tickers**, and properties do not need `enable_statistics()`. BUG-0013 is
> **not blocked**. See "Measured, and mostly falsified" below. The retraction
> is kept in full because the reasoning error is the instructive part.

## Symptom

BUG-0013 offers a falsification criterion:

> If `write_stopped` is zero the hypothesis above is wrong.

Read literally against a production build, that criterion can never fail to
acquit — because the counters behind it are not running.

## Root cause

`opts.enable_statistics()` appears exactly once in `crates/flint-storage/src/rocks.rs`,
inside `#[test] fn bloom_filter_catches_in_range_misses()`, on that test's own
local `Options` value. Zero occurrences before the `#[cfg(test)]` boundary.

    origin/main   #[cfg(test)] 388   enable_statistics 462
    c1371d9       #[cfg(test)] 420   enable_statistics 494

(The two differ by ~32 lines because BUG-0017's fix inserted above the test
module. Same finding in both trees; quote a line number with its tree.)

So it is **test-local, not merely test-gated** — there is no shared helper to
lift out of `cfg(test)`. `open_with_retention` enabling statistics is a new
decision, and the production open path has never had them.

## The actual defect

Not "statistics are off" — that is a defensible performance choice, and they
carry real overhead. The defect is that **off and zero are the same bytes to
the caller.**

`RocksKv::write_stall()` returns `property_int_value(...).unwrap_or(0)`. A
disabled counter and a genuinely idle one are one value. Every consumer —
`FLINTINFO`'s `write_stopped` and `delayed_write_rate`, the exporter, any
future diagnosis — reads a confident zero and cannot know it measured nothing.

This is the day's recurring failure mode in library form: a checker that cannot
answer, returning output indistinguishable from an answer, in the direction the
investigator expects. Five instances were hit by hand on 2026-08-18 (a zsh
word-split reported as a merge CONFLICT; a Prometheus 5m lookback reported as a
failed dedupe; a drill root on a mounted volume reported as a product failure;
a missing binary reported as 8/8 reproduction; a wrong grep pattern nearly
reported as a missing dependency). This one is the same shape, but compiled in
and permanent.

## Scope beyond BUG-0013

Anything ticker-backed inherits it — bloom `useful`/`full-positive`, compaction
counts, read amplification, stall micros. Each will read zero, in production,
for a reason unrelated to the system's behaviour. Left as a code comment this
reads as intentional to anyone without the history, which is why it is filed
rather than annotated.

## Fix

**Make absence loud, then decide the default separately.** A ticker read while
statistics are disabled should fail — an error, not a zero. Once "cannot
answer" is distinguishable from "answered zero", the flag's default becomes a
performance question rather than a correctness one, and can be off.

The alternatives are both worse: default-off silently reproduces this trap for
the next person; default-on taxes every production node AND still misleads
anyone who later turns it off.

For BUG-0013 specifically, the confirming measurement additionally needs a
**positive control** — assert the counter MOVES before trusting any zero from
it. Without that, its three-way criterion collapses to "hypothesis dead" on
every run.

`Ticker::StallMicros` (`rocksdb.stall.micros`, cumulative) and
`Histogram::WriteStall` (`rocksdb.db.write.stall`) are the instruments that
measurement wants — both reachable via `get_ticker_count`, both inert until
statistics are enabled.

## Measured, and mostly falsified

Probed on the **production** open path (`RocksKv::open`, statistics off), after
5000 puts:

    PROBE property "rocksdb.is-write-stopped"         -> Ok(Some(0))
    PROBE property "rocksdb.actual-delayed-write-rate" -> Ok(Some(0))
    PROBE property "rocksdb.num-running-compactions"   -> Ok(Some(0))
    PROBE property "rocksdb.live-sst-files-size"       -> Ok(Some(0))
    PROBE property "rocksdb.no-such-property-at-all"   -> Ok(None)
    PROBE ticker   rocksdb.stall.micros                -> Ok(None)

Three things follow, and each contradicts something above.

1. **The two counters `write_stall()` reads are live.** `Ok(Some(0))`, not
   `Ok(None)`. They are properties; the statistics flag does not gate them.
2. **BUG-0013's criterion is sound.** "If `write_stopped` is zero the
   hypothesis is wrong" reads a counter that is running. That measurement was
   never blocked, and this file said it was.
3. **"Cannot answer" was already distinguishable — it was being discarded.**
   An unavailable property returns `Ok(None)`, and a genuine ticker
   (`rocksdb.stall.micros`) returns the same `Ok(None)`. `write_stall()` ended
   in `.ok().flatten().unwrap_or(0)`, which folded both into a healthy zero.

So the diagnosis ("statistics are off, therefore the counters are dead") was
wrong, and the prescription ("make absence loud") was right, for a different
reason than the one given.

**How the wrong claim was made.** `enable_statistics()` was found only inside
`#[cfg(test)]`, and the conclusion was drawn from that one fact without ever
reading a counter. Properties and tickers were treated as one thing because
they arrive through one function, `property_int_value`. Nothing was measured —
in a file whose entire subject is the difference between "measured zero" and
"never measured".

## Fixed

`RocksKv::write_stall()` returns `Option<(u64, u64)>`. `None` means the engine
could not answer and is not the same value as `Some((0, 0))`.

`FLINTINFO` keeps `write_stopped` and `delayed_write_rate` numeric — so the
exporter, which re-emits every numeric field as a gauge, needs no change — and
gains **`write_stall_readable:0|1`** beside them. That is the existing
`disk_unknown_samples` idiom: publish readability next to the metric rather
than poisoning the metric. The pair is read ONCE per FLINTINFO so the flag
cannot describe a different call than the values beside it.

Verified end to end, both directions:

    === rocks engine ===          === mem engine (cannot answer) ===
    write_stopped:0               write_stopped:0
    delayed_write_rate:0          delayed_write_rate:0
    write_stall_readable:1        write_stall_readable:0

Identical `write_stopped:0` on both; the flag is the only thing separating a
measured zero from an engine with no such counter at all.

A test pins both halves (`write_stall_is_readable_on_the_production_open_path`):
a **positive control** that the production open path really can read the pair —
so if these ever do become statistics-gated, the test fails instead of
FLINTINFO publishing a healthy-looking zero — and an assertion that an unknown
property still reads `Ok(None)`, because a rocksdb upgrade answering
`Ok(Some(0))` there would collapse the distinction again without a line of
Flint changing.

## Still open, and genuinely as filed

Real statistics tickers — `Ticker::StallMicros`, bloom `useful`/
`full-positive`, compaction counts, read amplification — **are** inert without
`enable_statistics()`, and reach `property_int_value` as `Ok(None)`. Nothing
in Flint reads one today. When something does, it must not use the
`.unwrap_or(0)` idiom, and enabling statistics remains a separate performance
decision. `sst_bytes()` still ends in `.unwrap_or(0)`; its property is live, so
this is latent rather than active.

## Related

- BUG-0013 — the measurement this blocks, and whose criterion it would silently satisfy
- BUG-0017 — same file, also a production default left at RocksDB's choice
