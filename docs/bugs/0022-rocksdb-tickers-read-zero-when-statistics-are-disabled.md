# BUG-0022: RocksDB tickers read zero in production because statistics are never enabled, and zero is indistinguishable from absent (OPEN)

Status: OPEN, found 2026-08-18 while preparing BUG-0013's confirming
measurement · Severity: **medium-high** — it does not break the product, it
breaks every diagnosis that reaches for a ticker, and it does so by returning
the answer the investigator expects

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

## Related

- BUG-0013 — the measurement this blocks, and whose criterion it would silently satisfy
- BUG-0017 — same file, also a production default left at RocksDB's choice
