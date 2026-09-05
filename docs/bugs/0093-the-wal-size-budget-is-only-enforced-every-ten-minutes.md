# BUG-0093 — the WAL archive's size budget is only enforced every 10 minutes, and the TTL is what sets that

**Found** 2026-09-04 while building a drill for the roadmap's replacement Phase 1
gate condition ("a replica absent for T seconds rejoins without a full sync").
The drill's positive control would not fire: writing 16x the archive budget past
an absent replica still let it rejoin incrementally.

Status: OPEN · Severity: medium — nothing is lost or corrupted, and the bound
does hold on a long enough horizon. What is wrong is that a budget documented as
continuous is evaluated on a timer, and the disk-headroom reasoning built on it
assumes the former.

## What is wrong

`rocks.rs:347` states the model this rests on:

> Companion byte budget. RocksDB applies whichever bound trips first, so raising
> only the TTL would have left the 1 GiB limit doing the pruning — which is the
> term that actually fired in the incident.

"Whichever bound trips first" is true only when a purge pass runs. The pass
itself is throttled, and the throttle is derived from the **TTL**
(`wal_manager.cc:155-166`, librocksdb-sys 0.17.3+10.4.2):

    uint64_t const time_to_check =
        ttl_enabled
            ? std::min(kDefaultIntervalToDeleteObsoleteWAL,
                       std::max(uint64_t{1}, db_options_.WAL_ttl_seconds / 2))
            : kDefaultIntervalToDeleteObsoleteWAL;

with `kDefaultIntervalToDeleteObsoleteWAL = 600` (`wal_manager.h:132`). At our
shipped `DEFAULT_WAL_TTL_SECONDS = 43_200`, that is `min(600, 21600)` — a purge
pass at most **once every 600 seconds**. Between passes nothing is deleted, so
the size budget is not a ceiling on the archive; it is a ceiling sampled every
ten minutes.

## Measured

A 4 MiB size limit, `FLINT_WRITE_BUFFER_MB=1`, 64 MiB written in 8 MiB steps,
varying ONLY `--wal-ttl-seconds`. Archive size in MB after each step:

| ttl | 8 | 16 | 24 | 32 | 40 | 48 | 56 | 64 |
|---|---|---|---|---|---|---|---|---|
| `1` | 8 | 16 | 24 | **7** | 15 | 23 | 31 | **39** |
| `43200` (shipped) | 8 | 16 | 24 | 32 | 40 | 48 | 56 | **64** |

At the shipped TTL the archive tracks bytes written exactly — **it is never
pruned once**. At `ttl=1` (`time_to_check` = 1 s) it prunes repeatedly. The seat
reports `wal_archive_mb: 4` throughout, in both arms, which is the budget it
asked for and not what the directory holds.

**An earlier version of this measurement wrote only 16 MiB and I read it as
REFUTING the TTL hypothesis** — 13 MB at `ttl=1` against 16 MB at `ttl=43200`
looked like "both overshoot, so the TTL is not the term". It was underpowered:
the pruning arm's sawtooth peak is close to 16 MB, so at that volume the two
arms are indistinguishable. The distinction only appears once the run outlasts
a purge interval. Recorded because the wrong conclusion was one plausible number
away.

## Why it matters

At the roadmap's measured ingest of 142 MB/s, 600 seconds is **~85 GB** of WAL
that can accumulate past an 8 GiB budget before anything is deleted. The disk
guard sees that only as free space falling, and BUG-0079's whole correction —
sizing the archive from the volume so the byte term prunes before the disk
fills — assumes the byte term prunes when it is exceeded rather than when the
timer next fires.

It also means **a test cannot make the size term bind quickly**, and the two
knobs are coupled in a way that makes that unavoidable: a small TTL gives fast
purge passes but then ages files out by time, so TTL becomes the binding term;
a TTL long enough for size to bind pushes the pass cadence to 600 s. That is
why `walgap_quarantine_drill.sh` uses `--wal-ttl-seconds 1` and destroys the
span by TTL, and why a size-bound window cannot be observed in under ten
minutes.

## What is NOT established

- **That this was the mechanism in BUG-0079's incident.** That was a sustained
  2 TB ingest over far more than ten minutes, so many purge passes ran; the size
  term firing there is consistent with this and not evidence against it.
- **The behaviour over hours at production scale.** Everything here is a 64 MiB
  run on a laptop with a 4 MiB budget. The cadence is a constant, so the
  arithmetic should carry, but it has not been observed at the shipped 8 GiB.
- **Whether 600 s is actually too slow in practice.** On a volume with headroom
  for ten minutes of ingest it is invisible. The defect is that nobody sized
  that headroom knowing the budget was sampled rather than enforced.

## What to do about it

Not obvious, and deliberately not decided here. `WAL_ttl_seconds` cannot be
lowered to tighten the cadence without also shortening the time window it
exists to provide — one knob, two effects. The options are to size the volume
for `budget + 600 s x peak ingest` and say so, to give the disk guard a term
for archive growth specifically, or to accept it and document the overshoot.
The first is the cheapest and is a documentation change to `docs/self-hosting.md`
plus the constant's own comment, which currently tells a reader the opposite.
