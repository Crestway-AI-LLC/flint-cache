# BUG-0093 — the WAL archive's size budget is only enforced every 10 minutes, and the TTL is what sets that (ACCOMMODATED 2026-09-05: volumes are sized for the overshoot)

**Found** 2026-09-04 while building a drill for the roadmap's replacement Phase 1
gate condition ("a replica absent for T seconds rejoins without a full sync").
The drill's positive control would not fire: writing 16x the archive budget past
an absent replica still let it rejoin incrementally.

Status: ACCOMMODATED 2026-09-05 (sizing rule adopted; the RocksDB behaviour is unchanged and unchangeable from here) · Severity: medium — nothing is lost or corrupted, and the bound
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

## What to do about it — DECIDED 2026-09-05: size the volume

Jeff's call: size the volume for `budget + 600 s x peak ingest` and document it.
The alternatives were a disk-guard term for archive growth specifically, or
accepting the overshoot silently. Lowering `WAL_ttl_seconds` to tighten the
cadence was never available — the same knob sets the retention window, so
buying a tighter bound would have cost the thing the bound protects.

**The number: 120 GB**, at the 200 MB/s reference `rocks.rs` already reasons
with. `600 s x 200 MB/s` is exactly 120 000 MB, so the figure is stated in
decimal GB and needs no conversion — the same quantity is 112 GiB, and both
appear in earlier drafts of this file. **They are one number, not a revision.**
Given how much a MiB/MB mix-up cost in BUG-0060 the same week, the unit is
pinned to the one the rate is quoted in.

It clears the highest rate ever measured here (142.2 MB/s, ADR-0026, one seat
at 138k ops/s = 85 GB) by 40%. Deployments that have measured their own rate
should use it; the term scales linearly and all but vanishes on a quiet fleet —
the playground's soak, at about 1 MB/s, needs 0.6 GB.

The budget is `clamp(volume / 4, 1 GiB, 256 GiB)`, so the rule resolves to:

| volume | reserve |
|---|---|
| under 1 TiB | `volume >= 1.34 x (data + 600s x rate)` (any unit, used consistently) |
| 1 TiB or more | `volume >= data + 275 GB + 600s x rate` (275 GB = the 256 GiB cap) |

Worked: a 1 TB disk (931 GiB) at 200 MB/s reserves **~370 GB** — 250 GB of
budget plus 120 GB of overshoot — leaving ~630 GB for data. At 1 MB/s the same
disk reserves 250 GB and change.

**Operators should measure rather than take 200 MB/s.** The rates above are
*logical*; what fills the archive is WAL bytes. `latest_seq` sampled twice N
seconds apart, times `wal_bytes_per_seq`, gives the right number directly and
already includes per-record overhead — which is where a logical rate understates
most, on small values.

Written up in `docs/self-hosting.md` 3b, and the three retention comments in
`rocks.rs` that asserted continuous enforcement are corrected.

**The failure mode if under-reserved is a shed, not a loss.** The archive grows
into the disk guard's threshold and ordinary writes return `-QUOTA` until a
purge reclaims. That is the designed behaviour; what makes it worth sizing for
is that it is a write outage on a node that looked like it had headroom.
