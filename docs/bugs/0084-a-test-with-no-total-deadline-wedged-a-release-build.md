# BUG-0084: a test with no total deadline wedged a release build

Status: FIXED 2026-09-02
Severity: medium (intermittent, but it stops a release cut dead)

## What happened

Cutting v0.1.0-rc.67 on 2026-09-02. The release build sat on one test for
**forty minutes** and would have died with its own 90-minute box:

    running 130 tests
    ....................................................................... 87/130
    ....................i.....................
    test migrate::ctl_reply_cap_tests::trickling_peer_is_refused_rather_than_buffered
      has been running for over 60 seconds

**It was blocked, not slow.** The instance reported healthy on both status
checks and **CPU sat at 0.24%**, flat across twenty minutes. On a 16-vCPU box a
single busy core reads about 6%, so nothing was running. That distinction is
the whole finding: the test's own comment warns about "the quadratic re-decode",
which makes slowness the expected story and is exactly the wrong conclusion
here.

Not a regression from the cut. `crates/flint-server/src/migrate.rs` is
byte-identical to v0.1.0-rc.66, `MAX_CTL_REPLY_BYTES` is the same 8 MiB, and
the test has been in since 49a137b5 (BUG-0060, 2026-08-27) — rc.66 built with
it.

**Intermittent** — but the first evidence I gave for that was worthless, and
the correction is worth more than the conclusion.

> **CORRECTED.** I originally wrote "three local runs gave 30s, 0s, 0s".
> **Those runs never executed the test.** Two mistakes compounded:
> `cargo test … trickling_peer_is_refused_rather_than_buffered -- --exact`
> requires the FULL module path, so the filter matched nothing; and this whole
> region is behind `#[cfg(feature = "rocks")]`, so without `--features rocks`
> the test is not in the binary at all — 102 tests instead of 130. **Cargo
> exits 0 when a filter matches nothing**, so five "PASSED in 0s" runs were
> five runs of nothing, and the 0s should have told me: a test that opens
> sockets and moves 8 MiB does not finish instantly.

Re-measured properly, with the full path and `--features rocks`, and a hard
kill at 150s so a hang could not masquerade as patience:

    original   24s PASS   18s PASS   18s PASS
    with fix   25s PASS   18s PASS   18s PASS

So the conclusion survives — it does not hang deterministically — and the real
figure is **~18 seconds per run**, not zero. The quadratic re-decode is genuine
and substantial; on a box running 130 tests in parallel it stretches. But the
release box sat at **0.24% CPU**, so what happened there was still a block, not
that stretch.

## The shape

The test spawns a peer that writes forever:

    let blob: Vec<u8> = b"$4\r\nabcd\r\n".repeat(6553);
    loop {
        if s.write_all(&blob).is_err() { return; }
    }

and a client bounded only *per read*:

    call_once_with(&addr, &[b"FLINTMIGRATIONS"], Duration::from_secs(30))

The 30 seconds is a read timeout, and the cap it is testing is a **byte** cap —
"a byte cap rather than a cumulative deadline, deliberately", as the code says.
Deliberate for the product; the consequence for the *test* is that nothing
bounds its total runtime. `write_all` has no write timeout either, so a peer
thread blocked on a full socket buffer waits forever at zero CPU, which is
precisely what was observed.

## Fix, 2026-09-02

Two changes, and neither tries to make the test faster — the point is that an
indefinite hang becomes a NAMED failure.

**A total deadline the test cannot exceed.** The call now runs on a worker
thread behind `recv_timeout(120s)`; on expiry the test panics with a message
saying it waited 120s for a peer that trickles forever, naming the byte cap
that should have refused it long before. 120s against a measured 18s is
deliberate slack: this polices hangs, not performance.

**A write timeout on the peer.** `set_write_timeout(5s)` on the writer socket,
so the thread cannot park forever on a full socket buffer once the client stops
reading. That is the shape that produces a block at zero CPU.

Verified: 3/3 pass with the fix at the same 18s, so it costs nothing. The
deadline path itself was exercised by shortening it to 1ms — which is also how
I discovered the filter had been matching nothing, because the "control" kept
reporting success.

## Fix shape, as originally written

1. **Give the test a total deadline it cannot exceed**, and fail with a message
   naming what it was waiting on. A test that can hang forever is a test that
   can hang a release, and "it passes locally" is not a bound.
2. **Set a write timeout on the peer side**, so the writer thread cannot park
   indefinitely on a full buffer.
3. Consider whether the suite as a whole needs a per-test cap in CI. This one
   was caught because a human was watching a release; nothing else would have
   noticed before the box self-terminated, and the failure would then have read
   as "the build box died" rather than "one test wedged".

## What it cost

One 45-minute build box, terminated by hand rather than left to its 90-minute
timer. The cut is otherwise clean: both release-branch gates green on the
frozen bytes.
