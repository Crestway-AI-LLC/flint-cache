# BUG-0084: a test with no total deadline wedged a release build

Status: OPEN
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

**Intermittent.** Run three times locally on the release profile: 30s (with the
compile), then 0s, then 0s. So it is a race, not a deterministic hang, which is
the worst shape for a release step — it passes every time you check it and
stops the one cut you needed.

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

## Fix shape

Not attempted here — rc.67's bytes are frozen and this is pre-existing.

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
