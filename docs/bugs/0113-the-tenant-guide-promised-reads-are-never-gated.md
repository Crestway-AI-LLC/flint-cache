# BUG-0113 — the tenant guide promised no quota ever blocks a read, and the ops/s quota does (FIXED 2026-09-06)

**Status: FIXED 2026-09-06.** Found 2026-09-06 auditing `docs/tenant-guide.md`
— the page tenants themselves read — for claims nothing checks · Severity:
medium-low — the behaviour is transient and retryable and the page already
says to retry `-THROTTLED`, so nobody is stuck. What was wrong is a stated
**invariant**, which is the kind of sentence a client stops designing around.

## What was wrong

Under "Two invariants worth knowing":

> **You can always read your data out.** No quota state ever blocks a read.

Two lines above, the same page defines `-THROTTLED` as "back-pressure: **your
ops/s quota**, a loss-protection guard, or admission control". So the page
calls the rate limit a quota and then says no quota gates a read.

The rate limit gates reads. `Topology::quota_gate` takes `is_write`, uses it
for the storage cap only, and then falls through to a token bucket that does
not consult it:

```rust
if over && is_write && !flint_commands::reduces_space(name) {
    return Some(Value::Error("QUOTA storage quota exceeded; ...".into()));
}
if rate_exempt || rate == 0 { return None; }
// ... token bucket, then:
Some(Value::Error("THROTTLED ops/s quota exceeded, retry with backoff".into()))
```

The proxy's own unit test settles what is intended, and it is not a slip:

```rust
assert!(t.quota_gate(ns, b"GET", false, false).is_none(), "first tenant op passes");
match t.quota_gate(ns, b"GET", false, false) {
    Some(Value::Error(e)) => assert!(e.starts_with("THROTTLED"), ...),
```

A `GET` is throttled once its burst token is spent. The page said that cannot
happen.

## What was right, and stays stated

- **The storage cap never blocks a read** — `over && is_write` is exactly
  that, and it is the invariant the sentence was reaching for.
- **The self-clear path is never shed** — space-reducing writes are exempt
  from the storage shed via `reduces_space`, so a full tenant can always
  delete their way out.

## A second, smaller gap in the same table

The page listed the space-reducing exemptions as `DEL`/`UNLINK`/`FLUSHALL`/
`EXPIRE`. `reduces_space` covers **seven**: those four plus `PEXPIRE`,
`EXPIREAT` and `PEXPIREAT`. Under-promising rather
than over-promising, so harmless on its own — but it collides with advice
this repository now gives elsewhere. `docs/retry-safety.md` tells clients to
**prefer** the absolute forms `EXPIREAT`/`PEXPIREAT` wherever a retry is
possible (BUG-0107), and a tenant following that advice while over quota was
using a command the guide did not list as exempt. It is; now it says so.

## Fix

Both invariants are scoped to the storage cap, which is what they were always
about, and the ops/s quota gets its own paragraph saying plainly that it paces
reads and space-reducing commands alike, that the answer is `-THROTTLED`, and
that it is a rate rather than a lockout. The exempt list is complete.

Nothing in the product changed. The page now describes what
`quota_gate` does.
