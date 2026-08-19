# BUG-0032: `flintctl start` asserts the inventory's FIRST pair member is a live master (OPEN)

Status: OPEN, found 2026-08-19 on the playground · Severity: medium — after
any failover, `start` panics whenever the declared-first member is down,
which converts "one seat needs restarting" into "the start path is dead"

## Symptom

    thread 'main' panicked at crates/flint-ctl/src/main.rs:3085:9:
    master 172.31.64.94:7002 up

Six times in eleven minutes, once per `flint-supervise` tick.

## Root cause

`crates/flint-ctl/src/main.rs`, in the start path:

```rust
for pair in &inv.pairs {
    assert!(
        wait_pong(&pair[0], &tls, Duration::from_secs(10)),
        "master {} up",
        pair[0]
    );
}
```

`pair[0]` is the first member **as written in the inventory**, not the
current master. The playground declares `pair 172.31.64.94:7002,172.31.64.94:7001`
and 7001 has been master since the failover of 2026-08-18. So the assert
demanded a PONG from the REPLICA and called it the master.

Two defects in one line. The label is wrong — it will mislead anyone reading
the panic, and it misled this investigation for several minutes. And the
check is fatal where it should be tolerant: `start` exists to bring seats up,
so a seat that is down is its input, not an error.

## Consequence

`flint-supervise` runs `start` every two minutes to restore stopped seats.
With the declared-first member down, `start` panicked before it could
restart anything — so the one mechanism that would have kept trying was
disabled by the condition it exists to fix. Combined with
[BUG-0031](0031-needs-reseed-is-cleared-by-the-start-that-should-honour-it.md)
it produced a stable failure: a node that could not re-seed, and a
supervisor that could not survive noticing.

## Fix

Assert on the pair having a reachable MASTER, resolved by role rather than
by position — the CP already knows which member holds the epoch. If no
member answers, that is the real finding and deserves a message that says
so. A member being down while the other serves is normal after a failover
and must not be fatal to `start`.

## Verification the fix needs

- a pair whose inventory-first member is down: `start` restarts it and exits
  0, with a positive control that the same command still fails when NEITHER
  member answers
- the message names the role it actually checked, and a failover before the
  check does not change which member the message refers to

## Related

- [BUG-0031](0031-needs-reseed-is-cleared-by-the-start-that-should-honour-it.md)
