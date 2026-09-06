# BUG-0116 — the capacity model sized a cluster on a constant halved a month earlier (FIXED 2026-09-06)

**Status: FIXED 2026-09-06.** Found 2026-09-06 auditing `docs/capacity-model.md`,
the sixth doc in the sweep that produced BUG-0103, 0107, 0108, 0109, 0112,
0113 and 0115 · Severity: medium-high — this is the page an operator sizes a
cluster from, and every error ran in the optimistic direction.

## Three errors, and they compound

### 1. The poll interval was stale by 2×

    | Controller cadence | poll 200 ms × confirm 3 ⇒ detection ≈ 0.6 s |

`--poll-ms` defaults to **100**, and has since 2026-08-02. The controller's
own comment dates it and says why:

> 100ms, halved from 200 on 2026-08-02. Detection is poll x confirm and it
> dominates the client-visible failover stall — measured at ~70% of it. On 7
> EC2 hosts through the proxy edge, 30 kills per setting: 200:3 gave p50 644ms
> / worst 753ms over 15 promotions, 100:2 gave p50 317ms / worst 322ms over
> 18. This lands between them at ~430ms.

Not overridden anywhere: `flintctl`'s inventory doc says `poll-ms 100`, and
the ops renderer emits `${FLINT_POLL_MS:-100}`.

**The table contradicted itself and nobody subtracted.** Two rows up it says
the client-observed failover outage is ~472 ms. Detection cannot be 0.6 s
inside a 0.472 s outage it is ~70% of. Those two rows describe the same
phenomenon at two vintages: 644 ms was the measured stall at 200:3, and 472 ms
is roughly the 100:3 world.

### 2. The detection formula is additive, not a maximum

The page said `confirm × max(poll, sweep_time)`, which makes any sweep under
the poll interval free. The loop does not work that way:

```rust
loop {
    std::thread::sleep(cfg.poll);
    ...
    for pair in pairs.iter_mut() { pair.tick(&cfg); }
```

`sleep` **then** sweep, both loops sequential — pairs in turn, and inside
`tick`, `self.nodes.iter().map(|a| observe(a))` node by node. The period is
`poll + sweep`. Every millisecond of probing adds to the interval instead of
hiding inside it, so the sweep budget is not "anything under 100 ms is free",
it is "whatever you spend, you pay".

### 3. A dark node costs 3 s, not 800 ms

The 800 ms figure is `call`'s read/write timeout, which applies **after** a
connection exists. Reaching an unreachable host costs
`TcpStream::connect_timeout(&sockaddr, Duration::from_secs(3))` — 3.75× more.
Three cases, and the page modelled only the middle one:

| node state | cost |
|---|---|
| host up, process dead (connection refused) | immediate |
| listening but hung | ~800 ms |
| host unreachable | **3 s** |

## What the corrected arithmetic gives

At poll 100, confirm 3, 2 ms per healthy node, `detection ≈ 3 × (100 + sweep)`:

| pairs per shard | nodes | healthy sweep | detection |
|---|---|---|---|
| 16 | 32 | 64 ms | ~490 ms |
| 24 | 48 | 96 ms | ~590 ms |
| 32 | 64 | 128 ms | ~680 ms |
| 64 | 128 | 256 ms | ~1.07 s |

The rule moves from **≤ 32 pairs per shard to ≤ 24**, and the v1 cluster
recommendation from 64 pairs across two shards to **48**. Both are
re-derivations from the corrected constants, not new measurements, and they
are labelled as such on the page.

## The claim that never followed from its own formula

> ≤ 32 pairs per controller shard (≈70 ms healthy sweep, so even 8
> simultaneous dark nodes keep detection inside 3× poll)

Eight dark nodes cost 6.4 s if they hang and 24 s if they are unreachable.
Detection then runs to tens of seconds. There is no poll value, and no shard
size, that makes 8 × 800 ms fit inside 600 ms — this one was wrong on
2026-08-02 as well, independently of the stale constant. The page now says
plainly that a correlated failure has no margin and that this is a property
of the sequential sweep rather than of the shard size.

## Why it went unread

Nothing checks this file. It is the one doc in `docs/` whose numbers are all
constants living in code, and the constants moved. The controller comment
that records the halving is thorough, dated and measured — the change was
done well; the page that depends on it was simply never opened again.
