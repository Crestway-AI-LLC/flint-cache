# BUG-0014: chaos_unreadable fails an acked write on a REPLICA kill (OPEN)

Status: OPEN, found 2026-08-18 from CI · Severity: high if real — the oracle
is asserting the durability claim, so either the claim broke or the oracle is
crying wolf, and both are worth an hour

## Symptom

`tools/chaos_unreadable_drill.sh`, every gate run since 2026-08-16:

    iter 1: pair 0: killed REPLICA; zero acked loss verified
    iter 2: pair 0: killed MASTER (writes in flight); RTO 22ms harness-promoted,
            not RTO [kill_ms=... max_hold_ms=2]; acked keys regressed: 20
            (all within the 1000ms cap); 3 key(s) unreadable after retries —
            retired from the ledger, NOT judged as loss
    thread 'main' panicked at crates/flint-chaos/src/main.rs:808:25:
    iter 3: REPLICA kill lost acked write at key3121: 18195 < 23739
    FAIL: scenario not exercised — need a MASTER kill followed by a REPLICA
          kill, got: REPLICA MASTER

The reported FAIL is downstream noise: iters 2 and 3 ARE master-then-replica,
but iter 3 panicked before it could print its kill line, so the drill's
positive control saw a truncated sequence. **The failure to investigate is the
panic, not the sequence check.**

## It is INTERMITTENT — confirmed 2026-08-18

The very next gate run (`32101901310`, an unrelated drill change) passed
`chaos_unreadable` in 5s with the same seed, same iterations, same binary
inputs. So this is not deterministic at seed 1, and any investigation that
concludes from a single green run that it is fixed will be wrong. Reproducing
it may take several runs; a fix needs several greens to mean anything.

### Observations, so the ratio exists

A bug whose record contains only its failures can never say when it stopped
happening. So passes are logged here too, with what they ran against.

| Date | Result | Ran against | Context |
|---|---|---|---|
| 2026-08-18 | FAIL | (CI) | the observation this bug was filed from |
| 2026-08-18 | PASS (7s) | `04df62e` `drill-pgrep-anchor` | full local gate, 116 steps, 0 fail |

**The PASS is one sample, not an acquittal.** `chaos_unreadable` is in the
gate's randomized CHAOS set (`tools/gates.sh:118`), and this bug's own finding
is that it fails on some runs and not others — so a green run is exactly what
an unfixed intermittent bug looks like most of the time. Recorded because the
ratio is the only thing that will eventually separate "fixed by something we
changed" from "did not fire today", and because a gate summary reading
"116 steps, zero failures" would otherwise retire this silently.

Nothing between these two rows was aimed at this bug; the storage change in
that gate (BUG-0017, info-LOG bounds) has no relationship to the acked-write
path.

## Established

- The drill runs `flint-chaos --port-base 6346 --iterations 3 --keys 4000
  --seed 1 --mode mixed --inject-unreadable N`. There is **no `--edge`**, so
  `shared.edge` is None and the post-kill read at `main.rs:797-801` takes
  `cluster.master_client()` — "the port the harness KNOWS is master".
- The assertion is `got >= last_acked` against the ledger snapshot. It read
  18195 where 23739 had been acked: **5,544 behind**, on key3121.
- Iteration 2 (the MASTER kill immediately before) tolerated a regression of
  20 keys as being inside the 1000 ms cap. Iteration 3's gap is two orders of
  magnitude deeper, so "the same settling window, one iteration later" does
  not obviously cover it.
- First red gate run is `31933995660` (2026-08-16T07:29Z), the commit after
  the last green `31930990162`. The commit between them is `f9782c4`
  "rejoin: verify the copy before discarding it — warm rejoins, probed
  rewinds", which changed what a marked boot does: a replica-role copy the
  master vouches for now rejoins WARM from its own cursor instead of being
  discarded and re-seeded.

## Explicitly ruled out

**Not the proxy's routing.** The first hypothesis was that a read through the
edge landed on a node that had been demoted and was mid-re-seed — the shape
`main.rs:790-796` describes as the original #126 symptom. That cannot be it:
this drill passes no `--edge`, so the read never touches a proxy. Recorded
because it is the natural first guess and it costs an hour to re-derive.

## Assumed, NOT established

Everything about the cause. In particular it is NOT established whether:

- the warm-rejoin path of `f9782c4` lets a copy rejoin at a cursor the ledger
  has already passed (which would make this a real durability regression), or
- `cluster.master_client()` resolves to the pre-promotion seat for iteration
  3 after the harness promoted in iteration 2 (which would make it an oracle
  bug, the #126 class recurring on the OTHER side of the same branch).

The 5,544-deep gap is equally consistent with both, and no evidence in hand
separates them.

## Where to start

1. Re-run alone, capturing the harness's own view:
   `./target/release/flint-chaos --port-base 6346 --iterations 3 --keys 4000
   --seed 1 --mode mixed` — note whether it reproduces WITHOUT
   `--inject-unreadable`, which separates the injector from the kill path.
2. At the panic, print which ADDRESS `master_client()` resolved to and that
   node's `FLINTINFO` role/epoch/last_applied. If it is not the promoted
   seat, this is an oracle bug and the fix belongs beside #126's.
3. If it IS the promoted seat, compare its `last_applied` to the ledger and
   walk back to whether the warm rejoin admitted a cursor it should have
   refused.

**Run it alone.** Five drills were invalidated during BUG-0011's investigation
by a manual reproduction left running in another shell; `fleet_guard` refused
them correctly and its message was the diagnosis.

## Related

- `crates/flint-chaos/src/main.rs:790-801` — #126's fix and the comment that
  names this exact symptom on the direct path
- BUG-0011 — the other open drill defect, and the run-it-alone rule
