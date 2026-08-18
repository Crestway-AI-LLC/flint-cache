# BUG-0014: chaos_unreadable fails an acked write on a REPLICA kill (OPEN)

Status: OPEN, found 2026-08-18 from CI · Severity: high if real — the oracle
is asserting the durability claim, so either the claim broke or the oracle is
crying wolf, and both are worth an hour

**Numbering, because git log points the wrong way.** Commit `6613c15`, which
fixed the other three gate failures, calls this one "BUG-0012" in its message.
It is BUG-0014; the number was reassigned before filing, because 0012 was
already the WAL-retention livelock. That commit carries the same correction as
a git note (`git log --notes`), but a fresh clone does not fetch notes, so the
correction lives here too. Do not follow the 0012 reference.

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

## Attempted reproduction, 2026-08-18 — 21 valid runs, ZERO reproductions

**It does not reproduce outside a full gate.** Three configurations, all on an
idle box with nothing else running:

| configuration | runs | reproduced |
|---|---|---|
| raw binary, drill's exact args | 6 | 0 |
| raw binary + `--stall-replica-ms` 600/1200/1800/3000 | 12 | 0 |
| `tools/chaos_unreadable_drill.sh` itself | 3 | 0 |

**"Valid" is load-bearing here.** An earlier attempt reported 8/8 failures that
were nothing of the sort: the worktree had no `flint-server`, every run died at
`cluster.rs:942` before reaching the assertion, and `rc=101` looked exactly like
an oracle panic. The runs above are gated on a precondition (both binaries
present and `flint-server` executes) and scored three ways — pass /
reproduced-with-diagnostic / failed-other — so "it failed" and "it never ran"
can no longer wear the same clothes. That guard immediately earned itself: the
first drill-script attempt returned `rc=126`, scored `failed-other`, and was a
missing exec bit on the volume, not a result.

**The lag sweep engaged, and its result argues against a cause rather than for
one.** `--stall-replica-ms` is real (`main.rs:124`, applied `:382-389`,
SIGSTOP on the replica) and the injection is visible in the output — acked keys
regressed at the master kill scaled 1688 / 2310 / 2343 / 2006 across the sweep.
**The failing run in this write-up regressed 20.** So these runs carried roughly
a hundred times more unreplicated tail than the failure did, and iteration 3's
replica-kill assertion stayed silent every time (`replica kills: zero`).

That weakens the third hypothesis — that a pre-kill ack survives the master-kill
retire and is re-judged by the next replica kill (BUG-0007's mechanism). If that
were it, more regression should make it fire more often, not never.

**So the trigger is something about the GATE that neither load nor the drill
script reproduces.** Everything this bug has ever fired in was a full gate run.
The remaining differences are the other drills and the machine state they leave
behind — and one is now known: `restart_drill.sh` leaks a seat (it is outside
`fleet.sh` entirely, with no `fleet_init`, no `fleet_guard` and an unscoped
`pkill -f` cleanup). Its port 6410 is disjoint from this drill's 6346-6353, so
it cannot collide; it can only compete for CPU and IO.

**Next step: run it inside a full gate with the instrumentation in place**, not
solo. "Intermittent" may be the wrong label — nothing here was random, and the
one environment it has ever failed in is the one not yet tried with a probe
attached.

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
