# BUG-0147 — a drill seeds through a pipe that discards the rejection

**Status:** **FIXED 2026-09-15** — every foreground seed in the core drills now
runs through `fleet_load_resp`, which reports the refusal. The BRING-UP
question this bug also raised is deliberately still open; see the last section.

## The failure, and what it did not say

```
== seed 2000 keys in slot 8450 ({mig2})
FAIL: seed not readable ()
```

An empty value, with no cause attached. The seed itself is:

```sh
awk 'BEGIN{...}' | valkey-cli -p 7235 -a tok-acme --no-auth-warning --pipe >/dev/null 2>&1
BEFORE=$($A GET '{mig2}:k00000')
[ "$BEFORE" = "v00000" ] || { echo "FAIL: seed not readable ($BEFORE)"; exit 1; }
```

**`--pipe`'s output and its stderr both go to `/dev/null`.** `valkey-cli --pipe`
reports errors and a final `errors: N` summary, and none of it survives. So
every reason 2,000 writes might not land — the tenant not yet pushed to the
proxy, an auth rejection, a `-MOVED`, a shed — arrives at the reader as the
same empty string.

## Why it matters more than it looks

The drill was **red on a gate** and the failure was not diagnosable from its
own output. Distinguishing "the seed was rejected" from "the seed landed and
the read is wrong" is the whole question, and those are opposite
investigations: the first is a bring-up race in the drill, the second is data
loss in the product.

It is the same defect class this repo keeps cataloguing — **a check whose
failure cannot be reported** — sitting one layer below the thing under test.

## Evidence it is a bring-up race and not the product

On the same box and the same tree, run alone rather than 4-wide, the drill
passed **twice**:

```
seeded; sample {mig2}:k00000 = v00000
PASS: migrate-slots moved a range operator-directed — CP-committed, keys intact, zero acked loss
```

It also passed in the immediately preceding gate run and uses none of the
verbs the commits under test touched. So the seed is being rejected under
parallel load, most likely before the CP's tenant push reaches the proxy —
and the drill's `sleep`-based bring-up is what would need the fix.

## The fix, which is small

Keep `--pipe`'s output and assert it. `valkey-cli --pipe` prints
`errors: 0` and `replies: N` on success; a drill that captures those can say
*"the seed was refused, 2000 errors"* instead of `()`. Then, separately,
decide whether the bring-up needs a readiness wait rather than a sleep — but
that decision needs the diagnostic first, which is the point.

**Not fixed here**: this is someone else's drill, it is intermittent rather
than broken, and changing a bring-up under a drill that is currently green
most of the time deserves its own run rather than riding a control-plane fix.

## What the fix turned out to be, which is not what this file proposed

The section above says "keep `--pipe`'s output and assert it", and that was
right about the mechanism and wrong about the work. **`tools/lib/fleet.sh`
already had `fleet_load_resp`**, which captures the pipe, reads `errors:` and
`replies:` off it, distinguishes a `-THROTTLED` shed from an error it does not
recognise, fails on a short load, and fails loudly on a load that delivered
nothing at all — "this is NOT shedding; it is a dead or unreachable seat".
Five drills were already using it. It was written for BUG-0035 in August and
has been the answer to this question the whole time.

So the bug was not a missing capability. It was **fourteen call sites that
bypassed the one implementation**, and the reason they could is worth naming:

> `fleet_load_resp` built its own `valkey-cli` invocation and had no way to
> pass a tenant token. Every drill that seeds THROUGH A PROXY — which is every
> tenant-facing drill — therefore could not use it, and each one hand-rolled
> `| valkey-cli ... --pipe >/dev/null` instead.

One optional argument closed that. A helper that cannot do the thing its
callers need is not neutral: it gets copied around, and the copies lose the
parts that were the point. Every one of those fourteen sites kept the pipe and
dropped the reporting.

**The sites, all now routed:** `migrate_slots` (this bug's own),
`decommission`, `fanout_timeout`, `expand_fill`, `m3_exit` (fifty tenants, of
which five DBSIZEs were spot-checked), `rebalance_execute` (three),
`reseed` (the deliberate WAL gap — where a shed shortens the gap the rest of
the drill depends on, and was invisible), `scan`, `tenant_rebalance` (two),
`slot_cutover_recovery` (two).

Three `--pipe` sites are deliberately NOT routed, and are not oversights: two
are backgrounded continuous load generators (`loaded_promote`, `slot_migrate`)
rather than seeds with a reply count to check, and `roll_shed` already pipes
the output into something that reads it.

One further change in the same file: the `env [load]` note now prints ONCE per
drill instead of once per call. `m3_exit` seeds fifty times in a loop, and
fifty samples of what else is running on the box answer nothing the first one
did not — while the sibling scan behind it is a `ps` walk, so they are not
free either.

## The site the sweep did not reach, and a ratchet so the next one cannot hide

The sweep above covered every **seed**. `loaded_promote_drill.sh:101` was not a
seed and so was not in its population: it is the background load the drill
exists to apply, piped into `--pipe` from a detached subshell with both streams
discarded. Its own positive control already asked the right question —

    fail "seq_lag never left 0 — the writer is not loading the pair ...
          (is valkey-cli --pipe keeping up?)"

— and had no way to answer it. The feeder now writes `$FEEDLOG`, and that
control prints its last lines before failing. A file rather than a variable
because the writer is a background job.

**`assert_pipe_output_is_kept`** sits beside `assert_bootstrap_failures_say_why`
— the check `b7b74a9` added when the same defect was swept out of 23 drills'
bootstrap lines — and matches only the both-streams form, since `>/dev/null`
alone still leaves stderr, where a refused connection lands. Tri-state like its
neighbour: zero drills found is *examined nothing*, not clean.

**It skips comment lines, and that is not a detail.** The fix above quotes the
broken form in a comment explaining what it replaced, so a matcher that cannot
tell code from prose refuses the very commit that fixed the bug. That is
OPS-0232's lesson — a guard that fires where it cannot matter teaches people a
switch to pass it with — and it was caught by running the guard against the fix
commit before shipping it, not by reasoning about it.

Verified both ways: against `432f561` it names `loaded_promote_drill.sh:101`
and nothing else, and the comment at `migrate_slots_drill.sh:61` is not among
its hits.

## Still open, on purpose: the bring-up

This bug had two halves and only one is fixed. The seed is still preceded by a
`sleep` rather than a wait for the tenant to be live on the proxy, and that is
the most likely cause of the original red gate. Fixing it now would mean
changing a bring-up under an intermittent drill **in the same change that
removes the only reason the next failure will be legible** — and the section
above says why that order is wrong: the decision needs the diagnostic first.

The next time `migrate_slots` goes red under a 4-wide gate, its log will say
`errors: 2000, replies: 2000` or it will say the seed landed and the read was
wrong. Those are opposite investigations, and until today the drill printed the
same empty `()` for both.
