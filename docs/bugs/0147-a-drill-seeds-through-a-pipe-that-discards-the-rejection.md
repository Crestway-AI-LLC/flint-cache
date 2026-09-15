# BUG-0147 — a drill seeds through a pipe that discards the rejection

**Status:** OPEN — found 2026-09-14 when `migrate_slots` failed a gate and the
message could not say why.

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
