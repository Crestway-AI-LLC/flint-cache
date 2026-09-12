# BUG-0135: an unquotad tenant was throttled while a pinned neighbour ran (FIXED 2026-09-12 — confirmation pending)

Status: **FIXED 2026-09-12 — confirmation pending** · Severity: the defect is
in the **drill**, not the product. The assertion could not tell the failure it
existed to catch from the product working correctly, so it reported the second
as the first. Read rather than reproduced, which is why confirmation is still
pending: the next occurrence now prints which mechanism refused the write.

**Not BUG-0134**, which is why the run hung rather than reported, and is
fixed. **Not BUG-0132.** This file exists so the next occurrence is read as a
second data point rather than a first.

## What was observed

`gate` on `cb9ede6`, job `gate (drills)`, artifact `gate-logs-drills`,
`drill-tenant_quota.log` in full:

```
  env [guard]: load 3.24, 1.48, 0.65 | mem 14.5 of 15.6 GiB free | no sibling processes
  (6 seat(s) belong to 4 live peer drill(s) in this suite -- not foreign)
== cluster: CP + master + TWO proxies; acme quota 400 ops/s (subset 2), globex unlimited
== rate, one proxy: acme hammers proxy 1 only -> its 200/s SHARE binds
  proxy1 alone: 200 ops/s accepted (share 200), 3200 throttled, 600 offered
== rate, fleet: acme hammers BOTH proxies -> shares sum to the 400/s budget
  fleet: 397 ops/s accepted across both proxies {7911: '198', 7912: '198'} (budget 400), 6360 throttled, 1200 offered
== isolation: globex (unlimited) at full speed while acme is pinned
Traceback (most recent call last):
  File "<stdin>", line 31, in <module>
  File "<stdin>", line 20, in measure
AssertionError: unquotad tenant got throttled
```

Line 20 is `assert b"THROTTLED" not in b` inside `measure`. Line 31 is the
`beside` call — globex measured **while** `acme_hammer` runs. So:

- **`solo` passed.** The same socket, the same assertion, 1.5s earlier with no
  neighbour: clean.
- **`beside` failed** within 3s of the hammer starting.
- Both quota arms *before* it passed, and passed tightly: 397/s accepted
  against a 400/s budget, split 198/198 across the two proxies.

## What is established, and what is not

**Established.** The reply globex read contained `THROTTLED`. globex has no
quota (`CPTENANTQUOTA` is set for acme only, per the drill's own bring-up).
The box was busy: load 3.24 at drill start, four drills at a time on a 4-vCPU
runner, six seats belonging to four peer drills.

**Not established.** Everything else. In particular *which* tenant's bucket
produced the reply, because the drill records the assertion and not the frame.

Two candidate explanations, and nothing yet separates them:

1. **A shed path that is not per-tenant.** If some admission gate ahead of the
   per-tenant bucket sheds under queue pressure, it would engage only when the
   box is slow enough to build a queue — never on a fast one. That would make
   an isolation guarantee a function of the hardware, which is the class Jeff
   named on 2026-09-11 (*correctness should be orthogonal to underlying
   hardware*) and the class BUG-0133 turned out to be.
2. **A measurement artifact.** `measure` reads until the first `\r\n` and
   asserts on that buffer. A reply boundary landing inside a `recv(128)` on a
   loaded box, or a frame from the wrong socket, would produce this line
   without any tenant having been throttled.

Hypothesis 1 is a product defect; hypothesis 2 is a drill defect. Reading the
limiter and the proxy's admission path answers it faster than waiting for a
recurrence, and that reading has not been done.

## Why it is open rather than closed by absence

It has happened once in 100 gate runs across both repos. It was **invisible**
when it happened: the assertion hung the drill instead of failing it
(BUG-0134), the job was killed at the 60-minute cap, and the result reads
`cancelled`. Nobody looked for 26 hours.

That part is fixed. The drill now fails in the ordinary way, and a recurrence
will arrive with its log, its `FAIL` line in the run summary, and the fleet
torn down. **Absence over the next few runs is not evidence** — the shape only
appeared once in a hundred, and the two arms before it show the limiter
working at the same moment.

## The answer: the assertion could not discriminate

`assert b"THROTTLED" not in b` reads **any** refusal as an isolation failure.
That prefix is shared by about ten shed paths in this product, and only one of
them is per-tenant:

| where | message | keyed on |
|---|---|---|
| `flint-proxy/src/main.rs:1207` | `THROTTLED ops/s quota exceeded` | **the tenant's token bucket** |
| `flint-proxy/src/main.rs:156` | `proxy at connection capacity` | the proxy's connection count |
| `flint-server/src/main.rs:4326` | `live replicas below min-replicas-to-write` | the seat's replicas |
| `:4336` | `no live replica for longer than --widowed-grace-ms` | the seat's replicas |
| `:4359` | `replication lag exceeds limit` | replication |
| `:4383` | `replica too far behind the retained WAL` | WAL retention |
| `:4430` | `write would wait ~Nms (inflight x cost), past --write-deadline-ms` | **the node's own queue depth** |
| `:6553` | `full-sync slots busy` | migration slots |
| `write_queue.rs:249,263,271` | `async write queue full / stalled` | the queue |
| `flint-storage/src/admission.rs` | `collection read needs ~N bytes` | read admission |

**The last-but-three is the one that matters here.** `--write-deadline-ms`
sheds when `inflight x cost` exceeds the deadline — a property of how busy the
*node* is, with no reference to which tenant sent the write. Its own comment in
`flint-server` records that it has fired on CI at **2017-2033 ms against
measured peaks of 124-557 ms**, and names the cause as a spike in one of its
two terms: "compaction, a stalled disk, **a descheduled runner**".

This drill starts its master with no shed knob disabled —

```
$B --port 6985 --engine rocks --data-dir "$D/m" 2>"${FLEET_SCOPE}server.log" &
```

— and the failing run was four drills wide on a 4-vCPU runner at load 3.24
with nine seats live. That is the documented condition for that gate to fire.

So the original question — product defect or measurement artifact — resolves
to **the measurement**, though not in the way this file first guessed. The
guess was that `recv` framing might mis-read a reply. The actual defect is that
the assertion was never specific enough to support its own conclusion: a
refusal arrived, and the drill said "isolation broke" when the product had
said "my queue is too deep".

## The fix

Assert on the quota's **own message**, which is the only reply that means what
the drill claims:

```python
assert b"ops/s quota exceeded" not in b, (
    "unquotad tenant got the QUOTA refusal, which is the isolation "
    "claim breaking: " + b.decode(errors="replace").strip())
```

Other refusals are **counted, reported and excluded from the accepted rate** —
not ignored. A shed write is not a served one, so folding it into throughput
would hide the very degradation the `beside >= solo*0.5` ratio asserts on, and
each distinct reason is printed as `EVIDENCE:`, which `gates.sh` surfaces even
when the drill passes. The old code would also have raised `IndexError` on an
all-shed window; the p99 is `nan` there now and says so.

Verified against both kinds of refusal:

| stub reply | wanted | got |
|---|---|---|
| `+OK` | accepted, nothing shed | accepted, `shed={}` |
| `THROTTLED ops/s quota exceeded` | **assertion fires** | fires, message names it |
| `THROTTLED write would wait ~2033ms … --write-deadline-ms` | no assertion, counted as shed | no assertion, shed counted, excluded from the rate |

## Why confirmation is still pending

Nothing here proves the write-deadline gate is what fired on 2026-09-12. The
log recorded the assertion's own text, not the reply, because the assertion
never kept it — that is the same defect one layer down. What is established is
that the claim the drill made was not supported by the evidence it collected,
and that a tenant-agnostic mechanism known to fire under exactly those
conditions was live in that fleet.

**The next occurrence decides it**, and now arrives with the refusing
mechanism named. If it ever prints `ops/s quota exceeded` against globex, that
is a real isolation defect and this file should be reopened with that line in
it.

## What would have closed it, before the answer arrived

Either of:

- **A read of the shed path** showing that every `-THROTTLED` reply is
  attributable to the bucket of the tenant that sent the command, with no
  global or per-connection gate ahead of it — with the drill's assertion then
  tightened to record the frame and the tenant, not just the substring; or
- **a reproduction**, which needs the condition rather than the load: a
  loaded box is easy to produce and this arm has run hundreds of times on
  loaded boxes without firing, so the missing ingredient is not slowness
  alone.

Do not close it on a green run. The two arms that *passed* in this same log
are the reason: the limiter demonstrably works at the same moment the
isolation claim broke.
