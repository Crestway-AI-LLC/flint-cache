# BUG-0083 — the build read has one attempt, and every way it can fail looks identical (OPEN)

**Found** 2026-09-01, looking for the cause of an intermittent
`admin_gated_proxy` failure on the public gate at 69c190b. Found in the code,
not in the failure — whether it is the cause of that particular red is
**not yet established** (see below).

## What is wrong

`crates/flint-ctl/src/main.rs:900`, `proxystats_field`:

```rust
let Ok(Value::Bulk(Some(raw))) = call_seq_on(
    &proxy_dial(inv, i), &tls, &seq,
    Duration::from_millis(1500), inv.client_tls,
) else {
    return None;
};
```

**One attempt, and no retry anywhere above it.** The `else` arm collapses every
distinguishable outcome into the same `None`:

- connect refused, because the seat is still restarting
- a TLS handshake that did not complete in time
- a read timeout (`set_read_timeout`, per read, 1500 ms)
- any non-`Bulk` reply — including the error `-NOAUTH`
- the proxy genuinely having no build to report

`roll_edge` treats `None` as fatal, and does so **after it has already rolled
every seat**, so a single missed read turns a completed roll into

    == UPGRADE ABORTED rolling proxy-7379: came up but would not report
       a build ... this roll cannot be verified

## Why this is a defect and not a tuning question

Nothing in the path distinguishes **"the proxy reports no build"** from
**"I failed to read it once."** Those warrant opposite responses: the first is
a genuine, fatal, un-verifiable roll; the second is a transient that a second
attempt would resolve. Collapsing them means the fatal case can never be
trusted and the transient case can never be survived.

`edge_roll_drill.sh`'s header records this shape three times already — rc.29
("the roll worked, the build column lied"), #102 (`verify --probe` could not
probe a client-TLS edge), rc.47 (rolled all six seats, then aborted with exit
3 while the proxy was serving the new build). Its own summary:

> Three times now the ROLL has been right and the READING of it wrong, which
> is why this drill asserts the reading and not just the outcome.

Each of the three was fixed by making one particular reading MORE CAPABLE — ask
over TLS, present the admin token. **None was fixed by making the reading
retry**, so each fix removed one way for the single attempt to fail and left
the single attempt in place.

Same family as **BUG-0081** (a benign `EAGAIN` on an idle watch socket rotates
the proxy's control-plane seat): a benign transient read treated as a hard
failure. Different code path — the build report is `flintctl -> proxy
PROXYSTATS` directly and never touches the CP watch — so this is a recurrence
of the pattern, not the same bug in a second place.

## Why an admin-gated fleet meets it more often

Not a tighter budget: `call_seq_on:597` sets the timeout with
`set_read_timeout`, which is PER READ, so the `AUTH` round trip gets its own
1500 ms. The reason is surface. An admin-gated fleet adds an `AUTH` exchange
and a dependency on the CP having pushed the admin digest (ADR-0006 D4) before
the read — more ways for one attempt to fail, against the same one attempt.

## The artifact, and why its EMPTINESS is the evidence

The failing run (public gate 33564635611, attempt 1, at 69c190b) tails 12 lines
of `upgrade.log` per `admin_gated_proxy_drill.sh:123`:

```
== upgrade --version-tag completes instead of aborting at the edge
FAIL: upgrade exited non-zero on an admin-gated fleet
  |   pair 0: 127.0.0.1:7441 demoted + drained; 127.0.0.1:7442 promoted at (0,3)
  |   started node-7441 (pid 95742)
  |   pair 0: old master rolled, tailing the new one warm
  | == controller
  |   started controller (pid 95924)
  | == control plane
  |   started cp (pid 95937)
  |   cp reports admingate-1
  | == proxies last (clients see one blip, over an already-new fleet)
  |   started proxy-7443 (pid 96051)
  | == UPGRADE ABORTED rolling proxy-7443: came up but would not report a build. ...
  |    the data plane is already on the new build (roll forward)
```

**No timeout. No connection error. No `-NOAUTH`. Nothing between "started
proxy-7443" and the abort.** A grep of the whole attempt for
NOAUTH/timeout/refused/reset/handshake returns only the drill's own positive
assertion earlier in the run (`proxy refuses pre-auth: NOAUTH admin token
required for this command`) — the gate working, not the roll failing.

That absence is not a missing clue. **It is the defect, demonstrated.** The log
cannot name which failure occurred because `else { return None }` discarded the
class before anything could log it. Asked which of two fixes the evidence
points at, the answer is that the evidence cannot point at either — on this run
or any future one.

Supporting, and labelled as consistent-with rather than proof: `started
proxy-7443 (pid 96051)` is the line immediately before the abort, with no
intervening output. The read happens directly after process start, which is the
timing a single-shot read against a not-yet-listening socket predicts.

## Provenance of the artifact above

Stated rather than inherited, at the reporting session's own insistence. It is
a SINGLE sample from a GitHub-hosted runner. The job was re-run before the tail
was extracted, so what is quoted is attempt 1 retrieved after attempt 2 had
completed. That `--attempt 1` returns the original bytes is BELIEVED, not
established; the content is self-consistent with what `--log-failed` showed
live before any rerun. Printed because a bug filed on an artifact inherits that
artifact's uncertainty, and this one is about a reading that could not say what
it had actually seen.

The independent run on a separate box at the same SHA **passed**
(`PASS  admin_gated_proxy  (34.8s)`). That is recorded so nobody later reads
this as unreproducible-and-therefore-minor, and it is worth being exact about
what it means: given the mechanism, a pass is a run in which the timing window
did not open. Greens BOUND THE RATE and cannot refute the defect; one red
demonstrates it. The natural reading of nine greens and one red is the
opposite, which is why this is spelled out. (That run also gated an rsync'd
working tree rather than a fresh checkout of the commit — same content hash,
clean tree. Load-bearing for a green, irrelevant for a red.)

## The operational consequence IS detected — but only on one side of the line

`tools/half_rolled_drill.sh` catches the split state this produces: *"a fleet
that stopped mid-roll must be NOTICED, and a fleet mid-roll must not be"*, and
its header names this exact failure as one of its two motivating cases.

**It lives in the ops repository and runs in the OPS gate.** It is not in this
repo and `half_rolled` appears nowhere in `tools/gates.sh` here — verified, not
assumed. So the fleet Crestway operates has a detector for the split, and every
deployment shipped under Elastic-2.0 does not: a customer rolling with
`flintctl upgrade` meets the same single-shot read, gets the same fleet with
pairs and cp on the new build and the edge on the old, and has nothing that
compares the seats to each other.

An aborted edge roll therefore goes unnoticed nowhere it is watched, and
unexplained everywhere. Whether that detector belongs above the open-core line
is a real decision and is NOT settled here; it is recorded as a gap for
whoever takes this.

## Fix, in order

1. **Make the failure distinguishable.** Return a `Result` whose error names
   the transport outcome, and have `roll_edge` report it. Until this exists,
   nobody can choose between (2) and (3) on any run — which is the state the
   artifact above documents.
2. **Then** retry, if the evidence shows a transient. One extra attempt removes
   the whole class.
3. **Or** fix digest-push ordering, if the evidence shows `-NOAUTH`.

Doing (2) or (3) first would be guessing which of them is needed, using a log
that was built to be unable to say.

This is ADR-0028's property one level down: the reading states a verdict
(`None`) in a form nothing can refute.

## The audit of the other readers — done 2026-09-01, and it found a worse one

The question "does any OTHER caller collapse a failed read into a fact about
the seat" is answered. Fourteen sites share the `let Ok(Value::Bulk(Some(..)))
= call(..) else` shape, but the shape alone is not the bug: it becomes one only
where a caller turns the resulting `None` into a DECISION.

**The controller does not have it, and shows what right looks like.**
`flint-controller/src/main.rs:457` falls through to `PING` and `socket_open`
to separate "down" from "up but hiccuping", with the reason stated:

> The promote decision needs this to avoid flapping a starved-but-listening
> master to death.

So the distinction is made where a wrong answer is most expensive, and was
omitted where the consequence looked cosmetic — a build string. That build
string aborts releases.

**`roll_node` has it, and in a worse form** (`flint-ctl/src/main.rs:5209`):

```rust
let got = info_field(addr, &tls, "build:").unwrap_or_default();
if &got != want {
    return Err(format!("{addr} reports build {got:?}, expected {want:?}"));
}
```

`unwrap_or_default()` turns a failed read into `""`, and the message then reads

    <addr> reports build "", expected "v0.1.0-rc.67"

which is not merely undifferentiated, it is FALSE. The node reported nothing;
the sentence says it reported the empty string. A reader trusting that message
would go looking for a build-stamping defect on a node that was never asked
successfully. The proxy version at least said something true and unhelpful.

This is the NODE half of the same roll, so both tiers of `flintctl upgrade`
carry the defect, and `4273` is the same `unwrap_or_default()` pattern in the
status path.

**Not fixed in this pass.** `info_field` is used widely across flintctl and
changing its type touches far more call sites than the two readers already
converted; it is the next batch, not a rider on this one. Recorded here so the
finding is not lost between gates.

## Still not established

- ~~Whether any OTHER caller treats a failed read as a fact about the seat.~~
  ANSWERED above: `roll_node` does, via `unwrap_or_default()`, and fabricates a
  quotation while doing it.
- Which transient actually fired. Recorded as UNKNOWN rather than guessed,
  which is the whole point of (1).
