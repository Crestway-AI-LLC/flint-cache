# BUG-0027: the gate's leak check is unscoped, so it blames and SIGKILLs another session's fleet (FIXED)

Status: **FIXED 2026-08-19**, found the same day when a peer session's drill
kept failing · Severity: **high** — it destroys a fleet the gate does not own,
with `kill -9` on rocks nodes mid-write, and reports the damage as a defect in
an innocent drill

## Symptom

A peer session was bootstrapping a 4-seat client-TLS fleet on ports 7191-7194
under `/tmp/flint-chaostls`. Every attempt failed at the fleet level, never at
TLS. They were tuning a "wait for 30s of quiet" bootstrap window, on the theory
that their cold start was losing a race against the gate's next bootstrap.

It was not a race. My gate was killing their seats. From one run's log:

    FAIL  restart  (0s)
            REFUSING TO RUN: this box already has Flint processes outside .../flint-restart-
          LEAKED: restart left 5 Flint process(es) running
            53201 ./target/release/flint-controlplane --port 7194 ... --state /tmp/flint-chaostls/sta
            53341 ./target/release/flint-proxy --port 7193 --control-plane 127.0.0.1:7194 ...
            53376 ./target/release/flint-controller --pairs 127.0.0.1:7191,127.0.0.1:7192 ...
            53468 ./target/release/flint-server --port 7191 ...
            53765 ./target/release/flint-server --port 7192 ...

Four such events in that single run, blamed on `restart`, `ctl_error`,
`proxy_tls` and `coproc_vec_rebuild` — none of which had anything to do with
those processes. Each one killed the peer's fleet.

## Read those two lines together

    REFUSING TO RUN: this box already has Flint processes outside <scope>
    LEAKED: restart left 5 Flint process(es) running

`fleet_guard` saw the same five processes, correctly identified them as **not
ours**, and declined to run. Then the leak check, one layer up, killed exactly
what the layer below had refused to touch. The gate contradicted its own
guard within two lines of output and killed on the strength of the wrong one.

`tools/lib/fleet.sh` exists because of this precise harm. Its header:

> Anyone with a live Flint fleet on the same machine loses it. `pkill -9` on a
> rocks node is a kill -9 mid-write.

The whole file is a scoping discipline built to stop that, and the gate
reintroduced the unscoped sweep above the abstraction written to prevent it.

## Root cause

    _leaked_seats() { pgrep -f 'target/release/flint-(server|proxy|controlplane|controller|agent)'; }

Global. Every Flint daemon on the box, with no notion of whose it is. The
caller then attributes the whole list to whichever step just finished and
`kill -9`s it.

"A process I leaked" and "a process someone else owns" were one observation.
The check could not distinguish them and reported one of them confidently —
the same defect as `0026`, except this one destroys data.

It also produces a **red gate for an innocent drill**, which is how it stayed
invisible: the failure reads as a drill with sloppy cleanup, which is a
plausible enough story that nobody looks past it. BUG-0020 was filed against
exactly that message and was right on its own facts; the check happened to be
telling the truth that time.

## Fix

Ownership is now decided the way `fleet.sh` already decides it — the drill's
**scope directory OR one of the ports it declares**. Both are required: a proxy
started `--port 6666 --pairs …` and a controller started `--pairs … --id PX`
carry no path at all, so a directory-only match misses them.

The declared ports come from the drill's own `fleet_init` line, parsed the same
way `assert_no_port_overlap` already parses it — no new source of truth.

And the two populations are now handled differently:

- **attributable to this drill** -> `LEAKED`, the step fails, killed (unchanged)
- **not attributable** -> a note, left running, the step does **not** fail

Not killing is the point. A stray kill is worse than a stray process, which is
`fleet.sh`'s rule and now the gate's.

**The search stays global; only the kill is scoped.** Raised by the peer whose
fleet this destroyed, and it is the sharper half of the fix: if the *search*
narrowed too, an absent report would mean either "the box is clean" or "the
check can no longer see", and those must not look alike. Global search plus a
scoped kill means no report is a real statement about the whole machine.

The note also says only what the check establishes. It reads "not attributable
to `<drill>`", not "someone else's" — all that is known is that the argv carries
neither the drill's scope directory nor a port it declares. Another session's
fleet looks like that; so does a drill that leaked a seat carrying neither
marker. Calling that "foreign" would be this same bug's confident attribution
pointed the other way, inside its own fix. The first draft of this fix did
exactly that.

### Verified, both directions, with a harness self-check

    harness: both functions loaded
    === ARM 1 — a FOREIGN fleet (another root, another port block) ===
      _leaked_seats restart  -> []          (not killed)
      _foreign_seats restart -> [23263]     (reported, left alone)
    === ARM 2 — a genuine leak: OUR root, and the drill's declared port ===
      _leaked_seats restart  -> [23307]     (ours — still caught)
      _foreign_seats restart -> [23263]     (not confused with ours)

The self-check earned its place. The first run of this harness printed empty
results for both arms and would have read as "ARM 1 passes" — the functions had
failed to load, because the `sed` range used to extract them ran past the end of
a one-line function and mangled the next. `command not found` on stderr was the
only tell. The harness now refuses to report unless both functions are defined.

## What it cost, and what it says about shared boxes

A peer spent an evening tuning a bootstrap waiter against a race that did not
exist, and their diagnosis — "every failure is fleet-level, never TLS" — was
exactly right and pointed at my gate the whole time. The general lesson is not
about `pgrep`: **when a check acts destructively on a shared resource, its
notion of ownership has to be at least as strict as the guard that already
refused to touch that resource.** Here the two disagreed and the destructive
one won.

## Related

- BUG-0026 — same shape, same file, no data loss
- BUG-0020 — the `restart(leaked)` message this check produced when it *was*
  right; the message is not evidence of ownership either way
- `tools/lib/fleet.sh` — the scoping discipline this bypassed
