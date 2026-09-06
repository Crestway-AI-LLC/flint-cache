# BUG-0098 — the proxy forwards the one reply that means "retry me"

Status: FIXED 2026-09-05 · found 2026-09-05 while confirming BUG-0041's cause,
and explicitly NOT that bug · Severity: medium — a client-visible error on a
path whose contract is "zero client-visible errors", reachable on every restart
and every re-seed, but not observed in the wild.

## The gap

`flint-server` answers data commands with `-LOADING Flint is loading the
dataset in memory` until its dataset is up. Its own comment says why that
spelling:

> *"This is Redis's `LOADING`, and it is deliberately the same word on the
> wire: `-LOADING` is a documented reply every mainstream client library
> already retries rather than treating as a hard failure."*

That argument is sound for a client talking to a node. Flint's clients do not
talk to the node — they talk to the proxy, whose header states its whole reason
to exist as absorbing exactly this class:

> *"Forward one client command, absorbing -MOVED / -TRYAGAIN / backend death
> within the retry budget."*

It absorbed MOVED, it absorbed TRYAGAIN, it absorbed READONLY, and it forwarded
`-LOADING` verbatim. The three arms enumerate CODES, and nobody added the
fourth. `grep -c LOADING crates/flint-proxy/src` returned **0**.

## Where it is reachable

A node answers LOADING for as long as its dataset takes to load. Every one of
these puts one in the routing table:

- a re-seeded replica coming back after `restart-node` — the ordinary tail of
  an `AttachReplica`, which the ops agent performs unattended;
- an ex-master revived in place after a failover, which is exactly what
  `tier2_promote_drill.sh` does on every run;
- any restart at all, including a rolling upgrade.

`discover_master` reads `role:master` out of FLINTINFO, so a loading node that
answers FLINTINFO can be selected and then refuse the data command.

## Three sites, and one of them is not a retry

- `forward` — the keyed retry loop. Absorbs it: rediscover, back off only when
  the re-probe still points here (BUG-0055's rule), retry until the deadline.
- `forward_collect` — the staged path. Same absorption; `forward` owns the
  deadline, so there is none to test there.
- `call_pinned` — inside a MULTI. Here it must ABORT, joining MOVED / TRYAGAIN
  / READONLY. Not for their reason — those mean "wrong node" and this means
  "not yet" — but with their outcome, because a node that is not queueing
  cannot hold a transaction. Passing the reply through as an ordinary one would
  show the client `-LOADING` for a command that should have said QUEUED, and
  then EXEC a transaction missing it. Reachable on the first keyed command,
  which is what opens the deferred MULTI.

The connection is NOT dropped, unlike READONLY's arm. That one drops because a
demoted node's session may be re-pointed; a loading node's socket is healthy and
only its dataset is not, so dropping would add a dial to every retry for
nothing.

## The test, and what writing it cost

`a_loading_backend_is_retried_and_never_reaches_the_client`: a fake backend
answers `-LOADING` to the first data command and `:1` to the second, which is a
restarted node finishing its load. The client must see `:1`.

Three false starts, each caught by the test rather than by inspection, and each
worth recording because they are what a fake backend has to know:

1. `$12` for an 11-byte `role:master`. `discover_master` rejected the probe,
   routing went to none, and the retry loop spent its whole 5 s budget with
   nowhere to go. **The capability assert is what caught this** — it counts
   data commands and refuses to pass when the backend saw fewer than two.
2. The fake answered `-LOADING` to the pool's `HELLO 3` handshake. The pool
   never got a usable connection, so every attempt failed as a dead backend and
   the LOADING path was never reached — while the capability assert passed,
   because a failed handshake still counts as a command seen. A `dial` needs a
   RESP3 **Map** for HELLO and a **Simple** string for the `FLINTNS` bind.
3. A bare `block_on`. The write path calls `spawn_local`, so the test needs a
   `LocalSet` — which is a truthful signal that a bare runtime is not the one
   this code lives in.

Mutation-checked: disabling the `forward` arm reddens it.

## Not BUG-0041

BUG-0041's two surviving occurrences both recorded `-ERR no reachable master
for this slot` at `+5.02s` — the proxy's own string, emitted when rediscovery
found no target at all and the budget expired. This defect was found while
ruling that out and is independent of it.
