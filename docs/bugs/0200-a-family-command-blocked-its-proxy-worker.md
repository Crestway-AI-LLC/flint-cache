# BUG-0200: a family command blocked its proxy worker, so every few vector writes on one connection failed after 5 s, and the worker's other clients waited behind each one (FIXED 2026-10-02)

**Status:** **FIXED 2026-10-02.** Found building ADR-0049 verification 3's
client, which loads vectors through the proxy over one connection. Held by
`coproc_vec_drill`'s new last section. Its commands, run against the unfixed
proxy outside the drill, gave 17 of 24 OK.
**Severity:** high wherever a co-processor serves clients, latent today. Vector
search is not offered to tenants, so no tenant sends family commands. But any
fleet started with `--coproc` (`chaos-cluster`, `scale-cluster`) has the
shape, and once vectors are offered an ordinary client fails one write in
every `--workers`. Worse, the data path stalls: every `VEC.*` command froze one
proxy worker, and every other connection on it, GETs included, for as long as
the command took. That is the isolation ADR-0010 D3 exists to promise.

## What was measured

flint-server (`--engine mem`), flint-proxy and flint-vec at public `1b419b2`,
on a laptop.

- **One connection, 24 `VEC.SET`s, `--workers 4`:** 17 OK and 7 failures,
  `COPROCUNAVAIL channel refused: ERR channel token expired`, in 35 s. Fixed:
  24 OK, under a second.
- **One connection, 30 `VEC.SET`s, 8 workers (the laptop's default):** a
  failure about every 8th write, each after a 5 s stall. The same writes from a
  fresh connection each never failed.
- **`--workers 1`:** no family write succeeds at all; `VEC.CREATE` answers
  `COPROCUNAVAIL` after 5 s, every time. A `GET` on another connection, sent
  10 ms into that `VEC.CREATE`, answered after **4.99 s**. Fixed: both at once.

## Mechanism

Each proxy worker is a single-threaded async runtime that serves the
connections dealt to it. `family_command` is async, but it reached the
co-processor through `coproc_call`, a blocking read with the family deadline
as its timeout, and it called it inline. So the worker stopped until the
co-processor replied.

A write on the co-processor dials back to the proxy edge with the command's
single-use `PROXYCHAN` token, to perform its durable side. The acceptor deals
that new connection to the next worker in turn. Once in every `--workers`
dial-backs, the turn came round to the worker that was blocked waiting for this
very reply. That connection could not be accepted until the blocking read gave
up. The token's 5 s deadline passed first, the co-processor's channel was
refused, and the write failed. The worker then served what had queued behind
it, ordinary reads included.

A client that opens a connection per command never showed it: its own new
connection also takes a turn in the rotation, so its dial-back always lands on
another worker. That is how `coproc_vec_drill` writes (one `valkey-cli` a
command), so the gate never saw it.

## Fix

`family_command` runs `coproc_call` on tokio's blocking pool and awaits it.
The worker keeps serving its other connections, a dial-back included. The
family in-flight slot (`--family-max-inflight`) is still held across the call,
so it bounds how many such calls run at once, as before.

## Verification

- `coproc_vec_drill` pins its proxy at `--workers 4` and ends with 24
  `VEC.SET`s on one connection. It asserts all 24 are OK and the set counts
  them. Against the unfixed proxy, the same commands give 17 of 24 OK (above).
- The `--workers 1` GET timing above, run against both builds.

## What this does not do

- `coproc_call` is still a blocking read with a dial per miss. It now costs
  a blocking-pool thread, not a worker, per command in flight.
- ADR-0010 D3's `coproc_shed_bench` measures the data path's read p99 under a
  busy co-processor. Why it passed with this in place was not examined.
