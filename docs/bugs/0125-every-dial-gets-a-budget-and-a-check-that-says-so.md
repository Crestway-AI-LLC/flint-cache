# BUG-0125 — every dial gets a budget, and a check that says so

Status: FIXED 2026-09-08 — eleven dial sites bounded, two of them previously
unbounded outright; `assert_dials_are_bounded` refuses a twelfth · Severity:
medium — one of the two was on the Raft RPC path, and an unbounded dial there
parks a control-plane RPC for the kernel's SYN-retry budget rather than failing
Found: 2026-09-08, auditing the class after the third instance of it
Component: `flint-tls` (`connect_edge`), `flint-controlplane` (Raft network),
and eight call sites across ctl / exporter / backup / journal / chaos

## What happened

Three bugs in two days turned out to be one defect wearing different clothes:

| | where | shape |
|---|---|---|
| BUG-0122 | chaos client `connect_master` | 3 s dial vs single-digit-ms holds |
| BUG-0123 | `flint-proxy::discover_master` | 3 s dial vs an 800 ms reply |
| BUG-0124 | `flint-controller::observe` | 3 s dial ×2 → 19.8 s worst detection |

Each was found by reading one function after a soak pointed at it. That is a
bad way to find the fourth, so this is the audit — and the audit found worse
than the three that prompted it.

## Two dials had no bound at all

**`flint_tls::connect_edge` used a bare `TcpStream::connect`.** That is the
precise hazard `connect` documents two screens below it, in a comment written
when `connect` was fixed:

> A bare TcpStream::connect has no timeout of its own, so a blackholed peer
> (host down harder than a RST — partition, SG change, hung NIC) parks the
> CALLER for the kernel's SYN-retry budget, **minutes**, regardless of any read
> timeout set after. **Every internal dialer** (controller sweep, server lease
> renewals, proxy back-ends) **comes through here**.

The edge dialer did not come through there, and nothing in the file said so.
Every caller of it states a reply budget — 1500 ms is the common one — in front
of a dial that could take minutes.

**`flint-controlplane::ha::exchange` used a bare async `TcpStream::connect`**
on the **Raft RPC path**: leader election and log replication between CP seats,
which is the fencing authority under ADR-0018. A seat that goes silent rather
than refusing parks the RPC instead of failing and letting Raft treat the peer
as unreachable. `flint_tls::aio` already bounds its dial with exactly this
pattern and the same 3 s constant; this path simply never got it.

## Eight more stated a budget and inherited another

`flint-journal` is the sharpest: it sets **400 ms** read and write timeouts and
its module header promises "best-effort with bounded timeouts" — while the dial
in front of them sat on the 3 s backstop, **7.5× the budget it advertises**.
The rest are `flint-ctl` (2), `flint-exporter` (2), `flint-backup` (2), and
`flint-chaos` (1), all at 1500 ms replies over a 3 s dial.

## Fix

`connect_edge` now routes through a new `connect_edge_within` and takes
`CONNECT_BACKSTOP` by default, so the "minutes" case is gone even for callers
that express no opinion. Every one of the eleven sites now passes **the budget
it had already chosen for its own reply** — `timeout`, `read_timeout`, 400 ms,
1500 ms — rather than inheriting a ceiling meant for a different problem. The
Raft dial takes `RAFT_DIAL_TIMEOUT`, matching `aio`'s constant.

## The check, which is the actual deliverable

`assert_dials_are_bounded` (in `tools/gates.sh`) fails on:

- a bare `TcpStream::connect` with no timeout on it or the line above, and
- a dial with no budget sitting within 8 lines above a **tighter** reply
  timeout — the exact shape of 0122, 0123 and 0124.

It reports coverage (20 dials across 72 files) and fails when it matches
nothing, so it cannot certify the tree by reading none of it.

**It found three faults in itself before it found none in the tree**, each
recorded in its comments because each is a way this kind of check goes quietly
wrong:

1. A **comment** naming `TcpStream::connect` was flagged as a dial — the check
   reported its own fix's explanation as the defect.
2. The wrapper `tokio::time::timeout(D, TcpStream::connect(a))` puts the bound
   on the **same line**; only the previous line was inspected, so the check
   flagged the very fix it exists to bless.
3. Coverage counted only *unbounded* dials, so converting every site — the
   goal — would have emptied the population and tripped the no-dials guard. A
   check that fails when the codebase becomes correct teaches people to delete
   it. Coverage now counts bounded dials too, which is why the number went from
   8 to 20.

Verified by injection: reverting the `flint-journal` site to the unbounded form
makes the check fail, and restoring it makes the check pass.
