# BUG-0036: the sibling-project guard cannot see a sibling project's test run (FIXED)

Status: FIXED 2026-08-20 · Severity: medium — it is the guard that exists to
stop contention being misread as flakiness, and it was blind to the most
common form of contention

## Symptom

Four gate runs on 2026-08-19/20, same binaries throughout (built 17:22, the
build that passed 117/0 at 17:25), produced a rotating cast of failures:

| gate | failures |
|---|---|
| 21 | `repl` |
| 22 | `edge_roll`, `json`, `chaos` — my own CPU burners, see BUG-0035 |
| 23 | `repl`, `controller`, `tenant_rebalance`, `chaos` |
| 24 | `proxy`, `controller_stall`, `scan` |

Most are startup-timing failures: `Could not connect to Valkey at
127.0.0.1:7679: Connection refused`, `proxy … never answered PROXYSTATS within
10s`, `controller never promoted 6631`, `control plane seat … up`. A seat did
not begin listening inside a fixed budget.

Throughout, another session on this box was running flint-kv work.
`fleet_guard` never objected.

## The defect

`fleet_guard` ALREADY refuses to run when a sibling Flint-family project has a
fleet up, and its reason is exactly this situation, written before it happened:

> Two fleets sharing a box contend for CPU and disk, and the result shows up as
> a flaky drill rather than as the collision it is.

`_fleet_sibling` identified siblings by BASENAME:

    if (exe !~ /^flint-[a-z0-9]+-[a-z0-9-]+$/) next

which the file justifies structurally — sibling projects namespace their
binaries `flint-<project>-<component>`, this workspace's are one segment.

That holds for a sibling's SHIPPED binaries and fails for everything else it
runs. The processes actually on the box were

    /Volumes/FlintDev/cargo-target/flint-kv/release/cold-modify
    /Volumes/FlintDev/cargo-target/flint-kv/debug/deps/ttl-ccbacfe2d0cd3f35 --list
    /Volumes/FlintDev/cargo-target/flint-kv/debug/deps/wal_archive-50eda22bf74a10ed --list

Test and helper binaries are named whatever cargo calls them. `cold-modify` and
`ttl-ccbacfe2d0cd3f35` match no naming convention at all — and a `cargo test`
run is precisely when a sibling project loads the box hardest.

So the guard returned clean, every time, for the entire class of contention it
was written to catch. A check that cannot see the thing it is looking for, and
whose silence is indistinguishable from an all-clear.

## The anchor lesson, in reverse

BUG-0034 taught: choose an anchor only the TARGET can carry. `--data-dir`
works for finding flint-server spawns; the literal `flint-server` does not.

This is the same lesson from the other end: the anchor must be carried by ALL
of the target, not just its best-behaved members. The naming convention is
real, documented, and followed — by the binaries the project ships. It was
never true of the binaries the project TESTS with, and those are the ones that
run during a test sweep.

What every one of them does carry is the project's cargo target directory.

## The fix

`_fleet_sibling` keeps the name rule and adds a path rule: an executable
running out of `…/flint-<project>/{release,debug}/` or
`…/flint-<project>/{release,debug}/deps/`.

Ours never match — this workspace builds to `…/target/release/`, and `target`
is not `flint-<project>`. Nor is a bare `flint`, so another checkout of THIS
project stays with `_fleet_foreign`, which is anchored on our own binary names
and is the correct owner of that case.

## Verification

Positive control was live and not constructed: a flint-kv test binary was on
the box while the fix was written, invisible to the old rule and listed by the
new one.

Table-driven, then mutated to prove the test bites:

| path | expected | got |
|---|---|---|
| `…/wt/flint-gatefix/target/release/flint-server` | ours | ours |
| `./target/release/flint-server` | ours | ours |
| `…/cargo-target/flint-kv/release/cold-modify` | SIBLING | SIBLING |
| `…/cargo-target/flint-kv/debug/deps/ttl-ccba…` | SIBLING | SIBLING |
| `…/cargo-target/flint/release/flint-server` | ours (bare `flint`) | ours |
| `/usr/local/bin/flint-kv-server` | SIBLING (name rule) | SIBLING |
| `/Users/x/proj/target/debug/deps/foo-123` | unrelated | unrelated |
| `…/cargo-target/flint-vec/release/bench-harness` | SIBLING | SIBLING |

Removing the path clauses drops the flagged set to the one name-rule row, so
the clause is load-bearing rather than decorative.

## What this does NOT explain

BUG-0035's shed at the shipped lag cap. That is a `-THROTTLED` from the master,
not a startup timeout, and it survived four controlled reproduction attempts.
Contention is a candidate trigger for it and nothing more; the guard refusing
earlier would have prevented the misattribution, not the observation.

## Consequence, stated plainly

The gate will now REFUSE to run while a sibling project is building or testing
on this box, where before it ran and produced failures that looked like ours.
That is the intended behaviour of the check as already written, now actually
reachable. It also means the three sessions sharing this machine need to take
turns for real — a two-party handover protocol was governing a resource with
at least three users.
