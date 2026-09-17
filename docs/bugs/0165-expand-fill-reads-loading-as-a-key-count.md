# BUG-0165: `expand_fill` reads `-LOADING` as a key count (FIXED 2026-09-17)

Status: **FIXED 2026-09-17**, found the same day when it reddened a gate run ·
Severity: **low as a defect, medium as a gate** — the product is fine; a drill
that fails for a harness reason costs whoever finds it a diagnosis, and this one
reports a product-shaped sentence while doing it.

## What failed

```
== expand: a second pair joins holding NOTHING
FAIL: the joining pair is not empty (LOADING Flint is loading the dataset in memory keys) — a fill would prove nothing
```

The message reads as a product fault: a pair that was supposed to be empty
holds keys. It is not. `LOADING Flint is loading the dataset in memory` is the
node's error reply, captured into `EMPTY` by

```sh
fleet_wait_listen $P1
EMPTY=$(valkey-cli -p $P1 DBSIZE)
[ "$EMPTY" = "0" ] || { echo "FAIL: the joining pair is not empty ($EMPTY keys) ..."; exit 1; }
```

`fleet_wait_listen` waits for the socket to ACCEPT. Since #176 a node binds and
answers from INSIDE its load — deliberately, so a starting seat is not mistaken
for a dead one — so the accept proves nothing about `DBSIZE`.

## The fifth spelling of a mistake the library already names

`tools/lib/fleet.sh` says it outright, in `fleet_wait_ping`'s own comment:
#176 "silently converted 50 callers of this function from *wait until ready* to
*wait until alive*, and everything after them began racing a node that answers
data commands with `-LOADING`" — four drills reddened main one at a time, "on
four different spellings of the same mistake — PONG alone, a bind, a non-empty
field, a fixed sleep". This is the fifth: a bind, followed by a data command.

## The fix

`fleet_wait_ping $P1`, the ready predicate, in place of `fleet_wait_listen`.
One line, in the drill, because the library is already correct.

## The sweep, recorded rather than performed

Twenty-four other `fleet_wait_listen` sites across the drills are followed
within a few lines by a data command or a fixed sleep. **None of them has been
measured failing**, and they are not being rewritten here: a blanket conversion
would also hit `loading_visible`, which has to catch a replica *while* it
reports `loading:1` and whose whole subject the ready-wait would delete.

They are listed here so the next occurrence is recognised as a family rather
than diagnosed from scratch:

`backup`, `controller`, `failover` (x4), `internal_mtls` (x3), `lease` (x2),
`loaded_promote` (x2), `min_replicas`, `promote_notice`, `proxy_backpressure`
(x2), `read_under_stall`, `rw_isolation`, `slot_moved`, `tenant`,
`txn_failure`, `widowed_grace` (x2).

The ones that pair a bind with a fixed sleep are the interesting half: they
pass because the sleep happens to cover the load window on an unloaded box,
which is a property of the box rather than of the drill.
