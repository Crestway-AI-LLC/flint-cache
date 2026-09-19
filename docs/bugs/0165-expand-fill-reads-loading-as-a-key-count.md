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

## The sweep: the census, and the three that were the same defect

**Corrected 2026-09-18. The first version of this section said "twenty-four
other sites" and that was an undercount presented as a census** — it came from
a grep that looked six lines ahead and skipped anything with a ready-wait in
view. Counted properly, `fleet_wait_listen` has **163 call sites**, classified
by the first non-comment line after them:

| what follows the bind | sites |
|---|---|
| a fixed `sleep` | 86 |
| something else (a second spawn, a log wait, a function definition) | 40 |
| a ready-wait already (`fleet_wait_ping`, `fleet_wait_log`) | 28 |
| a data command straight away | 9 |

**Three of the nine were this bug again** and are fixed here: `backup_s3` and
`backup_schedule` each pipe a corpus (300 and 100 `SET`s) into a freshly
spawned rocks node, with only `tail -1` looking at the reply — a `-LOADING`
refusal there is a short corpus and a confusing failure two assertions later.
`expand_fill`'s FIRST node had the same gap as the joining pair this bug was
filed for; its `PING == PONG` check cannot stand in for readiness, because
since #176 PONG is exactly what a loading node answers.

**The other six are not defects.** Four are `PING` loops against a proxy or
`flint-vec`, which have no loading state — `fleet_ready`'s own comment says a
server that does not implement `FLINTINFO` is ready as soon as it answers.
`coproc_exempt` and `family_route_cp` define a shell variable on the next line
and wait properly before using it.

**`loaded_promote` is deliberately left alone**, for the reason
`loading_visible` is: a drill whose subject is promotion *while loading* needs
the window a ready-wait would close. Converting it would be the same mistake as
a check that cannot fail, wearing the opposite hat.

**The 86 fixed sleeps stay, and stay recorded.** None has been measured
failing. Each is a bind followed by a number somebody chose, which is a
property of the box rather than of the drill — the honest description is that
they are unproven, not that they are wrong, and rewriting 86 drills on a
hypothesis is how a green suite becomes an unfamiliar one.

