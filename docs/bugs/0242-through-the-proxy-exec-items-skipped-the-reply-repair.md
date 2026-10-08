# BUG-0242: through the proxy, a reply inside EXEC skipped the per-command repair (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `an_exec_item_is_repaired_as_its_command_is`
(`flint-proxy`) and the corpus cases "inside a transaction, JSON replies
keep their shapes" and "inside a transaction, BF replies keep their
shapes". The gate runs both through the proxy in both dialects, and
RedisJSON 8.2.8 and RedisBloom 8.2.8 also pass them.
**Severity:** medium. A RESP2 client through the proxy got a differently
shaped reply for three commands, but only inside MULTI/EXEC.

## What happened

Found while checking BUG-0239's BF.INFO change through a local proxy
(2026-10-08). A RESP2 client:

```
MULTI; JSON.TYPE j $.a; JSON.NUMINCRBY j $.a 1; BF.INFO b CAPACITY; EXEC
  seat:   [["integer"], "[3]", [10]]
  proxy:  [[["integer"]], [3], ["Capacity", 10]]
```

The proxy reads seats in RESP3. `repair_reply` puts a reply back into the
client's dialect where the two differ in more than rendering: JSON.TYPE's
extra RESP3 layer, NUMINCRBY's RESP3 array for its RESP2 text, and, with
BUG-0239, a one-field BF.INFO's one-pair map. It ran on a command's reply,
but EXEC's array only had its nulls re-typed, so each queued command's item
came through unrepaired. The first two were wrong since those repairs were
written. The third would have shipped with BUG-0239, whose gate was stopped
to fold this in.

The corpus missed it because no case put these commands in a transaction,
and the runner's RESP3 fold had the same gap: it folded by the top-level
command, which inside a transaction is EXEC.

## The fix

The proxy keeps, for each queued command whose reply needs a repair, the
command itself, and EXEC applies the repair to its item (`exec_item`). The
corpus runner keeps the commands queued since MULTI and folds each EXEC item
as that command's own reply is folded.
