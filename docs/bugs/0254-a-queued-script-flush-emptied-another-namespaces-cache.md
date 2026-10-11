# BUG-0254: a queued SCRIPT FLUSH emptied another namespace's script cache (FIXED 2026-10-10)

**Status:** **FIXED 2026-10-10**. MULTI's queue-time probe answers as before
and no longer touches the script cache or the pub/sub broker.
**Severity:** low. Two effects, both inside namespace 0's bounded script cache:
- a queued `SCRIPT FLUSH` emptied that cache;
- a queued `SCRIPT LOAD` added its script there, which could push namespace
  0's own scripts out.

Keys were never touched, and neither was any tenant's own cache.

## What happened

Inside `MULTI`, a seat decides whether a command poisons the transaction by
running it against a throwaway store (`queue_time_error`, ADR-0012 D1, and
BUG-0181 for `CROSSSLOT`). The store is thrown away. The script cache is not:
it belongs to the process, keyed by namespace. The probing dispatcher was
built for the default namespace, "0", so for any connection:

    MULTI
    SCRIPT FLUSH             -> QUEUED, and namespace 0's scripts were gone
    SCRIPT LOAD "return 1"   -> QUEUED, and namespace 0 had cached it

A tenant's own namespace was flushed correctly at `EXEC`. Namespace 0 had
already been flushed at queue time, even when the transaction was then
discarded. Namespace 0 belongs to direct connections and pre-tenancy tools,
not to any tenant.

`PUBLISH` was never probed: the queue step checks its arity itself, because
it "reaches subscribers, whatever store it runs against". The same reasoning
covered the script cache, but no one had applied it there.

Found while building `redis.acl_check_cmd`, which needed the same probe to
ask "is this a command Flint serves?" without running it.

## Fixed

- A dispatcher can be a probe (`Dispatcher::probing`). A probe answers every
  command as it would, but leaves alone the two places a command reaches
  beyond the store: the script cache (`SCRIPT LOAD`, `SCRIPT FLUSH`) and
  the broker (`PUBLISH`).
- Both users of the probe now build one: MULTI's `queue_time_error`, and
  `served`, which `redis.acl_check_cmd` calls.
- `a_probe_leaves_the_script_cache_alone` caches a script in namespace 0,
  queues `SCRIPT FLUSH` and `SCRIPT LOAD`, and checks that the first script
  is still there and the second is not.
