# BUG-0260: a failed tenant wipe could not be finished, and the namespace's next tenant read the keys (FIXED 2026-10-12)

**Status:** **FIXED 2026-10-12.** `tenant add` refuses a namespace that still
holds keys and no tenant. A new `flintctl tenant wipe <ns>` finishes a
removal whose wipe failed. `tenant remove` keeps a namespace another tenant
shares.
**Severity:** high where it applies, and no case is known. It took two
things:
- a pair that could not be reached while a tenant was being removed;
- the same namespace then given to a new tenant.

That tenant read the removed tenant's keys. The playground names a tenant's
namespace after the tenant (`playground-approve`), so approving a removed
name again was the path there. No case could have been seen before ops
OPS-0390 (2026-10-12), which first kept the reaper's flintctl output. Found by
ops (OPS-0392) while writing a runbook entry.

## What happened

`tenant remove` ran four steps:
1. look up the tenant's namespace (`CPTENANTS`), and die `no such tenant` if
   there is none;
2. delete the tenant record (`CPDELTENANT`);
3. on each pair master, `FLINTNS <ns>` then `FLUSHALL`, printing
   `WIPE FAILED (re-run)` when that failed;
4. ask each co-processor to forget the namespace (BUG-0255).

Its doc comment said a re-run is safe. But a re-run stops at step 1, because
the record step 2 deleted was the only thing naming the namespace. No other
flintctl command wiped a namespace, so the hint could not be followed.

Step 3 could also not report what it promised. A pair with no reachable
master made the whole command `die` there, with no `WIPE FAILED` line, part-way
through the pairs and before any co-processor was asked.

`tenant add` asked the co-processors to forget the namespace (BUG-0255) but
never looked at the pairs. A removed tenant's keys were the next tenant's.

**Found while fixing it.** The control plane lets tenants that are not placed
share a namespace. Removing one of them wiped the namespace anyway, deleting
the keys of the tenant that still used them.

## Fixed

- **`tenant add` and `tenant add-on-pair`** ask each pair master for its
  `DBSIZE` in the namespace first. They refuse while one holds keys, has no
  master, or cannot count. The refusal names `tenant wipe`. They do not wipe
  on their own: whether the keys are wanted is the operator's call.
  - A namespace that a tenant holds is shared on purpose, and is not
    checked. Its keys are that tenant's.
- **`flintctl tenant wipe <ns>`** wipes a namespace that no tenant holds: its
  keys on every pair master and its index on every co-processor. It refuses
  a namespace a tenant holds. It exits non-zero until every pair and
  co-processor has confirmed, so it can be run until it succeeds.
- **`tenant remove`** reports a pair it cannot reach as `WIPE FAILED`, with
  what each member answered, and goes on to the other pairs and the
  co-processors.
  - Its last line names `tenant wipe <ns>` when anything failed. It still
    exits 0, because the tenant is removed; ops' playground reaper reads the
    `WIPE FAILED` line.
  - A re-run that finds no tenant names `tenant wipe`.
  - It keeps a namespace that another tenant still holds.

## Verification

`tenant_remove_drill` covers each case, using a second inventory whose one
pair is at a port where nothing listens. The control plane answers through
it, and the pair does not.
- **A removal whose wipe fails** exits 0 and prints `WIPE FAILED` and the
  remedy. The token is dead, and the master still holds the 20 keys.
- **A re-run of the removal** fails with `no such tenant` and names
  `tenant wipe`.
- **`tenant add` on the namespace** is refused while the keys are there
  (`still holds 20 key(s)`), and refused through the dead pair (`has no
  master to say`). The refused token reads nothing.
- **`tenant wipe`** of a live tenant's namespace is refused, and that
  tenant's 50 keys are untouched. Through the dead pair it exits non-zero.
  Then it wipes, the master's `DBSIZE` is 0, and the namespace is added again
  empty.
- **A namespace two tenants share**: removing one prints `kept`, and the
  other still has its 50 keys.
- `tenant_namespace_tests`: the `CPTENANTS` parse, and which tenant holds a
  namespace.
- **Controls**, each failing the drill at its own step:
  - flintctl as it was exits 1 on the removal whose wipe fails;
  - a `tenant add` that skips the pairs lets the add over the 20 keys
    succeed;
  - a `tenant remove` that ignores a sharer wipes the shared namespace.
- `ctl_error_drill`, whose `tenant add` against a control plane that is not
  there must still fail as `tenant add failed`, passes.
