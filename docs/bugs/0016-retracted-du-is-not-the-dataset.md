# BUG-0016: RETRACTED — `verify` was not lying; `du` is not the dataset

Status: **RETRACTED 2026-08-18, same day it was filed.** Kept rather than
deleted because the mistake is more instructive than the bug would have been,
and because the product makes the same one (see BUG-0017).

## What I claimed

That `flintctl verify` reported `VERIFY OK` on a playground pair whose replica
held 1% of the data — master 887 MB against replica 8.3 MB, 107x apart — and
that acting on that OK would have failed `try.crestwayai.com` over onto a
near-empty seat. Severity: high.

## Why it was wrong

Both numbers came from `du` on the RocksDB data directory. That directory is
dominated by RocksDB's **info LOG** — its human-readable diagnostic log, not
data. Measured properly:

    node-7002 (master)   sst 264K (4 files)   live WAL  28K
    node-7001 (replica)  sst 432K (4 files)
    node-7002 LOG.old.1787029335642650        813 MB

The "887 MB master" was 813 MB of debug log plus 71 MB of a second rotated
log. The replica I called hollow held **more** SST bytes than the master. The
pair was in sync the whole time. `verify` was right and I was measuring
RocksDB's chatter.

The disconfirming evidence was in my own hands an hour before I filed this: I
had recorded "2 sst" on the master and "4 sst" on the replica and read straight
past it. Comparable SST counts against a claimed 107x data gap should have
stopped the filing.

## What is left of the claim

The narrow code fact — `verify` establishes that a replica is STREAMING and
never that it HOLDS anything — is probably still true, and a hollow-replica
check would still have value. But it is now an untested hypothesis with no
observed failure behind it, so it does not get to be a high-severity bug. If
someone wants it, it needs a constructed reproduction (hollow a replica's data
dir while leaving its cursor current) rather than this write-up's evidence.

## The lesson, which does generalise

**`du` on a data directory is not a measure of a dataset**, and every consumer
that treats it as one inherits this error. The product does exactly that — the
disk guard, the capacity model and the metering all read directory size — which
is BUG-0017, and a much better bug than the one I filed.

Cost of the mistake: adeploy, a re-seed that was not needed (harmless — it
left a clean copy), and a high-severity bug report built on a directory listing.
Nothing was lost, because the check that would have caught it — compare the
thing, not a description of it — is the same check this file exists to argue for.

## Related

- BUG-0017 — the real finding: unbounded RocksDB info LOG, and the measurement
  error it causes in the guard, the capacity model and the meter
- `0002-verify-ok-on-single-copy.md` — the actual verify bug, from a real
  observation
