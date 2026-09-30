# BUG-0191: a Raft control-plane node whose store would not parse started empty (FIXED 2026-09-27)

**Status:** **FIXED 2026-09-27**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** high: a node silently forgot every tenant, pair and vote.

## What was measured

Found reading the control plane for ADR-0053, then measured by a unit test
against the code as it was. A store this build wrote was cut in half, and
then replaced by one naming a record the build does not know. Both times
`Store::open` returned a store of registry version 0, not a refusal:

    let inner: Persisted = std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();

## Why it matters

Any failure to read or parse the file became `Persisted::default()`: no
vote, no log, no registry. The node then ran as though it were new. Two
ordinary events produce such a file:

- **A rollback.** A newer release that adds a `Mutation` variant writes
  records an older binary cannot parse. Rolled back, the older binary read
  the whole store as empty. This is why ADR-0053 adds no new variant.
- **Damage.** A file damaged on disk, or edited by hand.

A Raft node that forgets its vote can vote twice in one term, and one that
forgets its log can be elected by a peer that has forgotten the same.

The single-node control plane already refused to start on an unparseable
registry (`State::load_or_new`, with the reasoning in its comment). The Raft
store was written without that care.

## The fix

A missing file is still a new node. A file that cannot be read, or will not
parse, stops the node with a message saying why and what to do: restore it
from a backup, or run the release that wrote it. It does not suggest
starting on an empty store, for the reason above.

This protects releases from this one on. A binary already shipped still
loads such a file as empty, so a rollback to one must not meet a store
holding records it cannot read. For a new `Mutation` variant that means two
releases: the reader first, the writer later.

Covered by two unit tests in `raft.rs`: a missing store is a new node and a
written one loads, and a truncated or unknown-record store stops the node.
The second failed before the fix.
