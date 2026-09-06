# BUG-0099 — `node-env` accepts a name no seat reads, and says nothing

Status: FIXED 2026-09-05 · found while reading BUG-0013's tuning proposal,
which names `flintctl node-env` as the delivery channel for the engine knobs ·
Severity: low, and the low severity is the problem — the failure is a tuning
that quietly does not happen, on a fleet where the operator believes it did.

## What happens

`node-env K=V` is the only way to reach the engine's compaction knobs on a
managed fleet: `flintctl` spawns the seats and the operator never touches a
command line. The parser accepts any non-empty key:

```rust
"node-env" => {
    if let Some((k, v)) = val.split_once('=') {
        let (k, v) = (k.trim(), v.trim());
        if !k.is_empty() {
            inv.node_env.push((k.to_string(), v.to_string()));
        }
    }
}
```

So `node-env FLINT_BG_JOB=4` — one character short of the real name — is
accepted, written into the inventory, forwarded to every seat local and
remote, read by nothing, and reported nowhere. Nothing fails. The operator has
changed a compaction knob in their head and not on the fleet, and the only
symptom is that the tuning they came to apply does not appear in the numbers.

That is the shape this codebase keeps meeting from the other side: **an
operator control that reports nothing because it was never wired.** Here it is
wired, and the NAME is wrong, which is indistinguishable from outside.

## Why it matters here specifically

BUG-0013 spent three weeks measuring these knobs and ends with a pairing an
operator is meant to apply per seat, above a bracketed crossover. Every one of
those applications goes through this parser. A silent typo does not produce a
wrong number — it produces the BASELINE number, which looks like the tuning
was tried and did not work.

## Fix

A warning, listing the names a seat actually reads. Three deliberate limits:

- **A warning, not a refusal.** `node-env` is an escape hatch by design —
  "extra environment for every flint-server seat" — and an operator may want
  `RUST_LOG`, or something a future build reads. The list is a snapshot of
  another crate's behaviour, so being wrong about it must cost a spurious line
  and never a rejected command.
- **`FLINT_`-prefixed names only.** A non-Flint name is plainly deliberate, and
  warning about `RUST_LOG` would train the operator to ignore the line.
- **No pairing rule.** BUG-0013's recommendation says `FLINT_LEVEL_BASE_MB=64`
  *with* `FLINT_BG_JOBS=4`, "never one alone", and a warning enforcing that was
  drafted and dropped: the same file measures `bg_jobs=4` ALONE at **+93%** on
  a 96 GB seat. The warning would have fired on a configuration its own
  evidence supports. The pairing is advice about a regime, not a rule about a
  command.

## What keeps the list honest

`SEAT_ENV_NAMES` lives in flintctl, which does not link `flint-storage` or
`flint-server` — it spawns them. `node_env_names_match_the_seat` scans their
SOURCE and fails in BOTH directions: a name a seat reads that the list omits
(flintctl would warn about a working knob), and a name the list carries that no
seat reads (a typo of it would pass unwarned). Mutation-checked both ways.

A source scan is a weak instrument, chosen because the alternative is a list
that goes stale silently — and silence is the entire defect being fixed.

**The capability assert earned its place on the first run.** The scan joined
`CARGO_MANIFEST_DIR` with `src` before the relative path and resolved to
`crates/flint-ctl/flint-storage/src`, which does not exist. It read zero files
and would have certified any list; the assert that the scan must find at least
eight names is the only reason that was caught rather than shipped.
