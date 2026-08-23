
## 2026-08-23 — the caller side is now enumerated, and it is clean

The deferral above says reinstating rejection "needs the caller side
enumerated too, which means every direct `flint-server` invocation in
`tools/` — and ideally the spurious flags removed there first, so rejection
cannot break anything by construction."

Enumerated. **142 genuine `flint-server` invocations across `tools/`, and
every flag any of them passes is one the binary reads.** The 34 `arg()` call
sites plus the five early-exit flags (`--help`, `-h`, `--version`, `-V`,
`--build-version`) cover the caller side completely.

`--advertise`, the flag that broke the previous attempt, is gone:
`slot_map_drill.sh` now passes it to `$PX`, the proxy, which is the binary
that reads it. Nothing removed it for this reason — it was corrected in the
normal course of work — which is worth knowing, because the blocker recorded
here expired without anyone noticing.

**So the prerequisite is met and rejection is implementable.** It is not
implemented here: the last attempt hung two gates, so it wants its own change
with a full gate behind it, not a paragraph in a bug note.

### The enumeration was wrong four times first, and every version looked right

Recorded because the result is an EMPTY list, and an empty list is the shape
that lies:

1. **Line-level matching** scored every flag on any line mentioning
   flint-server. Reported `--release` and `--features` in 59 files: those are
   `cargo build --release --features rocks -p flint-server`.
2. **Binary-as-argument** was not excluded, so `--server-bin
   ./target/release/flint-server` counted as an invocation.
3. **`$B` matched inside `$BK`.** `backup_drill.sh:33` sets
   `B=...flint-server` and line 34 sets `BK=...flint-backup`, so every
   `flint-backup` call was scored as a seat invocation and its flags —
   `--pairs`, `--to`, `--from`, `--into`, `--snap-root` — reported as spurious.
4. **`flint-server` matched inside the feature spec `flint-server/rocks`**, so
   `cargo clippy --workspace --all-targets --keep-going` contributed
   `--keep-going`.

Three of those four are the same defect: an unanchored match on a name that is
a substring of another name. It is the third form of it found in this
repository this week.

**The clean result is only worth anything because of the control printed
beside it**: of 142 invocations found, 121 pass `--port`. A scan that had
silently stopped matching would also report zero spurious flags, and the two
outcomes are identical on the page.
