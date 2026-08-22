# BUG-0043: `FLINTCONFIG lag-hard-ms` reports success and applies a different value

Status: FIXED 2026-08-22, same day · Severity: low as a defect, higher as a
measurement hazard — the knob is used by drills to arm controls, and a knob
that lies about its value makes every threshold-ramp above it meaningless.

## What happens

`FLINTCONFIG lag-hard-ms 200` against a seat at the shipped defaults returns
success, and the seat then reports `lag_hard_ms:500`.

`repl_hub.rs:133`:

    pub fn set_lag_hard_ms(&self, v: u64) {
        let clamped = v.max(self.lag_soft_ms.load(Ordering::Relaxed));
        self.lag_hard_ms.store(clamped, Ordering::Relaxed);
    }

The shipped soft cap is 500, so any hard cap below 500 becomes 500. The
caller is told nothing.

**The clamp itself is correct and should stay.** A hard cap below the soft one
is incoherent: the soft gate would fire after the hard one, which is not an
ordering the code can honour. The defect is the silence, not the arithmetic.

## Why this is worse than it looks

It was found by `roll_shed_drill.sh`, whose positive control walks the cap
down — 200, 50, 10, 2, 1 — until a roll sheds. The drill reads the value back
after setting it, and failed with:

    FAIL: FLINTCONFIG lag-hard-ms 200 did not take (seat reports 500)

Without that read-back the ramp would have set 200, been given 500, measured
at 500, found no shed, and tightened to 50 — also 500 — and so on down to 1.
**Five distinct thresholds, one actual value, and a confident conclusion that
the property held at all of them.** The drill would have reported a green
positive control having never armed anything, which is precisely the failure
BUG-0042 documents for `controller_ha`.

That is the general hazard: a knob used to arm a control must be verified to
have moved, or the control's whole range collapses to one point without
saying so.

## The same repository already decided this the other way

`WriteQueue::set_soft_cap` (`crates/flint-server/src/write_queue.rs`) takes
the opposite choice deliberately: asked for a soft cap above the hard channel
capacity it REFUSES rather than clamping, because an operator who asked for a
number and got a different one has no way to find out. Two adjacent runtime
knobs, opposite conventions, and the refusing one is right.

## Remedy

Return an error naming the effective floor — `-ERR lag-hard-ms 200 is below
lag-soft-ms 500; lower lag-soft-ms first` — so the operator learns the
ordering constraint at the moment it bites. Setting both in one command would
also work and is a larger change.

`set_lag_soft_ms` has the mirror behaviour (`repl_hub.rs:127`): raising soft
above hard silently raises hard. Same treatment, same reason.

Until then, callers must set `lag-soft-ms` first and read the value back.
`roll_shed_drill.sh` does both, with the reason written at the call site.

## Fixed

The FLINTCONFIG handler now refuses an incoherent pair and names the fix:

    ERR lag-hard-ms 200 is below lag-soft-ms 500; lower lag-soft-ms first
    ERR lag-soft-ms 800 is above lag-hard-ms 500; raise lag-hard-ms first

**The clamp in the setters stays, deliberately.** The two paths want opposite
things. A config file with an incoherent pair should still boot the fleet on a
coherent one rather than refuse to start; an operator typing a value at
runtime needs to learn that it was not the value applied. So the refusal lives
in the interactive handler and the clamp remains the safety net beneath it.

The consequence for callers is that the ORDER now matters and reverses with
direction: lowering the pair sets soft first, raising it sets hard first,
because only one end moves per command and the pair must stay coherent
throughout. `roll_shed_drill.sh` does both and says why at each site.

Control 5 of that drill is the regression test: it asks for a hard cap one
below the soft cap, requires an error mentioning `lag-soft-ms`, and then
re-reads the cap to confirm the refused command moved nothing. The second half
matters as much as the first — a handler that errors AND applies the value
would pass a test that only checked the reply.

## Related

- BUG-0035 — the drill this was found by, and the roll-shed question it serves
- BUG-0042 — a control that passed for four runs without ever arming
