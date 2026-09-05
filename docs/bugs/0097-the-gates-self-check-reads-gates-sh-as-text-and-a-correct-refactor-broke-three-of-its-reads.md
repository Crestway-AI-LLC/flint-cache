# BUG-0097: the gate's self-check reads `gates.sh` as text, and a correct refactor broke three of its reads (FIXED)

Status: **FIXED 2026-09-05** · found by triaging a red `main` · Severity:
medium — `gates.sh` is the gate, so this reddens every push until it is fixed,
and it reddens them for a change that is right.

## What was red

`4bcba0c` failed `GATES FAILED: gates` on `main`. That commit is correct: it
made `msrv` opt-in, splitting the stage list into

    DEFAULT_STAGES="check conformance drills chaos"
    OPT_IN_STAGES="msrv"
    ALL_STAGES="$DEFAULT_STAGES $OPT_IN_STAGES"
    …
    STAGES="${*:-$DEFAULT_STAGES}"

`gates_drill.sh` verifies `gates.sh` by **reading it as text**, and three of its
reads were written against the shape that line had before.

## The three, in the order they surfaced

| read | why it broke | now |
|---|---|---|
| `sed -n 's/^ALL_STAGES="\(.*\)"$/\1/p'` | the line holds `$DEFAULT_STAGES $OPT_IN_STAGES`, so the check reported the two **variable names** as the declared stages | evaluate every simple `NAME="…"` above `want()` and print `$ALL_STAGES`, so it follows any shape the declaration takes |
| `grep -oE 'want [a-z_]+'` | matched `local want scanned rest`, a variable **declaration** whose second name is `scanned` — reported as a stage dispatched but never declared | require a command position: `(^\|[;&\|] *\|if )want [a-z_]+`, which every real call has and a declaration does not |
| `$0 == "STAGES=\"${*:-$ALL_STAGES}\""` | the default is now `$DEFAULT_STAGES`, so the equality never held and the forced copy was never made | match the form, `^STAGES="\$\{\*:-\$[A-Z_]+\}"$` |

Each fix avoids naming the thing it was tripped by. A list of component
variables, or a `grep -v local`, would be one more thing to keep in sync with
the file being read — which is this bug again, one turn later.

## What the drill got right

**It failed closed every time.** The third read could have quietly produced an
unforced copy and asserted against an untouched script; instead the `forge`
function exits 3 and says so in as many words: *"the dispatch could not be
forced and the backstop below would have been asserted against an untouched
script."* Two of the three failures were that guard firing, not a wrong answer.

And the second read's complaint was the right complaint for a wrong input: *"a
stage in one and not the other is either a stage nobody can ask for, or one
that is accepted and then runs nothing."* That IS a real defect when it is
true. It was not true here.

## The general shape, worth keeping

A source-reading check is coupled to the source's syntax, not just its
meaning — so a refactor that preserves meaning can still break it, and it
breaks as a red gate blaming the refactor. The defence is to read for the
VALUE where that is possible (evaluate the assignment) and for the ROLE where
it is not (a call position, an assignment form), never for one spelling.

Same family as `assert_license_headers_are_this_repos`, whose needle is
assembled rather than written because a literal matches the check itself.

## Found by

`main` being red, and the ops repo's four consecutive reds sending me to look
at both gates.
