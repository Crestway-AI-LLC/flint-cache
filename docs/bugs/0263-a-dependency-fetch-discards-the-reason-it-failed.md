# BUG-0263: a client_compat dependency fetch discards the reason it failed, and a skip reddened a gate with only a guess as evidence

**Status:** **FIXED 2026-10-16** — all sixteen fetch sites keep their reason,
and `client_compat_fetch_reasons_drill.sh` holds that against the file. One
question is deliberately left open below, because it is a decision and not a
patch: whether the gate box should be able to fetch at all.
**Severity:** low in the product, real in a verdict — it cost the reading of
one EC2 gate and would cost the next.
**Area:** `tools/client_compat_drill.sh`.

## What happened

The public `drills` gate of `83d4a2fe` (2026-10-16, box
`i-01d58e133ec935dc0`) came back `GATES FAILED: client_compat(skipped)` after
157 steps. The drill's own last line was

    PASS: client compatibility — redis-py, rq, celery, limits, python-redis-lock,
    node-redis, ioredis, node redlock, rate-limit-redis, rate-limiter-flexible,
    bullmq, go-redis, redsync, redis_rate, rails-cache-store, redlock-rb,
    sidekiq, sidekiq server

Eighteen clients passed. One was skipped, and the whole evidence for why was:

    == asynq: SKIP (could not fetch the module; offline?)

**With a question mark.** The fetch ran under `>/dev/null 2>&1`, so the run
held no way to tell offline from a wrong version from a blocked module proxy
from a timeout. A gate verdict rests on it, and `gates.sh` treats a skipped
client as a failure deliberately — correctly, because a silent skip deletes
coverage from a run someone is about to trust.

## The cause is not recoverable, and three guesses at it were wrong

The first version of this write-up named a cause: asynq entered the drill
around 2026-10-09, after the gate AMI was baked, so its was the one Go module
not cached on a box with no egress. **That was inferred from which clients
survived, and testing it off-box ruled out every part of it.**

| guess | tested against | result |
|---|---|---|
| the box has no egress | the gate box gets a PUBLIC IP (`run.sh` reads `PublicIpAddress`, so a public subnet with an IGW route) and its security group authorizes INGRESS only, leaving AWS's default allow-all egress in force. Provisioning itself runs `dnf install` and `pip3 install -q redis` over the network | **ruled out** |
| the Go toolchain is too old for asynq | `asynq@v0.26.0` declares `go 1.24.0` — but `redsync@v4.18.0` declares `go 1.26.0` and RAN in that same box run, so the toolchain there was at least 1.26 | **ruled out** |
| the module or the pin is bad | `go get github.com/hibiken/asynq@v0.26.0` fetches cleanly off-box, pulling four deps | **ruled out** |

Two of those three were ruled out from the run's OWN pulled log, which does
survive: `== client: redsync v4.18.0, on go-redis v9` is in it, and that
module declares `go 1.26.0`, so the box's toolchain was at least 1.26 and
asynq's 1.24 was never the problem. `activesupport 8.1.4 redis-rb 6.0.0` ran in
the same file, three lines after the asynq skip.

What is left — a transient fetch failure, a rate limit, a proxy hiccup,
something asynq-specific — cannot be chosen between, because the drill
discarded the one line that would have said. The log has everything except
that.

**That is the actual cost of this bug, and it is larger than "a vague
message".** A plausible, confidently-written, WRONG cause went into a bug
file and an index row, and would have gone into the decision about re-baking
the AMI. Being able to reconstruct a story from surrounding facts is not the
same as having evidence, and the two feel identical from the inside.

### "offline?" does not merely fail to help — it points away

Running this drill on macOS skips the three Ruby clients. The old message would
have read `could not install activesupport and redis; offline?`. The captured
reason, measured 2026-10-16:

    ERROR:  Error installing activesupport:
    	securerandom requires Ruby version >= 3.1.0. The current ruby version is 2.6.10.210.

The system Ruby, not the network. A reader chasing "offline?" would have
checked connectivity, found it fine, and been left with nothing — which is
roughly the afternoon this bug cost.

## Two corrections to this write-up, both found by fixing it

**The count was wrong: sixteen sites, not twelve.** The first pass grepped
`go get` and `npm install`, so it reported 5 npm + 6 go. `pip install`,
`gem install` and `-m venv` have the identical shape and were missed — four
Python sites, three Ruby. A pattern narrower than its set, counted as if it
were the set.

**And the fix as first conceived would not have worked on eight of them.**
Removing `>/dev/null 2>&1` and capturing the output is only half of it, because
the fetch's own quiet flag throws the reason away too. Measured on
2026-10-16 against a package that does not exist:

| fetch | on failure, captured | verdict |
|---|---|---|
| `npm install --silent` | **0 bytes** | silenced, not quiet |
| `npm install --loglevel=error` | 492 bytes, incl. the 404 | use this |
| `gem install --no-document --silent` | **0 bytes** | silenced, not quiet |
| `gem install --no-document` | 83 bytes, names the gem | drop `--silent` |
| `pip install -q` | 451 bytes, keeps `ERROR:` | safe, kept |
| `python -m venv`, `go get` | full stderr | no quiet flag to fix |

So five npm and three gem sites would have been "fixed" into printing
`(nothing -- the fetch produced no output at all)`. A fix that looks applied
and is not is worse than the bug, because the next reader stops looking. The
helper now says *silenced, not quiet* for an empty capture, so that state names
itself rather than reading as a clean skip.

## Fixed, and not

**Fixed, all sixteen sites.** Each captures combined output into a variable and
hands it to one helper, `skip_with_reason`, which quotes the last six lines
indented. npm's `--silent` became `--loglevel=error` and gem's was dropped, for
the reason measured above. Behaviour is otherwise unchanged: a missing
dependency still skips, and the gate still reds — the run now says why.

Readiness **probes** keep `>/dev/null 2>&1` on purpose. `rq_ready`,
`celery_ready` and the rest answer a question; their failure is the answer, not
an error, and capturing it would print a stack trace as though something had
gone wrong.

**Held by a check, offline.** `tools/client_compat_fetch_reasons_drill.sh`
(CORE) asserts three rules against the drill: no fetch discards its output, no
npm or gem fetch carries `--silent`, and no fetch-failure skip is printed by a
bare `echo`. Each rule is run against a deliberately broken copy and must name
the break — and that caught a real hole: the first draft looked for `go get`
and `pip install`, spellings this file never uses (it calls `"$GO" get` and
`"$VENV/bin/pip" install`), so rule A was silent on the real file for the same
reason a clean file is silent. It also counts the fetch lines the pattern can
see, and fails if that drops below 14, so the rule cannot narrow until it
examines nothing. It runs no fetch, so it passes on the box that could not
fetch.

## Settled 2026-10-16 by the next gate run: the fetch was transient

The gate run of `8bbe5aba` came back **GATES PASSED — 158 steps**, with
`PASS client_compat (210.8s)` and no skip. `gates.sh` reds on a skipped client,
so a full pass means **asynq fetched**. Both runs launched from the same baked
AMI, `ami-075ef8483550c9693` — box `i-01d58e133ec935dc0` then,
`i-0756a41a0305e5f15` now, about ninety minutes apart. Nothing about the module,
the pin, the toolchain or the AMI's cache changed between them, and the only
change to that code path was the error capture itself.

So the box can fetch asynq, which closes the three guesses properly: it was a
transient failure at fetch time.

### The decision, with the measurements behind it

**Do not pre-seed the clients' dependencies at bake time.** It looks like it
removes the run-time dependency and does not: any client added after the last
bake still needs a working fetch, so the dependency is hidden rather than
removed, and a re-bake silently becomes a prerequisite for adding a client —
which is the coupling nothing states today and which this bug's first,
wrong write-up assumed already existed.

**Do not add a retry layer either.** Measured 2026-10-16, these fetchers
already retry:

| fetcher | built-in behaviour on a failed fetch |
|---|---|
| npm | `fetch-retries` = **2**, `fetch-retry-maxtimeout` 60 s |
| pip | `--retries` = **5** |
| go | `GOPROXY=https://proxy.golang.org,direct` — falls back to the origin |

A failure that defeats two npm retries, five pip retries, or a proxy-then-direct
fallback is not one bad second at a registry. Wrapping another loop around that
would make a legible failure slower and less legible, and it would be built on
a cause nobody has read.

**The diagnostic IS the fix.** The next occurrence prints the reason instead of
"offline?", and the choice between a pin, a retry and a pre-seed can be made
from it. Choosing now would be the same guessing this write-up was corrected
for.

**Still open, but the question changed once the guesses were tested.** It was
posed as "AMI carries the deps, or the launcher ships them, or the box gets
egress". The box already HAS egress, so the third option is the status quo, and
the real question is whether it should be: a gate that fetches from npm, PyPI,
RubyGems and proxy.golang.org at run time can red because a registry had a bad
minute, and reliability is the first principle here. Pre-seeding every client's
deps at bake time removes that, at the price of making a re-bake a prerequisite
for adding a client — which nothing states today, and which is the one part of
the original story still worth acting on. Not decided here: the skip now names
its own cause, which is what makes the choice an informed one instead of a
guess, and that was this bug.

**One thing this exposed in the private ops repo, and it is not what it first
looked like.** The log was briefly written up here as not kept at all. It is
kept: `run.sh` tees the run on the box and pulls it to
`/tmp/flint-gate-box-logs-$(date +%s)`, and 75 such directories are on this
laptop going back to 2026-09-19. What is missing is any POINTER to it — 648 of
649 `docs/gate-ledger.jsonl` rows carry `log: null` — so a run's own evidence
is found by grepping `/tmp` for a string you remember from the output, in a
directory named by epoch seconds, before the next reboot clears it. Filed
separately against ops.
