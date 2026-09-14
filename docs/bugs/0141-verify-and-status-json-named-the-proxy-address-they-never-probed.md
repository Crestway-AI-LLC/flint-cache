# BUG-0141: `verify` and `status --json` named the proxy address they never probed (FIXED 2026-09-14)

Status: **FIXED 2026-09-14**, found the same day ·
Severity: **medium** — no fleet misbehaves; what is wrong is what an operator
and a machine consumer are told. `status --json` is the worse half: its
`"addr"` is parsed by things that cannot ask a follow-up question, and it
named an address nothing had tested.

**This is [BUG-0139](0139-cp-host-named-a-machine-that-placement-ignored.md)'s
own miss**, which is itself BUG-0138's. 0139 found the `status` proxy row
reporting `inv.proxies[i]` beside an up/DOWN that `proxy_up` had decided
against `proxy_dial`, fixed that row, and did not go looking for the same
shape anywhere else. It is in two more places.

## The two sites

    verify_checks    note(proxy_up(inv, i), "proxy up", p.clone())
    status_json      "addr": json_str(proxy),  "up": proxy_up(inv, i)

In both, `proxy_up(inv, i)` probes `proxy_dial(inv, i)` — the advertise
address when declared, else `proxy-host`, else the bind line — and the
address reported beside it is `inv.proxies[i]`, the bind line. On the
playground those differ: the probe reaches `try.crestwayai.com:7379`, the
name carrying the tenant-facing DNS and the edge certificate, and the report
says `0.0.0.0:7379`.

So `verify` prints a green line about an address it never touched, and a
consumer of `status --json` reading `{"addr": "0.0.0.0:7379", "up": true}`
has no way to learn that the truth of `up` belongs to a different string.

## The third time, and what finally caught it

BUG-0110 fixed the proxy DIAL. BUG-0139 fixed the `status` display. This is
`verify` and the JSON. Each fix was correct; each was scoped by reading the
function in front of me rather than by enumerating the field's uses — the
same failure BUG-0138 diagnosed and BUG-0139 repeated.

`tools/cp_dial_sites_drill.sh` from BUG-0139's follow-up is now
`tools/bind_dial_sites_drill.sh` and covers `inv.proxies` as well as
`inv.cp`, with a per-field exempt set. **It found both of these sites
immediately**, and two more things worth recording:

- **A fix I believed I had made was not in the tree.** An earlier edit script
  aborted on a bad anchor *before* writing, so the `status --json` change was
  silently lost. The build still passed, because nothing about that line has
  to compile differently. The drill caught my own regression, on the same day
  I wrote it, which is the most direct argument for it I can give.
- **`launch`'s second proxy pass** held the bind line only to hand it to
  `proxy_down_help`, whose message — *proxy `<bind>` (dialled at `<dial>`)* —
  is the one place the difference is the subject. That helper now derives
  both forms itself, so no caller has to hold the bind line to build it.

## Also here: one spelling of the proxy seat name

`format!("proxy-{}", port_of(proxy))` appeared in four places, two of which
are the spawn in `launch` and the stop in `roll_edge`. They agreed. The CP
has `cp_seat_name` precisely because its two spellings did not, and that
comment records the cost: a stop that found nothing, reported the process
already gone, then failed `wait_port_free` because the real seat was alive
and holding the port. `proxy_seat_name` (and `proxy_port` beneath it) is that
lesson applied before it is paid for again.

## Not established

- ~~Whether `verify`'s other rows have the same shape.~~ **Examined since,
  and they are clean.** The proxy row is fixed and the CP rows go through
  `cp_dial`; the pair rows report what they dialled — `down.push(addr.clone())`
  pushes the address the probe used — so they are self-consistent. The
  structural reason is worth keeping: a `pair` line has to carry real
  addresses to distinguish its two members, so unlike a single `cp`,
  `proxy` or `coproc` line it cannot degenerate to a wildcard.
- The drill covers `inv.cp` and `inv.proxies`. `inv.coprocs` is the third
  bind-and-dial field and is [BUG-0140](0140-the-coproc-line-is-a-bind-address-handed-to-every-proxy.md);
  adding it here would flag a defect nobody has decided how to fix, so it is
  left until that decision is made.
