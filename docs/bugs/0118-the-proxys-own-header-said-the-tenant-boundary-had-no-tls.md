# BUG-0118 — the proxy's own header said the tenant boundary had no TLS (FIXED 2026-09-06)

**Status: FIXED 2026-09-06** · Severity: medium — no code is wrong; a
customer-visible source header contradicted the product and another document
in the same repository.

## What it said

`crates/flint-proxy/src/main.rs`, module header, unchanged since v0:

    //! v0 scope, deliberately deferred: TLS, metering, cross-slot
    //! scatter-gather (multi-key commands route by FIRST key), hot-key
    //! absorption, RESP3, inline commands.

Six items. **Four of them shipped.**

| claimed deferred | actual state | where it is |
|---|---|---|
| TLS | **shipped** | `--tls-cert`/`--tls-key` build a `ReloadableServerConfig::watch_edge`, snapshotted per connection so rotation needs no restart; `chaos_edge_tls`, `edge_ca_trust` and `edge_roll` drill it |
| metering | **shipped** | `commands_read_total` / `commands_write_total` in PROXYSTATS; the agent's `metering.rs` bills from them and `billing_reconcile_drill` holds the reconciliation at 0.00% drift |
| RESP3 | **shipped** | negotiated per connection with `HELLO`; `docs/command-support.md` §"Protocols" documents it and says why it is not cosmetic |
| inline commands | **shipped** | parsed and bounded by `MAX_INLINE_LEN`; `verify` asserts "inline command accepted" and `client_compat` covers it |
| cross-slot scatter-gather | still deferred | `key_of` returns the FIRST key; the comment at the routing site says so |
| hot-key absorption | still deferred | the sketch ships and is read by PROXYHOTKEYS, the exporter and the agent, and "never toggles behavior" |

## Why it matters more than a stale comment usually would

**It is the tenant boundary, and it said it had no TLS.** A self-hoster
reading the proxy's own source to decide whether tenant traffic is encrypted
gets the wrong answer from the most authoritative-looking place — the file that
implements it.

**And the repository contradicts itself.** `docs/command-support.md` has a
whole section explaining that RESP3 is supported, negotiated with `HELLO`, and
that it matters because *"redis-py 8 defaults to RESP3 and sends its
credentials inside the handshake"*. Two documents, one repository, opposite
claims, and the one that is wrong sits in the source.

This is the third instance of one shape this week — ADR-0030's `Status` line
saying `proposed` nine days after acceptance, the M4 lane saying the remote
runner was unbuilt three days after it was built, and now this. **A state
claimed in two places and maintained in one.** The claim always rots in the
place where nothing checks it, and "deferred" lists are exactly that place:
written once when the answer was obvious, never revisited, because nothing
fails when they go stale.

## How it surfaced

Not by reading the proxy. By auditing `docs/roadmap.md`'s M3 line, which reads
`metering/scatter-gather/per-tenant quotas remain` — per-tenant quotas have
shipped with two drills. Chasing that back to the source found a longer stale
list in the header the roadmap was echoing.

Worth naming as a method: **a "deferred" claim is testable, and the test is
cheap** — one grep per noun against the code that would implement it. Six nouns
took about ten minutes and found four wrong.

## The fix

The header now names the two that remain, with where the reader can verify
each, and records the four that shipped rather than silently dropping them —
someone who read the old list deserves to meet the correction. `key_of` and
the hot-key sketch are cited by name so the remaining two are checkable the
same way.

`docs/roadmap.md`'s M3 line is corrected in the same change.

## No check is proposed, deliberately

The honest general form — *does any prose claim something is deferred that the
code implements?* — needs to understand prose, and a keyword check over
"deferred"/"remains" would be a list nobody reads plus a count that moves for
the wrong reasons. What is genuinely checkable here already exists: bug index
markers, drill citations (`assert_docs_cite_drills_the_gate_runs`), and the
command matrix versus the guides. This one is caught by audit, and the audit is
written down above so it can be repeated.
