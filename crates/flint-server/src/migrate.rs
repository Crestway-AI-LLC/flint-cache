// SPDX-License-Identifier: Elastic-2.0
//! Slot migration: the data-shipping, cutover, and per-slot gating half of
//! rebalancing (ADR-0004). Extracted from main.rs in the review-driven
//! module split — everything here shares the same unit (a (namespace, slot)
//! pair) and the same durable truth (the manifest CF's migration records).
//!
//! The lifecycle, source S -> destination D:
//!   1. D: FLINTMIGRATEIN pulls a bulk snapshot then the live tail
//!      (flintmigrateout on S ships both) — data flows, ownership unchanged;
//!   2. S: FLINTSLOTFREEZE sheds writes (-TRYAGAIN) while D drains the tail;
//!   3. S: FLINTSLOTMOVED records the terminal handoff -> -MOVED redirects;
//!   4. check_slot_gate enforces all of it on every keyed command.

#[cfg(feature = "rocks")]
use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "rocks")]
use std::sync::atomic::Ordering;

#[cfg(feature = "rocks")]
use flint_resp::{Decoded, decode};
use flint_resp::{Value, encode};
use flint_storage::Kv;
#[cfg(feature = "rocks")]
use flint_storage::rocks::RocksKv;

use crate::RocksHandle;
#[cfg(feature = "rocks")]
use crate::{commands, internal_connect};

/// Average-rate limiter for a bulk copy that shares a node with live traffic.
/// Reads its cap (bytes/sec; 0 = unlimited) after each flush and sleeps so
/// cumulative throughput stays under it. Reading the cap per flush makes it
/// hot-reloadable (FLINTCONFIG) mid-copy.
///
/// Two callers, same disease. Slot migration (`MIGRATE_RATE_BYTES`, #83) —
/// above all a traffic-policy move shipping the HOTTEST slots — must not
/// saturate disk/net and inflate live p99. Full-sync serving
/// (`FULLSYNC_RATE_BYTES`, #184) is the same story with worse timing: the node
/// streaming a checkpoint to a re-seeding replica is, after a failover, the
/// master that was promoted seconds earlier and is now carrying the pair's
/// whole write load alone. Measured on soak run 23, unthrottled: promotion
/// completed at +700 ms and the write path then stalled 11.9 s while a
/// 104-file checkpoint went out.
///
/// The cap is a `&'static` rather than a copied number so a FLINTCONFIG change
/// reaches a copy already in flight.
#[cfg(feature = "rocks")]
pub(crate) struct Pacer {
    start: std::time::Instant,
    sent: u64,
    cap: &'static std::sync::atomic::AtomicU64,
}

#[cfg(feature = "rocks")]
impl Pacer {
    pub(crate) fn new(cap: &'static std::sync::atomic::AtomicU64) -> Self {
        Self {
            start: std::time::Instant::now(),
            sent: 0,
            cap,
        }
    }

    /// Account `n` freshly-sent bytes, then sleep if we are ahead of the cap.
    pub(crate) fn pace(&mut self, n: usize) {
        self.sent += n as u64;
        let rate = self.cap.load(std::sync::atomic::Ordering::Relaxed);
        if rate == 0 {
            return;
        }
        // Time this many bytes SHOULD have taken at the cap; sleep the deficit.
        let required = std::time::Duration::from_secs_f64(self.sent as f64 / rate as f64);
        let elapsed = self.start.elapsed();
        if required > elapsed {
            std::thread::sleep(required - elapsed);
        }
    }
}

/// Source side of a slot move: ship every row of `slot` (all CFs) as a bulk
/// snapshot, then stream the slot-filtered live tail so writes that landed
/// during the copy also reach the destination. Ownership does NOT change here
/// — this is the data-shipping half; the atomic cutover (IMPORTING/MIGRATING
/// plus -MOVED routing) is a separate step (ADR-0004). The destination
/// decides when it is drained and closes the connection.
#[cfg(feature = "rocks")]
pub(crate) fn flintmigrateout(
    mut stream: flint_tls::Stream,
    rocks: Option<RocksHandle>,
    args: &[Vec<u8>],
) -> std::io::Result<()> {
    use flint_storage::encoding::{Cf, slot_prefix};
    use flint_storage::repl::{ReplError, ReplOp};

    let mut out = Vec::new();
    let Some(kv) = rocks else {
        encode(
            &Value::Error("ERR FLINTMIGRATEOUT requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let Some(slot) = args
        .get(1)
        .and_then(|r| std::str::from_utf8(r).ok())
        .and_then(|s| s.parse::<u16>().ok())
    else {
        encode(
            &Value::Error("ERR FLINTMIGRATEOUT <slot> [ns]".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let ns: &[u8] = args.get(2).map(|v| v.as_slice()).unwrap_or(b"0");
    // Refuse to ship a slot this node does not own: after a cutover the
    // rows here (if any) are purge-pending ghosts, and re-exporting them
    // would hand out data whose ownership already changed hands.
    {
        use flint_storage::manifest::{self, MigrationPhase};
        if let Some(rec) = manifest::read_migration(kv.as_ref(), ns, slot)
            && matches!(rec.phase, MigrationPhase::Moved | MigrationPhase::Importing)
        {
            encode(
                &Value::Error(format!("ERR slot {slot} not owned (phase {:?})", rec.phase)),
                &mut out,
            );
            return stream.write_all(&out);
        }
    }
    let prefixes: Vec<Vec<u8>> = [Cf::Metadata, Cf::Subkey, Cf::ZScore]
        .iter()
        .map(|&cf| slot_prefix(cf, ns, slot))
        .collect();
    let matches = |key: &[u8]| prefixes.iter().any(|p| key.starts_with(p));

    // Snapshot BEFORE scanning: any write after this point reappears in the
    // tail (Put is idempotent, so double-shipping a row is harmless).
    let snapshot = kv.latest_seq();
    encode(&Value::Simple("MIGRATEOUT-OK".into()), &mut out);
    stream.write_all(&out)?;

    // Bulk phase: every row of the slot, across all CFs — streamed with
    // periodic flushes, so neither the row list nor the encode buffer ever
    // holds a whole slot in memory.
    let mut bulk_rows: u64 = 0;
    let mut pacer = Pacer::new(&crate::MIGRATE_RATE_BYTES);
    for prefix in &prefixes {
        out.clear();
        let mut io_err: Option<std::io::Error> = None;
        kv.for_each_prefix(prefix, &mut |k, v| {
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"P".to_vec())),
                    Value::Bulk(Some(k.to_vec())),
                    Value::Bulk(Some(v.to_vec())),
                ])),
                &mut out,
            );
            bulk_rows += 1;
            if out.len() >= 1 << 20 {
                let n = out.len();
                if let Err(e) = stream.write_all(&out) {
                    io_err = Some(e);
                    return false;
                }
                out.clear();
                pacer.pace(n); // cap the copy rate right after each 1MB flush
            }
            true
        });
        if let Some(e) = io_err {
            return Err(e);
        }
        if !out.is_empty() {
            let n = out.len();
            stream.write_all(&out)?;
            pacer.pace(n);
        }
    }
    out.clear();
    encode(
        &Value::Array(Some(vec![
            Value::Bulk(Some(b"BULK-END".to_vec())),
            Value::Integer(snapshot as i64),
            Value::Integer(bulk_rows as i64),
        ])),
        &mut out,
    );
    stream.write_all(&out)?;

    // Tail phase: slot-filtered live ops from the snapshot, plus a CAUGHTUP
    // heartbeat carrying (cursor, head) so the destination knows when it has
    // drained. Loops until the destination closes (write error) — same
    // lifetime model as flintsync.
    let mut cursor = snapshot;
    loop {
        match kv.updates_since_budgeted(cursor, flint_storage::repl::REPL_TAIL_BUDGET_BYTES) {
            Ok(batches) => {
                out.clear();
                for batch in &batches {
                    for op in &batch.ops {
                        match op {
                            ReplOp::Put { key, value } if matches(key) => encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"P".to_vec())),
                                    Value::Bulk(Some(key.clone())),
                                    Value::Bulk(Some(value.clone())),
                                ])),
                                &mut out,
                            ),
                            ReplOp::Delete { key } if matches(key) => encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"D".to_vec())),
                                    Value::Bulk(Some(key.clone())),
                                ])),
                                &mut out,
                            ),
                            _ => {}
                        }
                    }
                    cursor = batch.last_seq;
                }
                encode(
                    &Value::Array(Some(vec![
                        Value::Bulk(Some(b"CAUGHTUP".to_vec())),
                        Value::Integer(cursor as i64),
                        Value::Integer(kv.latest_seq() as i64),
                    ])),
                    &mut out,
                );
                if stream.write_all(&out).is_err() {
                    return Ok(());
                }
                if batches.is_empty() {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
            Err(ReplError::WalGap(_)) => {
                out.clear();
                encode(
                    &Value::Error("WALGAP migration restart required".into()),
                    &mut out,
                );
                let _ = stream.write_all(&out);
                return Ok(());
            }
            Err(e) => {
                eprintln!("migrateout stream error: {e:?}");
                return Ok(());
            }
        }
    }
}

/// Destination side of a slot move (drives FLINTMIGRATEOUT on the source):
/// apply the bulk snapshot then the slot-filtered tail as local writes, so
/// the imported slot joins THIS node's replicated data.
///
/// With `self_addr = Some(me)` it also performs the CUTOVER, in the order
/// that is safe under interruption (see docs/design.md §2.3, ADR-0004):
///   1. mark this slot Importing here (we redirect it to the source until we
///      own it);
///   2. pull bulk + tail to a first caught-up point;
///   3. FREEZE the slot on the source (Migrating: writes shed -TRYAGAIN);
///   4. drain the final tail to a second caught-up point;
///   5. flip DEST-FIRST: clear our Importing (we now own via the base claim),
///      then tell the source FLINTSLOTMOVED (it now answers -MOVED to us).
///
/// Dest-owns-before-source-disowns avoids a redirect loop, and because writes
/// are frozen from step 3 the brief dual-read window is over identical data.
/// Any pre-flip failure rolls back our Importing so we don't strand the slot.
#[cfg(feature = "rocks")]
fn migrate_in(
    kv: &Arc<RocksKv>,
    src: &str,
    slot: u16,
    self_addr: Option<&str>,
    migration_active: &Arc<AtomicBool>,
    ns: &[u8],
) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};

    // Step 1 (cutover): mark Importing before pulling.
    if let Some(_me) = self_addr {
        if let Err(e) = set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Importing, src) {
            return Value::Error(format!("ERR set importing fenced: {e:?}"));
        }
        migration_active.store(true, Ordering::Relaxed);
    }
    // Whether the SOURCE has been frozen. A Cell, not a plain bool, because
    // `rollback` below has to read the CURRENT value: whether a rollback must
    // also reach the source depends on how far the cutover got.
    let frozen = std::cell::Cell::new(false);

    // Roll back OUR Importing record — and, if we froze the source, unfreeze
    // it too (docs/bugs/0024).
    //
    // Clearing only our own record leaves the source in Migrating, shedding
    // every write to the slot with -TRYAGAIN, with no migration in flight and
    // nobody driving one. The destination is the right actor here because it
    // KNOWS it aborted; the controller's reconcile can only infer that from an
    // absent record, and inferring it is what destroys data (docs/bugs/0025).
    //
    // FLINTSLOTABORT refuses a slot that is already Moved, so this cannot
    // un-disown a handoff that actually completed — the safety is in the
    // source's own check, not in our timing.
    let rollback = || {
        if self_addr.is_none() {
            // Data-ship only: we never claimed the slot, so there is nothing
            // of ours to roll back and no source state we created.
            return;
        }
        if !frozen.get() {
            // The source was never frozen, so no other node is holding state
            // on our behalf. Clearing our own record is the whole rollback.
            manifest::clear_migration(kv.as_ref(), ns, slot);
            return;
        }

        // We froze the source. RECORD WHY WE STOPPED before doing anything
        // else (docs/bugs/0025).
        //
        // Deleting our Importing record here is what made an abort and a
        // completed flip the same bytes: the cutover clears the SAME record
        // one step before the flip, when we genuinely own the slot. A
        // controller seeing a frozen source and no record on us read that
        // absence as "the flip half-completed", told the source to disown, and
        // the source purged every row. Aborted says which of the two happened.
        //
        // It is durable and it lands FIRST, so dying anywhere below leaves the
        // fact recoverable rather than leaving a lie.
        if let Err(e) = set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Aborted, src) {
            eprintln!("cutover rollback: slot {slot} could not record Aborted: {e:?}");
        }

        // Then unfreeze the source. We are the right actor: we KNOW we
        // aborted, where the controller could only infer it. FLINTSLOTABORT
        // refuses a slot that is already Moved, so this cannot un-disown a
        // handoff that actually completed.
        let r = call_retrying(
            src,
            &[b"FLINTSLOTABORT", slot.to_string().as_bytes(), ns],
            3,
            std::time::Duration::from_secs(5),
        );
        if matches!(r, Ok(Value::Simple(_))) {
            // Source is released and we own nothing: the marker has served its
            // purpose and would otherwise redirect our clients to the source
            // forever.
            manifest::clear_migration(kv.as_ref(), ns, slot);
        } else {
            // KEEP the Aborted marker. It is now the only durable evidence
            // that the source is frozen on our account, and it is what stops a
            // reconcile from mistaking that freeze for a half-done flip.
            eprintln!(
                "cutover rollback: slot {slot} could not be unfrozen on {src} ({r:?}) \
                 — Aborted marker retained so recovery unfreezes it rather than \
                 completing a flip onto us"
            );
        }
    };

    let mut stream = match internal_connect(src) {
        Ok(s) => s,
        Err(e) => {
            rollback();
            return Value::Error(format!("ERR migrate connect {src}: {e}"));
        }
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(vec![
            Value::Bulk(Some(b"FLINTMIGRATEOUT".to_vec())),
            Value::Bulk(Some(slot.to_string().into_bytes())),
            Value::Bulk(Some(ns.to_vec())),
        ])),
        &mut out,
    );
    if stream.write_all(&out).is_err() {
        rollback();
        return Value::Error("ERR migrate send failed".into());
    }

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    let mut applied: u64 = 0;
    let mut past_bulk = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(frame, used)) => {
                buf.drain(..used);
                match frame {
                    Value::Simple(s) if s == "MIGRATEOUT-OK" => {}
                    Value::Error(e) => {
                        rollback();
                        return Value::Error(format!("ERR migrate source: {e}"));
                    }
                    Value::Array(Some(items)) => match items.as_slice() {
                        [
                            Value::Bulk(Some(t)),
                            Value::Bulk(Some(k)),
                            Value::Bulk(Some(v)),
                        ] if t == b"P" => {
                            kv.put(k, v);
                            applied += 1;
                        }
                        [Value::Bulk(Some(t)), Value::Bulk(Some(k))] if t == b"D" => {
                            kv.delete(k);
                        }
                        [
                            Value::Bulk(Some(t)),
                            Value::Integer(_snap),
                            Value::Integer(_rows),
                        ] if t == b"BULK-END" => {
                            past_bulk = true;
                        }
                        [
                            Value::Bulk(Some(t)),
                            Value::Integer(cursor),
                            Value::Integer(head),
                        ] if t == b"CAUGHTUP" && past_bulk && cursor >= head => {
                            match self_addr {
                                // Data-ship only: caught up, done.
                                None => return Value::Simple(format!("MIGRATEIN-OK {applied}")),
                                Some(me) => {
                                    if !frozen.get() {
                                        // Step 3: freeze the slot on the source.
                                        //
                                        // Retried, because a lost REPLY here
                                        // used to roll us back while leaving
                                        // the source frozen — the source acts
                                        // on the command, not on our reading
                                        // of its answer. FLINTSLOTFREEZE is
                                        // idempotent at a bumped epoch.
                                        match call_retrying(
                                            src,
                                            &[
                                                b"FLINTSLOTFREEZE",
                                                slot.to_string().as_bytes(),
                                                me.as_bytes(),
                                                ns,
                                            ],
                                            3,
                                            std::time::Duration::from_secs(5),
                                        ) {
                                            Ok(Value::Simple(_)) => frozen.set(true),
                                            other => {
                                                rollback();
                                                return Value::Error(format!(
                                                    "ERR freeze failed: {other:?}"
                                                ));
                                            }
                                        }
                                        // Loop again to drain the frozen tail.
                                    } else {
                                        // Step 5: flip dest-first, then source.
                                        manifest::clear_migration(kv.as_ref(), ns, slot);
                                        // A generous budget, retried. The
                                        // source does not reply until it has
                                        // purged every row of the slot, so
                                        // this reply's latency scales with the
                                        // data — a fixed 5s was a guess about
                                        // someone else's slot size.
                                        match call_retrying(
                                            src,
                                            &[
                                                b"FLINTSLOTMOVED",
                                                slot.to_string().as_bytes(),
                                                me.as_bytes(),
                                                ns,
                                            ],
                                            3,
                                            std::time::Duration::from_secs(60),
                                        ) {
                                            Ok(Value::Simple(_)) => {
                                                return Value::Simple(format!(
                                                    "MIGRATEIN-OK {applied} cutover"
                                                ));
                                            }
                                            // The source ANSWERED, with a
                                            // refusal. That is a real claim
                                            // about its state and is reported
                                            // as one.
                                            Ok(other) => {
                                                return Value::Error(format!(
                                                    "ERR cutover handoff refused by source (we own): {other:?}"
                                                ));
                                            }
                                            // No answer. This used to read
                                            // "source not disowned", which is
                                            // a claim about the source made
                                            // without asking it — and the one
                                            // case it can fire, the source has
                                            // usually disowned anyway, because
                                            // the durable Moved record commits
                                            // BEFORE the reply is generated
                                            // (docs/bugs/0024). Say what is
                                            // known and what is not.
                                            // No answer to the flip. Rather
                                            // than report uncertainty we
                                            // could resolve, ASK the source —
                                            // FLINTMIGRATIONS ALL is
                                            // read-only and shows the
                                            // terminal Moved records the bare
                                            // form hides. Two of the four
                                            // answers are definite; the other
                                            // two stay uncertain and say so,
                                            // because a source with no record
                                            // is NOT a source that disowned
                                            // (docs/bugs/0025).
                                            Err(e) => {
                                                return match ask_source_phase(src, slot, ns) {
                                                    SourcePhase::Disowned => {
                                                        Value::Simple(format!(
                                                            "MIGRATEIN-OK {applied} cutover (reply lost; source confirms Moved on re-query)"
                                                        ))
                                                    }
                                                    SourcePhase::StillHeld(phase) => {
                                                        Value::Error(format!(
                                                            "ERR cutover handoff INCOMPLETE (we own the slot; the source \
                                                         answered a re-query and still holds it in phase '{phase}' — \
                                                         it has NOT disowned, so retry the flip): {e}"
                                                        ))
                                                    }
                                                    SourcePhase::NoRecord => Value::Error(format!(
                                                        "ERR cutover handoff UNCONFIRMED (we own the slot; the source \
                                                         answered a re-query and has NO record for this slot, which is \
                                                         not the same as having disowned it — it may have cleaned up \
                                                         after a completed move or never held one; check the source's \
                                                         slot ownership before acting): {e}"
                                                    )),
                                                    SourcePhase::Unreachable(why) => {
                                                        Value::Error(format!(
                                                            "ERR cutover handoff UNCONFIRMED after retries (we own the slot; \
                                                         the re-query also failed: {why}. Its Moved record commits \
                                                         before it replies, so a lost reply does not mean a lost \
                                                         handoff; check the source before acting): {e}"
                                                        ))
                                                    }
                                                };
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            Ok(Decoded::NeedMore) => {
                if std::time::Instant::now() > deadline {
                    rollback();
                    return Value::Error("ERR migrate timed out before drain".into());
                }
                match stream.read(&mut chunk) {
                    Ok(0) => {
                        rollback();
                        return Value::Error("ERR migrate source closed".into());
                    }
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(ref e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) => {
                        rollback();
                        return Value::Error(format!("ERR migrate read: {e}"));
                    }
                }
            }
            Err(_) => {
                rollback();
                return Value::Error("ERR migrate decode".into());
            }
        }
    }
}

/// What the SOURCE says about a slot, asked rather than inferred.
///
/// Four outcomes, and only two of them are definite. `NoRecord` is
/// deliberately NOT folded into "disowned": the whole point of BUG-0025 is
/// that a missing record and a completed handoff produce the same silence,
/// and collapsing them here would reintroduce the bug one layer up.
#[cfg(feature = "rocks")]
enum SourcePhase {
    /// The source holds a terminal Moved record: it HAS disowned the slot.
    Disowned,
    /// The source holds a live record in some other phase: it has NOT disowned.
    StillHeld(String),
    /// The source answered and has no record at all. Ambiguous by construction
    /// — it may have cleaned up after a completed move, or never had one.
    NoRecord,
    /// We could not ask. Says nothing about the source.
    Unreachable(String),
}

/// Ask the source for its own view of `slot`, using the read-only ALL form.
#[cfg(feature = "rocks")]
fn ask_source_phase(src: &str, slot: u16, ns: &[u8]) -> SourcePhase {
    let reply = call_retrying(
        src,
        &[b"FLINTMIGRATIONS", b"ALL"],
        2,
        std::time::Duration::from_secs(10),
    );
    let rows = match reply {
        Ok(Value::Array(Some(rows))) => rows,
        Ok(other) => return SourcePhase::Unreachable(format!("unexpected reply: {other:?}")),
        Err(e) => return SourcePhase::Unreachable(e.to_string()),
    };
    classify_source_phase(&rows, slot, ns)
}

/// The row-reading half of `ask_source_phase`, split out so it can be tested
/// without a server.
///
/// This is the part that ROTS: it parses `"slot phase peer ns"` positionally,
/// a format produced somewhere else entirely (`flintmigrations`). A column
/// added there, or a phase renamed, turns a definite answer into `NoRecord` —
/// and `NoRecord` is the arm that reports UNCERTAINTY, so the failure mode is
/// silent: the cutover keeps working and merely stops being able to resolve a
/// lost reply. Nothing would have caught that.
#[cfg(feature = "rocks")]
fn classify_source_phase(rows: &[Value], slot: u16, ns: &[u8]) -> SourcePhase {
    let want_slot = slot.to_string();
    for r in rows {
        let Value::Bulk(Some(b)) = r else { continue };
        let line = String::from_utf8_lossy(b).into_owned();
        // "slot phase peer ns"
        let mut f = line.splitn(4, ' ');
        let (Some(s_slot), Some(s_phase), Some(_peer), Some(s_ns)) =
            (f.next(), f.next(), f.next(), f.next())
        else {
            continue;
        };
        // BOTH must match. A row for the right slot in the WRONG namespace is
        // a different slot as far as ownership goes, and treating it as a hit
        // would report another tenant's phase as this one's.
        if s_slot != want_slot || s_ns.as_bytes() != ns {
            continue;
        }
        return if s_phase == "moved" {
            SourcePhase::Disowned
        } else {
            SourcePhase::StillHeld(s_phase.to_string())
        };
    }
    SourcePhase::NoRecord
}

/// FLINTMIGRATEIN <src host:port> <slot> [<self advertise addr>]. With the
/// self address it performs the full cutover (freeze/drain/flip); without it,
/// only ships the data (no ownership change).
#[cfg(feature = "rocks")]
pub(crate) fn flintmigratein(
    rocks: &Option<RocksHandle>,
    migration_active: &Arc<AtomicBool>,
    args: &[Vec<u8>],
) -> Value {
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTMIGRATEIN requires the rocks engine".into());
    };
    let (Some(src), Some(slot)) = (
        args.get(1).and_then(|r| std::str::from_utf8(r).ok()),
        args.get(2)
            .and_then(|r| std::str::from_utf8(r).ok())
            .and_then(|s| s.parse::<u16>().ok()),
    ) else {
        return Value::Error("ERR FLINTMIGRATEIN <host:port> <slot> [self-addr] [ns]".into());
    };
    let self_addr = args.get(3).and_then(|r| std::str::from_utf8(r).ok());
    let ns: &[u8] = args.get(4).map(|v| v.as_slice()).unwrap_or(b"0");
    migrate_in(kv, src, slot, self_addr, migration_active, ns)
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn flintmigratein(
    _rocks: &Option<RocksHandle>,
    _migration_active: &Arc<AtomicBool>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTMIGRATEIN requires a build with --features rocks".into())
}

/// The next monotonic, fence-safe epoch for a slot's migration record: above
/// both any existing record and the base claim.
#[cfg(feature = "rocks")]
fn next_migration_epoch(kv: &RocksKv, ns: &[u8], slot: u16) -> flint_storage::manifest::Epoch {
    use flint_storage::manifest::{self, Epoch};
    // The base claim is node-wide (ns "0" carries it in v0); the record
    // fence is per (ns, slot).
    let base = manifest::read_claim(kv, b"0")
        .map(|c| c.epoch)
        .unwrap_or(Epoch::ZERO);
    let cur = manifest::read_migration(kv, ns, slot)
        .map(|r| r.epoch)
        .unwrap_or(Epoch::ZERO);
    let hi = base.max(cur);
    Epoch {
        generation: hi.generation,
        counter: hi.counter + 1,
    }
}

/// Write a phase override for one slot at a fenced next epoch.
#[cfg(feature = "rocks")]
fn set_slot_phase(
    kv: &RocksKv,
    ns: &[u8],
    slot: u16,
    phase: flint_storage::manifest::MigrationPhase,
    peer: &str,
) -> Result<(), flint_storage::manifest::ManifestError> {
    use flint_storage::manifest::{self, MigrationRecord};
    manifest::set_migration(
        kv,
        &MigrationRecord {
            ns: ns.to_vec(),
            slot,
            phase,
            peer: peer.to_string(),
            epoch: next_migration_epoch(kv, ns, slot),
        },
    )
}

/// FLINTSLOTMOVED <slot> <host:port>: terminal cutover on the source — durable
/// Moved override so this node answers -MOVED for the slot.
#[cfg(feature = "rocks")]
pub(crate) fn flintslotmoved(
    rocks: &Option<RocksHandle>,
    migration_active: &Arc<AtomicBool>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::MigrationPhase;
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSLOTMOVED requires the rocks engine".into());
    };
    let (Some(slot), Some(peer)) = (
        args.get(1)
            .and_then(|r| std::str::from_utf8(r).ok())
            .and_then(|s| s.parse::<u16>().ok()),
        args.get(2).and_then(|r| std::str::from_utf8(r).ok()),
    ) else {
        return Value::Error("ERR FLINTSLOTMOVED <slot> <host:port> [ns]".into());
    };
    let ns: &[u8] = args.get(3).map(|v| v.as_slice()).unwrap_or(b"0");
    match set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Moved, peer) {
        Ok(()) => {
            migration_active.store(true, Ordering::Relaxed);
            // Purge the disowned slot's rows (all CFs): the destination owns
            // the data now, this copy is dead weight that would keep skewing
            // DBSIZE/fill metrics — the rebalance planner would chase it
            // forever (found by the execute drill). Ordering is safe: the
            // durable Moved record lands FIRST, so clients already get
            // -MOVED; a crash mid-purge leaves invisible orphans that a
            // retried FLINTSLOTMOVED (idempotent at a bumped epoch) or the
            // GC can finish clearing. The deletes ride the WAL, so replicas
            // drop the slot too.
            let purged = purge_slot_rows(kv.as_ref(), ns, slot);
            Value::Simple(format!(
                "OK slot {slot} moved to {peer} ({purged} rows purged)"
            ))
        }
        Err(e) => Value::Error(format!("ERR slot move fenced: {e:?}")),
    }
}

/// Delete every row of `slot` across all CFs. Streaming collect-then-delete
/// (v0; a real range-delete is an engine refinement). Returns rows removed.
#[cfg(feature = "rocks")]
fn purge_slot_rows(kv: &RocksKv, ns: &[u8], slot: u16) -> usize {
    use flint_storage::encoding::{Cf, slot_prefix};
    let mut purged = 0;
    for cf in [Cf::Metadata, Cf::Subkey, Cf::ZScore] {
        let prefix = slot_prefix(cf, ns, slot);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        kv.for_each_prefix(&prefix, &mut |k, _| {
            keys.push(k.to_vec());
            true
        });
        for k in &keys {
            kv.delete(k);
        }
        purged += keys.len();
    }
    purged
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn flintslotmoved(
    _rocks: &Option<RocksHandle>,
    _migration_active: &Arc<AtomicBool>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTSLOTMOVED requires a build with --features rocks".into())
}

/// FLINTSLOTFREEZE <slot> <dest>: mark the slot Migrating so the source sheds
/// writes to it (-TRYAGAIN) while the destination drains the final tail.
#[cfg(feature = "rocks")]
pub(crate) fn flintslotfreeze(
    rocks: &Option<RocksHandle>,
    migration_active: &Arc<AtomicBool>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::MigrationPhase;
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSLOTFREEZE requires the rocks engine".into());
    };
    let (Some(slot), Some(peer)) = (
        args.get(1)
            .and_then(|r| std::str::from_utf8(r).ok())
            .and_then(|s| s.parse::<u16>().ok()),
        args.get(2).and_then(|r| std::str::from_utf8(r).ok()),
    ) else {
        return Value::Error("ERR FLINTSLOTFREEZE <slot> <dest> [ns]".into());
    };
    let ns: &[u8] = args.get(3).map(|v| v.as_slice()).unwrap_or(b"0");
    match set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Migrating, peer) {
        Ok(()) => {
            migration_active.store(true, Ordering::Relaxed);
            Value::Simple(format!("OK slot {slot} frozen"))
        }
        Err(e) => Value::Error(format!("ERR slot freeze fenced: {e:?}")),
    }
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn flintslotfreeze(
    _rocks: &Option<RocksHandle>,
    _migration_active: &Arc<AtomicBool>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTSLOTFREEZE requires a build with --features rocks".into())
}

/// FLINTSLOTSTATS: live metadata-row counts per (namespace, slot) — one
/// "slot count ns" bulk per non-empty unit — in one streaming pass over the
/// 'M' CF across ALL namespaces (subkey/zscore rows belong to a key counted
/// via its meta row). Envelope: `M | ns_len | ns | slot(2 BE) | user_key`.
/// The (ns, slot) unit is the migration unit, so this is exactly the
/// executor's selection input. Memory is bounded by the number of distinct
/// non-empty (ns, slot) units. v0 counts expired-but-unswept rows too — a
/// fill approximation, not billing.
pub(crate) fn flintslotstats(store: &dyn Kv) -> Value {
    use std::collections::HashMap;
    let mut counts: HashMap<(Vec<u8>, u16), u64> = HashMap::new();
    store.for_each_prefix(b"M", &mut |k, _| {
        // k = M | ns_len | ns | slot(2) | key
        if let Some(&ns_len) = k.get(1) {
            let ns_end = 2 + ns_len as usize;
            if let Some(ns) = k.get(2..ns_end)
                && let Some(raw) = k.get(ns_end..ns_end + 2)
            {
                let slot = u16::from_be_bytes([raw[0], raw[1]]);
                *counts.entry((ns.to_vec(), slot)).or_insert(0) += 1;
            }
        }
        true
    });
    // A (ns, slot) this node has disowned (Moved) or is still importing must
    // not be offered to the rebalance planner: its rows are purge-pending
    // ghosts or an incomplete copy (found by the execute drill: ghost rows
    // kept the planner chasing the same slots forever).
    use flint_storage::manifest::{MigrationPhase, scan_all_migrations};
    for rec in scan_all_migrations(store) {
        if matches!(rec.phase, MigrationPhase::Moved | MigrationPhase::Importing) {
            counts.remove(&(rec.ns.clone(), rec.slot));
        }
    }
    let mut rows: Vec<((Vec<u8>, u16), u64)> = counts.into_iter().collect();
    rows.sort();
    Value::Array(Some(
        rows.into_iter()
            .map(|((ns, slot), n)| {
                Value::Bulk(Some(
                    format!("{slot} {n} {}", String::from_utf8_lossy(&ns)).into_bytes(),
                ))
            })
            .collect(),
    ))
}

/// FLINTMIGRATIONS: the records on this node that a recovering controller must
/// ACT on — Importing, Migrating and Aborted — across ALL namespaces, one
/// "slot phase peer ns" bulk each. Moved is excluded: it is settled ownership,
/// not a move awaiting resolution.
#[cfg(feature = "rocks")]
pub(crate) fn flintmigrations(rocks: &Option<RocksHandle>, args: &[Vec<u8>]) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTMIGRATIONS requires the rocks engine".into());
    };
    // FLINTMIGRATIONS ALL: every record regardless of phase, including the
    // terminal Moved ones the bare form deliberately hides. Read-only, and
    // additive — the controller's parser already ignores phases it does not
    // recognise, so an older controller talking to a newer node is unaffected.
    //
    // It exists so a destination holding an UNCONFIRMED handoff can ASK the
    // source what it thinks, instead of reading the bare form's silence as
    // "disowned". That inference is what BUG-0025 is about: the bare form
    // filters to needs_recovery(), so a source that HAS disowned reports
    // nothing, and so does a source that never had the record at all.
    let all = match args.get(1) {
        None => false,
        Some(a) if a.eq_ignore_ascii_case(b"ALL") => true,
        // An unrecognised argument must not quietly answer the bare question.
        // Returning the filtered set here would make `FLINTMIGRATIONS ALLL`
        // indistinguishable from `FLINTMIGRATIONS ALL` that found no terminal
        // records — the same two-states-one-output defect this command was
        // added to remove.
        Some(a) => {
            return Value::Error(format!(
                "ERR FLINTMIGRATIONS takes no argument or ALL, got '{}'",
                String::from_utf8_lossy(a)
            ));
        }
    };
    // needs_recovery, not is_inflight: an Aborted record has nothing in flight
    // and is exactly the row that tells the controller to unfreeze a source
    // instead of completing a flip onto a destination that gave up
    // (docs/bugs/0025). Omitting it would leave the controller reading the same
    // absence it used to misread.
    let rows: Vec<Value> = manifest::scan_all_migrations(kv.as_ref())
        .into_iter()
        .filter(|r| all || r.phase.needs_recovery())
        .map(|r| {
            let phase = match r.phase {
                MigrationPhase::Importing => "importing",
                MigrationPhase::Migrating => "migrating",
                MigrationPhase::Moved => "moved",
                MigrationPhase::Aborted => "aborted",
            };
            Value::Bulk(Some(
                format!(
                    "{} {phase} {} {}",
                    r.slot,
                    r.peer,
                    String::from_utf8_lossy(&r.ns)
                )
                .into_bytes(),
            ))
        })
        .collect();
    Value::Array(Some(rows))
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn flintmigrations(_rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTMIGRATIONS requires a build with --features rocks".into())
}

/// FLINTSLOTABORT <slot>: clear an in-flight record (rollback/unfreeze).
/// Refuses a terminal Moved override — settled ownership is not aborted.
#[cfg(feature = "rocks")]
pub(crate) fn flintslotabort(rocks: &Option<RocksHandle>, args: &[Vec<u8>]) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSLOTABORT requires the rocks engine".into());
    };
    let Some(slot) = args
        .get(1)
        .and_then(|r| std::str::from_utf8(r).ok())
        .and_then(|s| s.parse::<u16>().ok())
    else {
        return Value::Error("ERR FLINTSLOTABORT <slot> [ns]".into());
    };
    let ns: &[u8] = args.get(2).map(|v| v.as_slice()).unwrap_or(b"0");
    match manifest::read_migration(kv.as_ref(), ns, slot) {
        Some(r) if r.phase == MigrationPhase::Moved => {
            Value::Error("ERR slot is Moved (settled ownership), not in-flight".into())
        }
        // ABANDONING AN IMPORT IS NOT THE SAME AS HAVING NEVER STARTED ONE
        // (docs/bugs/0025). This node pulled part of a slot it does not own;
        // deleting the record would drop it back to its base claim and let it
        // serve that partial copy as though it were whole, and would leave a
        // recovering controller unable to tell this from a completed flip.
        // Record the abort instead. `peer` is preserved: it is the source,
        // which still owns the slot and is where clients must go.
        Some(r) if r.phase == MigrationPhase::Importing => {
            match set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Aborted, &r.peer) {
                Ok(()) => Value::Simple(format!("OK slot {slot} import aborted (recorded)")),
                Err(e) => Value::Error(format!("ERR slot abort fenced: {e:?}")),
            }
        }
        // Migrating: a SOURCE being unfrozen resumes owning the slot outright,
        // so its record goes away. Aborted: the marker has done its job.
        Some(_) => {
            manifest::clear_migration(kv.as_ref(), ns, slot);
            Value::Simple(format!("OK slot {slot} migration aborted"))
        }
        None => Value::Simple(format!("OK slot {slot} had no in-flight migration")),
    }
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn flintslotabort(_rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTSLOTABORT requires a build with --features rocks".into())
}

/// Per-slot gate for a command's key: -MOVED if the slot was handed off
/// (Moved) or is being imported here (Importing); -TRYAGAIN if a WRITE hits a
/// slot frozen mid-cutover (Migrating). None means serve normally.
#[cfg(feature = "rocks")]
pub(crate) fn check_slot_gate(
    rocks: &Option<RocksHandle>,
    ns: &[u8],
    args: &[Vec<u8>],
    is_write: bool,
) -> Option<Value> {
    use flint_storage::manifest::{self, MigrationPhase};
    let kv = rocks.as_ref()?;
    let key = commands::command_key(args)?;
    let slot = flint_slot::slot_for_key(key);
    // Migration records are per (ns, slot): tenant A's moved slot must not
    // redirect tenant B, whose rows did not move.
    match manifest::read_migration(kv.as_ref(), ns, slot)?.phase {
        // Aborted belongs with Importing, not with "no record": this node
        // started an import and gave up, so it holds a PARTIAL copy and the
        // peer (the source) still owns the slot. Serving from here would hand
        // clients a truncated slot that looks whole (docs/bugs/0025).
        MigrationPhase::Moved | MigrationPhase::Importing | MigrationPhase::Aborted => {
            let peer = manifest::read_migration(kv.as_ref(), ns, slot)?.peer;
            Some(Value::Error(format!("MOVED {slot} {peer}")))
        }
        MigrationPhase::Migrating if is_write => {
            Some(Value::Error("TRYAGAIN slot migrating, retry".into()))
        }
        MigrationPhase::Migrating => None, // reads still served by the source
    }
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn check_slot_gate(
    _rocks: &Option<RocksHandle>,
    _ns: &[u8],
    _args: &[Vec<u8>],
    _is_write: bool,
) -> Option<Value> {
    None
}

/// `call_once` with a bounded retry, for the cutover's two control calls.
///
/// A timeout is NOT evidence about the peer (docs/bugs/0024). `call_once`
/// ending in `Err` means only "no reply arrived in the budget" — the source
/// may have done the work and had its reply lost, which is exactly what
/// happens when the reply is gated behind unbounded work. Both cutover
/// commands are idempotent at a bumped epoch (see `flintslotmoved`), so
/// asking again is the cheap way to turn "I do not know" into an answer.
///
/// Only `Err` is retried. A `Value::Error` reply is the source ANSWERING with
/// a refusal — retrying that would just re-run a decision the source already
/// made, and would hide it behind the last attempt's text.
#[cfg(feature = "rocks")]
fn call_retrying(
    addr: &str,
    args: &[&[u8]],
    attempts: u32,
    budget: std::time::Duration,
) -> std::io::Result<Value> {
    let mut last = None;
    for i in 0..attempts.max(1) {
        match call_once_with(addr, args, budget) {
            Ok(v) => return Ok(v),
            Err(e) => {
                last = Some(e);
                // Brief, growing pause: if the source is mid-purge, hammering
                // it competes with the very work we are waiting on.
                if i + 1 < attempts {
                    std::thread::sleep(std::time::Duration::from_millis(200 * u64::from(i + 1)));
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("no attempt made")))
}

/// The largest control reply this path will buffer before giving up.
///
/// Every verb that reaches here — FLINTSLOTFREEZE, FLINTSLOTMOVED,
/// FLINTSLOTABORT, FLINTMIGRATIONS — replies with STATUS, not data; the
/// largest is a list of in-flight migrations. 8 MiB is orders of magnitude
/// more than any of them needs and still ~2000x below a seat's RAM, which is
/// the side to err on: this exists to stop a malfunctioning peer exhausting
/// us, not to police reply size. If it ever fires on a legitimate reply, the
/// reply grew a data-sized component and THAT is the bug.
#[cfg(feature = "rocks")]
const MAX_CTL_REPLY_BYTES: usize = 8 * 1024 * 1024;

/// As `call_once`, with the read budget named by the caller.
///
/// The budget is a parameter because the two cutover calls are not the same
/// shape of work. `FLINTSLOTMOVED` does not reply until the source has purged
/// every row of the slot — measured at ~3.8us/row, so 30k rows is 0.125s but
/// the cost scales with the slot, and rebalancing exists precisely to move the
/// biggest ones. A fixed 5s is a guess about someone else's dataset.
#[cfg(feature = "rocks")]
fn call_once_with(
    addr: &str,
    args: &[&[u8]],
    budget: std::time::Duration,
) -> std::io::Result<Value> {
    let mut s = internal_connect(addr)?;
    s.set_read_timeout(Some(budget))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
        )),
        &mut out,
    );
    s.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                // BUG-0060 pass 6. `set_read_timeout` above is a PER-READ
                // timeout, and this loop has no other bound: a peer that
                // sends *some* bytes inside every timeout window keeps it
                // alive indefinitely while `buf` grows, and the timeout never
                // fires because it measures whether one read stalled, not
                // whether the call has taken too long. The drain loop 500
                // lines above gets this right with a cumulative deadline.
                //
                // The decoder's own caps do not save us: MAX_BULK_LEN is
                // 512 MiB and MAX_ARRAY_LEN is 1,048,576, so a legal reply is
                // bounded only by their PRODUCT, and the whole thing must be
                // buffered before `decode` returns Complete. One million
                // elements at 1 KB each is 1 GB resident, from a reply that
                // breaks no rule.
                //
                // A byte cap rather than a cumulative deadline, deliberately.
                // The budget is sized by the caller for a long SILENT wait --
                // FLINTSLOTMOVED does not answer until the source has purged
                // the slot -- so turning it into a deadline would start
                // failing legitimate slow migrations. Capping bytes bounds
                // the memory AND the loop (a trickling peer now hits this)
                // while leaving the timing contract exactly as it was.
                if buf.len() > MAX_CTL_REPLY_BYTES {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "control reply exceeded {MAX_CTL_REPLY_BYTES} bytes without \
                             completing; peer {addr} is trickling or malfunctioning"
                        ),
                    ));
                }
                let n = s.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "closed",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{e:?}"),
                ));
            }
        }
    }
}

#[cfg(not(feature = "rocks"))]
pub(crate) fn flintmigrateout(
    mut stream: flint_tls::Stream,
    _rocks: Option<RocksHandle>,
    _args: &[Vec<u8>],
) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTMIGRATEOUT requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

#[cfg(all(test, feature = "rocks"))]
mod source_phase_tests {
    use super::{SourcePhase, classify_source_phase};
    use flint_resp::Value;

    fn row(s: &str) -> Value {
        Value::Bulk(Some(s.as_bytes().to_vec()))
    }

    /// The shape that made BUG-0024's lost-reply branch resolvable: the source
    /// reports the slot as `moved`, so the handoff DID complete and an
    /// unconfirmed flip becomes a definite success.
    #[test]
    fn a_moved_row_for_this_slot_is_disowned() {
        let rows = [row("13624 moved 127.0.0.1:6571 0")];
        assert!(matches!(
            classify_source_phase(&rows, 13624, b"0"),
            SourcePhase::Disowned
        ));
    }

    #[test]
    fn any_other_phase_is_still_held_and_carries_the_phase() {
        for phase in ["migrating", "importing", "aborted"] {
            let rows = [row(&format!("13624 {phase} 127.0.0.1:6571 0"))];
            match classify_source_phase(&rows, 13624, b"0") {
                SourcePhase::StillHeld(p) => assert_eq!(p, phase),
                other => panic!("{phase} must be StillHeld, got {:?}", DebugArm(&other)),
            }
        }
    }

    /// The two ways a row can look like a match and not be one. Both must fall
    /// through to NoRecord rather than being read as this slot's phase —
    /// reporting another namespace's ownership as ours is the failure this
    /// guards.
    #[test]
    fn wrong_slot_or_wrong_namespace_is_not_a_match() {
        let rows = [row("13624 moved 127.0.0.1:6571 0")];
        assert!(
            matches!(
                classify_source_phase(&rows, 99, b"0"),
                SourcePhase::NoRecord
            ),
            "a different slot must not match"
        );
        assert!(
            matches!(
                classify_source_phase(&rows, 13624, b"other-ns"),
                SourcePhase::NoRecord
            ),
            "the same slot in a different namespace must not match"
        );
    }

    /// Absence stays absence. This is the arm BUG-0025 exists to protect: an
    /// empty answer must NOT be folded into "disowned".
    #[test]
    fn no_rows_is_no_record_not_disowned() {
        assert!(matches!(
            classify_source_phase(&[], 13624, b"0"),
            SourcePhase::NoRecord
        ));
    }

    /// A malformed row must be skipped, not panic and not match. The format is
    /// produced by another function and a truncated line is what a version
    /// skew looks like.
    #[test]
    fn malformed_rows_are_skipped_and_the_good_one_still_wins() {
        let rows = [
            row("13624"),
            row("13624 moved"),
            Value::Integer(7),
            row("13624 moved 127.0.0.1:6571 0"),
        ];
        assert!(matches!(
            classify_source_phase(&rows, 13624, b"0"),
            SourcePhase::Disowned
        ));
    }

    // SourcePhase has no Debug (it is only ever rendered into operator text),
    // so give the panic above something to print without widening the type.
    struct DebugArm<'a>(&'a SourcePhase);
    impl std::fmt::Debug for DebugArm<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                SourcePhase::Disowned => write!(f, "Disowned"),
                SourcePhase::StillHeld(p) => write!(f, "StillHeld({p})"),
                SourcePhase::NoRecord => write!(f, "NoRecord"),
                SourcePhase::Unreachable(e) => write!(f, "Unreachable({e})"),
            }
        }
    }
}

#[cfg(all(test, feature = "rocks"))]
mod ctl_reply_cap_tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    /// A peer that trickles forever must be refused, not buffered forever.
    ///
    /// This is the failure `set_read_timeout` cannot see: it bounds how long
    /// ONE read may stall, and a peer that sends a few bytes inside every
    /// window never stalls a read while `buf` grows without limit. The test
    /// therefore has to trickle rather than go silent — going silent is the
    /// case that already worked, and asserting on it would pass with the cap
    /// deleted.
    #[test]
    fn trickling_peer_is_refused_rather_than_buffered() {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = l.local_addr().expect("listener addr").to_string();

        std::thread::spawn(move || {
            let (mut s, _) = match l.accept() {
                Ok(v) => v,
                Err(_) => return,
            };
            let mut sink = [0u8; 4096];
            let _ = std::io::Read::read(&mut s, &mut sink);
            // A legal, never-completing reply: an array header promising far
            // more elements than ever arrive, then an endless dribble of
            // well-formed bulks. decode() returns NeedMore forever.
            if s.write_all(b"*1000000\r\n").is_err() {
                return;
            }
            // 64 KiB of well-formed bulks per write rather than ten bytes.
            // The property under test is "sends something inside every
            // timeout window while never completing", which chunk size does
            // not change. (It barely helps the runtime -- the cost is the
            // quadratic re-decode noted above, not the write count -- but it
            // keeps the peer from being the bottleneck.)
            let blob: Vec<u8> = b"$4\r\nabcd\r\n".repeat(6553);
            loop {
                if s.write_all(&blob).is_err() {
                    return;
                }
            }
        });

        // Budget generously: the point is that the cap fires on BYTES, and a
        // short budget would let the read timeout claim the win instead and
        // the test would pass with the cap removed.
        let r = call_once_with(
            &addr,
            &[b"FLINTMIGRATIONS"],
            std::time::Duration::from_secs(30),
        );
        let e = r.expect_err("a never-completing reply must be refused");
        assert_eq!(
            e.kind(),
            std::io::ErrorKind::InvalidData,
            "expected the byte cap, got: {e}"
        );
        assert!(
            e.to_string().contains("exceeded"),
            "error should name the cap it hit: {e}"
        );
    }
}
