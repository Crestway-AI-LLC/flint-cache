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
                if let Err(e) = stream.write_all(&out) {
                    io_err = Some(e);
                    return false;
                }
                out.clear();
            }
            true
        });
        if let Some(e) = io_err {
            return Err(e);
        }
        if !out.is_empty() {
            stream.write_all(&out)?;
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
    let rollback = || {
        if self_addr.is_some() {
            manifest::clear_migration(kv.as_ref(), ns, slot);
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
    let mut frozen = false; // cutover: has the source been frozen yet?
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
                                    if !frozen {
                                        // Step 3: freeze the slot on the source.
                                        match call_once(
                                            src,
                                            &[
                                                b"FLINTSLOTFREEZE",
                                                slot.to_string().as_bytes(),
                                                me.as_bytes(),
                                                ns,
                                            ],
                                        ) {
                                            Ok(Value::Simple(_)) => frozen = true,
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
                                        match call_once(
                                            src,
                                            &[
                                                b"FLINTSLOTMOVED",
                                                slot.to_string().as_bytes(),
                                                me.as_bytes(),
                                                ns,
                                            ],
                                        ) {
                                            Ok(Value::Simple(_)) => {
                                                return Value::Simple(format!(
                                                    "MIGRATEIN-OK {applied} cutover"
                                                ));
                                            }
                                            other => {
                                                // We already own (Importing
                                                // cleared); the source failing
                                                // to disown is a controller
                                                // reconcile, not data loss.
                                                return Value::Error(format!(
                                                    "ERR cutover handoff incomplete (we own; source not disowned): {other:?}"
                                                ));
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

/// FLINTMIGRATIONS: the in-flight (Importing/Migrating) records on this node
/// across ALL namespaces, one "slot phase peer ns" bulk each — the recovery
/// input for the controller.
#[cfg(feature = "rocks")]
pub(crate) fn flintmigrations(rocks: &Option<RocksHandle>) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTMIGRATIONS requires the rocks engine".into());
    };
    let rows: Vec<Value> = manifest::scan_all_migrations(kv.as_ref())
        .into_iter()
        .filter(|r| r.phase.is_inflight())
        .map(|r| {
            let phase = match r.phase {
                MigrationPhase::Importing => "importing",
                MigrationPhase::Migrating => "migrating",
                MigrationPhase::Moved => "moved",
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
pub(crate) fn flintmigrations(_rocks: &Option<RocksHandle>) -> Value {
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
        MigrationPhase::Moved | MigrationPhase::Importing => {
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

/// Send one command to `addr` and read one reply (used by the cutover
/// orchestration to freeze and hand off the slot on the source).
#[cfg(feature = "rocks")]
fn call_once(addr: &str, args: &[&[u8]]) -> std::io::Result<Value> {
    let mut s = internal_connect(addr)?;
    s.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
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
