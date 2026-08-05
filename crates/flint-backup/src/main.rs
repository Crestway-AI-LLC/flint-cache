// SPDX-License-Identifier: Elastic-2.0
//! `flint-backup` — the seat that produces and checks backup sets (ADR-0011
//! D8).
//!
//! Its own binary rather than a `flintctl` subcommand, for three reasons in
//! the order they matter: `flintctl` has no external dependencies and runs as
//! root on every host performing bootstrap and failover, so an object-store
//! SDK linked into it would put that whole dependency tree on the cluster's
//! control path; backup holds bucket credentials while `flintctl` holds mesh
//! certs and the SSH identity, and they should not share a process; and
//! backup is scheduled, retrying and monitored, which is the shape the
//! controller and the agent already have.
//!
//! Takes `--pairs` rather than an inventory, exactly as `flint-controller`
//! does. `flintctl backup on` knows the inventory and spawns this with the
//! flags derived from it, so the inventory parser keeps one implementation.
//!
//! ```text
//! flint-backup run    --pairs <a,b;c,d> --cp-state <path> --to <dir>
//! flint-backup verify --from <dir>
//! ```
//!
//! `verify` takes only a store location: it must work when the cluster it
//! came from no longer exists, which is the case it is for.

mod s3;

use flint_backup::store::{LocalDir, ObjectStore};
use flint_backup::{Manifest, ObjectRecord, PairRecord};
use flint_resp::{Decoded, Value, decode, encode};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn die(msg: &str) -> ! {
    eprintln!("flint-backup: {msg}");
    std::process::exit(1)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1))
        .cloned()
}

/// A store from a location spec: `s3://bucket/prefix` or a filesystem
/// path. Every subcommand goes through this, so the format code and the
/// drills exercise the same call sites whichever store is behind them —
/// S3 arrives as a second implementation, not a second code path.
fn open_store(spec: &str) -> Box<dyn ObjectStore> {
    if spec.starts_with("s3://") {
        match s3::S3Store::from_spec(spec) {
            Ok(s) => Box::new(s),
            Err(e) => die(&format!("{spec}: {e}")),
        }
    } else {
        Box::new(LocalDir::new(spec))
    }
}

/// Join a set id onto a location spec, for either kind of store.
fn join_spec(base: &str, id: &str) -> String {
    format!("{}/{id}", base.trim_end_matches('/'))
}

/// Materialise a remote set into a temp directory, verified.
///
/// `restore-ns` reads pair checkpoints by opening them as RocksDB
/// DIRECTORIES, which an object store cannot be — so a remote set stages
/// through disk first. Verification happens against the STAGED copy: it is
/// the copy that will be read, and a download that corrupted in transit
/// must be refused with the same words a corrupted bucket would be.
#[cfg(feature = "rocks")]
fn stage_set(store: &dyn ObjectStore) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("flint-restore-stage-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let local = LocalDir::new(&dir);
    let keys = store
        .list("")
        .unwrap_or_else(|e| die(&format!("list set: {e}")));
    if keys.is_empty() {
        die("the set location lists no objects");
    }
    for key in &keys {
        let mut r = store
            .open(key)
            .unwrap_or_else(|e| die(&format!("fetch {key}: {e}")));
        // Stream through a temp file, then let the local store place it
        // with its own write-then-rename discipline.
        let tmp = dir.join(".staging-one");
        if let Some(parent) = tmp.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut f =
            std::fs::File::create(&tmp).unwrap_or_else(|e| die(&format!("stage {key}: {e}")));
        std::io::copy(&mut r, &mut f).unwrap_or_else(|e| die(&format!("stage {key}: {e}")));
        drop(f);
        local
            .put_file(key, &tmp)
            .unwrap_or_else(|e| die(&format!("place {key}: {e}")));
        let _ = std::fs::remove_file(&tmp);
    }
    dir
}

/// One command to one node, over the mesh when the fleet is TLS.
fn call(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
    timeout: Duration,
) -> std::io::Result<Value> {
    let mut s = flint_tls::connect(addr, tls)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(Duration::from_millis(1500)))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            args.iter()
                .map(|a| Value::Bulk(Some(a.as_bytes().to_vec())))
                .collect(),
        )),
        &mut out,
    );
    s.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16384];
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
            Err(e) => return Err(std::io::Error::other(format!("{e:?}"))),
        }
    }
}

fn info_field(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    field: &str,
) -> Option<String> {
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["FLINTINFO"], Duration::from_millis(1500))
    else {
        return None;
    };
    String::from_utf8_lossy(&raw)
        .split(['\r', '\n'])
        .find(|l| l.starts_with(field))
        .map(|l| l.trim_start_matches(field).trim().to_string())
}

/// The member of `pair` that is serving as master.
///
/// Asked of the nodes rather than assumed from position: a pair's members
/// swap on every failover, and backing up whichever one is listed first
/// would silently capture the REPLICA — which is behind by the async tail,
/// so every such backup would inherit the RPO window on top of its own age
/// (ADR-0011 rejects backing up the replica for exactly this reason).
fn master_of(pair: &[String], tls: &Option<Arc<flint_tls::ClientConfig>>) -> Option<String> {
    pair.iter()
        .find(|a| info_field(a, tls, "role:").as_deref() == Some("master"))
        .cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("run") => run(),
        Some("verify") => verify(),
        Some("restore") => restore(),
        Some("restore-ns") => restore_ns(),
        Some("schedule") => schedule(),
        Some("inspect") => inspect(),
        Some("--build-version") => println!("{}", flint_build::version(env!("CARGO_PKG_VERSION"))),
        _ => {
            eprintln!(
                "usage:\n  \
                 flint-backup run        --pairs <a,b;c,d> --cp-state <path> --to <dir|s3://bucket/prefix> \
                 [--tls <certs-dir>] [--snap-root <dir>]\n  \
                 flint-backup verify     --from <dir|s3://...>\n  \
                 flint-backup restore    --from <dir|s3://...> --into <dir>\n  \
                 flint-backup restore-ns --from <dir|s3://...> --ns <src> --into-ns <dest> \
                 --cp <addr> --proxy-name <registered-proxy> [--tls <certs-dir>]\n  \
                 flint-backup schedule   --pairs <a,b;c,d> --cp-state <path> --to <dir|s3://...> \
                 --every <dur> [--verify-every <dur>] [--rehearse-every <dur>] [--keep <n>] \
                 [--status-file <path>] [--jitter <dur>] [--tls <certs-dir>] [--snap-root <dir>]"
            );
            std::process::exit(2)
        }
    }
}

fn verify() {
    let from = arg("--from").unwrap_or_else(|| die("verify needs --from <dir|s3://...>"));
    let store = open_store(&from);
    match flint_backup::load_verified(store.as_ref()) {
        Ok((m, r)) => {
            println!(
                "OK {} — {} objects, {} bytes, taken {} by {}",
                r.id, r.objects, r.bytes, m.finished_ms, m.release
            );
        }
        // The exit status is the answer, so a scheduler can branch on it
        // without parsing prose.
        Err(e) => die(&e.to_string()),
    }
}

/// Restore a verified set into a directory that does not exist yet.
///
/// D3: restore only ever CREATES. `--into` must be absent; each pair's
/// checkpoint materialises under `<into>/pair<i>` and the control plane's
/// state file lands beside them. Nothing that is currently serving is
/// touched, so a bug here cannot damage a live cluster.
///
/// D4: the system rows are SCRUBBED before any node ever opens the copy.
/// A checkpoint carries the source's manifest rows in the Kv — role,
/// claims, migrations — so a naive restore produces a node that durably
/// believes it is master at the source's epoch: the exact ingredient for
/// the split-brain that epoch fencing exists to prevent. Role is cleared
/// (the restored node starts on a fresh epoch line), claims are dropped
/// (the restored CP registry is the authority they are re-derived from),
/// and migration rows are dropped because they reference a peer that does
/// not exist in the new cluster.
#[cfg(feature = "rocks")]
fn restore() {
    use flint_storage::Kv;
    let from = arg("--from").unwrap_or_else(|| die("restore needs --from <dir>"));
    let into = arg("--into").unwrap_or_else(|| die("restore needs --into <dir>"));
    let dest = Path::new(&into);
    if dest.exists() {
        die(&format!(
            "{into} already exists — restore only ever creates (ADR-0011 D3); \
             point --into at a path that does not exist"
        ));
    }
    let store = open_store(&from);
    // Verification is not separable from restore: the one entry point both
    // loads and checks, so a corrupt or tampered set is refused before a
    // single byte lands in the destination.
    let (manifest, report) = match flint_backup::load_verified(store.as_ref()) {
        Ok(v) => v,
        Err(e) => die(&e.to_string()),
    };
    println!(
        "set {} verified: {} objects, {} bytes",
        report.id, report.objects, report.bytes
    );

    for pair in &manifest.pairs {
        let pair_dir = dest.join(format!("pair{}", pair.index));
        std::fs::create_dir_all(&pair_dir)
            .unwrap_or_else(|e| die(&format!("create {}: {e}", pair_dir.display())));
        let prefix = format!("pairs/{}/", pair.index);
        for obj in manifest
            .objects
            .iter()
            .filter(|o| o.key.starts_with(&prefix))
        {
            let name = &obj.key[prefix.len()..];
            let mut r = store
                .open(&obj.key)
                .unwrap_or_else(|e| die(&format!("open {}: {e}", obj.key)));
            let mut f = std::fs::File::create(pair_dir.join(name))
                .unwrap_or_else(|e| die(&format!("create {name}: {e}")));
            std::io::copy(&mut r, &mut f).unwrap_or_else(|e| die(&format!("copy {name}: {e}")));
        }

        // The D4 scrub, counted so the report is evidence rather than a
        // claim: "scrubbed nothing" on a checkpoint cut from a live master
        // would mean the scan missed the rows, not that they were absent —
        // every master has a role row.
        let kv = flint_storage::rocks::RocksKv::open(&pair_dir)
            .unwrap_or_else(|e| die(&format!("open restored pair {}: {e}", pair.index)));
        let mut doomed: Vec<Vec<u8>> = Vec::new();
        kv.for_each_prefix(b"\x00flint\x00", &mut |k, _| {
            doomed.push(k.to_vec());
            true
        });
        let mut roles = 0u32;
        let mut claims = 0u32;
        let mut migrations = 0u32;
        let mut cursors = 0u32;
        for k in &doomed {
            if k.as_slice() == flint_storage::manifest::ROLE_KEY {
                roles += 1;
            } else if k.starts_with(flint_storage::manifest::CLAIM_KEY_PREFIX) {
                claims += 1;
            } else if k.starts_with(flint_storage::manifest::MIGRATION_KEY_PREFIX) {
                migrations += 1;
            } else if k.as_slice() == flint_storage::repl::REPL_STATE_KEY {
                // The replication cursor: a position in the SOURCE master's
                // WAL. That lineage does not exist in the restored cluster,
                // so a carried cursor describes a stream nobody can serve —
                // dropped for the same reason the migration rows are.
                // (Found by this scrub's own fail-closed arm on the first
                // real checkpoint, not by reading the code.)
                cursors += 1;
            } else {
                // A system row this build does not recognise gets the same
                // treatment an unknown manifest key gets: refusal. Carrying
                // it forward restores state with a meaning nobody checked.
                die(&format!(
                    "restored pair {} holds a system row this build does not know: {:?} — \
                     refusing to carry it into a new cluster",
                    pair.index,
                    String::from_utf8_lossy(k)
                ));
            }
            kv.delete(k);
        }
        // Deletes must be DURABLE before this process reports success: the
        // node that opens this dir next trusts the scrub already happened.
        kv.flush_checked()
            .unwrap_or_else(|e| die(&format!("pair {}: scrub flush failed: {e}", pair.index)));
        if roles == 0 {
            die(&format!(
                "pair {} had no role row to scrub — the checkpoint was not cut on a \
                 serving master, or the scan is broken; either way this set is not \
                 what the manifest says it is",
                pair.index
            ));
        }
        // Trust nothing, including this process: drop the handle, reopen the
        // directory cold, and prove the rows are gone THERE. The scrub was
        // once observed reporting success while the role row survived a
        // reopen — nondeterministically — and a scrub that is wrong is not
        // a degraded restore, it is the split-brain ingredient D4 exists to
        // remove. The reopen is the only observer position equivalent to
        // the node that boots on this directory next.
        drop(kv);
        let kv = flint_storage::rocks::RocksKv::open(&pair_dir)
            .unwrap_or_else(|e| die(&format!("reopen restored pair {}: {e}", pair.index)));
        let mut survivors = Vec::new();
        kv.for_each_prefix(b"\x00flint\x00", &mut |k, _| {
            survivors.push(String::from_utf8_lossy(k).escape_debug().to_string());
            true
        });
        if !survivors.is_empty() {
            die(&format!(
                "pair {}: system rows SURVIVED the scrub across a reopen: {} —                  the restore is not safe to boot; nothing further was written",
                pair.index,
                survivors.join(", ")
            ));
        }
        println!(
            "pair {} restored from {} (epoch {} at seq {}): scrubbed {} role, {} claim(s), {} migration(s), {} repl cursor(s) — verified gone across a reopen",
            pair.index, pair.master, pair.epoch, pair.seq, roles, claims, migrations, cursors
        );
    }

    // The CP state file: pairs, ranges, tenants, tokens, quotas. Placed for
    // the operator to hand to `flintctl bootstrap`; restore does not start
    // anything.
    let mut r = store
        .open("cp-state")
        .unwrap_or_else(|e| die(&format!("open cp-state: {e}")));
    let mut f = std::fs::File::create(dest.join("cp-state"))
        .unwrap_or_else(|e| die(&format!("create cp-state: {e}")));
    std::io::copy(&mut r, &mut f).unwrap_or_else(|e| die(&format!("copy cp-state: {e}")));

    println!(
        "OK restored {} into {into} — {} pair(s) + cp-state, system rows scrubbed; \
         nothing was started, and the restored cluster must mint its own CA",
        manifest.id,
        manifest.pairs.len()
    );
}

#[cfg(not(feature = "rocks"))]
fn restore() {
    die("restore requires a build with --features rocks (the D4 scrub opens the engine)");
}

/// Namespace-scoped restore (ADR-0011 D5): materialise one tenant's data
/// from a backup set into a NEW namespace in a LIVE cluster.
///
/// The load-bearing decision is WHERE each row lands: by the cluster's
/// ownership NOW, never the topology recorded in the backup. Slots move
/// between pairs on rebalance and expand, and placing by the backup's
/// topology would put rows on a node the proxy never routes that slot to —
/// present, checksummed, and unreachable. So ownership comes from the same
/// place the proxies get it: the CP's snapshot (default ranges plus the
/// exceptions table), fetched at restore time.
///
/// The set is opened READ-ONLY. A normal engine open rewrites CURRENT and
/// the WAL, which would corrupt the set's own checksums — reading a backup
/// must never be the thing that makes it unrestorable.
///
/// Nothing here touches the destination's live namespace: rows are
/// re-enveloped from <src ns> to <dest ns> and applied there (D3b — a bug
/// in restore cannot damage serving data). System rows never enter the
/// picture: the scan covers user CFs only, which is why this mode needs no
/// D4 scrub (the ADR calls this out as the structurally safer mode).
#[cfg(feature = "rocks")]
fn restore_ns() {
    use flint_storage::Kv;
    let from = arg("--from").unwrap_or_else(|| die("restore-ns needs --from <set>"));
    let src_ns = arg("--ns").unwrap_or_else(|| die("restore-ns needs --ns <source-ns>"));
    let dest_ns = arg("--into-ns").unwrap_or_else(|| die("restore-ns needs --into-ns <dest-ns>"));
    let cp = arg("--cp").unwrap_or_else(|| die("restore-ns needs --cp <addr>"));
    // The CP filters its snapshot per proxy (tokens and exceptions are a
    // blast-radius boundary), so the map is requested AS a registered proxy
    // that serves the destination tenant.
    let proxy_name =
        arg("--proxy-name").unwrap_or_else(|| die("restore-ns needs --proxy-name <proxy-addr>"));
    if src_ns == dest_ns {
        die(
            "--into-ns must differ from --ns: restore materialises a NEW namespace \
             beside the live one (ADR-0011 D3); activation is a separate step",
        );
    }
    let tls = arg("--tls").map(|d| {
        flint_tls::client_config(
            &format!("{d}/ca.crt"),
            &format!("{d}/int.crt"),
            &format!("{d}/int.key"),
        )
        .unwrap_or_else(|e| die(&format!("load mesh certs from {d}: {e}")))
    });

    // A remote set stages through disk (the checkpoints are opened as
    // RocksDB directories); a local one is read in place. Either way the
    // copy that gets VERIFIED is the copy that gets READ.
    let from = if from.starts_with("s3://") {
        let staged = stage_set(open_store(&from).as_ref());
        staged.to_string_lossy().into_owned()
    } else {
        from
    };
    let store = LocalDir::new(&from);
    let (manifest, report) = match flint_backup::load_verified(&store) {
        Ok(v) => v,
        Err(e) => die(&e.to_string()),
    };
    println!(
        "set {} verified: {} objects, {} bytes",
        report.id, report.objects, report.bytes
    );

    // Current ownership, from the CP: pair addresses + default ranges, and
    // the exceptions that override them for the DESTINATION namespace.
    let snap = match call(
        &cp,
        &tls,
        &["CPSNAPSHOT", &proxy_name],
        Duration::from_secs(5),
    ) {
        Ok(Value::Array(Some(items))) => items,
        other => die(&format!("CPSNAPSHOT via {cp}: {other:?}")),
    };
    let bulk = |i: usize| -> String {
        match snap.get(i) {
            Some(Value::Bulk(Some(b))) => String::from_utf8_lossy(b).into_owned(),
            _ => String::new(),
        }
    };
    let (pairs_spec, exc_spec) = (bulk(2), bulk(5));
    let mut pair_addrs: Vec<Vec<String>> = Vec::new();
    let mut ranges: Vec<Option<(u16, u16)>> = Vec::new();
    for entry in pairs_spec.split(';').filter(|s| !s.is_empty()) {
        let (members, range) = match entry.split_once('|') {
            Some((m, r)) => {
                let range = r
                    .split_once('-')
                    .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)));
                (m, range)
            }
            None => (entry, None),
        };
        pair_addrs.push(members.split(',').map(str::to_string).collect());
        ranges.push(range);
    }
    if pair_addrs.is_empty() {
        die("the CP snapshot names no pairs — is the cluster bootstrapped?");
    }
    // Exceptions for the destination namespace: "ns:lo[-hi]:pair;...".
    let mut exceptions: Vec<(u16, u16, usize)> = Vec::new();
    for entry in exc_spec.split(';').filter(|s| !s.is_empty()) {
        let mut f = entry.split(':');
        let (Some(ns), Some(span), Some(pair)) = (f.next(), f.next(), f.next()) else {
            continue;
        };
        if ns != dest_ns {
            continue;
        }
        let (lo, hi) = match span.split_once('-') {
            Some((a, b)) => (a.parse().ok(), b.parse().ok()),
            None => (span.parse().ok(), span.parse().ok()),
        };
        if let (Some(lo), Some(hi), Ok(pair)) = (lo, hi, pair.parse()) {
            exceptions.push((lo, hi, pair));
        }
    }
    let owner_of = |slot: u16| -> usize {
        exceptions
            .iter()
            .find(|(lo, hi, _)| (*lo..=*hi).contains(&slot))
            .map(|(_, _, p)| *p)
            .or_else(|| flint_slot::default_pair(slot, &ranges, pair_addrs.len()))
            .unwrap_or(0)
    };
    // Resolve each pair's MASTER once, the same way the backup side does:
    // by asking, never by position.
    let masters: Vec<String> = pair_addrs
        .iter()
        .enumerate()
        .map(|(i, p)| {
            master_of(p, &tls)
                .unwrap_or_else(|| die(&format!("pair {i} has no master among {p:?}")))
        })
        .collect();

    // Stream rows out of every pair checkpoint in the set. The tenant's
    // rows can be on ANY source pair (the backup's topology), and each row
    // routes independently — which is exactly what makes this correct
    // across topology change.
    const BATCH_ROWS: usize = 200;
    const BATCH_BYTES: usize = 1 << 20;
    let mut batches: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![Vec::new(); pair_addrs.len()];
    let mut batch_bytes: Vec<usize> = vec![0; pair_addrs.len()];
    let mut sent: Vec<u64> = vec![0; pair_addrs.len()];
    let flush = |pair: usize, batch: &mut Vec<(Vec<u8>, Vec<u8>)>, bytes: &mut usize| {
        if batch.is_empty() {
            return 0u64;
        }
        let mut cmd: Vec<Vec<u8>> = Vec::with_capacity(2 + batch.len() * 2);
        cmd.push(b"FLINTNSRESTORE".to_vec());
        cmd.push(dest_ns.as_bytes().to_vec());
        for (k, v) in batch.iter() {
            cmd.push(k.clone());
            cmd.push(v.clone());
        }
        let n = batch.len() as u64;
        match call_raw(&masters[pair], &tls, &cmd, Duration::from_secs(30)) {
            Ok(Value::Simple(_)) => {}
            other => die(&format!(
                "restore batch to pair {pair} ({}): {other:?}",
                masters[pair]
            )),
        }
        batch.clear();
        *bytes = 0;
        n
    };
    for pair in &manifest.pairs {
        // The checkpoint is read in place, read-only — see the fn comment.
        let src_dir = std::path::Path::new(&from).join(format!("pairs/{}", pair.index));
        let kv = flint_storage::rocks::RocksKv::open_read_only(&src_dir)
            .unwrap_or_else(|e| die(&format!("open set pair {} read-only: {e}", pair.index)));
        for cf in *b"MSZ" {
            let mut prefix = vec![cf, src_ns.len() as u8];
            prefix.extend_from_slice(src_ns.as_bytes());
            kv.for_each_prefix(&prefix, &mut |k, v| {
                // Re-envelope: swap the namespace, keep everything from the
                // slot onward — the slot derives from the USER key, which
                // is unchanged, so it stays byte-identical.
                let tail = &k[2 + src_ns.len()..];
                let slot = u16::from_be_bytes([tail[0], tail[1]]);
                let mut nk = Vec::with_capacity(2 + dest_ns.len() + tail.len());
                nk.push(cf);
                nk.push(dest_ns.len() as u8);
                nk.extend_from_slice(dest_ns.as_bytes());
                nk.extend_from_slice(tail);
                let owner = owner_of(slot);
                batches[owner].push((nk, v.to_vec()));
                batch_bytes[owner] += k.len() + v.len();
                if batches[owner].len() >= BATCH_ROWS || batch_bytes[owner] >= BATCH_BYTES {
                    sent[owner] += flush(owner, &mut batches[owner], &mut batch_bytes[owner]);
                }
                true
            });
        }
    }
    for pair in 0..pair_addrs.len() {
        sent[pair] += flush(pair, &mut batches[pair], &mut batch_bytes[pair]);
    }

    let total: u64 = sent.iter().sum();
    if total == 0 {
        // Loud, not a quiet success: an empty restore of a namespace the
        // manifest was supposed to contain usually means the wrong --ns.
        die(&format!(
            "the set contains no rows for namespace {src_ns:?} — nothing was restored \
             (wrong --ns, or the tenant was empty at backup time)"
        ));
    }
    let placed: Vec<String> = sent
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(i, n)| format!("pair {i}: {n} rows"))
        .collect();
    println!(
        "OK restored {total} rows of {src_ns:?} into namespace {dest_ns:?} by current \
         ownership ({}); the live namespace was not touched",
        placed.join(", ")
    );
}

#[cfg(not(feature = "rocks"))]
fn restore_ns() {
    die("restore-ns requires a build with --features rocks");
}

/// `call` for pre-encoded binary arguments (envelope keys are not UTF-8).
#[cfg(feature = "rocks")]
fn call_raw(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[Vec<u8>],
    timeout: Duration,
) -> std::io::Result<Value> {
    let mut s = flint_tls::connect(addr, tls)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(timeout))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.clone()))).collect(),
        )),
        &mut out,
    );
    s.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16384];
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
            Err(e) => return Err(std::io::Error::other(format!("{e:?}"))),
        }
    }
}

/// List a data dir's system rows — ADR-0011's verification item 2 made a
/// command. "The scrub is asserted, not assumed": a drill checking only
/// user keys would pass with the split-brain hazard fully intact, so this
/// reads the rows the scrub is about, directly, with no server in between.
/// Exit 0 and `none` on a clean dir; exit 1 with one line per surviving row
/// otherwise, so a drill can branch on the status.
#[cfg(feature = "rocks")]
fn inspect() {
    use flint_storage::Kv;
    let dir = arg("--data-dir").unwrap_or_else(|| die("inspect needs --data-dir <dir>"));
    let kv = flint_storage::rocks::RocksKv::open(Path::new(&dir))
        .unwrap_or_else(|e| die(&format!("open {dir}: {e}")));
    let mut rows = Vec::new();
    kv.for_each_prefix(b"\x00flint\x00", &mut |k, v| {
        rows.push((k.to_vec(), v.len()));
        true
    });
    if rows.is_empty() {
        println!("none — no system rows");
        return;
    }
    for (k, len) in &rows {
        println!(
            "{} ({len} bytes)",
            String::from_utf8_lossy(k).escape_debug()
        );
    }
    std::process::exit(1)
}

#[cfg(not(feature = "rocks"))]
fn inspect() {
    die("inspect requires a build with --features rocks");
}

/// `30s` / `15m` / `24h` / bare seconds.
fn parse_dur(s: &str) -> Option<std::time::Duration> {
    let (num, mul) = match s.as_bytes().last()? {
        b's' => (&s[..s.len() - 1], 1u64),
        b'm' => (&s[..s.len() - 1], 60),
        b'h' => (&s[..s.len() - 1], 3600),
        _ => (s, 1),
    };
    num.parse::<u64>()
        .ok()
        .map(|n| std::time::Duration::from_secs(n * mul))
}

/// Set ids under a store root, oldest first. The id embeds the start
/// millisecond, so lexical order is age order.
fn set_ids(store: &dyn ObjectStore) -> Vec<String> {
    let mut ids: Vec<String> = store
        .list("")
        .unwrap_or_default()
        .iter()
        .filter_map(|k| k.split('/').next().map(str::to_string))
        .filter(|p| p.starts_with("backup-"))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Does this set have its manifest — i.e., did its `run` COMPLETE? A set
/// is written objects-first, manifest last, so a manifest-less prefix is a
/// run that died mid-upload: refusable by verify/restore (no index), but
/// invisible to a prune that ranks by id alone.
fn manifested(store: &dyn ObjectStore, id: &str) -> bool {
    !store
        .list(&format!("{id}/{}", flint_backup::MANIFEST_KEY))
        .unwrap_or_default()
        .is_empty()
}

/// The millisecond timestamp a set id embeds, if it parses.
fn set_id_ms(id: &str) -> Option<u64> {
    id.strip_prefix("backup-").and_then(|t| t.parse().ok())
}

/// The policy loop — ADR-0011 D8's three job kinds on flint-sched.
///
/// Each job re-invokes THIS binary's own subcommand rather than calling
/// into shared functions: the thing being scheduled is exactly the command
/// an operator would type, so a rehearsal that passes here proves the
/// command that will be run during an incident, not a cousin of it. It
/// also means a wedged job is a killable child process, not a poisoned
/// thread in the scheduler.
///
/// The status file's load-bearing line is `rehearsed_set` / its age: the
/// alertable metric is the age of the newest artifact that has actually
/// been RESTORED, never the run count — a nightly job that succeeds and
/// produces unrestorable output is indistinguishable from a healthy one by
/// run count, and that is the failure this whole ADR exists to prevent.
fn schedule() {
    use std::sync::{Arc, Mutex};
    let to = arg("--to").unwrap_or_else(|| die("schedule needs --to <dir|s3://...>"));
    let pairs = arg("--pairs").unwrap_or_else(|| die("schedule needs --pairs"));
    let cp_state = arg("--cp-state").unwrap_or_else(|| die("schedule needs --cp-state"));
    let every = arg("--every")
        .and_then(|v| parse_dur(&v))
        .unwrap_or_else(|| die("schedule needs --every <dur>"));
    let verify_every = arg("--verify-every").and_then(|v| parse_dur(&v));
    let rehearse_every = arg("--rehearse-every").and_then(|v| parse_dur(&v));
    let keep: usize = arg("--keep").and_then(|v| v.parse().ok()).unwrap_or(7);
    if keep == 0 {
        die("--keep 0 would prune every set the moment it lands");
    }
    let status_file = arg("--status-file");
    let jitter = arg("--jitter")
        .and_then(|v| parse_dur(&v))
        .unwrap_or(std::time::Duration::ZERO);
    let exe = std::env::current_exe().unwrap_or_else(|e| die(&format!("current_exe: {e}")));

    // One child invocation; the trimmed last stdout line is the summary.
    // Arc'd because each job closure keeps its own handle for the life of
    // the scheduler.
    let exe = std::sync::Arc::new(exe);
    let invoke = move |args: &[&str]| -> Result<String, String> {
        let out = std::process::Command::new(exe.as_ref())
            .args(args)
            .output()
            .map_err(|e| format!("spawn: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let last = stdout.lines().last().unwrap_or("").to_string();
        if out.status.success() {
            Ok(last)
        } else {
            let err = String::from_utf8_lossy(&out.stderr);
            Err(err.lines().last().unwrap_or(&last).to_string())
        }
    };

    // The newest set that a REHEARSAL restored, and when. Shared with the
    // status writer; the backup job never touches it, which is the point.
    let rehearsed: Arc<Mutex<Option<(String, u64)>>> = Arc::new(Mutex::new(None));

    let mut sched = flint_sched::Scheduler::new();
    {
        let invoke = invoke.clone();
        // Computed BEFORE the job closure takes `to`: the first-run delay
        // reads the store once at startup.
        let initial_delay = {
            let store = open_store(&to);
            let newest_ms = set_ids(store.as_ref())
                .iter()
                .rev()
                .find(|id| manifested(store.as_ref(), id))
                .and_then(|id| set_id_ms(id));
            let since = newest_ms
                .map(|ms| now_ms().saturating_sub(ms))
                .unwrap_or(u64::MAX);
            let remaining = (every.as_millis() as u64).saturating_sub(since);
            std::time::Duration::from_millis(remaining)
                + flint_sched::Scheduler::startup_jitter(jitter)
        };
        let (to, pairs, cp_state) = (to.clone(), pairs.clone(), cp_state.clone());
        let tls = arg("--tls");
        let snap = arg("--snap-root");
        sched.add(
            flint_sched::Job::new("backup", every, every / 8, move || {
                let mut a: Vec<String> = vec![
                    "run".into(),
                    "--pairs".into(),
                    pairs.clone(),
                    "--cp-state".into(),
                    cp_state.clone(),
                    "--to".into(),
                    to.clone(),
                ];
                if let Some(t) = &tls {
                    a.extend(["--tls".into(), t.clone()]);
                }
                if let Some(sr) = &snap {
                    a.extend(["--snap-root".into(), sr.clone()]);
                }
                let refs: Vec<&str> = a.iter().map(String::as_str).collect();
                let summary = invoke(&refs)?;
                let store = open_store(&to);
                let ids = set_ids(store.as_ref());
                let wipe = |id: &str| -> bool {
                    store
                        .list(&format!("{id}/"))
                        .unwrap_or_default()
                        .iter()
                        .all(|key| store.delete(key).is_ok())
                };
                // Retention prunes to the newest `keep` COMPLETED sets, and
                // sweeps dead partials, AFTER the new set landed and
                // self-verified. The split matters twice over: a run killed
                // mid-upload leaves a manifest-less prefix that verify and
                // restore refuse but a rank-by-id prune would COUNT — so
                // enough crashes would prune restorable sets while keeping
                // husks, and the husks would accumulate storage forever
                // (#123's shape, in the bucket). A partial is declared dead
                // only once it is older than a full backup interval: newer
                // ones may be another invocation mid-upload, and sweeping a
                // set that is still being written is worse than waiting one
                // interval to collect it.
                let dead_before = now_ms().saturating_sub(every.as_millis() as u64);
                let mut swept = 0usize;
                let completed: Vec<&String> = ids
                    .iter()
                    .filter(|id| {
                        if manifested(store.as_ref(), id) {
                            return true;
                        }
                        if set_id_ms(id).is_some_and(|ms| ms < dead_before) && wipe(id) {
                            swept += 1;
                        }
                        false
                    })
                    .collect();
                let mut pruned = 0usize;
                if completed.len() > keep {
                    for id in &completed[..completed.len() - keep] {
                        if !wipe(id) {
                            return Ok(format!("{summary} (prune of {id} incomplete)"));
                        }
                        pruned += 1;
                    }
                }
                let mut notes = Vec::new();
                if pruned > 0 {
                    notes.push(format!("pruned {pruned} old set(s)"));
                }
                if swept > 0 {
                    notes.push(format!("swept {swept} dead partial(s)"));
                }
                Ok(if notes.is_empty() {
                    summary
                } else {
                    format!("{summary} ({})", notes.join(", "))
                })
            }),
            // Not plain jitter: a seat restarted minutes after its nightly
            // backup would immediately cut a redundant one. The store
            // already records when the newest completed set was taken, so
            // the first run lands where the cadence would have put it —
            // and a store with no sets, or one nobody can list, backs up
            // immediately, because "cannot tell" must fail toward taking a
            // backup, not skipping one.
            initial_delay,
        );
    }
    // Verify and rehearse target the newest COMPLETED set, never the
    // newest prefix: a backup mid-upload is always briefly the newest
    // prefix, and both jobs would chase a set that has no manifest yet —
    // reporting a healthy backup pipeline as failing exactly while it
    // works. (Found by the drill's planted partial, which sorted last.)
    let newest_completed = |to: &str| -> Result<String, String> {
        let store = open_store(to);
        set_ids(store.as_ref())
            .iter()
            .rev()
            .find(|id| manifested(store.as_ref(), id))
            .cloned()
            .ok_or_else(|| "no completed sets yet".into())
    };
    if let Some(vd) = verify_every {
        let invoke = invoke.clone();
        let to = to.clone();
        sched.add(
            flint_sched::Job::new("verify", vd, vd / 8, move || {
                let newest = newest_completed(&to)?;
                invoke(&["verify", "--from", &join_spec(&to, &newest)])
            }),
            flint_sched::Scheduler::startup_jitter(jitter),
        );
    }
    if let Some(rd) = rehearse_every {
        let to = to.clone();
        let rehearsed = rehearsed.clone();
        sched.add(
            flint_sched::Job::new("rehearse", rd, rd / 8, move || {
                let newest = newest_completed(&to)?;
                let dest = std::env::temp_dir()
                    .join(format!("flint-rehearse-{}-{newest}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dest);
                let r = invoke(&[
                    "restore",
                    "--from",
                    &join_spec(&to, &newest),
                    "--into",
                    &dest.to_string_lossy(),
                ]);
                let _ = std::fs::remove_dir_all(&dest);
                let summary = r?;
                *rehearsed.lock().expect("rehearsed lock") = Some((newest.clone(), now_ms()));
                Ok(summary)
            }),
            flint_sched::Scheduler::startup_jitter(jitter),
        );
    }

    eprintln!(
        "schedule: backup every {every:?}, verify {verify_every:?}, rehearse {rehearse_every:?}, keep {keep}, to {to}"
    );
    // The status file is rewritten after every pass — atomically, since an
    // exporter may read it mid-write.
    let stop = std::sync::atomic::AtomicBool::new(false);
    loop {
        let next = sched.tick(std::time::Instant::now());
        if let Some(path) = &status_file {
            let mut s = String::new();
            for (name, st) in sched.stats() {
                let last = match &st.last {
                    Some(Ok(m)) => format!("ok {m}"),
                    Some(Err(e)) => format!("err {e}"),
                    None => "pending".into(),
                };
                s.push_str(&format!(
                    "job {name} runs {} failures {} consecutive {} last_ok_ms {} {last}
",
                    st.runs,
                    st.failures,
                    st.consecutive_failures,
                    st.last_ok_wall_ms.unwrap_or(0),
                ));
            }
            if let Some((id, at)) = rehearsed.lock().expect("rehearsed lock").clone() {
                s.push_str(&format!(
                    "rehearsed_set {id}
"
                ));
                s.push_str(&format!(
                    "rehearsed_at_ms {at}
"
                ));
                s.push_str(&format!(
                    "rehearsed_age_s {}
",
                    now_ms().saturating_sub(at) / 1000
                ));
            } else {
                s.push_str(
                    "rehearsed_set none
",
                );
            }
            let tmp = format!("{path}.tmp");
            if std::fs::write(&tmp, &s).is_ok() {
                let _ = std::fs::rename(&tmp, path);
            }
        }
        let sleep = next
            .map(|n| n.saturating_duration_since(std::time::Instant::now()))
            .unwrap_or(std::time::Duration::from_secs(1))
            .min(std::time::Duration::from_secs(1));
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(sleep);
    }
}

fn run() {
    let to = arg("--to").unwrap_or_else(|| die("run needs --to <dir>"));
    let cp_state = arg("--cp-state").unwrap_or_else(|| die("run needs --cp-state <path>"));
    let pairs_spec = arg("--pairs").unwrap_or_else(|| die("run needs --pairs <a,b;c,d>"));
    let pairs: Vec<Vec<String>> = pairs_spec
        .split(';')
        .filter(|p| !p.is_empty())
        .map(|p| p.split(',').map(str::to_string).collect())
        .collect();
    if pairs.is_empty() {
        die("--pairs named no pair");
    }
    let tls = arg("--tls").map(|d| {
        flint_tls::client_config(
            &format!("{d}/ca.crt"),
            &format!("{d}/int.crt"),
            &format!("{d}/int.key"),
        )
        .unwrap_or_else(|e| die(&format!("load mesh certs from {d}: {e}")))
    });

    let started = now_ms();
    let id = format!("backup-{started}");
    // A set is written to its own location and never into an occupied one:
    // a half-written set sharing a prefix with a good one is how a restore
    // picks up objects from two different backups. Checked by LISTING, the
    // one emptiness probe both stores can answer.
    let set_spec = join_spec(&to, &id);
    let store = open_store(&set_spec);
    match store.list("") {
        Ok(keys) if keys.is_empty() => {}
        Ok(_) => die(&format!("{set_spec} already holds objects")),
        Err(e) => die(&format!("list {set_spec}: {e}")),
    }

    // A dedicated snapshot root, NOT the controller's. FLINTSNAPSHOT writes
    // <root>/<id> and repoints <root>/LATEST, and the controller's
    // spare-restore seeds a replacement node from whatever LATEST names.
    // Sharing the root would make every backup silently redirect disaster
    // recovery at a checkpoint this process is about to delete.
    let snap_root = arg("--snap-root").unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("flint-backup-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    });

    let mut objects = Vec::new();
    let mut records = Vec::new();
    for (i, pair) in pairs.iter().enumerate() {
        let master = master_of(pair, &tls)
            .unwrap_or_else(|| die(&format!("pair {i} has no master among {pair:?}")));
        let epoch = info_field(&master, &tls, "role_epoch:").unwrap_or_default();
        let seq = info_field(&master, &tls, "latest_seq:")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let pair_root = format!("{snap_root}/p{i}");
        // Generous: a checkpoint hard-links SSTs so it is near-instant, but
        // it flushes memtables first and that is real I/O on a busy master.
        let reply = call(
            &master,
            &tls,
            &["FLINTSNAPSHOT", &pair_root],
            Duration::from_secs(120),
        );
        let snap_id = match reply {
            Ok(Value::Simple(s)) => s.trim_start_matches("OK ").to_string(),
            other => die(&format!("pair {i} snapshot on {master}: {other:?}")),
        };
        let dir = Path::new(&pair_root).join(&snap_id);

        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| die(&format!("read checkpoint {}: {e}", dir.display())));
        for entry in entries {
            let entry = entry.unwrap_or_else(|e| die(&format!("checkpoint entry: {e}")));
            if !entry.path().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = format!("pairs/{i}/{name}");
            store
                .put_file(&key, &entry.path())
                .unwrap_or_else(|e| die(&format!("store {key}: {e}")));
            objects.push(record(store.as_ref(), &key));
        }
        // Delete the checkpoint as soon as its bytes are safe. A checkpoint
        // hard-links the live SSTs, so leaving one behind pins those files
        // against compaction reclaim: cheap the moment it is cut, and a
        // disk leak that grows with every scheduled run if it is not.
        let _ = std::fs::remove_dir_all(&pair_root);

        records.push(PairRecord {
            index: i,
            master,
            epoch,
            seq,
        });
    }

    // The control plane: pairs, slot ranges, tenants, hashed tokens, quotas.
    // Without it a restore has keys and nothing that can route to them.
    store
        .put_file("cp-state", Path::new(&cp_state))
        .unwrap_or_else(|e| die(&format!("capture cp-state from {cp_state}: {e}")));
    objects.push(record(store.as_ref(), "cp-state"));

    let manifest = Manifest {
        id: id.clone(),
        started_ms: started,
        finished_ms: now_ms(),
        release: flint_build::version(env!("CARGO_PKG_VERSION")),
        cp_source: cp_state,
        pairs: records,
        objects,
    };
    store
        .write(flint_backup::MANIFEST_KEY, manifest.render().as_bytes())
        .unwrap_or_else(|e| die(&format!("write manifest: {e}")));

    // Verify what was just written, before reporting success. A backup
    // nobody has read back is a claim; the cost is one pass over bytes that
    // are still in page cache, and it is the only thing standing between
    // "the job succeeded" and "the job produced something restorable".
    match flint_backup::load_verified(store.as_ref()) {
        Ok((_, r)) => println!(
            "OK {id} — {} objects, {} bytes, verified",
            r.objects, r.bytes
        ),
        Err(e) => die(&format!("set written but does not verify: {e}")),
    }
}

fn record(store: &dyn ObjectStore, key: &str) -> ObjectRecord {
    let mut r = store
        .open(key)
        .unwrap_or_else(|e| die(&format!("reopen {key}: {e}")));
    let mut bytes = 0u64;
    // Hash and size in one streaming pass over what was actually STORED,
    // not over the source file. Checksumming the input would certify a copy
    // nobody checked; this way a store that truncates is caught here rather
    // than at restore.
    let mut counting = Counting {
        inner: &mut r,
        n: &mut bytes,
    };
    let sha256 = flint_tls::sha256_stream_hex(&mut counting)
        .unwrap_or_else(|e| die(&format!("hash {key}: {e}")));
    ObjectRecord {
        key: key.to_string(),
        bytes,
        sha256,
    }
}

struct Counting<'a> {
    inner: &'a mut Box<dyn Read>,
    n: &'a mut u64,
}

impl Read for Counting<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        *self.n += n as u64;
        Ok(n)
    }
}
