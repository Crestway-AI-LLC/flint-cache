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
        Some("inspect") => inspect(),
        Some("--build-version") => println!("{}", flint_build::version(env!("CARGO_PKG_VERSION"))),
        _ => {
            eprintln!(
                "usage:\n  \
                 flint-backup run     --pairs <a,b;c,d> --cp-state <path> --to <dir> \
                 [--tls <certs-dir>] [--snap-root <dir>]\n  \
                 flint-backup verify  --from <dir>\n  \
                 flint-backup restore --from <dir> --into <dir>"
            );
            std::process::exit(2)
        }
    }
}

fn verify() {
    let from = arg("--from").unwrap_or_else(|| die("verify needs --from <dir>"));
    let store = LocalDir::new(&from);
    match flint_backup::load_verified(&store) {
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
    let store = LocalDir::new(&from);
    // Verification is not separable from restore: the one entry point both
    // loads and checks, so a corrupt or tampered set is refused before a
    // single byte lands in the destination.
    let (manifest, report) = match flint_backup::load_verified(&store) {
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
    // A set is written to its own directory and never into an existing one:
    // a half-written set sharing a directory with a good one is how a
    // restore picks up objects from two different backups.
    let root = Path::new(&to).join(&id);
    if root.exists() {
        die(&format!("{} already exists", root.display()));
    }
    let store = LocalDir::new(&root);

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
            objects.push(record(&store, &key));
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
    objects.push(record(&store, "cp-state"));

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
    match flint_backup::load_verified(&store) {
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
