// SPDX-License-Identifier: Elastic-2.0
//! The channel's side of `FT.*` (ADR-0060), for the co-processor binary: each
//! [`FtPlan`] performed over the command's channel, the documents a routed
//! write touched read back and indexed, an index backfilled from its
//! prefixes' keys, and a namespace's indexes loaded again on a rebuild.
//!
//! A routed write and every read that indexes a document hold that key's
//! lock across the write, the read and the commit, so the index takes each
//! key's states in the order they were written: the last read is of the last
//! write, and nothing else writes the key (a node refuses it, ADR-0060's
//! amendment).

use crate::{err, now_ms, read_reply, run_persist, send_cmd};
use flint_resp::Value;
use flint_vec::Store;
use flint_vec::ft::IndexDef;
use flint_vec::ftstore::{
    self, DocRead, FtPlan, Reads, SearchPlan, WritePlan, ft_alias_list_key, ft_backfill_key,
    ft_def_key, ft_docs_key, ft_list_key,
};
use flint_vec::{INDEX_BUCKETS, bucket_of};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;

/// How many keys one `SCAN` of a backfill asks each pair for.
const SCAN_COUNT: &[u8] = b"200";

/// The locks a document's writes and reads take, by a hash of its namespace
/// and key. Fixed in number, so they cost nothing per key.
struct KeyLocks(Box<[Mutex<()>]>);

impl KeyLocks {
    fn new() -> Self {
        KeyLocks((0..256).map(|_| Mutex::new(())).collect())
    }

    fn shard(&self, ns: &[u8], key: &[u8]) -> usize {
        let mut h: u32 = 0x811c_9dc5;
        for b in ns.iter().chain([0u8].iter()).chain(key) {
            h ^= *b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        h as usize % self.0.len()
    }

    /// Hold every key's lock, taken in one order so two writers of
    /// overlapping keys cannot deadlock.
    fn lock<'a>(&'a self, ns: &[u8], keys: &[&[u8]]) -> Vec<MutexGuard<'a, ()>> {
        let mut shards: Vec<usize> = keys.iter().map(|k| self.shard(ns, k)).collect();
        shards.sort_unstable();
        shards.dedup();
        shards
            .into_iter()
            .map(|s| self.0[s].lock().unwrap_or_else(|p| p.into_inner()))
            .collect()
    }
}

/// An `FT.CREATE` backfill not yet finished: where its `SCAN` is.
#[derive(Clone)]
pub(crate) struct Fill {
    index: Vec<u8>,
    prefixes: Vec<Vec<u8>>,
    at: usize,
    cursor: Vec<u8>,
}

impl Fill {
    fn start(index: &[u8], def: &IndexDef) -> Self {
        Fill {
            index: index.to_vec(),
            prefixes: def.prefixes.clone(),
            at: 0,
            cursor: b"0".to_vec(),
        }
    }
}

/// The backfills of indexes made on a namespace already loaded, by
/// namespace and index, and whether a chunk of one is running.
type Fills = Mutex<HashMap<(Vec<u8>, Vec<u8>), (Fill, bool)>>;

/// The process's key locks and backfills: one co-processor, one of each.
static LOCKS: LazyLock<KeyLocks> = LazyLock::new(KeyLocks::new);
static FILLS: LazyLock<Fills> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Forget `ns`'s backfills (FLINTNSDROP, BUG-0255): its indexes are gone.
pub(crate) fn forget(ns: &[u8]) {
    FILLS
        .lock()
        .expect("fills lock")
        .retain(|(n, _), _| n.as_slice() != ns);
}

/// Where a namespace's rebuild of its `FT.*` indexes is up to.
#[derive(Clone)]
pub(crate) enum FtLoad {
    /// Reading the index list and each definition.
    Defs,
    /// Walking the document list from `bucket`, with the keys found so far
    /// and the backfills to run after.
    Docs {
        bucket: u16,
        keys: Vec<Vec<u8>>,
        fills: Vec<Fill>,
    },
    /// Reading each document back, from `next`.
    Read {
        keys: Vec<Vec<u8>>,
        next: usize,
        fills: Vec<Fill>,
    },
    /// Backfilling the indexes whose `FT.CREATE` did not finish.
    Backfill { fills: Vec<Fill> },
}

/// What one channel call came to.
enum Got {
    Reply(Value),
    /// The channel's budget is spent: the expected end of a chunk.
    Spent,
}

/// One command on the channel. `Err` for a channel that failed.
fn call(ch: &mut flint_tls::Stream, parts: &[&[u8]]) -> Result<Got, String> {
    send_cmd(ch, parts).map_err(|e| format!("channel write: {e}"))?;
    match read_reply(ch).map_err(|e| format!("channel read: {e}"))? {
        Value::Error(e) if e.contains("budget") => Ok(Got::Spent),
        v => Ok(Got::Reply(v)),
    }
}

/// Open this command's channel.
pub(crate) fn open(
    callback: &[u8],
    token: &[u8],
    edge: &Option<Arc<flint_tls::ClientConfig>>,
) -> Result<flint_tls::Stream, Value> {
    let addr = std::str::from_utf8(callback).map_err(|_| err("ERR bad callback address"))?;
    let mut ch = flint_tls::connect_edge(addr, edge)
        .map_err(|e| err(&format!("COPROCUNAVAIL channel dial failed: {e}")))?;
    let _ = ch.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = ch.set_write_timeout(Some(Duration::from_secs(5)));
    // Closed with a reset when it drops, after its last reply is read, so
    // a write's channel leaves no TIME_WAIT here (BUG-0269).
    let _ = ch.set_abortive_close();
    send_cmd(&mut ch, &[b"PROXYCHAN", token])
        .map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?;
    if let Value::Error(e) =
        read_reply(&mut ch).map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?
    {
        return Err(err(&format!("COPROCUNAVAIL channel refused: {e}")));
    }
    Ok(ch)
}

/// A family command's one channel: the one its grant opens, or one a rebuild
/// chunk opened on the same grant and handed back once it finished the
/// namespace (ADR-0017's D3 amendment). A grant opens one channel only.
pub(crate) struct Grant<'a> {
    pub(crate) token: &'a [u8],
    pub(crate) callback: &'a [u8],
    pub(crate) edge: &'a Option<Arc<flint_tls::ClientConfig>>,
    pub(crate) warm: Option<flint_tls::Stream>,
}

impl Grant<'_> {
    /// The channel: the one handed back, else this grant's own.
    pub(crate) fn open(&mut self) -> Result<flint_tls::Stream, Value> {
        match self.warm.take() {
            Some(ch) => Ok(ch),
            None => open(self.callback, self.token, self.edge),
        }
    }
}

/// Make `prefixes` the namespace's FT index prefixes on every pair
/// (ADR-0060 amendment 2): the proxy fans `FLINTINDEXED` out to every master
/// and fails if any cannot be reached. Every write under them that does not
/// come through a channel is refused from then on.
fn register(ch: &mut flint_tls::Stream, prefixes: &[Vec<u8>]) -> Result<(), Value> {
    let mut parts: Vec<&[u8]> = vec![b"PROXYINDEXED"];
    parts.extend(prefixes.iter().map(|p| p.as_slice()));
    match call(ch, &parts) {
        Ok(Got::Reply(Value::Simple(_))) => Ok(()),
        Ok(Got::Reply(Value::Error(e))) => Err(err(&format!(
            "ERR the index's prefixes could not be registered on every pair: {e}"
        ))),
        Ok(Got::Reply(other)) => Err(err(&format!("ERR PROXYINDEXED answered {other:?}"))),
        Ok(Got::Spent) => Err(err("ERR the channel's budget ran out")),
        Err(e) => Err(err(&format!("COPROCUNAVAIL {e}"))),
    }
}

/// Read `key` back as `reads` says: `HGETALL`, `JSON.GET key $`, and, when
/// `ttl`, its TTL. `Ok(None)` when the budget ran out part way.
fn read_doc(
    ch: &mut flint_tls::Stream,
    key: &[u8],
    reads: Reads,
    ttl: bool,
) -> Result<Option<DocRead>, String> {
    let mut d = DocRead::default();
    if reads.0 {
        match call(ch, &[b"HGETALL", key])? {
            Got::Spent => return Ok(None),
            Got::Reply(Value::Array(Some(items))) => {
                let mut flat = items.into_iter().filter_map(|v| match v {
                    Value::Bulk(Some(b)) => Some(b),
                    _ => None,
                });
                let mut pairs = Vec::new();
                while let (Some(f), Some(v)) = (flat.next(), flat.next()) {
                    pairs.push((f, v));
                }
                d.hash = Some(pairs);
            }
            // WRONGTYPE: not a hash, so not a document of an ON HASH index.
            Got::Reply(_) => d.hash = Some(Vec::new()),
        }
    }
    if reads.1 {
        match call(ch, &[b"JSON.GET", key, b"$"])? {
            Got::Spent => return Ok(None),
            Got::Reply(Value::Bulk(Some(b))) => {
                // `$` answers an array of the one match.
                d.json = serde_json::from_slice::<serde_json::Value>(&b)
                    .ok()
                    .and_then(|v| match v {
                        serde_json::Value::Array(mut a) if a.len() == 1 => a.pop(),
                        _ => None,
                    });
            }
            Got::Reply(_) => {}
        }
    }
    let exists = d.hash.as_ref().is_some_and(|h| !h.is_empty()) || d.json.is_some();
    if ttl && exists {
        match call(ch, &[b"PTTL", key])? {
            Got::Spent => return Ok(None),
            Got::Reply(Value::Integer(ms)) if ms >= 0 => {
                d.expires_at = Some(now_ms().saturating_add(ms as u64));
            }
            Got::Reply(_) => {}
        }
    }
    Ok(Some(d))
}

/// Serve one `FT.*` command or routed write for `ns` on this command's
/// channel.
pub(crate) fn handle(
    ns: &[u8],
    args: &[Vec<u8>],
    store: &Arc<Mutex<Store>>,
    g: &mut Grant<'_>,
) -> Value {
    let plan = store
        .lock()
        .expect("store lock")
        .plan_ft(ns, args, now_ms());
    match plan {
        FtPlan::Reply(v) => {
            // A search or FT.INFO on an index still backfilling lends this
            // command's channel to the backfill, which a search would
            // otherwise wait on for ever: no other command has a channel
            // to spare.
            let lend = matches!(&v, Value::Error(e) if e.starts_with("LOADING"))
                || args[0].eq_ignore_ascii_case(b"FT.INFO");
            if lend && let Some(name) = args.get(1) {
                let index = store
                    .lock()
                    .expect("store lock")
                    .ft_resolve(ns, name)
                    .unwrap_or_else(|| name.clone());
                lend_to_fill(ns, &index, store, g);
            }
            v
        }
        FtPlan::Alias {
            alias,
            index,
            persist,
        } => {
            let mut ch = match g.open() {
                Ok(ch) => ch,
                Err(e) => return e,
            };
            if let Err(e) = run_persist(&mut ch, &persist) {
                return e;
            }
            store
                .lock()
                .expect("store lock")
                .ft_set_alias(ns, alias, index);
            Value::Simple("OK".into())
        }
        FtPlan::Create {
            index,
            def,
            persist,
        } => {
            let mut ch = match g.open() {
                Ok(ch) => ch,
                Err(e) => return e,
            };
            // The three steps of the amendment. First every pair refuses a
            // write under the new prefixes that does not come through here,
            // so the backfill below misses nothing written meanwhile; a pair
            // that cannot be reached fails the command before anything of
            // the index is kept.
            let mut prefixes = store.lock().expect("store lock").ft_prefixes(ns);
            prefixes.extend(def.prefixes.iter().cloned());
            prefixes.sort();
            prefixes.dedup();
            if let Err(e) = register(&mut ch, &prefixes) {
                return e;
            }
            if let Err(e) = run_persist(&mut ch, &persist) {
                return e;
            }
            store
                .lock()
                .expect("store lock")
                .ft_create(ns, index.clone(), def.clone());
            let fill = Fill::start(&index, &def);
            run_fill(&mut ch, ns, fill, store);
            Value::Simple("OK".into())
        }
        FtPlan::Drop {
            index,
            persist,
            docs,
        } => {
            let mut ch = match g.open() {
                Ok(ch) => ch,
                Err(e) => return e,
            };
            if let Err(e) = run_persist(&mut ch, &persist) {
                return e;
            }
            store.lock().expect("store lock").ft_drop(ns, &index);
            // The prefixes left are the only ones guarded now. A pair that
            // misses this still sends their writes here, which writes them.
            let left = store.lock().expect("store lock").ft_prefixes(ns);
            if let Err(e) = register(&mut ch, &left) {
                eprintln!(
                    "flint-vec: ns {:?} FT.DROPINDEX: {e:?}; the dropped prefixes stay guarded until the next rebuild",
                    String::from_utf8_lossy(ns)
                );
            }
            FILLS
                .lock()
                .expect("fills lock")
                .remove(&(ns.to_vec(), index.clone()));
            if let Some(docs) = docs.filter(|d| !d.is_empty()) {
                let keys: Vec<&[u8]> = docs.iter().map(|k| k.as_slice()).collect();
                let _held = LOCKS.lock(ns, &keys);
                let mut cmd: Vec<&[u8]> = vec![b"DEL"];
                cmd.extend(keys.iter().copied());
                match call(&mut ch, &cmd) {
                    Ok(Got::Reply(Value::Error(e))) => return Value::Error(e),
                    Ok(Got::Reply(_)) => {}
                    Ok(Got::Spent) => {
                        return err("ERR FT.DROPINDEX DD: the channel's budget ran out");
                    }
                    Err(e) => return err(&format!("COPROCUNAVAIL {e}")),
                }
                let mut st = store.lock().expect("store lock");
                for k in &docs {
                    st.ft_reindex(ns, k, &DocRead::default());
                }
            }
            Value::Simple("OK".into())
        }
        FtPlan::Search(plan) => search(&plan, g),
        FtPlan::Write(plan) => write(ns, &plan, store, g),
    }
}

/// `FT.SEARCH`: read each hit back for the reply.
fn search(plan: &SearchPlan, g: &mut Grant<'_>) -> Value {
    if plan.nocontent || plan.hits.keys.is_empty() {
        return ftstore::search_reply(plan, &[]);
    }
    let mut ch = match g.open() {
        Ok(ch) => ch,
        Err(e) => return e,
    };
    let reads = match plan.on {
        flint_vec::ft::On::Hash => (true, false),
        flint_vec::ft::On::Json => (false, true),
    };
    let mut docs = Vec::with_capacity(plan.hits.keys.len());
    for (key, _) in &plan.hits.keys {
        match read_doc(&mut ch, key, reads, false) {
            Ok(Some(d)) => docs.push(Some(d)),
            Ok(None) => {
                return err(
                    "ERR FT.SEARCH reads each result back and this LIMIT is past the channel's budget: ask for fewer",
                );
            }
            Err(e) => return err(&format!("COPROCUNAVAIL {e}")),
        }
    }
    ftstore::search_reply(plan, &docs)
}

/// A routed write: the document list, the write, then each key it covers
/// read back and indexed, all under the keys' locks.
fn write(ns: &[u8], plan: &WritePlan, store: &Arc<Mutex<Store>>, g: &mut Grant<'_>) -> Value {
    let keys: Vec<&[u8]> = plan.keys.iter().map(|(k, _)| k.as_slice()).collect();
    let _held = LOCKS.lock(ns, &keys);
    let mut ch = match g.open() {
        Ok(ch) => ch,
        Err(e) => return e,
    };
    if let Err(e) = run_persist(&mut ch, &plan.before) {
        return e;
    }
    let cmd: Vec<&[u8]> = plan.cmd.iter().map(|a| a.as_slice()).collect();
    let reply = match call(&mut ch, &cmd) {
        Ok(Got::Reply(v)) => v,
        Ok(Got::Spent) => return err("ERR the channel's budget ran out before the write"),
        Err(e) => return err(&format!("COPROCUNAVAIL {e}")),
    };
    if matches!(reply, Value::Error(_)) || plan.keys.is_empty() {
        return reply;
    }
    // A delete needs no read: under its lock nothing writes the key again.
    let deleted = matches!(
        plan.cmd[0].to_ascii_uppercase().as_slice(),
        b"DEL" | b"UNLINK"
    );
    for (key, reads) in &plan.keys {
        let read = if deleted {
            Ok(Some(DocRead::default()))
        } else {
            read_doc(&mut ch, key, *reads, true)
        };
        match read {
            Ok(Some(d)) => {
                let left = store.lock().expect("store lock").ft_reindex(ns, key, &d);
                for why in left {
                    eprintln!(
                        "flint-vec: ns {:?} key {:?} left out of index {why}",
                        String::from_utf8_lossy(ns),
                        String::from_utf8_lossy(key)
                    );
                }
            }
            // The write landed and its read did not: the index would answer
            // the key as it was. Take the key out, so a search never returns
            // what is no longer there, and say so; the tenant's retry of the
            // write puts it back.
            Ok(None) | Err(_) => {
                store
                    .lock()
                    .expect("store lock")
                    .ft_reindex(ns, key, &DocRead::default());
                eprintln!(
                    "flint-vec: ns {:?} key {:?} written but not read back; out of its index until written again",
                    String::from_utf8_lossy(ns),
                    String::from_utf8_lossy(key)
                );
                return err(
                    "COPROCUNAVAIL the write landed but could not be indexed; write the key again",
                );
            }
        }
    }
    reply
}

/// A glob that matches every key under `prefix`.
fn glob(prefix: &[u8]) -> Vec<u8> {
    let mut g = Vec::with_capacity(prefix.len() + 1);
    for &b in prefix {
        if matches!(b, b'*' | b'?' | b'[' | b']' | b'\\') {
            g.push(b'\\');
        }
        g.push(b);
    }
    g.push(b'*');
    g
}

/// Backfill as much of `fill` as this channel's budget allows: `Ok(None)`
/// when every prefix's keys are in, else where to resume.
fn fill_chunk(
    ch: &mut flint_tls::Stream,
    ns: &[u8],
    mut fill: Fill,
    store: &Arc<Mutex<Store>>,
) -> Result<Option<Fill>, String> {
    while fill.at < fill.prefixes.len() {
        let pattern = glob(&fill.prefixes[fill.at]);
        let page = match call(
            ch,
            &[
                b"SCAN",
                &fill.cursor,
                b"MATCH",
                &pattern,
                b"COUNT",
                SCAN_COUNT,
            ],
        )? {
            Got::Spent => return Ok(Some(fill)),
            Got::Reply(Value::Array(Some(page))) if page.len() == 2 => page,
            Got::Reply(other) => return Err(format!("SCAN answered {other:?}")),
        };
        let next = match &page[0] {
            Value::Bulk(Some(c)) => c.clone(),
            other => return Err(format!("SCAN cursor {other:?}")),
        };
        let keys: Vec<Vec<u8>> = match &page[1] {
            Value::Array(Some(ks)) => ks
                .iter()
                .filter_map(|k| match k {
                    Value::Bulk(Some(b)) => Some(b.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        // A page cut short by the budget is read again from its start next
        // time: indexing a key twice gives the same index.
        for key in &keys {
            let Some(reads) = store.lock().expect("store lock").ft_reads(ns, key) else {
                continue;
            };
            let _held = LOCKS.lock(ns, &[key.as_slice()]);
            let Some(d) = read_doc(ch, key, reads, true)? else {
                return Ok(Some(fill));
            };
            if d.hash.as_ref().is_some_and(|h| !h.is_empty()) || d.json.is_some() {
                match call(ch, &[b"SADD", &ft_docs_key(bucket_of(key)), key])? {
                    Got::Spent => return Ok(Some(fill)),
                    Got::Reply(Value::Error(e)) => return Err(format!("SADD: {e}")),
                    Got::Reply(_) => {}
                }
            }
            // Into the index being filled, and not over a write's copy
            // (BUG-0271).
            store
                .lock()
                .expect("store lock")
                .ft_fill_doc(ns, &fill.index, key, &d);
        }
        if next == b"0" {
            fill.at += 1;
        }
        fill.cursor = next;
    }
    // Every key is in. The marker goes last; a crash before it only makes
    // the next rebuild backfill again.
    match call(ch, &[b"DEL", &ft_backfill_key(&fill.index)])? {
        Got::Spent => Ok(Some(Fill {
            at: fill.prefixes.len(),
            ..fill
        })),
        _ => Ok(None),
    }
}

/// Run `fill` on this channel, then record where it got to.
fn run_fill(ch: &mut flint_tls::Stream, ns: &[u8], fill: Fill, store: &Arc<Mutex<Store>>) {
    let index = fill.index.clone();
    let key = (ns.to_vec(), index.clone());
    match fill_chunk(ch, ns, fill.clone(), store) {
        Ok(None) => {
            store.lock().expect("store lock").ft_set_ready(ns, &index);
            FILLS.lock().expect("fills lock").remove(&key);
        }
        Ok(Some(more)) => {
            FILLS.lock().expect("fills lock").insert(key, (more, false));
        }
        Err(e) => {
            eprintln!(
                "flint-vec: ns {:?} index {:?} backfill: {e}; resuming on the next search",
                String::from_utf8_lossy(ns),
                String::from_utf8_lossy(&index)
            );
            FILLS.lock().expect("fills lock").insert(key, (fill, false));
        }
    }
}

/// Lend this command's channel to `index`'s backfill, when one is waiting
/// and none is running.
fn lend_to_fill(ns: &[u8], index: &[u8], store: &Arc<Mutex<Store>>, g: &mut Grant<'_>) {
    let key = (ns.to_vec(), index.to_vec());
    let fill = {
        let mut f = FILLS.lock().expect("fills lock");
        match f.get_mut(&key) {
            Some((fill, running)) if !*running => {
                *running = true;
                fill.clone()
            }
            _ => return,
        }
    };
    match g.open() {
        Ok(mut ch) => run_fill(&mut ch, ns, fill, store),
        Err(_) => {
            if let Some((_, running)) = FILLS.lock().expect("fills lock").get_mut(&key) {
                *running = false;
            }
        }
    }
}

/// One chunk of a namespace's `FT.*` rebuild on a rebuild's channel:
/// `Ok(None)` when every index is in and serving, else where to resume.
/// Installs nothing once `generation` is past (BUG-0255).
pub(crate) fn load_chunk(
    ch: &mut flint_tls::Stream,
    ns: &[u8],
    store: &Arc<Mutex<Store>>,
    mut at: FtLoad,
    generation: u64,
) -> Result<Option<FtLoad>, String> {
    let current = || store.lock().expect("store lock").generation(ns) == generation;
    loop {
        at = match at {
            FtLoad::Defs => {
                let names = match call(ch, &[b"SMEMBERS", &ft_list_key()])? {
                    Got::Spent => return Ok(Some(FtLoad::Defs)),
                    Got::Reply(Value::Array(Some(ms))) => ms
                        .into_iter()
                        .filter_map(|v| match v {
                            Value::Bulk(Some(b)) => Some(b),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                    Got::Reply(_) => Vec::new(),
                };
                let mut fills = Vec::new();
                let mut defs = Vec::new();
                for name in names {
                    let row = match call(ch, &[b"GET", &ft_def_key(&name)])? {
                        Got::Spent => return Ok(Some(FtLoad::Defs)),
                        Got::Reply(Value::Bulk(Some(b))) => b,
                        Got::Reply(_) => continue,
                    };
                    let Some(def) = std::str::from_utf8(&row).ok().and_then(IndexDef::decode)
                    else {
                        eprintln!(
                            "flint-vec: ns {:?} index {:?} left out: its definition row is one this binary cannot read",
                            String::from_utf8_lossy(ns),
                            String::from_utf8_lossy(&name)
                        );
                        continue;
                    };
                    match call(ch, &[b"EXISTS", &ft_backfill_key(&name)])? {
                        Got::Spent => return Ok(Some(FtLoad::Defs)),
                        Got::Reply(Value::Integer(1)) => fills.push(Fill::start(&name, &def)),
                        Got::Reply(_) => {}
                    }
                    defs.push((name, def));
                }
                // Every alias, with its index, in one read (amendment 3, part 4).
                let aliases: Vec<(Vec<u8>, Vec<u8>)> =
                    match call(ch, &[b"SMEMBERS", &ft_alias_list_key()])? {
                        Got::Spent => return Ok(Some(FtLoad::Defs)),
                        Got::Reply(Value::Array(Some(ms))) => ms
                            .into_iter()
                            .filter_map(|v| match v {
                                Value::Bulk(Some(b)) => ftstore::parse_alias_member(&b),
                                _ => None,
                            })
                            .collect(),
                        Got::Reply(_) => Vec::new(),
                    };
                if !current() {
                    return Ok(None);
                }
                let mut st = store.lock().expect("store lock");
                for (name, def) in defs {
                    st.ft_create(ns, name, def);
                }
                for (alias, index) in aliases {
                    st.ft_set_alias(ns, alias, Some(index));
                }
                if st.ft_names(ns).is_empty() {
                    return Ok(None);
                }
                let prefixes = st.ft_prefixes(ns);
                drop(st);
                // Every rebuild writes the prefixes again, so a pair that lost
                // its row, restored from an older backup, has it again before
                // this namespace serves (ADR-0060 amendment 2).
                if let Err(e) = register(ch, &prefixes) {
                    return Err(format!("{e:?}"));
                }
                FtLoad::Docs {
                    bucket: 0,
                    keys: Vec::new(),
                    fills,
                }
            }
            FtLoad::Docs {
                mut bucket,
                mut keys,
                fills,
            } => {
                while bucket < INDEX_BUCKETS {
                    match call(ch, &[b"SMEMBERS", &ft_docs_key(bucket)])? {
                        Got::Spent => {
                            return Ok(Some(FtLoad::Docs {
                                bucket,
                                keys,
                                fills,
                            }));
                        }
                        Got::Reply(Value::Array(Some(ms))) => {
                            keys.extend(ms.into_iter().filter_map(|v| match v {
                                Value::Bulk(Some(b)) => Some(b),
                                _ => None,
                            }));
                        }
                        Got::Reply(_) => {}
                    }
                    bucket += 1;
                }
                FtLoad::Read {
                    keys,
                    next: 0,
                    fills,
                }
            }
            FtLoad::Read {
                keys,
                mut next,
                fills,
            } => {
                while next < keys.len() {
                    let key = &keys[next];
                    let Some(reads) = store.lock().expect("store lock").ft_reads(ns, key) else {
                        next += 1;
                        continue;
                    };
                    let _held = LOCKS.lock(ns, &[key.as_slice()]);
                    let Some(d) = read_doc(ch, key, reads, true)? else {
                        return Ok(Some(FtLoad::Read { keys, next, fills }));
                    };
                    if current() {
                        store.lock().expect("store lock").ft_reindex(ns, key, &d);
                    }
                    next += 1;
                }
                FtLoad::Backfill { fills }
            }
            FtLoad::Backfill { mut fills } => {
                while let Some(f) = fills.first().cloned() {
                    match fill_chunk(ch, ns, f, store)? {
                        Some(more) => {
                            fills[0] = more;
                            return Ok(Some(FtLoad::Backfill { fills }));
                        }
                        None => {
                            fills.remove(0);
                        }
                    }
                }
                if current() {
                    let mut st = store.lock().expect("store lock");
                    for name in st.ft_names(ns) {
                        st.ft_set_ready(ns, &name);
                    }
                }
                return Ok(None);
            }
        };
    }
}
