// SPDX-License-Identifier: Elastic-2.0
//! A namespace's `FT.*` indexes in the co-processor (ADR-0060). Each command
//! is planned under the store lock as what it needs of the channel, and
//! committed once that has landed; the binary does the channel's part, as it
//! does for `VEC.*`.
//!
//! The documents are the tenant's own keys. The co-processor writes them for
//! the tenant (ADR-0060 A: a node refuses any other writer of a key under an
//! index's prefix), reads each back, and indexes what it read, so the index
//! is never ahead of the durable document and never silently behind it.

use crate::ft::{self, IndexDef, On, Search};
use crate::ftagg::{self, Cursor, CursorState, Row};
use crate::ftexpr::Datum;
use crate::ftindex::{FtIndex, Hits, Source};
use crate::{Cap, Persist, Store, bucket_of, durable_key, err};
use flint_resp::Value;
use serde_json::Value as Json;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering::Relaxed;

/// `FT.CREATE`'s refusal while the co-processor runs without `--ft`: the
/// release that brings `FT.*` makes an index only when asked to (ADR-0060,
/// two releases). An index made while it was on is still served, so it can
/// be searched, written and dropped, and its prefixes are never left
/// guarding writes no one serves.
pub const FT_OFF: &str =
    "ERR FT.* is not enabled on this co-processor: FT.CREATE needs --ft (ADR-0060)";

/// The durable list of a namespace's index names (`kind = b'f'`).
pub fn ft_list_key() -> Vec<u8> {
    durable_key(b'f', b"", b"")
}

/// An index's definition row (`kind = b'd'`): [`IndexDef::encode`].
pub fn ft_def_key(index: &[u8]) -> Vec<u8> {
    durable_key(b'd', index, b"")
}

/// Present while an index's backfill has not finished (`kind = b'b'`), so a
/// rebuild knows to read its prefixes again rather than trust the document
/// list, which holds only what the co-processor has written.
pub fn ft_backfill_key(index: &[u8]) -> Vec<u8> {
    durable_key(b'b', index, b"")
}

/// The durable set of a namespace's aliases (`kind = b'a'`), each member an
/// [`alias_member`], so one read gives a rebuild every alias with its index
/// (ADR-0060 amendment 3, part 4).
pub fn ft_alias_list_key() -> Vec<u8> {
    durable_key(b'a', b"", b"")
}

/// `<alias length>:<alias><index>`: both names in one member, so moving an
/// alias is a remove and an add, never a read and a rewrite.
pub fn alias_member(alias: &[u8], index: &[u8]) -> Vec<u8> {
    let mut m = format!("{}:", alias.len()).into_bytes();
    m.extend_from_slice(alias);
    m.extend_from_slice(index);
    m
}

/// An [`alias_member`]'s alias and index.
pub fn parse_alias_member(m: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let colon = m.iter().position(|&b| b == b':')?;
    let len: usize = std::str::from_utf8(&m[..colon]).ok()?.parse().ok()?;
    let rest = &m[colon + 1..];
    (len > 0 && len < rest.len()).then(|| (rest[..len].to_vec(), rest[len..].to_vec()))
}

/// One bucket of the durable list of the documents the co-processor has
/// written under any index's prefix (`kind = b'k'`), which a rebuild reads
/// them back from. Bucketed as a set's ids are (`INDEX_BUCKETS`).
pub fn ft_docs_key(bucket: u16) -> Vec<u8> {
    durable_key(b'k', b"", format!("{bucket:03}").as_bytes())
}

/// The data writes the co-processor makes for a key an index covers. The
/// three clients write documents with nothing else (ADR-0059's survey),
/// and redisvl's `update_by_filter` with a script that runs `HSET` or
/// `JSON.MERGE` on its one declared key (ADR-0060 amendment 4); every
/// other write to such a key is refused, by the node, and here.
pub fn routed_write(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"HSET"
            | b"HMSET"
            | b"HDEL"
            | b"DEL"
            | b"UNLINK"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"PERSIST"
            | b"JSON.SET"
            | b"JSON.MERGE"
            | b"JSON.DEL"
            | b"JSON.FORGET"
            | b"EVAL"
            | b"EVALSHA"
    )
}

/// Whether the co-processor answers `name` as an `FT.*` command: the
/// family itself, and the writes a proxy sends it for an indexed key.
pub fn is_ft_command(name: &[u8]) -> bool {
    (name.len() > 3 && name[..3].eq_ignore_ascii_case(b"FT.")) || routed_write(name)
}

/// The keys a routed write writes. A script's are the keys it declares: the
/// node lets a script from the channel write no other key under an index's
/// prefix (ADR-0060 amendment 4), so they are all it can have written there.
fn write_keys(args: &[Vec<u8>]) -> &[Vec<u8>] {
    match args.first().map(|n| n.to_ascii_uppercase()).as_deref() {
        Some(b"DEL" | b"UNLINK") => &args[1..],
        Some(b"EVAL" | b"EVALSHA") => args
            .get(2)
            .and_then(|n| std::str::from_utf8(n).ok()?.parse::<usize>().ok())
            .and_then(|n| args.get(3..3usize.checked_add(n)?))
            .unwrap_or(&[]),
        _ => &args[1..args.len().min(2)],
    }
}

/// A document as the channel read it back: `HGETALL` when an `ON HASH`
/// index covers its key, `JSON.GET key $` when an `ON JSON` one does, and
/// its TTL. `None` for a read that was not made or found nothing.
#[derive(Debug, Default)]
pub struct DocRead {
    pub hash: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub json: Option<Json>,
    pub expires_at: Option<u64>,
}

/// What a key needs read back: `(HGETALL, JSON.GET)`.
pub type Reads = (bool, bool);

pub enum FtPlan {
    /// Serve now.
    Reply(Value),
    /// `FT.CREATE`: write `persist`, register the namespace's prefixes on
    /// every pair, commit the index, and backfill it from its prefixes'
    /// keys before it answers a search.
    Create {
        index: Vec<u8>,
        def: IndexDef,
        persist: Vec<Persist>,
    },
    /// `FT.DROPINDEX`: write `persist`, commit, and register the prefixes
    /// left; with `DD`, delete `docs` too.
    Drop {
        index: Vec<u8>,
        persist: Vec<Persist>,
        docs: Option<Vec<Vec<u8>>>,
    },
    /// `FT.ALIASADD`, `FT.ALIASUPDATE` and `FT.ALIASDEL`: write `persist`,
    /// then point `alias` at `index`, or with `None` remove it.
    Alias {
        alias: Vec<u8>,
        index: Option<Vec<u8>>,
        persist: Vec<Persist>,
    },
    /// `FT.SEARCH`: the hits, then each document read for the reply.
    Search(SearchPlan),
    /// `FT.AGGREGATE` and `FT.CURSOR READ` (ADR-0064): a page of rows, read
    /// back where the index does not hold a field, run and answered by
    /// [`aggregate_page`].
    Aggregate(Box<AggPlan>),
    /// A data write to keys an index may cover.
    Write(WritePlan),
}

/// One `FT.AGGREGATE` reply, or one page of its cursor.
pub struct AggPlan {
    pub plan: ftagg::Plan,
    pub on: On,
    pub page: Page,
    /// The cursor this page is of, new or taken out of the table for the
    /// read; the caller puts it back unless the page is its last.
    pub cursor: Option<Cursor>,
    /// A cursor's walk reached the index's end with this page.
    pub walked_out: bool,
}

pub enum Page {
    /// Rows for the pipeline to run over.
    Run(Vec<Row>),
    /// Rows a sort, group or `LIMIT` already made, with their names and
    /// this page's count.
    Ran {
        names: Vec<Box<str>>,
        rows: Vec<Row>,
        total: usize,
    },
}

/// The open `FT.AGGREGATE` cursors (ADR-0064), by id, over every namespace
/// as RediSearch's ids are; a read finds only its own namespace's.
#[derive(Default)]
pub struct Cursors {
    by_id: HashMap<u64, Cursor>,
    rng: u64,
}

/// RediSearch's `INDEX_CURSOR_LIMIT`.
pub const CURSORS_PER_INDEX: usize = 128;

impl Cursors {
    /// Drops every cursor idle past its time, as RediSearch's timer reaps
    /// them.
    fn sweep(&mut self, now: u64) {
        self.by_id.retain(|_, c| c.expires_at > now);
    }

    /// An id no open cursor has: random, non-zero, under 2^31.
    fn fresh_id(&mut self, now: u64) -> u64 {
        if self.rng == 0 {
            self.rng = now ^ 0x9E37_79B9_7F4A_7C15 ^ ((std::process::id() as u64) << 32);
        }
        loop {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            let id = self.rng & 0x7fff_ffff;
            if id != 0 && !self.by_id.contains_key(&id) {
                return id;
            }
        }
    }

    fn bytes(&self, ns: Option<&[u8]>) -> usize {
        self.by_id
            .values()
            .filter(|c| ns.is_none_or(|n| c.ns == n))
            .map(Cursor::bytes)
            .sum()
    }
}

pub struct SearchPlan {
    pub hits: Hits,
    pub on: On,
    pub nocontent: bool,
    /// `WITHSCORES`: each key's score after it (ADR-0063).
    pub withscores: bool,
    pub returns: Option<Vec<(Box<str>, Box<str>)>>,
    /// The KNN distance's name, for a KNN search.
    pub alias: Option<Box<str>>,
    /// Each further vector range's distance name, in the query's order
    /// (ADR-0060 amendment 5).
    pub more_aliases: Vec<Box<str>>,
    /// The attributes' names and paths, for a JSON `RETURN`.
    pub paths: Vec<(Box<str>, Box<str>)>,
}

pub struct WritePlan {
    /// The command, as the tenant sent it.
    pub cmd: Vec<Vec<u8>>,
    /// Each key it writes that an index covers, and what to read back.
    pub keys: Vec<(Vec<u8>, Reads)>,
    /// Before the write: each covered key into the durable document list, so
    /// a crash between leaves a name the rebuild finds nothing under rather
    /// than a document no rebuild finds.
    pub before: Vec<Persist>,
}

fn bulk(b: &[u8]) -> Value {
    Value::Bulk(Some(b.to_vec()))
}

fn int(n: usize) -> Value {
    Value::Integer(n as i64)
}

impl Store {
    /// Make `FT.*` indexes (ADR-0060). Off for a bare `Store`; the binary
    /// turns it on with `--ft`. Off, only `FT.CREATE` is refused.
    pub fn set_ft(&mut self, on: bool) {
        self.ft_on = on;
    }

    pub fn ft_enabled(&self) -> bool {
        self.ft_on
    }

    fn ft_index(&self, ns: &[u8], index: &[u8]) -> Option<&FtIndex> {
        self.ft.get(&(ns.to_vec(), index.to_vec()))
    }

    /// The index `name` names: an index of that name, or the one an alias of
    /// that name points at, as RediSearch looks a name up.
    pub fn ft_resolve(&self, ns: &[u8], name: &[u8]) -> Option<Vec<u8>> {
        if self.ft_index(ns, name).is_some() {
            return Some(name.to_vec());
        }
        let index = self.ft_aliases.get(&(ns.to_vec(), name.to_vec()))?;
        self.ft_index(ns, index).map(|_| index.clone())
    }

    /// The aliases pointing at `index`.
    fn ft_aliases_of(&self, ns: &[u8], index: &[u8]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = self
            .ft_aliases
            .iter()
            .filter(|((n, _), i)| n.as_slice() == ns && i.as_slice() == index)
            .map(|((_, a), _)| a.clone())
            .collect();
        out.sort();
        out
    }

    /// Point `alias` at `index`, or with `None` remove it, once its rows
    /// landed.
    pub fn ft_set_alias(&mut self, ns: &[u8], alias: Vec<u8>, index: Option<Vec<u8>>) {
        let k = (ns.to_vec(), alias);
        match index {
            Some(i) => {
                self.ft_aliases.insert(k, i);
            }
            None => {
                self.ft_aliases.remove(&k);
            }
        }
    }

    /// The indexes of `ns`, by name.
    pub fn ft_names(&self, ns: &[u8]) -> Vec<Vec<u8>> {
        let mut names: Vec<Vec<u8>> = self
            .ft
            .keys()
            .filter(|(n, _)| n.as_slice() == ns)
            .map(|(_, i)| i.clone())
            .collect();
        names.sort();
        names
    }

    /// Every prefix an index of `ns` covers, which each pair holds and
    /// guards (ADR-0060's amendments): sorted, without repeats.
    pub fn ft_prefixes(&self, ns: &[u8]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = self
            .ft
            .iter()
            .filter(|((n, _), _)| n.as_slice() == ns)
            .flat_map(|(_, ix)| ix.def().prefixes.iter().cloned())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// What reading `key` back needs, for the indexes of `ns` that cover it.
    pub fn ft_reads(&self, ns: &[u8], key: &[u8]) -> Option<Reads> {
        let mut r = (false, false);
        for ((n, _), ix) in &self.ft {
            if n.as_slice() == ns && ix.covers(key) {
                match ix.def().on {
                    On::Hash => r.0 = true,
                    On::Json => r.1 = true,
                }
            }
        }
        (r.0 || r.1).then_some(r)
    }

    /// Plan one `FT.*` command or routed write. `args[0]` is its name.
    pub fn plan_ft(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> FtPlan {
        let Some(name) = args.first() else {
            return FtPlan::Reply(err("ERR empty command"));
        };
        match name.to_ascii_uppercase().as_slice() {
            b"FT.CREATE" if !self.ft_on => FtPlan::Reply(err(FT_OFF)),
            b"FT.CREATE" => self.plan_ft_create(ns, args),
            b"FT.SEARCH" => self.plan_ft_search(ns, args, now),
            b"FT.AGGREGATE" => self.plan_ft_aggregate(ns, args, now),
            b"FT.CURSOR" => self.plan_ft_cursor(ns, args, now),
            b"FT.DROPINDEX" => self.plan_ft_drop(ns, args),
            b"FT.ALIASADD" | b"FT.ALIASUPDATE" | b"FT.ALIASDEL" => self.plan_ft_alias(ns, args),
            b"FT.INFO" => FtPlan::Reply(self.ft_info(ns, args, now)),
            b"FT._LIST" => FtPlan::Reply(Value::Array(Some(
                self.ft_names(ns).iter().map(|n| bulk(n)).collect(),
            ))),
            n if routed_write(n) => self.plan_ft_write(ns, args),
            other => FtPlan::Reply(err(&format!(
                "ERR {} is not supported (ADR-0060 serves FT.CREATE, FT.SEARCH, FT.INFO, \
                 FT._LIST, FT.DROPINDEX and the FT.ALIAS commands; ADR-0064 FT.AGGREGATE and \
                 FT.CURSOR)",
                String::from_utf8_lossy(other)
            ))),
        }
    }

    fn plan_ft_create(&self, ns: &[u8], args: &[Vec<u8>]) -> FtPlan {
        let (index, def) = match ft::parse_create(args) {
            Ok(x) => x,
            Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
        };
        if self.ft_index(ns, &index).is_some() {
            return FtPlan::Reply(err("ERR Index already exists"));
        }
        // An index's documents are its prefixes' keys, and the co-processor's
        // own rows are never one of them.
        if def
            .prefixes
            .iter()
            .any(|p| p.starts_with(crate::KEY_PREFIX) || crate::KEY_PREFIX.starts_with(p))
        {
            return FtPlan::Reply(err(
                "ERR FT.CREATE PREFIX overlaps the co-processor's reserved rows",
            ));
        }
        let persist = vec![
            // The backfill marker first, so a crash at any later point
            // leaves an index the rebuild backfills again.
            Persist::Put {
                key: ft_backfill_key(&index),
                val: b"1".to_vec(),
                ttl_ms: None,
            },
            Persist::Put {
                key: ft_def_key(&index),
                val: def.encode().into_bytes(),
                ttl_ms: None,
            },
            Persist::SAdd {
                key: ft_list_key(),
                member: index.clone(),
            },
        ];
        FtPlan::Create {
            index,
            def,
            persist,
        }
    }

    fn plan_ft_drop(&self, ns: &[u8], args: &[Vec<u8>]) -> FtPlan {
        let (index, dd) = match ft::parse_drop(args) {
            Ok(x) => x,
            Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
        };
        // An alias names its index here too, as in RediSearch.
        let Some((index, ix)) = self
            .ft_resolve(ns, &index)
            .and_then(|i| self.ft_index(ns, &i).map(|ix| (i, ix)))
        else {
            return FtPlan::Reply(err("ERR Unknown Index name"));
        };
        // The list member first: a crash after it leaves a definition row
        // no rebuild reads, never an index the tenant dropped coming back.
        let mut persist = vec![
            Persist::SRem {
                key: ft_list_key(),
                member: index.clone(),
            },
            Persist::Del {
                key: ft_def_key(&index),
            },
            Persist::Del {
                key: ft_backfill_key(&index),
            },
        ];
        // Its aliases go with it, as RediSearch clears them.
        persist.extend(
            self.ft_aliases_of(ns, &index)
                .into_iter()
                .map(|a| Persist::SRem {
                    key: ft_alias_list_key(),
                    member: alias_member(&a, &index),
                }),
        );
        let docs = dd.then(|| {
            let mut ks: Vec<Vec<u8>> = ix.keys().map(<[u8]>::to_vec).collect();
            ks.sort();
            ks
        });
        FtPlan::Drop {
            index,
            persist,
            docs,
        }
    }

    /// `FT.ALIASADD <alias> <index>`, `FT.ALIASUPDATE <alias> <index>` and
    /// `FT.ALIASDEL <alias>`, refused as RediSearch refuses them (ADR-0060
    /// amendment 3, part 4).
    fn plan_ft_alias(&self, ns: &[u8], args: &[Vec<u8>]) -> FtPlan {
        let cmd = args[0].to_ascii_uppercase();
        let del = cmd.as_slice() == b"FT.ALIASDEL";
        if args.len() != if del { 2 } else { 3 } {
            return FtPlan::Reply(err(&format!(
                "ERR wrong number of arguments for '{}' command",
                String::from_utf8_lossy(&args[0])
            )));
        }
        let alias = args[1].clone();
        let now = self.ft_aliases.get(&(ns.to_vec(), alias.clone())).cloned();
        let index = if del {
            if now.is_none() {
                return FtPlan::Reply(err("ERR Alias does not exist"));
            }
            None
        } else {
            let index = args[2].clone();
            if self.ft_index(ns, &index).is_none() {
                return FtPlan::Reply(err("ERR Unknown index name (or name is an alias itself)"));
            }
            if self.ft_index(ns, &alias).is_some() {
                return FtPlan::Reply(err("ERR Alias conflicts with an existing index name"));
            }
            if now.is_some() && cmd.as_slice() == b"FT.ALIASADD" {
                return FtPlan::Reply(err("ERR Alias already exists"));
            }
            Some(index)
        };
        // Off its old index first: a crash between the two leaves the alias
        // naming nothing, never the wrong index.
        let mut persist: Vec<Persist> = now
            .iter()
            .map(|old| Persist::SRem {
                key: ft_alias_list_key(),
                member: alias_member(&alias, old),
            })
            .collect();
        if let Some(i) = &index {
            persist.push(Persist::SAdd {
                key: ft_alias_list_key(),
                member: alias_member(&alias, i),
            });
        }
        FtPlan::Alias {
            alias,
            index,
            persist,
        }
    }

    /// The index `name` names in `ns`, ready to answer, or the reply that
    /// says why not, as `FT.SEARCH` answers.
    fn ft_ready(&self, ns: &[u8], name: &[u8]) -> Result<(Vec<u8>, &FtIndex), Value> {
        let found = self
            .ft_resolve(ns, name)
            .and_then(|i| Some((i.clone(), self.ft_index(ns, &i)?)));
        let Some((index, ix)) = found else {
            return Err(err(&format!(
                "ERR {}: no such index",
                String::from_utf8_lossy(name)
            )));
        };
        if !ix.ready() {
            return Err(err("LOADING the index is still being built, retry"));
        }
        Ok((index, ix))
    }

    /// `FT.AGGREGATE` (ADR-0064): the request compiled against the index,
    /// its matches as the pipeline's first rows; with `WITHCURSOR`, a new
    /// cursor, its first page the matches it walks first.
    fn plan_ft_aggregate(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> FtPlan {
        let Some(name) = args.get(1) else {
            return FtPlan::Reply(err("ERR FT.AGGREGATE <index> <query> [options]"));
        };
        let (index, ix) = match self.ft_ready(ns, name) {
            Ok(x) => x,
            Err(v) => return FtPlan::Reply(v),
        };
        let compiled = ftagg::parse(args).and_then(|req| {
            let q = ft::parse_query(&req.query, ix.def(), &req.params)?;
            let metrics: Vec<&str> = q.knn.iter().chain(&q.more).map(|k| &*k.alias).collect();
            let plan = ftagg::compile(&req, ix.def(), &metrics)?;
            Ok((req, q, plan))
        });
        let (req, q, plan) = match compiled {
            Ok(x) => x,
            Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
        };
        let hits = match ix.agg_matches(&q, req.scorer, req.addscores, now) {
            Ok(h) => h,
            Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
        };
        let Some(opts) = req.cursor else {
            let rows = hits.iter().map(|h| ix.agg_row(&plan, h)).collect();
            return FtPlan::Aggregate(Box::new(AggPlan {
                plan,
                on: ix.def().on,
                page: Page::Run(rows),
                cursor: None,
                walked_out: true,
            }));
        };
        let mut cursors = self.ft_cursors.lock().expect("cursors lock");
        cursors.sweep(now);
        let open = cursors
            .by_id
            .values()
            .filter(|c| c.ns == ns && c.index == index)
            .count();
        if open >= CURSORS_PER_INDEX {
            return FtPlan::Reply(err(&format!(
                "ERR INDEX_CURSOR_LIMIT of {CURSORS_PER_INDEX} has been reached for an index"
            )));
        }
        let id = cursors.fresh_id(now);
        drop(cursors);
        // A pipeline that only loads, applies and filters is walked as
        // RediSearch walks it, a page of matches at a time; any other is
        // computed at the first read, as RediSearch's sorter and grouper
        // read every match before their first row.
        let stream = !plan.buffered && q.knn.is_none() && !req.withcount;
        let (rows, walked_out, state) = if stream {
            let page: Vec<_> = hits.iter().take(opts.count).collect();
            let after = page.last().map_or(0, |h| h.seq);
            let rows = page.iter().map(|h| ix.agg_row(&plan, h)).collect();
            (
                rows,
                hits.len() < opts.count,
                CursorState::Stream {
                    req: req.clone(),
                    after,
                },
            )
        } else {
            let rows = hits.iter().map(|h| ix.agg_row(&plan, h)).collect();
            (
                rows,
                true,
                CursorState::Rows {
                    names: Vec::new(),
                    rows: VecDeque::new(),
                    total: req.withcount.then_some(0),
                },
            )
        };
        FtPlan::Aggregate(Box::new(AggPlan {
            on: ix.def().on,
            page: Page::Run(rows),
            cursor: Some(Cursor {
                id,
                ns: ns.to_vec(),
                index,
                generation: ix.generation(),
                count: opts.count,
                max_idle_ms: opts.max_idle_ms,
                expires_at: now + opts.max_idle_ms,
                plan: plan.clone(),
                state,
            }),
            plan,
            walked_out,
        }))
    }

    /// `FT.CURSOR READ <index> <id> [COUNT <n>]` and `FT.CURSOR DEL <index>
    /// <id>` (ADR-0064), in RediSearch's words. A read takes its cursor out
    /// of the table until its page is answered, so a second read of it
    /// meanwhile finds none, as in RediSearch.
    fn plan_ft_cursor(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> FtPlan {
        let sub = args.get(1).map(|a| a.to_ascii_uppercase());
        let sub = match sub.as_deref() {
            Some(b"READ") => "READ",
            Some(b"DEL") => "DEL",
            Some(_) => return FtPlan::Reply(err("ERR unknown subcommand")),
            None => {
                return FtPlan::Reply(err("ERR wrong number of arguments for 'FT.CURSOR' command"));
            }
        };
        if args.len() < 4 {
            return FtPlan::Reply(err(&format!(
                "ERR wrong number of arguments for 'FT.CURSOR|{sub}' command"
            )));
        }
        let Some(id) = std::str::from_utf8(&args[3])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return FtPlan::Reply(err("ERR Bad cursor ID"));
        };
        let mut count = None;
        if sub == "READ" && args.len() > 5 {
            if !args[4].eq_ignore_ascii_case(b"COUNT") {
                return FtPlan::Reply(err(&format!(
                    "ERR Unknown argument `{}`",
                    String::from_utf8_lossy(&args[4])
                )));
            }
            match std::str::from_utf8(&args[5])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(n) if n > 0 => count = Some(n),
                Some(_) => {}
                None => {
                    return FtPlan::Reply(err(&format!(
                        "ERR Bad value for COUNT: `{}`",
                        String::from_utf8_lossy(&args[5])
                    )));
                }
            }
        }
        let resolved = self.ft_resolve(ns, &args[2]);
        let mut cursors = self.ft_cursors.lock().expect("cursors lock");
        cursors.sweep(now);
        let mine = cursors.by_id.get(&id).is_some_and(|c| c.ns == ns);
        if resolved.is_none() {
            return FtPlan::Reply(err(&format!(
                "ERR {}: no such index",
                String::from_utf8_lossy(&args[2])
            )));
        }
        if sub == "DEL" {
            return FtPlan::Reply(if mine && cursors.by_id.remove(&id).is_some() {
                Value::Simple("OK".into())
            } else {
                err("ERR Cursor does not exist")
            });
        }
        let not_found = || FtPlan::Reply(err(&format!("ERR Cursor not found, id: {id}")));
        if !mine {
            return not_found();
        }
        let Some(mut c) = cursors.by_id.remove(&id) else {
            return not_found();
        };
        drop(cursors);
        let ix = match resolved.as_deref().and_then(|i| self.ft_index(ns, i)) {
            Some(ix) if c.index == resolved.clone().unwrap_or_default() => ix,
            _ => return not_found(),
        };
        if ix.generation() != c.generation {
            return FtPlan::Reply(err("ERR The index was dropped while the cursor was idle"));
        }
        if let Some(n) = count {
            c.count = n;
        }
        let n = c.count;
        match &mut c.state {
            CursorState::Stream { req, after } => {
                let hits = ft::parse_query(&req.query, ix.def(), &req.params)
                    .and_then(|q| ix.agg_matches(&q, req.scorer, req.addscores, now));
                let hits = match hits {
                    Ok(h) => h,
                    Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
                };
                let next: Vec<_> = hits.iter().filter(|h| h.seq > *after).collect();
                let page: Vec<_> = next.iter().take(n).collect();
                if let Some(last) = page.last() {
                    *after = last.seq;
                }
                let rows = page.iter().map(|h| ix.agg_row(&c.plan, h)).collect();
                FtPlan::Aggregate(Box::new(AggPlan {
                    plan: c.plan.clone(),
                    on: ix.def().on,
                    page: Page::Run(rows),
                    walked_out: next.len() < n,
                    cursor: Some(c),
                }))
            }
            CursorState::Rows { names, rows, total } => {
                let take = n.min(rows.len());
                let page: Vec<Row> = rows.drain(..take).collect();
                let page_total = total.unwrap_or(0);
                let names = names.clone();
                FtPlan::Aggregate(Box::new(AggPlan {
                    plan: c.plan.clone(),
                    on: ix.def().on,
                    page: Page::Ran {
                        names,
                        rows: page,
                        total: page_total,
                    },
                    walked_out: true,
                    cursor: Some(c),
                }))
            }
        }
    }

    /// Puts a cursor back after its page, idle from `now` (ADR-0064).
    pub fn ft_cursor_put(&self, mut c: Cursor, now: u64) {
        c.expires_at = now + c.max_idle_ms;
        let mut cursors = self.ft_cursors.lock().expect("cursors lock");
        cursors.by_id.insert(c.id, c);
    }

    /// What open cursors hold for `ns`, or for every namespace, in RAM:
    /// charged to the D4 meter with the indexes.
    pub(crate) fn ft_cursor_bytes(&self, ns: Option<&[u8]>) -> usize {
        self.ft_cursors.lock().expect("cursors lock").bytes(ns)
    }

    fn plan_ft_search(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> FtPlan {
        let Some(index) = args.get(1) else {
            return FtPlan::Reply(err("ERR FT.SEARCH <index> <query> [options]"));
        };
        let Some(ix) = self
            .ft_resolve(ns, index)
            .and_then(|i| self.ft_index(ns, &i))
        else {
            return FtPlan::Reply(err(&format!(
                "ERR {}: no such index",
                String::from_utf8_lossy(index)
            )));
        };
        if !ix.ready() {
            return FtPlan::Reply(err("LOADING the index is still being built, retry"));
        }
        let s: Search = match ft::parse_search(args, ix.def()) {
            Ok(s) => s,
            Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
        };
        let hits = match ix.search(&s, now) {
            Ok(h) => h,
            Err(e) => return FtPlan::Reply(err(&format!("ERR {e}"))),
        };
        FtPlan::Search(SearchPlan {
            hits,
            on: ix.def().on,
            nocontent: s.nocontent,
            withscores: s.withscores,
            returns: s.returns,
            alias: s.knn.map(|k| k.alias),
            more_aliases: s.more.into_iter().map(|k| k.alias).collect(),
            paths: ix
                .def()
                .attrs
                .iter()
                .map(|a| (a.field.name.clone(), a.path.clone()))
                .chain(
                    ix.def()
                        .vectors
                        .iter()
                        .map(|v| (v.name.clone(), v.path.clone())),
                )
                .collect(),
        })
    }

    fn plan_ft_write(&self, ns: &[u8], args: &[Vec<u8>]) -> FtPlan {
        let keys = write_keys(args);
        let script =
            args[0].eq_ignore_ascii_case(b"EVAL") || args[0].eq_ignore_ascii_case(b"EVALSHA");
        if keys.is_empty() && script {
            return FtPlan::Reply(err(
                "ERR a script that writes a key under an FT index's prefix declares it in KEYS \
                 (ADR-0060 amendment 4)",
            ));
        }
        if keys.is_empty() {
            return FtPlan::Reply(err(&format!(
                "ERR wrong number of arguments for '{}'",
                String::from_utf8_lossy(&args[0]).to_lowercase()
            )));
        }
        let covered: Vec<(Vec<u8>, Reads)> = keys
            .iter()
            .filter_map(|k| self.ft_reads(ns, k).map(|r| (k.clone(), r)))
            .collect();
        // D4: a namespace at its cap takes no new documents. The write is
        // the tenant's own key, so this is checked before it, as VEC.SET
        // checks; what one document will cost is not known until it is read
        // back, so a namespace may end a write a document past the cap.
        if !covered.is_empty() {
            match self.over_cap(ns, 1, 0) {
                Some(Cap::Namespace(used)) => {
                    self.refused_ns.fetch_add(1, Relaxed);
                    return FtPlan::Reply(err(&format!(
                        "VECFULL index memory limit reached ({used} of {} bytes for this namespace)",
                        self.index_cap
                    )));
                }
                Some(Cap::Server) => {
                    self.refused_total.fetch_add(1, Relaxed);
                    return FtPlan::Reply(err(
                        "VECFULL index memory is full on this server; delete documents, or retry later",
                    ));
                }
                None => {}
            }
        }
        let deletes = matches!(args[0].to_ascii_uppercase().as_slice(), b"DEL" | b"UNLINK");
        let before = if deletes {
            Vec::new()
        } else {
            covered
                .iter()
                .map(|(k, _)| Persist::SAdd {
                    key: ft_docs_key(bucket_of(k)),
                    member: k.clone(),
                })
                .collect()
        };
        FtPlan::Write(WritePlan {
            cmd: args.to_vec(),
            keys: covered,
            before,
        })
    }

    /// Commit a planned `FT.CREATE` once its rows landed. The index answers
    /// no search until [`Store::ft_set_ready`].
    pub fn ft_create(&mut self, ns: &[u8], index: Vec<u8>, def: IndexDef) {
        self.ft
            .entry((ns.to_vec(), index))
            .or_insert_with(|| FtIndex::new(def));
    }

    /// The backfill or rebuild of `index` has read every document.
    pub fn ft_set_ready(&mut self, ns: &[u8], index: &[u8]) {
        if let Some(ix) = self.ft.get_mut(&(ns.to_vec(), index.to_vec())) {
            ix.set_ready();
        }
    }

    /// Drop `index` from memory, crediting what it held; whether it was
    /// there.
    pub fn ft_drop(&mut self, ns: &[u8], index: &[u8]) -> bool {
        let Some(ix) = self.ft.remove(&(ns.to_vec(), index.to_vec())) else {
            return false;
        };
        self.ft_aliases
            .retain(|(n, _), i| !(n.as_slice() == ns && i.as_slice() == index));
        let freed = ix.bytes();
        if let Some(e) = self.ns_bytes.get_mut(ns) {
            *e = e.saturating_sub(freed);
        }
        true
    }

    /// Index `key` as the channel read it back, in every index of `ns` that
    /// covers it, charging the change to the namespace. Returns why any
    /// index left it out (a vector of the wrong length), for the log.
    pub fn ft_reindex(&mut self, ns: &[u8], key: &[u8], read: &DocRead) -> Vec<String> {
        let mut left_out = Vec::new();
        let (mut new, mut old) = (0usize, 0usize);
        for ((n, name), ix) in self.ft.iter_mut() {
            if n.as_slice() != ns || !ix.covers(key) {
                continue;
            }
            old += ix.charge(key) + ix.held();
            let src = match (ix.def().on, &read.hash, &read.json) {
                (On::Hash, Some(h), _) if !h.is_empty() => Some(Source::Hash(h)),
                (On::Json, _, Some(j)) => Some(Source::Json(j)),
                _ => None,
            };
            match src {
                Some(src) => {
                    if let Err(e) = ix.put(key, src, read.expires_at) {
                        left_out.push(format!("{}: {e}", String::from_utf8_lossy(name)));
                    }
                }
                None => {
                    ix.remove(key);
                }
            }
            new += ix.charge(key) + ix.held();
        }
        let e = self.ns_bytes.entry(ns.to_vec()).or_default();
        *e = e.saturating_add(new).saturating_sub(old);
        left_out
    }

    /// A backfill's read of `key`, into `index` alone, and only when the
    /// index does not hold it yet (BUG-0271). A write since the index was
    /// made already put it there, read under the key's lock as the backfill
    /// reads it, so that copy is the key as it is. Putting it again would
    /// move it to the end of the write order that ties and unsorted results
    /// come in (ADR-0060 amendment 3, part 6), and putting it into every
    /// index covering the key, as [`Store::ft_reindex`] does for a write,
    /// would move it in indexes that are not backfilling at all. Why it was
    /// left out, if it was.
    pub fn ft_fill_doc(
        &mut self,
        ns: &[u8],
        index: &[u8],
        key: &[u8],
        read: &DocRead,
    ) -> Option<String> {
        let ix = self.ft.get_mut(&(ns.to_vec(), index.to_vec()))?;
        if !ix.covers(key) || ix.contains(key) {
            return None;
        }
        let src = match (ix.def().on, &read.hash, &read.json) {
            (On::Hash, Some(h), _) if !h.is_empty() => Source::Hash(h),
            (On::Json, _, Some(j)) => Source::Json(j),
            _ => return None,
        };
        let held = ix.held();
        let left_out = ix.put(key, src, read.expires_at).err();
        let added = (ix.charge(key) + ix.held()).saturating_sub(held);
        let e = self.ns_bytes.entry(ns.to_vec()).or_default();
        *e = e.saturating_add(added);
        left_out
    }

    /// Drop every expired document from every index, crediting what that
    /// frees; how many entries went. Called from [`Store::sweep_expired`].
    pub(crate) fn ft_sweep(&mut self, now: u64) -> usize {
        let mut swept = 0;
        for ((ns, _), ix) in self.ft.iter_mut() {
            let gone = ix.expired(now);
            if gone.is_empty() {
                continue;
            }
            let before = ix.held() + gone.iter().map(|k| ix.charge(k)).sum::<usize>();
            for k in &gone {
                ix.remove(k);
            }
            let freed = before.saturating_sub(ix.held());
            if let Some(e) = self.ns_bytes.get_mut(ns) {
                *e = e.saturating_sub(freed);
            }
            swept += gone.len();
        }
        swept
    }

    /// `FT.INFO <index>`: RediSearch's field names for what this index has,
    /// the ones `redisvl` reads to rebuild a schema and to count documents.
    fn ft_info(&self, ns: &[u8], args: &[Vec<u8>], _now: u64) -> Value {
        let [_, name] = args else {
            return err("ERR FT.INFO <index>");
        };
        // An alias answers for its index, under the index's own name.
        let Some((index, ix)) = self
            .ft_resolve(ns, name)
            .and_then(|i| self.ft_index(ns, &i).map(|ix| (i, ix)))
        else {
            return err("ERR Unknown index name");
        };
        let index = index.as_slice();
        let def = ix.def();
        let s = |x: &str| bulk(x.as_bytes());
        let mut attrs: Vec<Value> = def
            .attrs
            .iter()
            .map(|a| {
                let mut v = vec![
                    s("identifier"),
                    s(&a.path),
                    s("attribute"),
                    s(&a.field.name),
                    s("type"),
                    s(match a.field.kind {
                        crate::filter::FieldKind::Tag | crate::filter::FieldKind::FoldedTag => {
                            "TAG"
                        }
                        crate::filter::FieldKind::Numeric => "NUMERIC",
                        crate::filter::FieldKind::Text => "TEXT",
                        crate::filter::FieldKind::Geo => "GEO",
                    }),
                ];
                // RediSearch's layout, which redisvl reads back: the
                // valued options from the seventh element, then the flags.
                if matches!(
                    a.field.kind,
                    crate::filter::FieldKind::Tag | crate::filter::FieldKind::FoldedTag
                ) {
                    v.push(s("SEPARATOR"));
                    // RediSearch answers a JSON TAG's absent one as "".
                    v.push(bulk(if a.separator == 0 {
                        &[]
                    } else {
                        std::slice::from_ref(&a.separator)
                    }));
                }
                if a.field.kind == crate::filter::FieldKind::Text {
                    v.push(s("WEIGHT"));
                    v.push(s(a.weight.as_deref().unwrap_or("1")));
                }
                if a.field.kind == crate::filter::FieldKind::Tag {
                    v.push(s("CASESENSITIVE"));
                }
                v.extend(a.flags.iter().map(|f| s(f)));
                if a.noindex {
                    v.push(s("NOINDEX"));
                }
                Value::Array(Some(v))
            })
            .collect();
        for v in &def.vectors {
            attrs.push(Value::Array(Some(
                vec![
                    s("identifier"),
                    s(&v.path),
                    s("attribute"),
                    s(&v.name),
                    s("type"),
                    s("VECTOR"),
                    s("algorithm"),
                    s(&v.index.as_str().to_ascii_uppercase()),
                    s("data_type"),
                    s(v.ty.as_str()),
                    s("dim"),
                    int(v.dim),
                    s("distance_metric"),
                    s(&v.metric.as_str().to_ascii_uppercase()),
                ]
                .into_iter()
                .chain(v.hints.iter().flat_map(|(k, x)| {
                    // RediSearch spells `M` so, and the others in lower case.
                    let k = if &**k == "M" {
                        "M".to_string()
                    } else {
                        k.to_ascii_lowercase()
                    };
                    [s(&k), s(x)]
                }))
                .collect(),
            )));
        }
        Value::Array(Some(
            vec![
                s("index_name"),
                bulk(index),
                s("index_options"),
                Value::Array(Some(Vec::new())),
                s("index_definition"),
                Value::Array(Some(vec![
                    s("key_type"),
                    s(match def.on {
                        On::Hash => "HASH",
                        On::Json => "JSON",
                    }),
                    s("prefixes"),
                    Value::Array(Some(def.prefixes.iter().map(|p| bulk(p)).collect())),
                    s("default_score"),
                    s("1"),
                ])),
                s("attributes"),
                Value::Array(Some(attrs)),
                s("num_docs"),
                int(ix.len()),
                s("hash_indexing_failures"),
                int(ix.failures() as usize),
                s("indexing"),
                int(usize::from(!ix.ready())),
                s("percent_indexed"),
                s(if ix.ready() { "1" } else { "0" }),
            ]
            .into_iter()
            .chain(def.stopwords.iter().flat_map(|sw| {
                [
                    s("stopwords_list"),
                    Value::Array(Some(sw.iter().map(|w| s(w)).collect())),
                ]
            }))
            .collect(),
        ))
    }
}

/// A JSON value as `RETURN` answers it: a string as itself, anything else
/// as its JSON text.
fn json_text(v: &Json) -> Vec<u8> {
    match v {
        Json::String(s) => s.as_bytes().to_vec(),
        other => other.to_string().into_bytes(),
    }
}

/// The value at `$.a.b`, or the whole document for `$`.
fn json_path<'a>(doc: &'a Json, path: &str) -> Option<&'a Json> {
    if path == "$" {
        return Some(doc);
    }
    path.strip_prefix("$.")?
        .split('.')
        .try_fold(doc, |v, m| v.get(m))
}

/// What `path` holds in document `d`, as `LOAD` gives it: a HASH field as
/// written; a JSON string as itself, a number as a number, anything else
/// as its JSON text. A bare name on a JSON index is the member `$.<name>`.
fn doc_value(d: &DocRead, on: On, path: &str) -> Option<Datum> {
    match on {
        On::Hash => d
            .hash
            .as_ref()?
            .iter()
            .rev()
            .find(|(f, _)| f == path.as_bytes())
            .map(|(_, v)| Datum::str(v)),
        On::Json => {
            let p = if path.starts_with('$') {
                path.to_string()
            } else {
                format!("$.{path}")
            };
            json_path(d.json.as_ref()?, &p).map(|v| match v {
                Json::Number(n) => n
                    .as_f64()
                    .map_or_else(|| Datum::str(n.to_string()), Datum::Num),
                Json::Null => Datum::Null,
                other => Datum::Str(json_text(other).into()),
            })
        }
    }
}

/// `LOAD *`: every field of a HASH, in its order; a JSON document as `$`.
fn doc_fields(d: &DocRead, on: On) -> Vec<(Box<str>, Datum)> {
    match on {
        On::Hash => d
            .hash
            .iter()
            .flatten()
            .map(|(f, v)| (String::from_utf8_lossy(f).into(), Datum::str(v)))
            .collect(),
        On::Json => d
            .json
            .iter()
            .map(|j| ("$".into(), Datum::Str(j.to_string().into_bytes().into())))
            .collect(),
    }
}

/// Reads one document back for [`aggregate_page`]: `Ok(None)` when the
/// channel's budget is spent.
pub type DocReader<'a> = dyn FnMut(&[u8]) -> Result<Option<DocRead>, String> + 'a;

/// One `FT.AGGREGATE` reply, or one page of its cursor (ADR-0064): the
/// rows read back where the plan needs a field the index does not hold,
/// the pipeline run over them, and the reply, RESP2's shape. With the
/// cursor to put back when it has pages left. `read` reads one document,
/// `Ok(None)` when the channel's budget is spent.
pub fn aggregate_page(p: AggPlan, read: &mut DocReader<'_>, seed: u64) -> (Value, Option<Cursor>) {
    let AggPlan {
        plan,
        on,
        page,
        cursor,
        walked_out,
    } = p;
    let reads: Vec<(usize, Box<str>)> = plan
        .root
        .iter()
        .enumerate()
        .filter_map(|(s, k)| match &k.load {
            Some(ftagg::Load::Read(path)) => Some((s, path.clone())),
            _ => None,
        })
        .collect();
    // A document gone since it matched is left out, as RediSearch's loader
    // drops it.
    let gone = |d: &DocRead| match on {
        On::Hash => d.hash.as_ref().is_none_or(|h| h.is_empty()),
        On::Json => d.json.is_none(),
    };
    let mut read_rows = |rows: &mut Vec<Row>, late: bool| -> Result<(), Value> {
        let mut kept = Vec::with_capacity(rows.len());
        for mut r in rows.drain(..) {
            let Some((key, _)) = r.doc.clone() else {
                kept.push(r);
                continue;
            };
            let d = match read(&key) {
                Ok(Some(d)) => d,
                Ok(None) => {
                    return Err(err(
                        "ERR FT.AGGREGATE reads documents back and this page is past the channel's budget: ask for fewer (ADR-0064)",
                    ));
                }
                Err(e) => return Err(err(&format!("COPROCUNAVAIL {e}"))),
            };
            if gone(&d) {
                continue;
            }
            for (s, path) in &reads {
                if late && !plan.late_reads.contains(s) {
                    continue;
                }
                let v = doc_value(&d, on, path);
                if late {
                    r.vals[*s] = v;
                } else {
                    r.loads[*s] = v;
                }
            }
            if plan.load_all && !late {
                r.all = doc_fields(&d, on);
            }
            kept.push(r);
        }
        *rows = kept;
        Ok(())
    };
    let first = matches!(page, Page::Run(_));
    let (names, mut rows, total) = match page {
        Page::Run(mut rows) => {
            if plan.early_reads {
                if rows.len() > ftagg::MAX_RESULTS {
                    return (
                        err(&format!(
                            "ERR FT.AGGREGATE would read {} documents back, more than {}: narrow the query, or LOAD only what the index holds (ADR-0064)",
                            rows.len(),
                            ftagg::MAX_RESULTS
                        )),
                        None,
                    );
                }
                if let Err(v) = read_rows(&mut rows, false) {
                    return (v, None);
                }
            }
            match ftagg::run(&plan, rows, seed) {
                Ok(ran) => (ran.names, ran.rows, ran.total),
                Err(e) => return (err(&format!("ERR {e}")), None),
            }
        }
        Page::Ran { names, rows, total } => (names, rows, total),
    };
    let Some(mut c) = cursor else {
        rows.truncate(ftagg::MAX_RESULTS);
        if !plan.late_reads.is_empty()
            && let Err(v) = read_rows(&mut rows, true)
        {
            return (v, None);
        }
        return (ftagg::reply(total, &names, &rows), None);
    };
    // A page that fills its COUNT leaves the cursor open, as RediSearch's
    // does: the read after it is the one that answers id 0.
    let count = c.count;
    let (mut page_rows, page_total, done) = match &mut c.state {
        CursorState::Stream { .. } => {
            let n = rows.len();
            (rows, n, walked_out)
        }
        CursorState::Rows {
            names: keep,
            rows: rest,
            total: kept_total,
        } if first => {
            let take = count.min(rows.len());
            let head: Vec<Row> = rows.drain(..take).collect();
            *rest = rows.into();
            *keep = names.clone();
            if let Some(t) = kept_total {
                *t = total;
            }
            let done = head.len() < count;
            (head, total, done)
        }
        CursorState::Rows { .. } => {
            let done = rows.len() < count;
            (rows, total, done)
        }
    };
    if !plan.late_reads.is_empty()
        && let Err(v) = read_rows(&mut page_rows, true)
    {
        return (v, None);
    }
    let id = if done { 0 } else { c.id };
    let v = Value::Array(Some(vec![
        ftagg::reply(page_total, &names, &page_rows),
        Value::Integer(id as i64),
    ]));
    (v, (!done).then_some(c))
}

/// `FT.SEARCH`'s reply, RESP2's shape: the total, then each key, its score
/// with `WITHSCORES` as RediSearch puts it right after the key, and, unless
/// `NOCONTENT`, its fields. `docs[i]` is the read of `plan.hits.keys[i]`; a
/// document gone since the search chose it is left out.
pub fn search_reply(plan: &SearchPlan, docs: &[Option<DocRead>]) -> Value {
    let mut out = vec![int(plan.hits.total)];
    for (i, (key, dist)) in plan.hits.keys.iter().enumerate() {
        let score = plan
            .withscores
            .then(|| Value::Double(plan.hits.scores.get(i).copied().unwrap_or(0.0)));
        if plan.nocontent {
            out.push(bulk(key));
            out.extend(score);
            continue;
        }
        let Some(doc) = docs.get(i).and_then(|d| d.as_ref()) else {
            continue;
        };
        let more = plan.hits.more.get(i).map_or(&[][..], Vec::as_slice);
        let more_fields = || {
            plan.more_aliases
                .iter()
                .zip(more)
                .flat_map(|(a, d)| [bulk(a.as_bytes()), bulk(d.to_string().as_bytes())])
        };
        let distance = |name: &str| -> Option<Vec<u8>> {
            match (&plan.alias, dist) {
                (Some(a), Some(d)) if **a == *name => Some(d.to_string().into_bytes()),
                _ => plan
                    .more_aliases
                    .iter()
                    .zip(more)
                    .find(|(a, _)| ***a == *name)
                    .map(|(_, d)| d.to_string().into_bytes()),
            }
        };
        let mut fields: Vec<Value> = Vec::new();
        match (&plan.returns, plan.on) {
            (None, On::Hash) => {
                let Some(h) = doc.hash.as_ref().filter(|h| !h.is_empty()) else {
                    continue;
                };
                if let (Some(a), Some(d)) = (&plan.alias, dist) {
                    fields.push(bulk(a.as_bytes()));
                    fields.push(bulk(d.to_string().as_bytes()));
                }
                fields.extend(more_fields());
                for (f, v) in h {
                    fields.push(bulk(f));
                    fields.push(bulk(v));
                }
            }
            (None, On::Json) => {
                let Some(j) = &doc.json else {
                    continue;
                };
                if let (Some(a), Some(d)) = (&plan.alias, dist) {
                    fields.push(bulk(a.as_bytes()));
                    fields.push(bulk(d.to_string().as_bytes()));
                }
                fields.extend(more_fields());
                fields.push(bulk(b"$"));
                fields.push(bulk(j.to_string().as_bytes()));
            }
            (Some(rs), on) => {
                if doc.hash.as_ref().is_none_or(|h| h.is_empty()) && doc.json.is_none() {
                    continue;
                }
                for (name, as_name) in rs {
                    let v = distance(name).or_else(|| match on {
                        On::Hash => doc.hash.as_ref().and_then(|h| {
                            h.iter()
                                .rev()
                                .find(|(f, _)| f == name.as_bytes())
                                .map(|(_, v)| v.clone())
                        }),
                        On::Json => {
                            let path = plan
                                .paths
                                .iter()
                                .find(|(n, _)| n == name)
                                .map_or(&**name, |(_, p)| &**p);
                            doc.json
                                .as_ref()
                                .and_then(|j| json_path(j, path))
                                .map(json_text)
                        }
                    });
                    if let Some(v) = v {
                        fields.push(bulk(as_name.as_bytes()));
                        fields.push(Value::Bulk(Some(v)));
                    }
                }
            }
        }
        out.push(bulk(key));
        out.extend(score);
        out.push(Value::Array(Some(fields)));
    }
    Value::Array(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    fn blob(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    fn store() -> Store {
        let mut s = Store::new();
        s.set_ft(true);
        s
    }

    /// Plan and commit `FT.CREATE`, as the binary does once its rows land
    /// and its backfill is done.
    fn create(s: &mut Store, parts: &[&str]) {
        match s.plan_ft(b"ns", &a(parts), 0) {
            FtPlan::Create {
                index,
                def,
                persist,
            } => {
                assert_eq!(persist.len(), 3);
                s.ft_create(b"ns", index.clone(), def);
                s.ft_set_ready(b"ns", &index);
            }
            _ => panic!("not a create"),
        }
    }

    fn hash(fields: &[(&str, Vec<u8>)]) -> DocRead {
        DocRead {
            hash: Some(
                fields
                    .iter()
                    .map(|(f, v)| (f.as_bytes().to_vec(), v.clone()))
                    .collect(),
            ),
            ..DocRead::default()
        }
    }

    fn reply_err(p: FtPlan) -> String {
        match p {
            FtPlan::Reply(Value::Error(e)) => e,
            _ => panic!("not an error reply"),
        }
    }

    /// Plan an `FT.ALIAS*` and commit it as the binary does once its rows
    /// land; the members it removes and adds.
    fn alias(s: &mut Store, parts: &[&str]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        match s.plan_ft(b"ns", &a(parts), 0) {
            FtPlan::Alias {
                alias,
                index,
                persist,
            } => {
                let (mut gone, mut added) = (Vec::new(), Vec::new());
                for p in persist {
                    match p {
                        Persist::SRem { key, member } if key == ft_alias_list_key() => {
                            gone.push(member)
                        }
                        Persist::SAdd { key, member } if key == ft_alias_list_key() => {
                            added.push(member)
                        }
                        _ => panic!("an alias writes only its list"),
                    }
                }
                s.ft_set_alias(b"ns", alias, index);
                (gone, added)
            }
            _ => panic!("not an alias plan"),
        }
    }

    /// ADR-0060 amendment 3, part 4: an alias names its index wherever an
    /// index name is read, is refused as RediSearch refuses it, moves, and
    /// goes with its index.
    /// BUG-0271: a backfill puts a key only into the index it is filling,
    /// and not over a copy a write already put there, so write order holds
    /// in the index being filled and in every other index on the prefix.
    #[test]
    fn a_backfill_keeps_a_written_document_in_its_place() {
        let mut s = store();
        create(
            &mut s,
            &["FT.CREATE", "a", "PREFIX", "1", "d:", "SCHEMA", "t", "TAG"],
        );
        // `b` is made and still backfilling.
        let FtPlan::Create { index, def, .. } = s.plan_ft(
            b"ns",
            &a(&["FT.CREATE", "b", "PREFIX", "1", "d:", "SCHEMA", "t", "TAG"]),
            0,
        ) else {
            panic!("not a create");
        };
        s.ft_create(b"ns", index, def);
        let doc = |t: &str| hash(&[("t", t.as_bytes().to_vec())]);
        // Written while `b` backfills: d:2, then d:1, into both.
        s.ft_reindex(b"ns", b"d:2", &doc("x"));
        s.ft_reindex(b"ns", b"d:1", &doc("x"));
        // The backfill's scan reads keys in its own order, d:0 being a key
        // from before either index.
        for k in ["d:0", "d:1", "d:2"] {
            assert_eq!(s.ft_fill_doc(b"ns", b"b", k.as_bytes(), &doc("x")), None);
        }
        s.ft_set_ready(b"ns", b"b");
        let order = |s: &mut Store, ix: &str| -> Vec<String> {
            let FtPlan::Search(p) =
                s.plan_ft(b"ns", &a(&["FT.SEARCH", ix, "@t:{x}", "NOCONTENT"]), 0)
            else {
                panic!("not a search");
            };
            p.hits
                .keys
                .iter()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
                .collect()
        };
        assert_eq!(
            order(&mut s, "b"),
            ["d:2", "d:1", "d:0"],
            "b: the writes, then what the backfill found"
        );
        assert_eq!(
            order(&mut s, "a"),
            ["d:2", "d:1"],
            "a: untouched by b's backfill"
        );
        // A key the backfill finds gone, or not a document, adds nothing.
        assert_eq!(
            s.ft_fill_doc(b"ns", b"b", b"d:9", &DocRead::default()),
            None
        );
        assert_eq!(order(&mut s, "b").len(), 3);
    }

    #[test]
    fn an_alias_names_its_index_and_goes_with_it() {
        let mut s = store();
        for i in ["i", "j"] {
            let p = format!("{i}:");
            create(
                &mut s,
                &["FT.CREATE", i, "PREFIX", "1", &p, "SCHEMA", "a", "TAG"],
            );
        }
        let m = |a: &str, i: &str| alias_member(a.as_bytes(), i.as_bytes());
        assert_eq!(
            alias(&mut s, &["FT.ALIASADD", "live", "i"]),
            (vec![], vec![m("live", "i")])
        );
        assert!(matches!(
            s.plan_ft(b"ns", &a(&["FT.SEARCH", "live", "*"]), 0),
            FtPlan::Search(_)
        ));
        let Value::Array(Some(info)) = s.ft_info(b"ns", &a(&["FT.INFO", "live"]), 0) else {
            panic!("FT.INFO through an alias");
        };
        assert_eq!(info[1], bulk(b"i"), "answered under the index's own name");
        let refused = |s: &Store, parts: &[&str]| reply_err(s.plan_ft(b"ns", &a(parts), 0));
        assert!(refused(&s, &["FT.ALIASADD", "live", "j"]).contains("Alias already exists"));
        assert!(refused(&s, &["FT.ALIASADD", "x", "live"]).contains("name is an alias itself"));
        assert!(
            refused(&s, &["FT.ALIASADD", "j", "i"]).contains("conflicts with an existing index")
        );
        assert!(refused(&s, &["FT.ALIASDEL", "nope"]).contains("Alias does not exist"));
        assert!(refused(&s, &["FT.ALIASADD", "live"]).contains("wrong number of arguments"));
        // Moved: off the old index, onto the new.
        assert_eq!(
            alias(&mut s, &["FT.ALIASUPDATE", "live", "j"]),
            (vec![m("live", "i")], vec![m("live", "j")])
        );
        assert_eq!(s.ft_resolve(b"ns", b"live"), Some(b"j".to_vec()));
        alias(&mut s, &["FT.ALIASADD", "also", "j"]);
        // Dropping through an alias drops the index, and its aliases.
        let FtPlan::Drop { index, persist, .. } =
            s.plan_ft(b"ns", &a(&["FT.DROPINDEX", "live"]), 0)
        else {
            panic!("not a drop");
        };
        assert_eq!(index, b"j");
        let gone: Vec<_> = persist
            .iter()
            .filter_map(|p| match p {
                Persist::SRem { key, member } if *key == ft_alias_list_key() => {
                    Some(member.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(gone, [m("also", "j"), m("live", "j")]);
        s.ft_drop(b"ns", &index);
        assert_eq!(s.ft_resolve(b"ns", b"live"), None);
        assert_eq!(s.ft_resolve(b"ns", b"also"), None);
        assert_eq!(s.ft_resolve(b"ns", b"i"), Some(b"i".to_vec()));
        // A member carries both names, whatever is in them.
        let odd = alias_member(b"a:1", b"b:2");
        assert_eq!(
            parse_alias_member(&odd),
            Some((b"a:1".to_vec(), b"b:2".to_vec()))
        );
        assert_eq!(parse_alias_member(b"9:short"), None);
    }

    #[test]
    fn an_index_is_made_only_with_ft_and_served_either_way() {
        let mut s = store();
        create(
            &mut s,
            &["FT.CREATE", "i", "PREFIX", "1", "p:", "SCHEMA", "a", "TAG"],
        );
        s.set_ft(false);
        let e = reply_err(s.plan_ft(
            b"ns",
            &a(&["FT.CREATE", "j", "PREFIX", "1", "q:", "SCHEMA", "a", "TAG"]),
            0,
        ));
        assert!(e.contains("--ft"), "{e}");
        // The one made while it was on is still searched, written and dropped.
        assert!(matches!(
            s.plan_ft(b"ns", &a(&["FT.SEARCH", "i", "*"]), 0),
            FtPlan::Search(_)
        ));
        assert!(matches!(
            s.plan_ft(b"ns", &a(&["HSET", "p:1", "a", "x"]), 0),
            FtPlan::Write(_)
        ));
        assert!(matches!(
            s.plan_ft(b"ns", &a(&["FT.DROPINDEX", "i"]), 0),
            FtPlan::Drop { .. }
        ));
    }

    /// ADR-0063: `WITHSCORES` puts each score right after its key, as
    /// RediSearch's RESP2 reply does, with or without `NOCONTENT`.
    #[test]
    fn a_scored_search_puts_each_score_after_its_key() {
        let mut s = store();
        create(
            &mut s,
            &[
                "FT.CREATE",
                "idx",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "t",
                "TEXT",
            ],
        );
        s.ft_set_ready(b"ns", b"idx");
        for (k, t) in [("doc:1", "hello world"), ("doc:2", "hello")] {
            s.ft_reindex(b"ns", k.as_bytes(), &hash(&[("t", t.as_bytes().to_vec())]));
        }
        let plan = |extra: &[&str]| {
            let mut q = vec!["FT.SEARCH", "idx", "hello"];
            q.extend_from_slice(extra);
            let FtPlan::Search(p) = s.plan_ft(b"ns", &a(&q), 0) else {
                panic!("not a search");
            };
            p
        };
        let p = plan(&["NOCONTENT", "WITHSCORES"]);
        let Value::Array(Some(r)) = search_reply(&p, &[]) else {
            panic!("not an array");
        };
        // The shorter document scores higher under BM25STD.
        assert_eq!(r.len(), 5);
        assert_eq!((&r[0], &r[1]), (&Value::Integer(2), &bulk(b"doc:2")));
        let (Value::Double(a), Value::Double(b)) = (&r[2], &r[4]) else {
            panic!("scores are doubles: {r:?}");
        };
        assert!(a > b && *b > 0.0, "{a} {b}");
        let p = plan(&["WITHSCORES"]);
        let docs = [
            Some(hash(&[("t", b"hello".to_vec())])),
            Some(hash(&[("t", b"hello world".to_vec())])),
        ];
        let Value::Array(Some(r)) = search_reply(&p, &docs) else {
            panic!("not an array");
        };
        assert_eq!(r.len(), 7, "a key, its score and its fields, twice");
        assert!(matches!(r[2], Value::Double(_)) && matches!(r[3], Value::Array(_)));
    }

    #[test]
    fn a_write_under_an_index_is_planned_read_back_and_indexed() {
        let mut s = store();
        create(
            &mut s,
            &[
                "FT.CREATE",
                "idx",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "user",
                "TAG",
                "v",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ],
        );
        assert!(matches!(
            s.plan_ft(b"ns", &a(&["FT.CREATE", "idx", "PREFIX", "1", "x:", "SCHEMA", "a", "TAG"]), 0),
            FtPlan::Reply(Value::Error(e)) if e.contains("already exists")
        ));
        assert_eq!(s.ft_prefixes(b"ns"), vec![b"doc:".to_vec()]);
        // HSET under the prefix: the document list first, and a read-back.
        let FtPlan::Write(w) = s.plan_ft(b"ns", &a(&["HSET", "doc:1", "user", "alice"]), 0) else {
            panic!("not a write");
        };
        assert_eq!(w.keys, vec![(b"doc:1".to_vec(), (true, false))]);
        assert_eq!(w.before.len(), 1);
        // A key no index covers is written with nothing read back, as a
        // proxy that has not heard of a dropped index sends it.
        let FtPlan::Write(w) = s.plan_ft(b"ns", &a(&["DEL", "other", "doc:1"]), 0) else {
            panic!("not a write");
        };
        assert_eq!(w.keys.len(), 1);
        assert!(w.before.is_empty(), "a delete adds no document");
        // ADR-0060 amendment 4: a script's declared keys, the ones an index
        // covers, are read back; one declaring none is refused; JSON.MERGE
        // is planned as JSON.SET is.
        let FtPlan::Write(w) = s.plan_ft(
            b"ns",
            &a(&[
                "EVAL",
                "return redis.call('HSET', KEYS[1], 'a', 'b')",
                "2",
                "doc:7",
                "other",
                "x",
            ]),
            0,
        ) else {
            panic!("not a write");
        };
        assert_eq!(w.keys, vec![(b"doc:7".to_vec(), (true, false))]);
        assert_eq!(w.before.len(), 1);
        assert!(matches!(
            s.plan_ft(b"ns", &a(&["EVALSHA", "0123", "0"]), 0),
            FtPlan::Reply(Value::Error(e)) if e.contains("declares it in KEYS")
        ));
        let FtPlan::Write(w) = s.plan_ft(b"ns", &a(&["JSON.MERGE", "doc:8", "$", "{}"]), 0) else {
            panic!("not a write");
        };
        assert_eq!(w.keys.len(), 1);
        let left = s.ft_reindex(
            b"ns",
            b"doc:1",
            &hash(&[("user", b"Alice".to_vec()), ("v", blob(&[1.0, 0.0]))]),
        );
        assert!(left.is_empty());
        assert!(s.ns_mem_bytes(b"ns") > 0);
        let mut q = a(&[
            "FT.SEARCH",
            "idx",
            "@user:{alice}=>[KNN 1 @v $q]",
            "PARAMS",
            "2",
            "q",
        ]);
        q.push(blob(&[1.0, 0.0]));
        let FtPlan::Search(p) = s.plan_ft(b"ns", &q, 0) else {
            panic!("not a search");
        };
        assert_eq!(p.hits.total, 1);
        let doc = hash(&[("user", b"Alice".to_vec())]);
        let Value::Array(Some(r)) = search_reply(&p, &[Some(doc)]) else {
            panic!("not an array");
        };
        assert_eq!(r[0], Value::Integer(1));
        assert_eq!(r[1], bulk(b"doc:1"));
        let Value::Array(Some(fields)) = &r[2] else {
            panic!("no fields");
        };
        assert_eq!(fields[0], bulk(b"__v_score"));
        assert_eq!(fields[1], bulk(b"0"));
        // Read back as gone: out of the index, and its bytes credited.
        s.ft_reindex(b"ns", b"doc:1", &DocRead::default());
        assert_eq!(s.ns_mem_bytes(b"ns"), 0);
        // Dropped: forgotten, and the prefix is no longer guarded.
        let FtPlan::Drop { persist, docs, .. } =
            s.plan_ft(b"ns", &a(&["FT.DROPINDEX", "idx", "DD"]), 0)
        else {
            panic!("not a drop");
        };
        assert_eq!((persist.len(), docs), (3, Some(Vec::new())));
        assert!(s.ft_drop(b"ns", b"idx"));
        assert!(s.ft_prefixes(b"ns").is_empty());
        assert!(
            reply_err(s.plan_ft(b"ns", &a(&["FT.INFO", "idx"]), 0)).contains("Unknown index name")
        );
    }

    #[test]
    fn a_search_waits_for_its_backfill_and_json_returns_paths() {
        let mut s = store();
        let FtPlan::Create { index, def, .. } = s.plan_ft(
            b"ns",
            &a(&[
                "FT.CREATE",
                "j",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "j:",
                "SCHEMA",
                "$.user.name",
                "AS",
                "name",
                "TAG",
                "$.n",
                "AS",
                "n",
                "NUMERIC",
            ]),
            0,
        ) else {
            panic!("not a create");
        };
        s.ft_create(b"ns", index, def);
        let e = reply_err(s.plan_ft(b"ns", &a(&["FT.SEARCH", "j", "*"]), 0));
        assert!(e.starts_with("LOADING"), "{e}");
        let doc: Json = serde_json::from_str(r#"{"user":{"name":"Ann"},"n":7}"#).expect("json");
        s.ft_reindex(
            b"ns",
            b"j:1",
            &DocRead {
                json: Some(doc.clone()),
                ..DocRead::default()
            },
        );
        s.ft_set_ready(b"ns", b"j");
        let FtPlan::Search(p) = s.plan_ft(
            b"ns",
            &a(&[
                "FT.SEARCH",
                "j",
                "@name:{ann}",
                "RETURN",
                "3",
                "name",
                "AS",
                "who",
            ]),
            0,
        ) else {
            panic!("not a search");
        };
        let r = search_reply(
            &p,
            &[Some(DocRead {
                json: Some(doc),
                ..DocRead::default()
            })],
        );
        assert_eq!(
            r,
            Value::Array(Some(vec![
                Value::Integer(1),
                bulk(b"j:1"),
                Value::Array(Some(vec![bulk(b"who"), bulk(b"Ann")])),
            ]))
        );
        let Value::Array(Some(info)) = s.ft_info(b"ns", &a(&["FT.INFO", "j"]), 0) else {
            panic!("no info");
        };
        let at = info
            .iter()
            .position(|v| *v == bulk(b"num_docs"))
            .expect("num_docs");
        assert_eq!(info[at + 1], Value::Integer(1));
    }

    /// ADR-0064's tests: `FT.AGGREGATE` and its cursors through the plan
    /// and the page, with a reader that counts what it reads back.
    mod aggregate {
        use super::*;

        fn idx() -> Store {
            let mut s = store();
            create(
                &mut s,
                &[
                    "FT.CREATE",
                    "i",
                    "PREFIX",
                    "1",
                    "d:",
                    "SCHEMA",
                    "title",
                    "TEXT",
                    "SORTABLE",
                    "n",
                    "NUMERIC",
                    "t",
                    "TAG",
                ],
            );
            for (k, title, n, t) in [
                ("d:1", "Banana", "3", "x"),
                ("d:2", "apple", "1", "y"),
                ("d:3", "Cherry", "2", "x"),
                ("d:4", "date", "5", "x"),
            ] {
                s.ft_reindex(
                    b"ns",
                    k.as_bytes(),
                    &hash(&[
                        ("title", title.as_bytes().to_vec()),
                        ("n", n.as_bytes().to_vec()),
                        ("t", t.as_bytes().to_vec()),
                        ("extra", format!("e{k}").into_bytes()),
                    ]),
                );
            }
            s
        }

        /// One reply or page: planned, read back from `docs`, run; the
        /// cursor put back as the binary puts it.
        fn page(s: &Store, parts: &[&str], now: u64, reads: &mut usize) -> Value {
            match s.plan_ft(b"ns", &a(parts), now) {
                FtPlan::Aggregate(p) => {
                    let p = *p;
                    let mut read = |key: &[u8]| -> Result<Option<DocRead>, String> {
                        *reads += 1;
                        let k = String::from_utf8_lossy(key).into_owned();
                        Ok(Some(hash(&[("extra", format!("e{k}").into_bytes())])))
                    };
                    let (v, c) = aggregate_page(p, &mut read, 1);
                    if let Some(c) = c {
                        s.ft_cursor_put(c, now);
                    }
                    v
                }
                FtPlan::Reply(v) => v,
                _ => panic!("not an aggregate"),
            }
        }

        fn b(x: &str) -> Value {
            Value::Bulk(Some(x.as_bytes().to_vec()))
        }

        fn row(kv: &[&str]) -> Value {
            Value::Array(Some(kv.iter().map(|x| b(x)).collect()))
        }

        fn rows(v: &Value) -> Vec<Value> {
            let Value::Array(Some(items)) = v else {
                panic!("an array: {v:?}");
            };
            items[1..].to_vec()
        }

        /// A cursor's page: its rows and its id.
        fn cursor_page(v: &Value) -> (Vec<Value>, i64) {
            let Value::Array(Some(pair)) = v else {
                panic!("a cursor reply: {v:?}");
            };
            let Value::Integer(id) = pair[1] else {
                panic!("an id");
            };
            (rows(&pair[0]), id)
        }

        /// What the index holds answers with no read: the key, a NUMERIC
        /// value as a number, a SORTABLE TEXT folded where a step reads it.
        #[test]
        fn held_values_answer_without_a_read() {
            let s = idx();
            let mut reads = 0;
            let v = page(
                &s,
                &["FT.AGGREGATE", "i", "*", "LOAD", "2", "@__key", "@n"],
                0,
                &mut reads,
            );
            assert_eq!(reads, 0);
            assert_eq!(
                v,
                Value::Array(Some(vec![
                    Value::Integer(4),
                    row(&["__key", "d:1", "n", "3"]),
                    row(&["__key", "d:2", "n", "1"]),
                    row(&["__key", "d:3", "n", "2"]),
                    row(&["__key", "d:4", "n", "5"]),
                ]))
            );
            let v = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "@t:{x}",
                    "SORTBY",
                    "2",
                    "@title",
                    "DESC",
                    "MAX",
                    "2",
                ],
                0,
                &mut reads,
            );
            assert_eq!(reads, 0);
            assert_eq!(
                rows(&v),
                [row(&["title", "date"]), row(&["title", "cherry"])]
            );
            let v = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "GROUPBY",
                    "1",
                    "@t",
                    "REDUCE",
                    "COUNT",
                    "0",
                    "AS",
                    "c",
                    "REDUCE",
                    "SUM",
                    "1",
                    "@n",
                    "AS",
                    "s",
                    "SORTBY",
                    "2",
                    "@c",
                    "DESC",
                ],
                0,
                &mut reads,
            );
            assert_eq!(reads, 0);
            assert_eq!(
                rows(&v),
                [
                    row(&["t", "x", "c", "3", "s", "10"]),
                    row(&["t", "y", "c", "1", "s", "1"])
                ]
            );
        }

        /// A field outside the schema is read back, for the reply's rows
        /// only when no step reads it, and for every row first when one
        /// does.
        #[test]
        fn a_field_the_index_does_not_hold_is_read_back() {
            let s = idx();
            let mut reads = 0;
            let v = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "LOAD",
                    "1",
                    "@extra",
                    "SORTBY",
                    "2",
                    "@n",
                    "DESC",
                    "MAX",
                    "1",
                ],
                0,
                &mut reads,
            );
            assert_eq!(reads, 1, "the one row in the reply");
            assert_eq!(rows(&v), [row(&["extra", "ed:4", "n", "5"])]);
            reads = 0;
            let v = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "LOAD",
                    "1",
                    "@extra",
                    "FILTER",
                    "@extra == 'ed:2'",
                ],
                0,
                &mut reads,
            );
            assert_eq!(reads, 4, "every row, before the FILTER");
            assert_eq!(rows(&v), [row(&["extra", "ed:2"])]);
        }

        /// redisvl's bulk paging: a cursor walks the index in write order,
        /// a page at a time; a document written between reads comes on a
        /// later page; a page that fills COUNT leaves the cursor open, and
        /// the read after the last answers id 0.
        #[test]
        fn a_cursor_walks_the_index_as_it_changes() {
            let mut s = idx();
            let mut reads = 0;
            let first = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "@t:{x}",
                    "WITHCURSOR",
                    "COUNT",
                    "2",
                    "LOAD",
                    "1",
                    "__key",
                ],
                0,
                &mut reads,
            );
            let (r, id) = cursor_page(&first);
            assert_eq!(r, [row(&["__key", "d:1"]), row(&["__key", "d:3"])]);
            assert!(id > 0);
            s.ft_reindex(b"ns", b"d:5", &hash(&[("t", b"x".to_vec())]));
            let read = |s: &Store, reads: &mut usize| {
                page(s, &["FT.CURSOR", "READ", "i", &id.to_string()], 0, reads)
            };
            let (r, id2) = cursor_page(&read(&s, &mut reads));
            assert_eq!(r, [row(&["__key", "d:4"]), row(&["__key", "d:5"])]);
            assert_eq!(id2, id, "a full page leaves it open");
            let (r, id3) = cursor_page(&read(&s, &mut reads));
            assert!(r.is_empty());
            assert_eq!(id3, 0);
            assert_eq!(
                read(&s, &mut reads),
                Value::Error(format!("ERR Cursor not found, id: {id}"))
            );
            assert_eq!(reads, 0);
        }

        /// A sorted or grouped cursor computes its rows at the first read
        /// and pages them; DEL ends it.
        #[test]
        fn a_sorted_cursor_pages_what_it_computed() {
            let s = idx();
            let mut reads = 0;
            let first = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "LOAD",
                    "1",
                    "@__key",
                    "SORTBY",
                    "2",
                    "@n",
                    "ASC",
                    "MAX",
                    "3",
                    "WITHCURSOR",
                    "COUNT",
                    "2",
                ],
                0,
                &mut reads,
            );
            let (r, id) = cursor_page(&first);
            assert_eq!(
                r,
                [
                    row(&["__key", "d:2", "n", "1"]),
                    row(&["__key", "d:3", "n", "2"])
                ]
            );
            let next = page(
                &s,
                &["FT.CURSOR", "READ", "i", &id.to_string()],
                0,
                &mut reads,
            );
            let (r, id2) = cursor_page(&next);
            assert_eq!(r, [row(&["__key", "d:1", "n", "3"])]);
            assert_eq!(id2, 0, "fewer than COUNT: the last page");
            let again = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "GROUPBY",
                    "1",
                    "@t",
                    "WITHCURSOR",
                    "COUNT",
                    "1",
                ],
                0,
                &mut reads,
            );
            let (_, id) = cursor_page(&again);
            assert_eq!(
                page(
                    &s,
                    &["FT.CURSOR", "DEL", "i", &id.to_string()],
                    0,
                    &mut reads
                ),
                Value::Simple("OK".into())
            );
            assert_eq!(
                page(
                    &s,
                    &["FT.CURSOR", "DEL", "i", &id.to_string()],
                    0,
                    &mut reads
                ),
                Value::Error("ERR Cursor does not exist".into())
            );
        }

        /// A cursor is found only in its own namespace, only before it
        /// idles out, and says so when its index was dropped and made again.
        #[test]
        fn a_cursor_is_its_namespace_s_and_its_index_s() {
            let mut s = idx();
            let mut reads = 0;
            let first = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "WITHCURSOR",
                    "COUNT",
                    "1",
                    "MAXIDLE",
                    "1000",
                    "LOAD",
                    "1",
                    "@__key",
                ],
                0,
                &mut reads,
            );
            let (_, id) = cursor_page(&first);
            let other = s.plan_ft(
                b"other",
                &a(&["FT.CURSOR", "READ", "i", &id.to_string()]),
                0,
            );
            assert!(reply_err(other).contains("no such index"));
            assert_eq!(
                page(
                    &s,
                    &["FT.CURSOR", "READ", "i", &id.to_string()],
                    2000,
                    &mut reads
                ),
                Value::Error(format!("ERR Cursor not found, id: {id}")),
                "idle past MAXIDLE"
            );
            let first = page(
                &s,
                &[
                    "FT.AGGREGATE",
                    "i",
                    "*",
                    "WITHCURSOR",
                    "COUNT",
                    "1",
                    "LOAD",
                    "1",
                    "@__key",
                ],
                0,
                &mut reads,
            );
            let (_, id) = cursor_page(&first);
            s.ft_drop(b"ns", b"i");
            create(
                &mut s,
                &["FT.CREATE", "i", "PREFIX", "1", "d:", "SCHEMA", "t", "TAG"],
            );
            assert_eq!(
                page(
                    &s,
                    &["FT.CURSOR", "READ", "i", &id.to_string()],
                    0,
                    &mut reads
                ),
                Value::Error("ERR The index was dropped while the cursor was idle".into())
            );
        }

        #[test]
        fn an_index_holds_at_most_128_cursors() {
            let s = idx();
            let mut reads = 0;
            for _ in 0..CURSORS_PER_INDEX {
                let v = page(
                    &s,
                    &["FT.AGGREGATE", "i", "*", "WITHCURSOR", "COUNT", "1"],
                    0,
                    &mut reads,
                );
                assert!(cursor_page(&v).1 > 0);
            }
            assert_eq!(
                page(
                    &s,
                    &["FT.AGGREGATE", "i", "*", "WITHCURSOR", "COUNT", "1"],
                    0,
                    &mut reads
                ),
                Value::Error("ERR INDEX_CURSOR_LIMIT of 128 has been reached for an index".into())
            );
            assert!(s.ft_cursor_bytes(Some(b"ns")) > 0, "charged to the meter");
        }
    }
}
