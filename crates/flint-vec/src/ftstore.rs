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
use crate::ftindex::{FtIndex, Hits, Source};
use crate::{Cap, Persist, Store, bucket_of, durable_key, err};
use flint_resp::Value;
use serde_json::Value as Json;
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

/// One bucket of the durable list of the documents the co-processor has
/// written under any index's prefix (`kind = b'k'`), which a rebuild reads
/// them back from. Bucketed as a set's ids are (`INDEX_BUCKETS`).
pub fn ft_docs_key(bucket: u16) -> Vec<u8> {
    durable_key(b'k', b"", format!("{bucket:03}").as_bytes())
}

/// The data writes the co-processor makes for a key an index covers. The
/// three clients write documents with nothing else (ADR-0059's survey);
/// every other write to such a key is refused, by the node, and here.
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
            | b"JSON.DEL"
            | b"JSON.FORGET"
    )
}

/// Whether the co-processor answers `name` as an `FT.*` command: the
/// family itself, and the writes a proxy sends it for an indexed key.
pub fn is_ft_command(name: &[u8]) -> bool {
    (name.len() > 3 && name[..3].eq_ignore_ascii_case(b"FT.")) || routed_write(name)
}

/// The keys a routed write writes.
fn write_keys(args: &[Vec<u8>]) -> &[Vec<u8>] {
    match args.first().map(|n| n.to_ascii_uppercase()).as_deref() {
        Some(b"DEL" | b"UNLINK") => &args[1..],
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
    /// `FT.SEARCH`: the hits, then each document read for the reply.
    Search(SearchPlan),
    /// A data write to keys an index may cover.
    Write(WritePlan),
}

pub struct SearchPlan {
    pub hits: Hits,
    pub on: On,
    pub nocontent: bool,
    pub returns: Option<Vec<(Box<str>, Box<str>)>>,
    /// The KNN distance's name, for a KNN search.
    pub alias: Option<Box<str>>,
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
            b"FT.DROPINDEX" => self.plan_ft_drop(ns, args),
            b"FT.INFO" => FtPlan::Reply(self.ft_info(ns, args, now)),
            b"FT._LIST" => FtPlan::Reply(Value::Array(Some(
                self.ft_names(ns).iter().map(|n| bulk(n)).collect(),
            ))),
            n if routed_write(n) => self.plan_ft_write(ns, args),
            other => FtPlan::Reply(err(&format!(
                "ERR {} is not supported (ADR-0060 serves FT.CREATE, FT.SEARCH, FT.INFO, \
                 FT._LIST and FT.DROPINDEX)",
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
        let Some(ix) = self.ft_index(ns, &index) else {
            return FtPlan::Reply(err("ERR Unknown Index name"));
        };
        // The list member first: a crash after it leaves a definition row
        // no rebuild reads, never an index the tenant dropped coming back.
        let persist = vec![
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

    fn plan_ft_search(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> FtPlan {
        let Some(index) = args.get(1) else {
            return FtPlan::Reply(err("ERR FT.SEARCH <index> <query> [options]"));
        };
        let Some(ix) = self.ft_index(ns, index) else {
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
            returns: s.returns,
            alias: s.knn.map(|k| k.alias),
            paths: ix
                .def()
                .attrs
                .iter()
                .map(|a| (a.field.name.clone(), a.path.clone()))
                .chain(
                    ix.def()
                        .vector
                        .iter()
                        .map(|v| (v.name.clone(), v.path.clone())),
                )
                .collect(),
        })
    }

    fn plan_ft_write(&self, ns: &[u8], args: &[Vec<u8>]) -> FtPlan {
        let keys = write_keys(args);
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
        let [_, index] = args else {
            return err("ERR FT.INFO <index>");
        };
        let Some(ix) = self.ft_index(ns, index) else {
            return err("ERR Unknown index name");
        };
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
                    }),
                ];
                // RediSearch's layout, which redisvl reads back: the
                // valued options from the seventh element, then the flags.
                if matches!(
                    a.field.kind,
                    crate::filter::FieldKind::Tag | crate::filter::FieldKind::FoldedTag
                ) {
                    v.push(s("SEPARATOR"));
                    v.push(bulk(&[a.separator]));
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
        if let Some(v) = &def.vector {
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
                    s("FLOAT32"),
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

/// `FT.SEARCH`'s reply, RESP2's shape: the total, then each key and, unless
/// `NOCONTENT`, its fields. `docs[i]` is the read of `plan.hits.keys[i]`; a
/// document gone since the search chose it is left out.
pub fn search_reply(plan: &SearchPlan, docs: &[Option<DocRead>]) -> Value {
    let mut out = vec![int(plan.hits.total)];
    for (i, (key, dist)) in plan.hits.keys.iter().enumerate() {
        if plan.nocontent {
            out.push(bulk(key));
            continue;
        }
        let Some(doc) = docs.get(i).and_then(|d| d.as_ref()) else {
            continue;
        };
        let distance = |name: &str| -> Option<Vec<u8>> {
            match (&plan.alias, dist) {
                (Some(a), Some(d)) if **a == *name => Some(d.to_string().into_bytes()),
                _ => None,
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
}
