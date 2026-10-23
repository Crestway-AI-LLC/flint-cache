// SPDX-License-Identifier: Elastic-2.0
//! `FT.AGGREGATE` (ADR-0064): the request as RediSearch reads it, its
//! pipeline compiled against an index's schema, the pipeline run over rows,
//! and the reply. Written from RediSearch's rules (`aggregate_request.c`,
//! `pipeline_construction.c`, its reducers and its value comparison), not
//! its code.
//!
//! The index makes the first rows: each matching document, with what the
//! index holds of it (its sortable values, and what `LOAD` gives from a
//! field the index keeps as written). A field the index does not hold is
//! read back from the document by the caller, before the pipeline runs when
//! a step reads it, after it when only the reply does.

use crate::filter::FieldKind;
use crate::ft::{IndexDef, On};
use crate::ftexpr::{self, Ast, Datum, num_to_str};
use crate::ftquery::Scorer;
use flint_resp::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

/// The most rows a reply holds, as RediSearch answers when configured with
/// `MAXAGGREGATERESULTS 10000`: the same ceiling as an `FT.SEARCH` page
/// (ADR-0060 amendment 3, part 1).
pub const MAX_RESULTS: usize = 10_000;
/// RediSearch's `MAXSEARCHRESULTS`, which bounds a `LIMIT`'s offset.
const MAX_OFFSET: usize = 1_000_000;
/// A sorter with neither `MAX` nor `LIMIT` keeps this many rows.
const DEFAULT_LIMIT: usize = 10;
/// A cursor's page when `COUNT` names none.
pub const CURSOR_COUNT: usize = 1000;
/// A cursor's idle time, its default and its ceiling.
pub const CURSOR_MAX_IDLE_MS: u64 = 300_000;
const MAX_GROUPS: usize = 1_000_000;
const MAX_SORT_KEYS: usize = 8;
const MAX_SAMPLE: usize = 1000;

/// A reducer, by what it computes.
#[derive(Clone, Debug, PartialEq)]
pub enum Red {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    StdDev,
    ToList,
    /// `FIRST_VALUE ... [BY @s [ASC|DESC]]`.
    FirstValue(Option<(Box<str>, bool)>),
    RandomSample(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Reducer {
    pub red: Red,
    /// The property it reads; `None` for `COUNT`.
    pub prop: Option<Box<str>>,
    pub alias: Box<str>,
}

/// A `LIMIT` and a `SORTBY`, which RediSearch keeps as one step for each
/// stretch between `GROUPBY`s.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Arrange {
    pub keys: Vec<(Box<str>, bool)>,
    pub offset: usize,
    pub limit: usize,
    /// `LIMIT` was given; with `limit` 0, only the count is answered.
    pub limited: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// `LOAD`: each field's path and its name in the row.
    Load(Vec<(Box<str>, Box<str>)>),
    LoadAll,
    Group {
        props: Vec<Box<str>>,
        reducers: Vec<Reducer>,
    },
    Apply {
        expr: Ast,
        alias: Box<str>,
    },
    Filter(Ast),
    Arrange(Arrange),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CursorOpts {
    pub count: usize,
    pub max_idle_ms: u64,
}

/// What `FT.AGGREGATE` asked for, before it meets an index.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub query: Box<str>,
    pub params: HashMap<String, Vec<u8>>,
    pub scorer: Scorer,
    pub addscores: bool,
    pub withcount: bool,
    pub steps: Vec<Step>,
    pub cursor: Option<CursorOpts>,
}

fn arg(args: &[Vec<u8>], i: usize) -> Option<&[u8]> {
    args.get(i).map(Vec::as_slice)
}

fn text(b: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "an argument is not UTF-8".to_string())
}

/// RediSearch's `AC_GetVarArgs`: a count, then that many arguments.
fn var_args<'a>(args: &'a [Vec<u8>], i: &mut usize, what: &str) -> Result<&'a [Vec<u8>], String> {
    let n = match arg(args, *i) {
        None => {
            return Err(format!(
                "Bad arguments for {what}: Expected an argument, but none provided"
            ));
        }
        Some(a) => match text(a)?.parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return Err(format!(
                    "Bad arguments for {what}: Could not convert argument to expected type"
                ));
            }
        },
    };
    let start = *i + 1;
    if args.len() < start + n {
        return Err(format!(
            "Bad arguments for {what}: Expected an argument, but none provided"
        ));
    }
    *i = start + n;
    Ok(&args[start..start + n])
}

fn unsigned(a: Option<&[u8]>, what: &str, min: u64) -> Result<u64, String> {
    let Some(a) = a else {
        return Err(format!(
            "Bad arguments for {what}: Expected an argument, but none provided"
        ));
    };
    match text(a)?.parse::<u64>() {
        Ok(n) if n >= min => Ok(n),
        Ok(_) => Err(format!(
            "Bad arguments for {what}: Value is outside acceptable bounds"
        )),
        Err(_) => Err(format!(
            "Bad arguments for {what}: Could not convert argument to expected type"
        )),
    }
}

/// The arrange step of the current stretch: the last one not behind a
/// `GROUPBY`, as RediSearch's `AGPLN_GetArrangeStep` finds it.
fn arrange_at(steps: &[Step]) -> Option<usize> {
    for (i, s) in steps.iter().enumerate().rev() {
        match s {
            Step::Arrange(_) => return Some(i),
            Step::Group { .. } => return None,
            _ => {}
        }
    }
    None
}

/// A reducer's name for its output when `AS` names none, as RediSearch's
/// `getReducerAlias` makes it.
fn default_alias(name: &str, args: &[Vec<u8>]) -> Box<str> {
    let parts: Vec<String> = args
        .iter()
        .map(|a| {
            let s = String::from_utf8_lossy(a);
            s.strip_prefix('@').unwrap_or(&s).to_string()
        })
        .collect();
    format!("__generated_alias{}{}", name, parts.join(","))
        .to_ascii_lowercase()
        .into()
}

/// A reducer's property argument, `@` optional.
fn prop_arg(a: &[u8]) -> Result<Box<str>, String> {
    let s = text(a)?;
    Ok(s.strip_prefix('@').unwrap_or(s).into())
}

fn parse_reducer(args: &[Vec<u8>], i: &mut usize) -> Result<Reducer, String> {
    let name = text(
        arg(args, *i).ok_or("Bad arguments for REDUCE: Expected an argument, but none provided")?,
    )?
    .to_string();
    *i += 1;
    let up = name.to_ascii_uppercase();
    let rargs = var_args(args, i, &up)?;
    let alias = if arg(args, *i).is_some_and(|a| a.eq_ignore_ascii_case(b"AS")) {
        let a = arg(args, *i + 1)
            .ok_or("Bad arguments for AS: Expected an argument, but none provided")?;
        *i += 2;
        text(a)?.into()
    } else {
        default_alias(&name, rargs)
    };
    let one = |rargs: &[Vec<u8>]| -> Result<Box<str>, String> {
        rargs
            .first()
            .ok_or_else(|| format!("Missing arguments for {up}"))
            .and_then(|a| prop_arg(a))
    };
    let (red, prop) = match up.as_str() {
        "COUNT" => {
            if !rargs.is_empty() {
                return Err("Count accepts 0 values only".into());
            }
            (Red::Count, None)
        }
        "COUNT_DISTINCT" => (Red::CountDistinct, Some(one(rargs)?)),
        "SUM" => (Red::Sum, Some(one(rargs)?)),
        "AVG" => (Red::Avg, Some(one(rargs)?)),
        "MIN" => (Red::Min, Some(one(rargs)?)),
        "MAX" => (Red::Max, Some(one(rargs)?)),
        "STDDEV" => (Red::StdDev, Some(one(rargs)?)),
        "TOLIST" => (Red::ToList, Some(one(rargs)?)),
        "FIRST_VALUE" => {
            let p = one(rargs)?;
            let by = match rargs.get(1) {
                None => None,
                Some(b) if b.eq_ignore_ascii_case(b"BY") => {
                    let s = rargs
                        .get(2)
                        .ok_or_else(|| format!("Missing arguments for {up}"))?;
                    let asc = match rargs.get(3) {
                        None => true,
                        Some(d) if d.eq_ignore_ascii_case(b"ASC") => true,
                        Some(d) if d.eq_ignore_ascii_case(b"DESC") => false,
                        Some(d) => {
                            return Err(format!(
                                "Unknown argument `{}` at position 3 for {up}",
                                String::from_utf8_lossy(d)
                            ));
                        }
                    };
                    if let Some(x) = rargs.get(4) {
                        return Err(format!(
                            "Unknown argument `{}` at position 4 for {up}",
                            String::from_utf8_lossy(x)
                        ));
                    }
                    Some((prop_arg(s)?, asc))
                }
                Some(x) => {
                    return Err(format!(
                        "Unknown argument `{}` at position 1 for {up}",
                        String::from_utf8_lossy(x)
                    ));
                }
            };
            (Red::FirstValue(by), Some(p))
        }
        "RANDOM_SAMPLE" => {
            let p = one(rargs)?;
            let n = rargs
                .get(1)
                .ok_or_else(|| format!("Missing arguments for {up}"))?;
            let n: usize = text(n)?
                .parse()
                .map_err(|_| "Sample size must be a number".to_string())?;
            if n > MAX_SAMPLE {
                return Err("Sample size too large".into());
            }
            (Red::RandomSample(n), Some(p))
        }
        // ADR-0064, sub-decision 1: RediSearch's approximate reducers.
        "QUANTILE" | "COUNT_DISTINCTISH" => {
            return Err(format!(
                "REDUCE {up} is not supported: its answer is an approximation (ADR-0064)"
            ));
        }
        _ => return Err(format!("No such reducer: {name}")),
    };
    Ok(Reducer { red, prop, alias })
}

/// `FT.AGGREGATE <index> <query> [options and steps]`. The error is the
/// text after `ERR `.
pub fn parse(args: &[Vec<u8>]) -> Result<Request, String> {
    let query = text(arg(args, 2).ok_or("FT.AGGREGATE <index> <query> [options]")?)?;
    let mut r = Request {
        query: query.into(),
        params: HashMap::new(),
        scorer: Scorer::DEFAULT,
        addscores: false,
        withcount: false,
        steps: Vec::new(),
        cursor: None,
    };
    // RediSearch reads query options until the first step; after it, only
    // the steps and the options common to both.
    let mut in_steps = false;
    let mut i = 3;
    while let Some(o) = arg(args, i) {
        let at = i;
        i += 1;
        match o.to_ascii_uppercase().as_slice() {
            b"LIMIT" => {
                if args.len() < i + 2 {
                    return Err("LIMIT requires two arguments".into());
                }
                let n = |a: &[u8]| text(a).ok().and_then(|s| s.parse::<usize>().ok());
                let (Some(offset), Some(limit)) = (n(&args[i]), n(&args[i + 1])) else {
                    return Err("LIMIT needs two numeric arguments".into());
                };
                i += 2;
                if limit == 0 && offset != 0 {
                    return Err("The `offset` of the LIMIT must be 0 when `num` is 0".into());
                }
                if limit > MAX_RESULTS {
                    return Err(format!("LIMIT exceeds maximum of {MAX_RESULTS}"));
                }
                if offset > MAX_OFFSET {
                    return Err(format!("OFFSET exceeds maximum of {MAX_OFFSET}"));
                }
                let a = match arrange_at(&r.steps) {
                    Some(k) => k,
                    None => {
                        r.steps.push(Step::Arrange(Arrange::default()));
                        r.steps.len() - 1
                    }
                };
                if let Step::Arrange(a) = &mut r.steps[a] {
                    a.offset = offset;
                    a.limit = limit;
                    a.limited = true;
                }
            }
            b"SORTBY" => {
                let existing = arrange_at(&r.steps);
                if let Some(k) = existing
                    && let Step::Arrange(a) = &r.steps[k]
                    && !a.keys.is_empty()
                {
                    return Err(
                        "Multiple SORTBY steps are not allowed. Sort multiple fields in a single step"
                            .into(),
                    );
                }
                let toks = var_args(args, &mut i, "SORTBY")?;
                let mut keys: Vec<(Box<str>, bool)> = Vec::new();
                for t in toks {
                    let s = text(t)?;
                    if let Some(name) = s.strip_prefix('@') {
                        if keys.len() >= MAX_SORT_KEYS {
                            return Err(format!("Cannot sort by more than {MAX_SORT_KEYS} fields"));
                        }
                        keys.push((name.into(), true));
                    } else if s.eq_ignore_ascii_case("ASC") || s.eq_ignore_ascii_case("DESC") {
                        if let Some(k) = keys.last_mut() {
                            k.1 = s.eq_ignore_ascii_case("ASC");
                        }
                    } else {
                        return Err(format!("MISSING ASC or DESC after sort field ({s})"));
                    }
                }
                let mut max = None;
                if arg(args, i).is_some_and(|a| a.eq_ignore_ascii_case(b"MAX")) {
                    max = Some(unsigned(arg(args, i + 1), "MAX", 0)? as usize);
                    i += 2;
                }
                match existing {
                    Some(k) => {
                        if let Step::Arrange(a) = &mut r.steps[k] {
                            a.keys = keys;
                            if let Some(m) = max {
                                a.limit = m;
                            }
                        }
                    }
                    None if !keys.is_empty() || max.is_some_and(|m| m > 0) => {
                        r.steps.push(Step::Arrange(Arrange {
                            keys,
                            limit: max.unwrap_or(0),
                            ..Arrange::default()
                        }));
                    }
                    None => {}
                }
            }
            b"TIMEOUT" => {
                let t = arg(args, i).ok_or("Need argument for TIMEOUT")?;
                text(t)?
                    .parse::<u64>()
                    .map_err(|_| "TIMEOUT requires a non negative integer".to_string())?;
                i += 1;
            }
            b"WITHCURSOR" => {
                let mut c = CursorOpts {
                    count: CURSOR_COUNT,
                    max_idle_ms: CURSOR_MAX_IDLE_MS,
                };
                loop {
                    match arg(args, i).map(<[u8]>::to_ascii_uppercase).as_deref() {
                        Some(b"COUNT") => {
                            c.count = unsigned(arg(args, i + 1), "COUNT", 1)? as usize;
                            i += 2;
                        }
                        Some(b"MAXIDLE") => {
                            let m = unsigned(arg(args, i + 1), "MAXIDLE", 1)?;
                            c.max_idle_ms = m.min(CURSOR_MAX_IDLE_MS);
                            i += 2;
                        }
                        _ => break,
                    }
                }
                r.cursor = Some(c);
            }
            b"PARAMS" => {
                let kv = var_args(args, &mut i, "PARAMS")?;
                if kv.len() % 2 != 0 {
                    return Err("Parameters must be specified in PARAM VALUE pairs".into());
                }
                for [k, v] in kv.as_chunks::<2>().0 {
                    let name = text(k)?.to_string();
                    if r.params.insert(name.clone(), v.clone()).is_some() {
                        return Err(format!("Duplicate parameter `{name}`"));
                    }
                }
            }
            b"DIALECT" => {
                let d = arg(args, i).ok_or("Need an argument for DIALECT")?;
                if text(d)? != "2" {
                    return Err(format!(
                        "FT.AGGREGATE DIALECT {} is not supported: 2 only (ADR-0060)",
                        String::from_utf8_lossy(d)
                    ));
                }
                i += 1;
            }
            b"FORMAT" => {
                let f = arg(args, i).ok_or("Need an argument for FORMAT")?;
                if !f.eq_ignore_ascii_case(b"STRING") {
                    return Err(format!(
                        "FT.AGGREGATE FORMAT {} is not supported: STRING only (ADR-0064)",
                        String::from_utf8_lossy(f)
                    ));
                }
                i += 1;
            }
            b"GROUPBY" => {
                in_steps = true;
                let props = var_args(args, &mut i, "GROUPBY")?;
                let mut names = Vec::new();
                for p in props {
                    let s = text(p)?;
                    let Some(name) = s.strip_prefix('@') else {
                        return Err(format!(
                            "Bad arguments for GROUPBY: Unknown property `{s}`. Did you mean `@{s}`?"
                        ));
                    };
                    names.push(name.into());
                }
                let mut reducers = Vec::new();
                while arg(args, i).is_some_and(|a| a.eq_ignore_ascii_case(b"REDUCE")) {
                    i += 1;
                    reducers.push(parse_reducer(args, &mut i)?);
                }
                r.steps.push(Step::Group {
                    props: names,
                    reducers,
                });
            }
            b"APPLY" | b"FILTER" => {
                in_steps = true;
                let apply = o.eq_ignore_ascii_case(b"APPLY");
                let e = text(arg(args, i).ok_or(
                    "Bad arguments for APPLY/FILTER: Expected an argument, but none provided",
                )?)?;
                i += 1;
                let expr = ftexpr::parse(e)?;
                if apply {
                    let alias: Box<str> =
                        if arg(args, i).is_some_and(|a| a.eq_ignore_ascii_case(b"AS")) {
                            let a = arg(args, i + 1).ok_or("AS needs argument")?;
                            i += 2;
                            text(a)?.into()
                        } else {
                            e.into()
                        };
                    r.steps.push(Step::Apply { expr, alias });
                } else {
                    r.steps.push(Step::Filter(expr));
                }
            }
            b"LOAD" => {
                in_steps = true;
                if arg(args, i) == Some(b"*") {
                    i += 1;
                    r.steps.push(Step::LoadAll);
                    continue;
                }
                let fields = match arg(args, i).map(text).transpose()? {
                    Some(n) if n.parse::<usize>().is_err() => {
                        return Err(
                            "Bad arguments for LOAD: Expected number of fields or `*`".into()
                        );
                    }
                    _ => var_args(args, &mut i, "LOAD")?,
                };
                let mut out = Vec::new();
                let mut j = 0;
                while j < fields.len() {
                    let f = text(&fields[j])?;
                    let path: Box<str> = f.strip_prefix('@').unwrap_or(f).into();
                    let mut name = path.clone();
                    if fields
                        .get(j + 1)
                        .is_some_and(|a| a.eq_ignore_ascii_case(b"AS"))
                    {
                        let Some(n) = fields.get(j + 2) else {
                            return Err("LOAD path AS name - must be accompanied with NAME".into());
                        };
                        if n.eq_ignore_ascii_case(b"AS") {
                            return Err("Alias for LOAD cannot be `AS`".into());
                        }
                        name = text(n)?.into();
                        j += 2;
                    }
                    out.push((path, name));
                    j += 1;
                }
                r.steps.push(Step::Load(out));
            }
            b"SCORER" if !in_steps => {
                r.scorer = Scorer::parse(arg(args, i).ok_or("Need an argument for SCORER")?)?;
                i += 1;
            }
            b"ADDSCORES" if !in_steps => r.addscores = true,
            b"WITHCOUNT" if !in_steps => r.withcount = true,
            b"WITHOUTCOUNT" if !in_steps => r.withcount = false,
            b"SUMMARIZE" | b"HIGHLIGHT" | b"RETURN" if !in_steps => {
                return Err(format!(
                    "`{}` is not supported on FT.AGGREGATE",
                    String::from_utf8_lossy(o).to_ascii_uppercase()
                ));
            }
            b"VERBATIM"
            | b"NOSTOPWORDS"
            | b"INFIELDS"
            | b"INKEYS"
            | b"SLOP"
            | b"LANGUAGE"
            | b"EXPANDER"
            | b"PAYLOAD"
            | b"INORDER"
            | b"WITHSCORES"
            | b"WITHSORTKEYS"
            | b"WITHPAYLOADS"
            | b"NOCONTENT"
            | b"EXPLAINSCORE"
            | b"BM25STD_TANH_FACTOR"
                if !in_steps =>
            {
                return Err(format!(
                    "FT.AGGREGATE option {} is not supported (ADR-0064)",
                    String::from_utf8_lossy(o).to_ascii_uppercase()
                ));
            }
            _ => {
                return Err(format!(
                    "Unknown argument `{}` at position {at} for <main>",
                    String::from_utf8_lossy(o)
                ));
            }
        }
    }
    Ok(r)
}

/// Where a root row's value under one name comes from, besides a step.
#[derive(Clone, Debug, PartialEq)]
pub enum Load {
    /// The document's key.
    Key,
    /// What the index holds of attribute `at`, as `LOAD` gives it.
    Held(usize),
    /// The document's own field, or JSON path, read back.
    Read(Box<str>),
}

/// A name the root stretch knows.
#[derive(Clone, Debug, PartialEq)]
pub struct RootKey {
    pub name: Box<str>,
    /// A `SORTABLE` attribute: the value as its sorting vector holds it,
    /// when no step has written one.
    pub sv: Option<usize>,
    /// What `LOAD` writes under it.
    pub load: Option<Load>,
}

#[derive(Clone, Debug)]
enum Op {
    /// Root slots to fill from their loads.
    Load(Vec<usize>),
    LoadAll,
    Apply(Ast, usize),
    Filter(Ast),
    Group {
        keys: Vec<usize>,
        reducers: Vec<(Red, Option<usize>, Option<usize>)>,
    },
    Arrange {
        keys: Vec<(usize, bool)>,
        offset: usize,
        limit: usize,
        limited: bool,
    },
}

/// A request compiled against an index: its names resolved, its loads
/// sorted into what the index holds and what must be read.
#[derive(Clone, Debug)]
pub struct Plan {
    pub root: Vec<RootKey>,
    /// Each stretch's names, the root's first; a `GROUPBY` starts the next.
    names: Vec<Vec<Box<str>>>,
    ops: Vec<(usize, Op)>,
    /// The slots of the KNN's or range's distance and of `__score`.
    pub metric: Option<usize>,
    pub score: Option<usize>,
    /// A step reads a field that must be read back, so every row is read
    /// before the pipeline runs.
    pub early_reads: bool,
    /// Root slots read back for the reply's rows only, after it runs.
    pub late_reads: Vec<usize>,
    pub load_all: bool,
    /// The pipeline sorts, groups or pages with `LIMIT`, so it reads every
    /// match before its first row: a cursor over it pages a result computed
    /// at once.
    pub buffered: bool,
    pub withcount: bool,
}

/// Every property an expression reads that is not yet a name, resolved as
/// RediSearch resolves `APPLY` and `FILTER`: a `SORTABLE` attribute of the
/// root stretch, else an error.
struct Compiler<'a> {
    def: &'a IndexDef,
    root: Vec<RootKey>,
    names: Vec<Vec<Box<str>>>,
    load_all: bool,
    /// Root slots a step names without a `LOAD`, which RediSearch loads
    /// just before that step.
    implicit: Vec<usize>,
}

impl Compiler<'_> {
    fn seg(&self) -> usize {
        self.names.len() - 1
    }

    fn find(&self, name: &str) -> Option<usize> {
        self.names[self.seg()].iter().position(|n| **n == *name)
    }

    fn attr(&self, name: &str) -> Option<usize> {
        self.def.attrs.iter().position(|a| *a.field.name == *name)
    }

    fn sortable(&self, at: usize) -> bool {
        self.def.attrs[at].flags.iter().any(|f| &**f == "SORTABLE")
    }

    fn unf(&self, at: usize) -> bool {
        self.def.attrs[at].flags.iter().any(|f| &**f == "UNF")
    }

    fn add(&mut self, key: RootKey) -> usize {
        self.names[0].push(key.name.clone());
        self.root.push(key);
        self.root.len() - 1
    }

    /// What `LOAD` gives for attribute `at` on this index: the index's own
    /// value where it holds it as the document has it, else a read.
    fn load_of(&self, at: usize) -> Load {
        let a = &self.def.attrs[at];
        match (self.def.on, a.field.kind) {
            (On::Hash, FieldKind::Numeric) => Load::Held(at),
            // The index keeps a TEXT or TAG value as written, unless it is
            // SORTABLE without UNF, when it keeps it folded.
            (On::Hash, FieldKind::Text | FieldKind::Tag | FieldKind::FoldedTag)
                if !self.sortable(at) || self.unf(at) =>
            {
                Load::Held(at)
            }
            _ => Load::Read(a.path.clone()),
        }
    }

    /// The slot for `name` in the current stretch. Before the first
    /// `GROUPBY`, a schema attribute not yet named becomes one: a
    /// `SORTABLE` one read from its sorting vector, for any step; any
    /// other, loaded just before the step, for `GROUPBY`, `SORTBY` and a
    /// reducer (`any`), never for `APPLY` and `FILTER`.
    fn resolve(&mut self, name: &str, any: bool) -> Option<usize> {
        if let Some(s) = self.find(name) {
            return Some(s);
        }
        if self.seg() != 0 {
            return None;
        }
        if let Some(at) = self.attr(name) {
            if self.sortable(at) {
                return Some(self.add(RootKey {
                    name: name.into(),
                    sv: Some(at),
                    load: None,
                }));
            }
            if !any {
                return None;
            }
            let load = Some(self.load_of(at));
            let s = self.add(RootKey {
                name: name.into(),
                sv: None,
                load,
            });
            self.implicit.push(s);
            return Some(s);
        }
        if self.load_all {
            // After `LOAD *`, any field the document has may be named; the
            // load writes it under its own name.
            return Some(self.add(RootKey {
                name: name.into(),
                sv: None,
                load: None,
            }));
        }
        None
    }

    fn expr(&mut self, e: &Ast) -> Result<(), String> {
        for p in e.props() {
            if self.resolve(p, false).is_none() {
                return Err(format!("Property not loaded nor in pipeline: `{p}`"));
            }
        }
        Ok(())
    }
}

/// `req` against the index `def`, whose query's distance, if any, is
/// named `metric`.
pub fn compile(req: &Request, def: &IndexDef, metric: Option<&str>) -> Result<Plan, String> {
    let mut c = Compiler {
        def,
        root: Vec::new(),
        names: vec![Vec::new()],
        load_all: false,
        implicit: Vec::new(),
    };
    let metric = metric.map(|m| {
        c.add(RootKey {
            name: m.into(),
            sv: None,
            load: None,
        })
    });
    let score = req.addscores.then(|| {
        c.add(RootKey {
            name: "__score".into(),
            sv: None,
            load: None,
        })
    });
    let mut ops: Vec<(usize, Op)> = Vec::new();
    let mut buffered = false;
    for step in &req.steps {
        let seg = c.seg();
        match step {
            Step::Load(fields) => {
                if seg != 0 {
                    return Err("LOAD cannot be applied after projectors or reducers".into());
                }
                let mut slots = Vec::new();
                for (path, name) in fields {
                    let load = if **path == *"__key" {
                        Load::Key
                    } else {
                        match c
                            .attr(path)
                            .or_else(|| def.attrs.iter().position(|a| *a.path == **path))
                        {
                            Some(at) => c.load_of(at),
                            None => Load::Read(path.clone()),
                        }
                    };
                    let slot = match c.find(name) {
                        Some(s) => {
                            c.root[s].load = Some(load);
                            s
                        }
                        None => c.add(RootKey {
                            name: name.clone(),
                            sv: None,
                            load: Some(load),
                        }),
                    };
                    slots.push(slot);
                }
                ops.push((seg, Op::Load(slots)));
            }
            Step::LoadAll => {
                if seg != 0 {
                    return Err("LOAD cannot be applied after projectors or reducers".into());
                }
                c.load_all = true;
                ops.push((seg, Op::LoadAll));
            }
            Step::Apply { expr, alias } => {
                c.expr(expr)?;
                let slot = match c.find(alias) {
                    Some(s) => s,
                    None if seg == 0 => c.add(RootKey {
                        name: alias.clone(),
                        sv: None,
                        load: None,
                    }),
                    None => {
                        c.names[seg].push(alias.clone());
                        c.names[seg].len() - 1
                    }
                };
                ops.push((seg, Op::Apply(expr.clone(), slot)));
            }
            Step::Filter(e) => {
                c.expr(e)?;
                ops.push((seg, Op::Filter(e.clone())));
            }
            Step::Group { props, reducers } => {
                buffered = true;
                let mut keys = Vec::new();
                for p in props {
                    let s = c
                        .resolve(p, true)
                        .ok_or_else(|| format!("No such property `{p}`"))?;
                    keys.push(s);
                }
                let mut reds = Vec::new();
                for r in reducers {
                    let mut slot = |p: &str| {
                        c.resolve(p, true)
                            .ok_or_else(|| format!("Property not loaded nor in pipeline: `{p}`"))
                    };
                    let arg = r.prop.as_deref().map(&mut slot).transpose()?;
                    let by = match &r.red {
                        Red::FirstValue(Some((b, _))) => Some(slot(b)?),
                        _ => None,
                    };
                    reds.push((r.red.clone(), arg, by));
                }
                let mut next: Vec<Box<str>> = Vec::new();
                for n in props.iter().chain(reducers.iter().map(|r| &r.alias)) {
                    if next.contains(n) {
                        return Err(format!("Property `{n}` specified more than once"));
                    }
                    next.push(n.clone());
                }
                if !c.implicit.is_empty() {
                    ops.push((seg, Op::Load(std::mem::take(&mut c.implicit))));
                }
                ops.push((
                    seg,
                    Op::Group {
                        keys,
                        reducers: reds,
                    },
                ));
                c.names.push(next);
            }
            Step::Arrange(a) => {
                buffered = true;
                let mut keys = Vec::new();
                for (k, asc) in &a.keys {
                    let s = c
                        .resolve(k, true)
                        .ok_or_else(|| format!("Property `{k}` not loaded nor in schema"))?;
                    keys.push((s, *asc));
                }
                if !c.implicit.is_empty() {
                    ops.push((seg, Op::Load(std::mem::take(&mut c.implicit))));
                }
                ops.push((
                    seg,
                    Op::Arrange {
                        keys,
                        offset: a.offset,
                        limit: a.limit,
                        limited: a.limited,
                    },
                ));
            }
        }
    }
    // A read field a step reads must be in every row before the steps run;
    // one only the reply shows is read for the reply's rows. After a
    // GROUPBY no root field is in the reply.
    let grouped = c.names.len() > 1;
    let mut read_by_step = vec![false; c.root.len()];
    let mut mark = |s: usize| {
        if let Some(r) = read_by_step.get_mut(s) {
            *r = true;
        }
    };
    for (seg, op) in &ops {
        if *seg != 0 {
            continue;
        }
        match op {
            Op::Apply(e, _) | Op::Filter(e) => {
                for p in e.props() {
                    if let Some(s) = c.names[0].iter().position(|n| **n == *p) {
                        mark(s);
                    }
                }
            }
            Op::Group { keys, reducers } => {
                keys.iter().for_each(|s| mark(*s));
                for (_, a, b) in reducers {
                    a.iter().chain(b.iter()).for_each(|s| mark(*s));
                }
            }
            Op::Arrange { keys, .. } => keys.iter().for_each(|(s, _)| mark(*s)),
            Op::Load(_) | Op::LoadAll => {}
        }
    }
    let reads: Vec<usize> = (0..c.root.len())
        .filter(|s| matches!(c.root[*s].load, Some(Load::Read(_))))
        .collect();
    // `LOAD *` writes what the document has, so it reads every row first.
    let early_reads = reads.iter().any(|s| read_by_step[*s]) || c.load_all;
    let late_reads = if early_reads || grouped {
        Vec::new()
    } else {
        reads
    };
    Ok(Plan {
        root: c.root,
        names: c.names,
        ops,
        metric,
        score,
        early_reads,
        late_reads,
        load_all: c.load_all,
        buffered,
        withcount: req.withcount,
    })
}

/// One row of the pipeline.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Row {
    /// Its document's key and write order; none after a `GROUPBY`.
    pub doc: Option<(Box<[u8]>, u64)>,
    /// Its values, by the current stretch's slots; `None` is missing.
    pub vals: Vec<Option<Datum>>,
    /// The root stretch's sortable values, by slot.
    pub sv: Vec<Option<Datum>>,
    /// What each root slot's `LOAD` writes, by slot.
    pub loads: Vec<Option<Datum>>,
    /// `LOAD *`: the document's fields, in its order.
    pub all: Vec<(Box<str>, Datum)>,
}

impl Row {
    /// A root row of `plan` for one document, its values all missing.
    pub fn root(plan: &Plan, key: &[u8], seq: u64) -> Row {
        let n = plan.root.len();
        Row {
            doc: Some((key.into(), seq)),
            vals: vec![None; n],
            sv: vec![None; n],
            loads: vec![None; n],
            all: Vec::new(),
        }
    }

    fn get(&self, s: usize) -> Option<&Datum> {
        self.vals
            .get(s)
            .and_then(Option::as_ref)
            .or_else(|| self.sv.get(s).and_then(Option::as_ref))
    }
}

struct Bound<'a> {
    names: &'a [Box<str>],
    row: &'a Row,
}

impl ftexpr::Row for Bound<'_> {
    fn get(&self, name: &str) -> Option<&Datum> {
        let s = self.names.iter().position(|n| **n == *name)?;
        self.row.get(s)
    }
}

/// The order `SORTBY` gives two values, as RediSearch's `cmp_fields` does:
/// a number against a string that is not one as its text against the
/// string.
fn sort_cmp(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (Datum::Num(x), Datum::Str(s)) => match std::str::from_utf8(s)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
        {
            Some(y) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
            None => num_to_str(*x).as_bytes().cmp(s),
        },
        (Datum::Str(_), Datum::Num(_)) => sort_cmp(b, a).reverse(),
        _ => ftexpr::order(a, b).unwrap_or(Ordering::Equal),
    }
}

/// A value as a group's identity: typed, so the number 1 and the string
/// `"1"` are two groups.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum GKey {
    Null,
    Num(u64),
    Str(Box<[u8]>),
}

fn gkey(d: &Datum) -> GKey {
    match d {
        Datum::Null | Datum::Arr(_) => GKey::Null,
        Datum::Num(x) => GKey::Num(x.to_bits()),
        Datum::Str(s) => GKey::Str(s.clone()),
    }
}

/// A group's values for one of its keys: an array's elements, each a
/// group of its own; a missing value, or an empty array, is null.
fn members(d: Option<&Datum>) -> Vec<Datum> {
    match d {
        None => vec![Datum::Null],
        Some(Datum::Arr(a)) if a.is_empty() => vec![Datum::Null],
        Some(Datum::Arr(a)) => a.clone(),
        Some(d) => vec![d.clone()],
    }
}

/// A reducer's running state.
enum Acc {
    Count(usize),
    Distinct(Vec<GKey>),
    Sum(f64, usize),
    Min(f64),
    Max(f64),
    /// Welford's count, mean and sum of squares.
    Dev(usize, f64, f64),
    List(Vec<Datum>, Vec<GKey>),
    First(Option<Datum>, Option<Datum>, bool),
    Sample(Vec<Datum>, usize, u64),
}

impl Acc {
    fn new(r: &Red, seed: u64) -> Acc {
        match r {
            Red::Count => Acc::Count(0),
            Red::CountDistinct => Acc::Distinct(Vec::new()),
            Red::Sum | Red::Avg => Acc::Sum(0.0, 0),
            Red::Min => Acc::Min(f64::INFINITY),
            Red::Max => Acc::Max(f64::NEG_INFINITY),
            Red::StdDev => Acc::Dev(0, 0.0, 0.0),
            Red::ToList => Acc::List(Vec::new(), Vec::new()),
            Red::FirstValue(_) => Acc::First(None, None, false),
            Red::RandomSample(_) => Acc::Sample(Vec::new(), 0, seed | 1),
        }
    }

    fn add(&mut self, red: &Red, v: Option<&Datum>, by: Option<&Datum>) {
        let flat = |v: Option<&Datum>| -> Vec<Datum> {
            match v {
                Some(Datum::Arr(a)) => a.clone(),
                Some(d) => vec![d.clone()],
                None => Vec::new(),
            }
        };
        match self {
            Acc::Count(n) => *n += 1,
            Acc::Distinct(seen) => {
                if let Some(d) = v.filter(|d| **d != Datum::Null) {
                    let k = match d {
                        Datum::Arr(a) => GKey::Str(
                            a.iter()
                                .map(Datum::text)
                                .collect::<Vec<_>>()
                                .join(&0u8)
                                .into(),
                        ),
                        d => gkey(d),
                    };
                    if !seen.contains(&k) {
                        seen.push(k);
                    }
                }
            }
            Acc::Sum(s, n) => {
                if let Some(x) = v.and_then(Datum::to_number) {
                    *s += x;
                    *n += 1;
                }
            }
            Acc::Min(m) => {
                // RediSearch keeps the new value unless it is no smaller, so
                // a NaN takes the place, and is taken from, by the next.
                if let Some(x) = v.and_then(Datum::to_number)
                    && !matches!(x.partial_cmp(m), Some(Ordering::Greater | Ordering::Equal))
                {
                    *m = x;
                }
            }
            Acc::Max(m) => {
                if let Some(x) = v.and_then(Datum::to_number)
                    && !matches!(x.partial_cmp(m), Some(Ordering::Less | Ordering::Equal))
                {
                    *m = x;
                }
            }
            Acc::Dev(n, mean, m2) => {
                for x in flat(v).iter().filter_map(Datum::to_number) {
                    *n += 1;
                    let d = x - *mean;
                    *mean += d / *n as f64;
                    *m2 += d * (x - *mean);
                }
            }
            Acc::List(out, seen) => {
                for d in flat(v) {
                    if d == Datum::Null {
                        continue;
                    }
                    let k = gkey(&d);
                    if !seen.contains(&k) {
                        seen.push(k);
                        out.push(d);
                    }
                }
            }
            Acc::First(best, best_by, any) => {
                let Red::FirstValue(order) = red else {
                    return;
                };
                match order {
                    None => {
                        if !*any {
                            *best = v.cloned();
                            *any = true;
                        }
                    }
                    Some((_, asc)) => {
                        // A row with no value to sort by never wins.
                        let Some(b) = by.filter(|b| **b != Datum::Null) else {
                            if !*any {
                                *best = v.cloned();
                                *any = true;
                            }
                            return;
                        };
                        let better = match best_by {
                            None => true,
                            Some(cur) => {
                                let o = sort_cmp(b, cur);
                                if *asc {
                                    o == Ordering::Less
                                } else {
                                    o == Ordering::Greater
                                }
                            }
                        };
                        if better {
                            *best = v.cloned();
                            *best_by = Some(b.clone());
                            *any = true;
                        }
                    }
                }
            }
            Acc::Sample(res, seen, rng) => {
                let Red::RandomSample(n) = red else {
                    return;
                };
                let Some(d) = v else {
                    return;
                };
                *seen += 1;
                if res.len() < *n {
                    res.push(d.clone());
                } else {
                    *rng ^= *rng << 13;
                    *rng ^= *rng >> 7;
                    *rng ^= *rng << 17;
                    let j = (*rng % *seen as u64) as usize;
                    if j < *n {
                        res[j] = d.clone();
                    }
                }
            }
        }
    }

    fn done(self, red: &Red) -> Datum {
        match self {
            Acc::Count(n) => Datum::Num(n as f64),
            Acc::Distinct(s) => Datum::Num(s.len() as f64),
            Acc::Sum(s, n) => match red {
                Red::Avg if n > 0 => Datum::Num(s / n as f64),
                _ if n == 0 => Datum::Num(f64::NAN),
                _ => Datum::Num(s),
            },
            Acc::Min(m) | Acc::Max(m) => Datum::Num(m),
            Acc::Dev(n, _, m2) => Datum::Num(if n < 2 {
                0.0
            } else {
                (m2 / (n - 1) as f64).sqrt()
            }),
            Acc::List(out, _) => Datum::Arr(out),
            Acc::First(best, _, _) => best.unwrap_or(Datum::Null),
            Acc::Sample(res, _, _) => Datum::Arr(res),
        }
    }
}

/// The pipeline's answer: its rows, the names their slots hold, and the
/// total RediSearch counts for them.
#[derive(Debug)]
pub struct Ran {
    pub total: usize,
    pub rows: Vec<Row>,
    pub names: Vec<Box<str>>,
}

/// Runs `plan` over `rows`, which hold every value the plan's steps read.
/// RediSearch's count: the matches, less each row a `FILTER` drops, set to
/// the groups at each `GROUPBY`, as its `WITHCOUNT` counts them.
pub fn run(plan: &Plan, mut rows: Vec<Row>, seed: u64) -> Result<Ran, String> {
    let mut names: Vec<Box<str>> = plan.names[0].clone();
    let mut total = rows.len();
    for (seg, op) in &plan.ops {
        match op {
            Op::Load(slots) => {
                for r in &mut rows {
                    for s in slots {
                        if let Some(v) = r.loads.get(*s).cloned().flatten() {
                            r.vals[*s] = Some(v);
                        }
                    }
                }
            }
            Op::LoadAll => {
                for r in &mut rows {
                    let fields = std::mem::take(&mut r.all);
                    for (name, v) in fields {
                        let s = match names.iter().position(|n| *n == name) {
                            Some(s) => s,
                            None => {
                                names.push(name.clone());
                                names.len() - 1
                            }
                        };
                        if r.vals.len() <= s {
                            r.vals.resize(s + 1, None);
                            r.sv.resize(s + 1, None);
                        }
                        r.vals[s] = Some(v);
                    }
                }
            }
            Op::Apply(e, slot) => {
                for r in &mut rows {
                    let v = e.eval(&Bound {
                        names: &names,
                        row: r,
                    })?;
                    if r.vals.len() <= *slot {
                        r.vals.resize(*slot + 1, None);
                    }
                    r.vals[*slot] = Some(v);
                }
            }
            Op::Filter(e) => {
                let mut kept = Vec::with_capacity(rows.len());
                for r in rows {
                    let keep = e
                        .eval(&Bound {
                            names: &names,
                            row: &r,
                        })?
                        .truthy();
                    if keep {
                        kept.push(r);
                    } else {
                        total = total.saturating_sub(1);
                    }
                }
                rows = kept;
            }
            Op::Group { keys, reducers } => {
                let width = keys.len() + reducers.len();
                let mut order: Vec<Vec<GKey>> = Vec::new();
                let mut groups: HashMap<Vec<GKey>, (Vec<Datum>, Vec<Acc>)> = HashMap::new();
                for (n, r) in rows.iter().enumerate() {
                    // One membership for each combination of the keys'
                    // values, an array's elements each their own.
                    let mut combos: Vec<Vec<Datum>> = vec![Vec::new()];
                    for k in keys {
                        let ms = members(r.get(*k));
                        let mut next = Vec::with_capacity(combos.len() * ms.len());
                        for c in &combos {
                            for m in &ms {
                                let mut c = c.clone();
                                c.push(m.clone());
                                next.push(c);
                            }
                        }
                        combos = next;
                    }
                    for combo in combos {
                        let id: Vec<GKey> = combo.iter().map(gkey).collect();
                        let entry = match groups.get_mut(&id) {
                            Some(e) => e,
                            None => {
                                if groups.len() >= MAX_GROUPS {
                                    return Err(format!(
                                        "Aggregate GROUPBY exceeded MAX_AGGREGATE_GROUPS limit of {MAX_GROUPS} groups"
                                    ));
                                }
                                let accs = reducers
                                    .iter()
                                    .enumerate()
                                    .map(|(i, (red, ..))| {
                                        Acc::new(red, seed ^ ((n as u64) << 8) ^ i as u64)
                                    })
                                    .collect();
                                order.push(id.clone());
                                groups.entry(id.clone()).or_insert((combo.clone(), accs))
                            }
                        };
                        for (acc, (red, arg, by)) in entry.1.iter_mut().zip(reducers) {
                            acc.add(red, arg.and_then(|s| r.get(s)), by.and_then(|s| r.get(s)));
                        }
                    }
                }
                let mut out = Vec::with_capacity(order.len());
                for id in order {
                    let Some((vals, accs)) = groups.remove(&id) else {
                        continue;
                    };
                    let mut row = Row {
                        vals: Vec::with_capacity(width),
                        ..Row::default()
                    };
                    row.vals.extend(vals.into_iter().map(Some));
                    row.vals.extend(
                        accs.into_iter()
                            .zip(reducers)
                            .map(|(a, (red, ..))| Some(a.done(red))),
                    );
                    out.push(row);
                }
                total = out.len();
                rows = out;
                names = plan.names[seg + 1].clone();
            }
            Op::Arrange {
                keys,
                offset,
                limit,
                limited,
            } => {
                if *limited && *limit == 0 {
                    rows.clear();
                    continue;
                }
                let mut max = offset + limit;
                if max == 0 {
                    max = DEFAULT_LIMIT;
                }
                let max = max.min(MAX_RESULTS);
                if !keys.is_empty() {
                    let last_asc = keys.last().is_some_and(|k| k.1);
                    let mut keyed: Vec<(Vec<Option<Datum>>, usize, Row)> = rows
                        .into_iter()
                        .enumerate()
                        .map(|(i, r)| {
                            (keys.iter().map(|(s, _)| r.get(*s).cloned()).collect(), i, r)
                        })
                        .collect();
                    keyed.sort_by(|(a, ia, ra), (b, ib, rb)| {
                        for ((x, y), (_, asc)) in a.iter().zip(b).zip(keys) {
                            let o = match (x, y) {
                                (None, None) => Ordering::Equal,
                                (None, Some(_)) => return Ordering::Greater,
                                (Some(_), None) => return Ordering::Less,
                                (Some(x), Some(y)) => sort_cmp(x, y),
                            };
                            let o = if *asc { o } else { o.reverse() };
                            if o != Ordering::Equal {
                                return o;
                            }
                        }
                        // A tie goes by document id, in the direction of the
                        // last key; rows with no document keep their order.
                        match (&ra.doc, &rb.doc) {
                            (Some((_, x)), Some((_, y))) => {
                                if last_asc {
                                    x.cmp(y)
                                } else {
                                    y.cmp(x)
                                }
                            }
                            _ => ia.cmp(ib),
                        }
                    });
                    keyed.truncate(max);
                    rows = keyed.into_iter().map(|(_, _, r)| r).collect();
                }
                let take = if *limit > 0 { *limit } else { max };
                rows = rows.into_iter().skip(*offset).take(take).collect();
            }
        }
    }
    Ok(Ran { total, rows, names })
}

fn reply_val(d: &Datum) -> Value {
    match d {
        Datum::Null => Value::Bulk(None),
        Datum::Num(x) => Value::Bulk(Some(num_to_str(*x).into_bytes())),
        Datum::Str(s) => Value::Bulk(Some(s.to_vec())),
        Datum::Arr(a) => Value::Array(Some(a.iter().map(reply_val).collect())),
    }
}

/// A row as RediSearch's RESP2 reply gives it: each name with a value, in
/// the order the names were made, a missing value left out.
pub fn reply_row(names: &[Box<str>], r: &Row) -> Value {
    let mut out = Vec::new();
    for (s, name) in names.iter().enumerate() {
        if let Some(v) = r.get(s) {
            out.push(Value::Bulk(Some(name.as_bytes().to_vec())));
            out.push(reply_val(v));
        }
    }
    Value::Array(Some(out))
}

/// `[total, row, ...]`, RediSearch's RESP2 reply; with a cursor the caller
/// wraps it with the cursor's id.
pub fn reply(total: usize, names: &[Box<str>], rows: &[Row]) -> Value {
    let mut out = Vec::with_capacity(rows.len() + 1);
    out.push(Value::Integer(total as i64));
    out.extend(rows.iter().map(|r| reply_row(names, r)));
    Value::Array(Some(out))
}

/// What a cursor keeps between reads (ADR-0064).
#[derive(Debug)]
pub enum CursorState {
    /// A pipeline with no sort, group or `LIMIT`: the request, matched
    /// again at each read from after the last document it gave, in write
    /// order, as RediSearch's cursor walks document ids on the live index.
    Stream { req: Request, after: u64 },
    /// The rows a sort, group or `LIMIT` computed at the first read, still
    /// to give, with their names and their count for each page.
    Rows {
        names: Vec<Box<str>>,
        rows: std::collections::VecDeque<Row>,
        total: Option<usize>,
    },
}

/// An `FT.AGGREGATE ... WITHCURSOR`'s cursor, between reads.
#[derive(Debug)]
pub struct Cursor {
    pub id: u64,
    pub ns: Vec<u8>,
    pub index: Vec<u8>,
    /// The index's generation: a cursor outlives its index's drop only to
    /// say so.
    pub generation: u64,
    /// Rows a read gives; a read's `COUNT` changes it for the reads after.
    pub count: usize,
    pub max_idle_ms: u64,
    pub expires_at: u64,
    pub plan: Plan,
    pub state: CursorState,
}

impl Cursor {
    /// What it holds in RAM beyond its plan, for the D4 meter.
    pub fn bytes(&self) -> usize {
        match &self.state {
            CursorState::Stream { .. } => 256,
            CursorState::Rows { rows, .. } => 256 + rows.iter().map(row_bytes).sum::<usize>(),
        }
    }
}

fn datum_bytes(d: &Datum) -> usize {
    std::mem::size_of::<Datum>()
        + match d {
            Datum::Str(s) => s.len(),
            Datum::Arr(a) => a.iter().map(datum_bytes).sum(),
            Datum::Null | Datum::Num(_) => 0,
        }
}

/// What a row kept by a cursor costs the D4 meter.
pub fn row_bytes(r: &Row) -> usize {
    let vals = |v: &[Option<Datum>]| -> usize {
        std::mem::size_of_val(v) + v.iter().flatten().map(datum_bytes).sum::<usize>()
    };
    std::mem::size_of::<Row>()
        + r.doc.as_ref().map_or(0, |(k, _)| k.len())
        + vals(&r.vals)
        + vals(&r.sv)
        + vals(&r.loads)
}

#[cfg(test)]
mod tests;
