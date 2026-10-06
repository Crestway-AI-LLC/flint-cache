// SPDX-License-Identifier: Elastic-2.0
//! The rest of RedisJSON's command family (ADR-0055), beside the first eight
//! in `commands.rs`: MGET, MSET, MERGE, STRAPPEND, STRLEN, ARRINDEX,
//! ARRINSERT, ARRPOP, ARRTRIM, OBJKEYS, OBJLEN, TOGGLE, CLEAR, RESP and
//! DEBUG, plus JSON.GET's several paths and its formatting arguments.
//!
//! Every reply is RedisJSON v8.2.8's to the same command, checked against
//! the module, except where ADR-0055 records a difference. The module's
//! answers for a missing key, a missing path and a value of the wrong type
//! vary command by command; each handler states its own.

use super::*;
use crate::json_path::{self, Path, Step};
use serde_json::Value as J;

const NOT_AN_ARRAY: &str = "ERR Path does not exist or not an array";
const NOT_A_STRING: &str = "ERR Path does not exist or not a string";
const NOT_AN_OBJECT: &str = "ERR Path does not exist or not an object";
const NOT_A_BOOL: &str = "ERR Path does not exist or not a bool";
const NOT_AN_INTEGER: &str = "ERR value is not an integer or out of range";
const NOT_JSON: &str = "ERR value is not valid JSON";

/// What a per-location operation answers: `Ok(None)` for a value of the
/// wrong type, `Ok(Some((reply, changed)))` otherwise, or `Err(reply)` to
/// refuse the whole command, which then stores nothing.
type Answer = Result<Option<(Value, bool)>, Value>;

/// RedisJSON's error for a legacy path at a value of the wrong type.
fn wrongtype(expected: &str, found: &J) -> Value {
    Value::Error(format!(
        "WRONGTYPE wrong type of path value - expected {expected} but found {}",
        json_path::type_name(found)
    ))
}

fn parse_i64(raw: &[u8]) -> Option<i64> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

/// RedisJSON's equality for ARRINDEX: types must agree, so the integer `2`
/// is not the float `2.0`; objects compare member by member.
fn same_json(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Number(x), J::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i == j,
            (None, None) => x.as_f64() == y.as_f64(),
            _ => false,
        },
        (J::Array(x), J::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same_json(a, b))
        }
        (J::Object(x), J::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, a)| y.get(k).is_some_and(|b| same_json(a, b)))
        }
        _ => a == b,
    }
}

/// RFC 7396's merge, as RedisJSON applies it: an object patch merges member
/// by member, a null member deleting; anything else replaces the target.
fn merge_patch(target: &mut J, patch: J) {
    let J::Object(members) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = J::Object(serde_json::Map::new());
    }
    let J::Object(map) = target else {
        unreachable!("made an object above")
    };
    for (k, v) in members {
        if v.is_null() {
            map.remove(&k);
        } else {
            merge_patch(map.entry(k).or_insert(J::Null), v);
        }
    }
}

/// JSON.RESP's encoding: an object is `{` then key, value pairs, an array
/// `[` then its elements, `true`/`false` simple strings, `null` nil, and a
/// number an integer when it fits one, else a double.
fn json_to_resp(v: &J) -> Value {
    match v {
        J::Null => Value::Bulk(None),
        J::Bool(b) => Value::Simple(if *b { "true" } else { "false" }.into()),
        J::Number(n) => match n.as_i64() {
            Some(i) => Value::Integer(i),
            None => Value::Double(n.as_f64().unwrap_or(f64::NAN)),
        },
        J::String(s) => Value::Bulk(Some(s.as_bytes().to_vec())),
        J::Array(a) => {
            let mut out = vec![Value::Simple("[".into())];
            out.extend(a.iter().map(json_to_resp));
            Value::Array(Some(out))
        }
        J::Object(m) => {
            let mut out = vec![Value::Simple("{".into())];
            for (k, v) in m {
                out.push(Value::Bulk(Some(k.as_bytes().to_vec())));
                out.push(json_to_resp(v));
            }
            Value::Array(Some(out))
        }
    }
}

/// JSON.GET's INDENT, NEWLINE and SPACE, as RedisJSON writes them: each
/// member or element on its own NEWLINE, indented by INDENT per level, and
/// SPACE after a member's colon. An empty container stays `{}` or `[]`.
#[derive(Default)]
struct JsonFormat {
    indent: Vec<u8>,
    newline: Vec<u8>,
    space: Vec<u8>,
}

impl JsonFormat {
    fn is_plain(&self) -> bool {
        self.indent.is_empty() && self.newline.is_empty() && self.space.is_empty()
    }

    fn write(&self, v: &J, level: usize, out: &mut Vec<u8>) {
        let open = |out: &mut Vec<u8>, first: bool, level: usize| {
            if !first {
                out.push(b',');
            }
            out.extend_from_slice(&self.newline);
            for _ in 0..level {
                out.extend_from_slice(&self.indent);
            }
        };
        match v {
            J::Array(a) if !a.is_empty() => {
                out.push(b'[');
                for (i, e) in a.iter().enumerate() {
                    open(out, i == 0, level + 1);
                    self.write(e, level + 1, out);
                }
                open(out, true, level);
                out.push(b']');
            }
            J::Object(m) if !m.is_empty() => {
                out.push(b'{');
                for (i, (k, e)) in m.iter().enumerate() {
                    open(out, i == 0, level + 1);
                    out.extend(serde_json::to_vec(k).unwrap_or_default());
                    out.push(b':');
                    out.extend_from_slice(&self.space);
                    self.write(e, level + 1, out);
                }
                open(out, true, level);
                out.push(b'}');
            }
            other => out.extend(serde_json::to_vec(other).unwrap_or_default()),
        }
    }
}

impl<'a> Dispatcher<'a> {
    /// Shape per-location answers for the caller's dialect. Under `$` each
    /// location answers its own element, a value of the wrong type a null
    /// one. A legacy path names at most one location: its answer, `wrong`
    /// when it is the wrong type, `missing` when the path names nothing.
    fn json_shape(path: &Path, answers: Vec<Option<Value>>, missing: Value, wrong: Value) -> Value {
        if path.is_jsonpath() {
            return Value::Array(Some(
                answers
                    .into_iter()
                    .map(|a| a.unwrap_or(Value::Bulk(None)))
                    .collect(),
            ));
        }
        match answers.into_iter().next() {
            Some(Some(v)) => v,
            Some(None) => wrong,
            None => missing,
        }
    }

    /// A legacy path's one value, for the reads whose legacy error names
    /// the type found there.
    fn json_first<'d>(doc: &'d J, path: &Path) -> Option<&'d J> {
        json_path::select(doc, path)
            .into_iter()
            .next()
            .and_then(|loc| json_path::get(doc, &Path::internal(loc)))
    }

    /// Run `read` at every location `path` names, in document order.
    fn json_read(doc: &J, path: &Path, read: impl Fn(&J) -> Option<Value>) -> Vec<Option<Value>> {
        json_path::select(doc, path)
            .into_iter()
            .map(|loc| json_path::get(doc, &Path::internal(loc)).and_then(&read))
            .collect()
    }

    /// Run `op` at every location `path` names and answer in document
    /// order. Locations are edited last first, so an edit that moves array
    /// elements (ARRINSERT, ARRPOP, ARRTRIM, CLEAR) cannot move a location
    /// still to come. A location a union names twice is edited once per
    /// occurrence, in occurrence order, as RedisJSON does. Answers the
    /// per-location replies and whether anything changed.
    fn json_apply(
        doc: &mut J,
        path: &Path,
        mut op: impl FnMut(&mut J) -> Answer,
    ) -> Result<(Vec<Option<Value>>, bool), Value> {
        let locs = json_path::select(doc, path);
        let mut order: Vec<(Vec<Step>, Vec<usize>)> = Vec::new();
        for (i, loc) in locs.into_iter().enumerate() {
            match order.iter_mut().find(|(l, _)| *l == loc) {
                Some((_, seen)) => seen.push(i),
                None => order.push((loc, vec![i])),
            }
        }
        let total = order.iter().map(|(_, s)| s.len()).sum();
        let mut out: Vec<Option<Value>> = vec![None; total];
        let mut changed = false;
        for (loc, occurrences) in order.into_iter().rev() {
            let at = Path::internal(loc);
            for i in occurrences {
                let Some(slot) = json_path::get_mut(doc, &at) else {
                    continue;
                };
                if let Some((reply, edit)) = op(slot)? {
                    changed |= edit;
                    out[i] = Some(reply);
                }
            }
        }
        Ok((out, changed))
    }

    /// The write commands' shared shape: open the document (a missing key
    /// is RedisJSON's "could not perform this operation"), apply `op` at
    /// each location, save when anything changed, and answer in the
    /// caller's dialect.
    fn json_write(
        &self,
        args: &[Vec<u8>],
        path_arg: Option<&Vec<u8>>,
        legacy_err: &str,
        op: impl FnMut(&mut J) -> Answer,
    ) -> Value {
        let (path, doc) = match self.json_open(&args[1], path_arg) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(mut doc) = doc else {
            return err(NO_SUCH_KEY);
        };
        let (answers, changed) = match Self::json_apply(&mut doc, &path, op) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        if changed && let Some(e) = self.json_save(&args[1], &doc) {
            return e;
        }
        Self::json_shape(&path, answers, err(legacy_err), err(legacy_err))
    }

    /// `JSON.STRLEN key [path]` — a string's length in bytes.
    pub(super) fn cmd_json_strlen(&self, args: &[Vec<u8>]) -> Value {
        if !(2..=3).contains(&args.len()) {
            return arity_err("json.strlen");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return if path.is_jsonpath() {
                err(NO_SUCH_KEY)
            } else {
                Value::Bulk(None)
            };
        };
        let len = |v: &J| v.as_str().map(|s| Value::Integer(s.len() as i64));
        let answers = Self::json_read(&doc, &path, len);
        let wrong = match Self::json_first(&doc, &path) {
            Some(v) => wrongtype("string", v),
            None => err(PATH_MISSING),
        };
        Self::json_shape(&path, answers, err(PATH_MISSING), wrong)
    }

    /// `JSON.STRAPPEND key [path] value` — append a JSON string to each
    /// string matched, answering its new length in bytes. With no path the
    /// legacy root is the target.
    pub(super) fn cmd_json_strappend(&self, args: &[Vec<u8>]) -> Value {
        if !(3..=4).contains(&args.len()) {
            return arity_err("json.strappend");
        }
        let (path_arg, raw) = match args.len() {
            3 => (None, &args[2]),
            _ => (Some(&args[2]), &args[3]),
        };
        let Ok(tail) = serde_json::from_slice::<J>(raw) else {
            return err(NOT_JSON);
        };
        // A value that is not a JSON string fails at the first string it
        // would be appended to, as in RedisJSON; matches that are not
        // strings still answer null.
        self.json_write(args, path_arg, NOT_A_STRING, |v| {
            let J::String(s) = v else { return Ok(None) };
            let J::String(t) = &tail else {
                return Err(wrongtype("string", &tail));
            };
            s.push_str(t);
            Ok(Some((Value::Integer(s.len() as i64), true)))
        })
    }

    /// `JSON.OBJLEN key [path]` — an object's member count.
    pub(super) fn cmd_json_objlen(&self, args: &[Vec<u8>]) -> Value {
        if !(2..=3).contains(&args.len()) {
            return arity_err("json.objlen");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return if path.is_jsonpath() {
                err(NOT_AN_OBJECT)
            } else {
                Value::Bulk(None)
            };
        };
        let len = |v: &J| v.as_object().map(|m| Value::Integer(m.len() as i64));
        let answers = Self::json_read(&doc, &path, len);
        let wrong = match Self::json_first(&doc, &path) {
            Some(v) => wrongtype("object", v),
            None => Value::Bulk(None),
        };
        Self::json_shape(&path, answers, Value::Bulk(None), wrong)
    }

    /// `JSON.OBJKEYS key [path]` — an object's member names, in order.
    pub(super) fn cmd_json_objkeys(&self, args: &[Vec<u8>]) -> Value {
        if !(2..=3).contains(&args.len()) {
            return arity_err("json.objkeys");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return if path.is_jsonpath() {
                err(NO_SUCH_KEY)
            } else {
                Value::Bulk(None)
            };
        };
        let keys = |v: &J| {
            v.as_object().map(|m| {
                Value::Array(Some(
                    m.keys()
                        .map(|k| Value::Bulk(Some(k.as_bytes().to_vec())))
                        .collect(),
                ))
            })
        };
        let answers = Self::json_read(&doc, &path, keys);
        Self::json_shape(&path, answers, Value::Bulk(None), err(NOT_AN_OBJECT))
    }

    /// `JSON.TOGGLE key path` — flip each boolean. Under `$` each answers its
    /// new value as 1 or 0; under the legacy dialect as `true` or `false`.
    pub(super) fn cmd_json_toggle(&self, args: &[Vec<u8>]) -> Value {
        if args.len() != 3 {
            return arity_err("json.toggle");
        }
        let jsonpath = args[2].first() == Some(&b'$');
        self.json_write(args, Some(&args[2]), NOT_A_BOOL, |v| {
            let J::Bool(b) = v else { return Ok(None) };
            *b = !*b;
            let reply = match jsonpath {
                true => Value::Integer(i64::from(*b)),
                false => Value::Bulk(Some(b.to_string().into_bytes())),
            };
            Ok(Some((reply, true)))
        })
    }

    /// `JSON.ARRINDEX key path value [start [stop]]` — the first index of
    /// `value` in each array, or -1. `stop` 0 means the end; both clamp to
    /// the array, as RedisJSON's `normalize_arr_indices` does.
    pub(super) fn cmd_json_arrindex(&self, args: &[Vec<u8>]) -> Value {
        if !(4..=6).contains(&args.len()) {
            return arity_err("json.arrindex");
        }
        let Ok(needle) = serde_json::from_slice::<J>(&args[3]) else {
            return err(NOT_JSON);
        };
        let (Some(start), Some(stop)) = (
            args.get(4).map_or(Some(0), |a| parse_i64(a)),
            args.get(5).map_or(Some(0), |a| parse_i64(a)),
        ) else {
            return err(NOT_AN_INTEGER);
        };
        let (path, doc) = match self.json_open(&args[1], Some(&args[2])) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return err(PATH_MISSING);
        };
        let find = |v: &J| {
            let a = v.as_array()?;
            let len = a.len() as i64;
            let from = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len - 1)
            };
            let to = match stop {
                0 => len,
                s if s < 0 => (len + s).max(0),
                s => s.min(len),
            };
            let at = (len > 0 && from <= to)
                .then(|| (from..to).find(|&i| same_json(&a[i as usize], &needle)))
                .flatten();
            Some(Value::Integer(at.unwrap_or(-1)))
        };
        let answers = Self::json_read(&doc, &path, find);
        let wrong = match Self::json_first(&doc, &path) {
            Some(v) => wrongtype("array", v),
            None => err(PATH_MISSING),
        };
        Self::json_shape(&path, answers, err(PATH_MISSING), wrong)
    }

    /// `JSON.ARRINSERT key path index value [value ...]` — insert before
    /// `index` (negative counts from the end; the length appends). An index
    /// outside the array refuses the whole command.
    pub(super) fn cmd_json_arrinsert(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 5 {
            return arity_err("json.arrinsert");
        }
        let Some(index) = parse_i64(&args[3]) else {
            return err(NOT_AN_INTEGER);
        };
        let mut values = Vec::with_capacity(args.len() - 4);
        for raw in &args[4..] {
            match serde_json::from_slice::<J>(raw) {
                Ok(v) => values.push(v),
                Err(_) => return err(NOT_JSON),
            }
        }
        self.json_write(args, Some(&args[2]), NOT_AN_ARRAY, |v| {
            let J::Array(a) = v else { return Ok(None) };
            let len = a.len() as i64;
            let at = if index < 0 { len + index } else { index };
            if !(0..=len).contains(&at) {
                return Err(err("ERR index out of bounds"));
            }
            let at = at as usize;
            a.splice(at..at, values.iter().cloned());
            Ok(Some((Value::Integer(a.len() as i64), true)))
        })
    }

    /// `JSON.ARRPOP key [path [index]]` — remove and answer an element, as
    /// JSON text; the last by default, an index clamped to the array. An
    /// empty array answers nil.
    pub(super) fn cmd_json_arrpop(&self, args: &[Vec<u8>]) -> Value {
        if !(2..=4).contains(&args.len()) {
            return arity_err("json.arrpop");
        }
        let index = match args.get(3) {
            None => -1,
            Some(raw) => match parse_i64(raw) {
                Some(i) => i,
                None => return err(NOT_AN_INTEGER),
            },
        };
        self.json_write(args, args.get(2), NOT_AN_ARRAY, |v| {
            let J::Array(a) = v else { return Ok(None) };
            if a.is_empty() {
                return Ok(Some((Value::Bulk(None), false)));
            }
            let len = a.len() as i64;
            let at = if index < 0 {
                (len + index).max(0)
            } else {
                index.min(len - 1)
            };
            let popped = a.remove(at as usize);
            Ok(Some((Self::json_bulk(&popped), true)))
        })
    }

    /// `JSON.ARRTRIM key path start stop` — keep the inclusive range, as
    /// RedisJSON's `arr_trim` clamps it, and answer the new length.
    pub(super) fn cmd_json_arrtrim(&self, args: &[Vec<u8>]) -> Value {
        if args.len() != 5 {
            return arity_err("json.arrtrim");
        }
        let (Some(start), Some(stop)) = (parse_i64(&args[3]), parse_i64(&args[4])) else {
            return err(NOT_AN_INTEGER);
        };
        self.json_write(args, Some(&args[2]), NOT_AN_ARRAY, |v| {
            let J::Array(a) = v else { return Ok(None) };
            let len = a.len() as i64;
            let clamp = |i: i64| {
                if i < 0 {
                    len - len.min(-i)
                } else if len > 0 {
                    (len - 1).min(i)
                } else {
                    0
                }
            };
            let stop = clamp(stop);
            let start = if start < 0 || start < len {
                clamp(start)
            } else {
                stop + 1
            };
            let before = a.len();
            if start > stop || len == 0 {
                a.clear();
            } else {
                a.truncate(stop as usize + 1);
                a.drain(..start as usize);
            }
            let changed = a.len() != before;
            Ok(Some((Value::Integer(a.len() as i64), changed)))
        })
    }

    /// `JSON.CLEAR key [path]` — empty each non-empty object or array and zero
    /// each non-zero number, answering how many changed. A match inside
    /// another cleared one went with it and is not counted again.
    pub(super) fn cmd_json_clear(&self, args: &[Vec<u8>]) -> Value {
        if !(2..=3).contains(&args.len()) {
            return arity_err("json.clear");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(mut doc) = doc else {
            return err(NO_SUCH_KEY);
        };
        let clearable = |v: &J| match v {
            J::Object(m) => !m.is_empty(),
            J::Array(a) => !a.is_empty(),
            J::Number(n) => n.as_f64() != Some(0.0),
            _ => false,
        };
        let mut locs: Vec<Vec<Step>> = Self::json_targets(json_path::select(&doc, &path))
            .into_iter()
            .filter(|l| json_path::get(&doc, &Path::internal(l.clone())).is_some_and(clearable))
            .collect();
        let all = locs.clone();
        locs.retain(|l| !all.iter().any(|a| a.len() < l.len() && l.starts_with(a)));
        let mut cleared = 0i64;
        for loc in locs {
            let Some(v) = json_path::get_mut(&mut doc, &Path::internal(loc)) else {
                continue;
            };
            match v {
                J::Object(m) => m.clear(),
                J::Array(a) => a.clear(),
                _ => *v = J::from(0),
            }
            cleared += 1;
        }
        if cleared > 0
            && let Some(e) = self.json_save(&args[1], &doc)
        {
            return e;
        }
        Value::Integer(cleared)
    }

    /// `JSON.MERGE key path value` — RFC 7396 merge at each match. A path
    /// that matches nothing adds the value when it names one location whose
    /// parent is an object, as JSON.SET does; a multi-match path adds
    /// nothing, and a missing parent is refused, as JSON.SET refuses it.
    pub(super) fn cmd_json_merge(&self, args: &[Vec<u8>]) -> Value {
        if args.len() != 4 {
            return arity_err("json.merge");
        }
        let Ok(patch) = serde_json::from_slice::<J>(&args[3]) else {
            return err(NOT_JSON);
        };
        let (path, doc) = match self.json_open(&args[1], Some(&args[2])) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let key = &args[1];
        // A missing key takes the patch as its document, nulls and all, as
        // JSON.SET would: RedisJSON does not merge into nothing.
        let Some(mut doc) = doc else {
            if !path.is_root() {
                return err("ERR new objects must be created at the root");
            }
            return match self.json_save(key, &patch) {
                Some(e) => e,
                None => Value::Simple("OK".into()),
            };
        };
        let locs = json_path::select(&doc, &path);
        if locs.is_empty() {
            if path.selectors().is_some() {
                return err("ERR a multi-match path replaces existing values and adds none");
            }
            match json_path::set(&mut doc, &path, patch) {
                json_path::SetOutcome::Set | json_path::SetOutcome::Created => {}
                json_path::SetOutcome::MissingParent => {
                    return err(
                        "ERR path parent does not exist (intermediate levels are not created)",
                    );
                }
                json_path::SetOutcome::ShapeMismatch => {
                    return err("ERR path does not fit the document's shape at that position");
                }
            }
        } else {
            // Innermost first, so a merge into an ancestor cannot strand a
            // descendant's location; the final document is the same.
            for loc in locs.into_iter().rev() {
                if let Some(v) = json_path::get_mut(&mut doc, &Path::internal(loc)) {
                    merge_patch(v, patch.clone());
                }
            }
        }
        match self.json_save(key, &doc) {
            Some(e) => e,
            None => Value::Simple("OK".into()),
        }
    }

    /// `JSON.MGET key [key ...] path` — the path's JSON.GET answer from each
    /// key, nil for a key that is missing or not a document.
    pub(super) fn cmd_json_mget(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("json.mget");
        }
        let keys = &args[1..args.len() - 1];
        if let Some(e) = Self::crossslot(&keys[0], &keys[1..]) {
            return e;
        }
        let raw = String::from_utf8_lossy(&args[args.len() - 1]).to_string();
        let path = match json_path::parse(&raw) {
            Ok(p) => p,
            Err(json_path::PathError::Unsupported) => return err(UNSUPPORTED_PATH),
            Err(json_path::PathError::Malformed) => return err("ERR malformed JSON path"),
        };
        let one = |key: &Vec<u8>| -> Value {
            let Ok(Some(bytes)) = self.json.get(slot_for_key(key), key) else {
                return Value::Bulk(None);
            };
            let Ok(doc) = serde_json::from_slice::<J>(&bytes) else {
                return Value::Bulk(None);
            };
            if path.is_jsonpath() {
                let vals: Vec<J> = Self::json_selected(&doc, &path)
                    .into_iter()
                    .cloned()
                    .collect();
                return Self::json_bulk(&J::Array(vals));
            }
            match json_path::get(&doc, &path) {
                Some(v) => Self::json_bulk(v),
                None => Value::Bulk(None),
            }
        };
        Value::Array(Some(keys.iter().map(one).collect()))
    }

    /// `JSON.MSET key path value [key path value ...]` — JSON.SET each triple,
    /// all or nothing. Every triple is checked against the documents as they
    /// were before the command, as RedisJSON does, and then they are applied
    /// in order; nothing is stored unless all of them apply.
    pub(super) fn cmd_json_mset(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
            return arity_err("json.mset");
        }
        let triples: Vec<&[Vec<u8>]> = args[1..].chunks(3).collect();
        let keys: Vec<Vec<u8>> = triples.iter().map(|t| t[0].clone()).collect();
        if let Some(e) = Self::crossslot(&keys[0], &keys[1..]) {
            return e;
        }
        let mut before: Vec<(Vec<u8>, Option<J>)> = Vec::new();
        let mut work: Vec<(Vec<u8>, Option<J>)> = Vec::new();
        let mut steps = Vec::with_capacity(triples.len());
        for t in &triples {
            let Ok(value) = serde_json::from_slice::<J>(&t[2]) else {
                return err(NOT_JSON);
            };
            let (path, doc) = match self.json_open(&t[0], Some(&t[1])) {
                Ok(v) => v,
                Err(reply) => return reply,
            };
            if !before.iter().any(|(k, _)| *k == t[0]) {
                before.push((t[0].clone(), doc.clone()));
                work.push((t[0].clone(), doc));
            }
            // Checked against the document before the command.
            let mut probe = before
                .iter()
                .find(|(k, _)| *k == t[0])
                .and_then(|(_, d)| d.clone());
            if let Err(e) = Self::json_set_in(&mut probe, &path, value.clone(), false, false) {
                return e;
            }
            steps.push((t[0].clone(), path, value));
        }
        for (key, path, value) in steps {
            let slot = &mut work
                .iter_mut()
                .find(|(k, _)| *k == key)
                .expect("loaded above")
                .1;
            if let Err(e) = Self::json_set_in(slot, &path, value, false, false) {
                return e;
            }
        }
        for (key, doc) in &work {
            if let Some(doc) = doc
                && let Some(e) = self.json_save(key, doc)
            {
                return e;
            }
        }
        Value::Simple("OK".into())
    }

    /// `JSON.RESP key [path]` — the value in RESP terms (see [`json_to_resp`]).
    pub(super) fn cmd_json_resp(&self, args: &[Vec<u8>]) -> Value {
        if !(2..=3).contains(&args.len()) {
            return arity_err("json.resp");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return Value::Bulk(None);
        };
        let answers = Self::json_read(&doc, &path, |v| Some(json_to_resp(v)));
        Self::json_shape(&path, answers, err(PATH_MISSING), err(PATH_MISSING))
    }

    /// `JSON.DEBUG MEMORY key [path]` | `JSON.DEBUG HELP`. MEMORY answers the bytes each
    /// value occupies as stored, its JSON text. RedisJSON answers the size
    /// of its in-memory tree instead; the two are not the same number.
    pub(super) fn cmd_json_debug(&self, args: &[Vec<u8>]) -> Value {
        let Some(sub) = args.get(1) else {
            return arity_err("json.debug");
        };
        if sub.eq_ignore_ascii_case(b"HELP") {
            return Value::Array(Some(vec![
                Value::Bulk(Some(b"MEMORY <key> [path] - reports memory usage".to_vec())),
                Value::Bulk(Some(b"HELP                - this message".to_vec())),
            ]));
        }
        if !sub.eq_ignore_ascii_case(b"MEMORY") {
            return err("ERR unknown subcommand - try `JSON.DEBUG HELP`");
        }
        if !(3..=4).contains(&args.len()) {
            return arity_err("json.debug");
        }
        let (path, doc) = match self.json_open(&args[2], args.get(3)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return match path.is_jsonpath() {
                true => Value::Array(Some(Vec::new())),
                false => Value::Integer(0),
            };
        };
        let size = |v: &J| {
            Some(Value::Integer(
                serde_json::to_vec(v).map_or(0, |b| b.len()) as i64
            ))
        };
        let answers = Self::json_read(&doc, &path, size);
        Self::json_shape(&path, answers, err(PATH_MISSING), err(PATH_MISSING))
    }

    /// `JSON.GET key [INDENT s] [NEWLINE s] [SPACE s] [NOESCAPE] [path ...]`.
    /// The options may sit anywhere among the paths, and the last of each
    /// wins. Several path arguments answer one object keyed by path, each
    /// once, in the order given (RedisJSON's order is its hash map's): under
    /// `$` if any path is a `$` path, else legacy, where a missing path is
    /// an error.
    pub(super) fn cmd_json_get(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 {
            return arity_err("json.get");
        }
        let mut fmt = JsonFormat::default();
        let mut paths: Vec<&Vec<u8>> = Vec::new();
        let mut given = 0usize;
        let mut rest = args[2..].iter();
        while let Some(a) = rest.next() {
            let field = match a.to_ascii_uppercase().as_slice() {
                b"INDENT" => &mut fmt.indent,
                b"NEWLINE" => &mut fmt.newline,
                b"SPACE" => &mut fmt.space,
                b"NOESCAPE" => continue,
                _ => {
                    given += 1;
                    if !paths.contains(&a) {
                        paths.push(a);
                    }
                    continue;
                }
            };
            let Some(v) = rest.next() else {
                return arity_err("json.get");
            };
            *field = v.clone();
        }
        let render = |v: &J| match fmt.is_plain() {
            true => Self::json_bulk(v),
            false => {
                let mut out = Vec::new();
                fmt.write(v, 0, &mut out);
                Value::Bulk(Some(out))
            }
        };
        let (first, doc) = match self.json_open(&args[1], paths.first().copied()) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(doc) = doc else {
            return Value::Bulk(None);
        };
        // Two path arguments answer the object form even when they are the
        // same path, which then appears once.
        if given < 2 {
            if first.selectors().is_some() || first.is_jsonpath() {
                let vals: Vec<J> = Self::json_selected(&doc, &first)
                    .into_iter()
                    .cloned()
                    .collect();
                return render(&J::Array(vals));
            }
            return match json_path::get(&doc, &first) {
                Some(v) => render(v),
                None => err(PATH_MISSING),
            };
        }
        let mut parsed = Vec::with_capacity(paths.len());
        for raw in &paths {
            let text = String::from_utf8_lossy(raw).to_string();
            match json_path::parse(&text) {
                Ok(p) => parsed.push((text, p)),
                Err(json_path::PathError::Unsupported) => return err(UNSUPPORTED_PATH),
                Err(json_path::PathError::Malformed) => return err("ERR malformed JSON path"),
            }
        }
        let any_jsonpath = parsed.iter().any(|(_, p)| p.is_jsonpath());
        let mut out = serde_json::Map::new();
        for (text, p) in parsed {
            let v = if any_jsonpath {
                J::Array(Self::json_selected(&doc, &p).into_iter().cloned().collect())
            } else {
                match json_path::get(&doc, &p) {
                    Some(v) => v.clone(),
                    None => return err(PATH_MISSING),
                }
            };
            out.insert(text, v);
        }
        render(&J::Object(out))
    }
}
