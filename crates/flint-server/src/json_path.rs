// SPDX-License-Identifier: Elastic-2.0
//! The JSONPath subset Flint's JSON commands accept, and the resolution
//! primitives built on it.
//!
//! v1 is deliberately a SINGLE-MATCH subset: `$` (the root), object member
//! steps, and array index steps, in any mix — `$.user.tags[0]`,
//! `$["odd key"].n`, and the legacy dot form `user.tags[0]` (Redis accepts
//! both; a path not starting with `$` is treated as rooted). Negative array
//! indexes count from the end, like Redis.
//!
//! NOT in v1, and rejected with a clear error rather than half-honored:
//! wildcards (`$.a[*]`, `$.*`), recursive descent (`$..a`), slices
//! (`[0:2]`), and filter expressions (`?(@.x>1)`). Those turn every command
//! into a multi-match API — mutations would apply to N places — which is a
//! semantic step this type does not need to take before real usage tells us
//! which ones matter. A wrong guess here is a compatibility break later; an
//! unsupported-path error is not.
//!
//! The REPLY side of multi-match, though, we adopt today: see [`Mode`].

use serde_json::Value as J;

/// One resolution step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Object member.
    Key(String),
    /// Array index; negative counts from the end.
    Index(i64),
}

/// Which path dialect the caller used — and therefore which reply shape
/// they expect back.
///
/// RedisJSON has two, distinguished purely by a leading `$`, and clients
/// depend on the difference: `redis-py`'s `json().get(key, "$.a")` indexes
/// `[0]` off the reply, so answering a bare scalar breaks it.
///
/// - [`Mode::Legacy`] (`.a`, `a`, or no path at all): the reply is the
///   value itself, and a path that matches nothing is an ERROR.
/// - [`Mode::JsonPath`] (anything starting with `$`): the reply is a
///   CONTAINER of matches — one element here, since v1 is single-match —
///   and a path that matches nothing is an empty container, not an error.
///
/// Wrapping now is also what makes multi-match additive later: the array is
/// already the right shape, so `$..a` returning two elements would not
/// change any reply type. Shipping scalars would have made it a break.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    #[default]
    Legacy,
    JsonPath,
}

/// A parsed path: the steps from the document root (empty = the root), plus
/// the dialect it was written in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Path {
    pub steps: Vec<Step>,
    pub mode: Mode,
}

impl Path {
    /// A path built internally (parent walks), where the dialect is
    /// irrelevant because nothing is replied from it.
    fn internal(steps: Vec<Step>) -> Self {
        Self {
            steps,
            mode: Mode::Legacy,
        }
    }

    pub fn is_root(&self) -> bool {
        self.steps.is_empty()
    }

    pub fn is_jsonpath(&self) -> bool {
        self.mode == Mode::JsonPath
    }
}

/// Why a path could not be used. The command layer maps these to error
/// replies; keeping them distinct means "you typed a wildcard" never looks
/// like "your path is malformed".
#[derive(Debug, PartialEq, Eq)]
pub enum PathError {
    /// Syntactically wrong (unbalanced bracket, empty step, bad index).
    Malformed,
    /// Valid JSONPath, outside the supported subset (wildcard, recursive
    /// descent, slice, filter).
    Unsupported,
}

/// Parse the supported subset. Accepts `$`-rooted and legacy dot paths; an
/// empty path means the LEGACY root, because a caller who passed no path at
/// all (`JSON.GET key`) wants the document itself, not a one-element
/// container holding it.
pub fn parse(path: &str) -> Result<Path, PathError> {
    let s = path.trim();
    // The dialect is decided by the leading `$` alone, before any parsing,
    // so even a rejected path knows which shape its caller expected.
    let mode = if s.starts_with('$') {
        Mode::JsonPath
    } else {
        Mode::Legacy
    };
    // `.` is the legacy spelling of the root, still what older RedisJSON
    // docs and examples use; `$` is the JSONPath one.
    if s.is_empty() || s == "$" || s == "." {
        return Ok(Path {
            steps: Vec::new(),
            mode,
        });
    }
    // Reject the multi-match constructs explicitly, before parsing, so the
    // error names the reason instead of "malformed".
    if s.contains("..") || s.contains('*') || s.contains('?') || s.contains('@') {
        return Err(PathError::Unsupported);
    }
    let mut rest = s.strip_prefix('$').unwrap_or(s);
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('[') {
            // ["quoted key"] | ['quoted key'] | [index]
            let end = r.find(']').ok_or(PathError::Malformed)?;
            let inner = &r[..end];
            rest = &r[end + 1..];
            if inner.contains(':') {
                return Err(PathError::Unsupported); // slice
            }
            let unquoted = inner
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| inner.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')));
            match unquoted {
                Some(k) if !k.is_empty() => steps.push(Step::Key(k.to_string())),
                Some(_) => return Err(PathError::Malformed),
                None => {
                    let idx: i64 = inner.parse().map_err(|_| PathError::Malformed)?;
                    steps.push(Step::Index(idx));
                }
            }
        } else if let Some(r) = rest.strip_prefix('.') {
            let end = r.find(['.', '[']).unwrap_or(r.len());
            let name = &r[..end];
            if name.is_empty() {
                return Err(PathError::Malformed);
            }
            steps.push(Step::Key(name.to_string()));
            rest = &r[end..];
        } else {
            // Legacy leading segment with no dot ("user.name" style).
            let end = rest.find(['.', '[']).unwrap_or(rest.len());
            let name = &rest[..end];
            if name.is_empty() {
                return Err(PathError::Malformed);
            }
            steps.push(Step::Key(name.to_string()));
            rest = &rest[end..];
        }
    }
    Ok(Path { steps, mode })
}

/// Resolve an array index against a length: negative counts from the end.
/// None when out of range.
fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    let i = if idx < 0 { idx + len as i64 } else { idx };
    (i >= 0 && (i as usize) < len).then_some(i as usize)
}

/// Borrow the value at `path`, or None if any step is missing or the shape
/// disagrees (member step into an array, index into an object, …).
pub fn get<'v>(doc: &'v J, path: &Path) -> Option<&'v J> {
    let mut cur = doc;
    for step in &path.steps {
        cur = match (step, cur) {
            (Step::Key(k), J::Object(m)) => m.get(k)?,
            (Step::Index(i), J::Array(a)) => a.get(resolve_index(*i, a.len())?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Mutably borrow the value at `path` for in-place edits (NUMINCRBY,
/// ARRAPPEND). None when the path does not resolve.
pub fn get_mut<'v>(doc: &'v mut J, path: &Path) -> Option<&'v mut J> {
    let mut cur = doc;
    for step in &path.steps {
        cur = match (step, cur) {
            (Step::Key(k), J::Object(m)) => m.get_mut(k)?,
            (Step::Index(i), J::Array(a)) => {
                let idx = resolve_index(*i, a.len())?;
                a.get_mut(idx)?
            }
            _ => return None,
        };
    }
    Some(cur)
}

/// Outcome of a path-scoped write.
#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    Set,
    /// The parent exists but the leaf did not (a create), vs. an overwrite —
    /// the distinction NX/XX need.
    Created,
    /// The path's PARENT does not exist. Redis refuses to create
    /// intermediate levels, and so do we: a typo must not silently grow a
    /// document a shape the caller never asked for.
    MissingParent,
    /// The parent exists but cannot hold this step (index into an object,
    /// member into an array, index past the end).
    ShapeMismatch,
}

/// Write `value` at `path`, creating the LEAF only (never intermediates).
/// Appending to an array is expressed as the index == len.
pub fn set(doc: &mut J, path: &Path, value: J) -> SetOutcome {
    let Some((last, parents)) = path.steps.split_last() else {
        *doc = value; // root replace
        return SetOutcome::Set;
    };
    let parent_path = Path::internal(parents.to_vec());
    let Some(parent) = get_mut(doc, &parent_path) else {
        return SetOutcome::MissingParent;
    };
    match (last, parent) {
        (Step::Key(k), J::Object(m)) => {
            let existed = m.contains_key(k);
            m.insert(k.clone(), value);
            if existed {
                SetOutcome::Set
            } else {
                SetOutcome::Created
            }
        }
        (Step::Index(i), J::Array(a)) => {
            // index == len appends (Redis's JSON.ARRINSERT-at-end shape);
            // anything past that is a hole we refuse to punch.
            if *i == a.len() as i64 {
                a.push(value);
                return SetOutcome::Created;
            }
            match resolve_index(*i, a.len()) {
                Some(idx) => {
                    a[idx] = value;
                    SetOutcome::Set
                }
                None => SetOutcome::ShapeMismatch,
            }
        }
        _ => SetOutcome::ShapeMismatch,
    }
}

/// Remove the value at `path`. True when something was removed. The root
/// is never removed here (that is a whole-key DEL, which the command layer
/// routes to the store).
pub fn remove(doc: &mut J, path: &Path) -> bool {
    let Some((last, parents)) = path.steps.split_last() else {
        return false;
    };
    let parent_path = Path::internal(parents.to_vec());
    let Some(parent) = get_mut(doc, &parent_path) else {
        return false;
    };
    match (last, parent) {
        (Step::Key(k), J::Object(m)) => m.remove(k).is_some(),
        (Step::Index(i), J::Array(a)) => match resolve_index(*i, a.len()) {
            Some(idx) => {
                a.remove(idx);
                true
            }
            None => false,
        },
        _ => false,
    }
}

/// Redis's JSON.TYPE vocabulary for a value.
pub fn type_name(v: &J) -> &'static str {
    match v {
        J::Null => "null",
        J::Bool(_) => "boolean",
        J::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        J::String(_) => "string",
        J::Array(_) => "array",
        J::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(s: &str) -> Path {
        parse(s).expect("parse")
    }

    #[test]
    fn parses_root_dot_bracket_and_legacy_forms() {
        assert!(p("$").is_root());
        assert!(p("").is_root());
        assert_eq!(p("$.a").steps, vec![Step::Key("a".into())]);
        assert_eq!(p("a").steps, vec![Step::Key("a".into())]);
        assert_eq!(
            p("$.a.b[2]").steps,
            vec![Step::Key("a".into()), Step::Key("b".into()), Step::Index(2)]
        );
        assert_eq!(p("$[0]").steps, vec![Step::Index(0)]);
        assert_eq!(p("$[-1]").steps, vec![Step::Index(-1)]);
        assert_eq!(
            p(r#"$["odd key"].n"#).steps,
            vec![Step::Key("odd key".into()), Step::Key("n".into())]
        );
    }

    #[test]
    fn the_leading_dollar_alone_picks_the_dialect() {
        for s in ["$", "$.a", "$[0]", r#"$["k"]"#] {
            assert_eq!(p(s).mode, Mode::JsonPath, "{s}");
            assert!(p(s).is_jsonpath(), "{s}");
        }
        // No `$`, including the no-path-at-all case, is the legacy dialect:
        // `JSON.GET key` must answer the document, not `[document]`.
        for s in ["", ".a", "a", "a.b[0]", "."] {
            assert_eq!(p(s).mode, Mode::Legacy, "{s}");
        }
        // All three spellings of the root resolve identically; only the
        // reply shape they imply differs.
        assert!(p("$").is_root() && p("").is_root() && p(".").is_root());
    }

    #[test]
    fn rejects_multimatch_constructs_as_unsupported_not_malformed() {
        for s in ["$..a", "$.a[*]", "$.*", "$.a[0:2]", "$.a[?(@.x>1)]"] {
            assert_eq!(parse(s), Err(PathError::Unsupported), "{s}");
        }
        for s in ["$.a[", "$.a[x]", "$..", "$.a..b"] {
            assert!(parse(s).is_err(), "{s}");
        }
    }

    #[test]
    fn get_walks_objects_arrays_and_negative_indexes() {
        let d = json!({"user": {"tags": ["a", "b", "c"], "n": 3}});
        assert_eq!(get(&d, &p("$.user.n")), Some(&json!(3)));
        assert_eq!(get(&d, &p("$.user.tags[0]")), Some(&json!("a")));
        assert_eq!(get(&d, &p("$.user.tags[-1]")), Some(&json!("c")));
        assert_eq!(get(&d, &p("$.user.missing")), None);
        assert_eq!(get(&d, &p("$.user.tags[9]")), None);
        // Shape disagreements resolve to None, never a panic.
        assert_eq!(get(&d, &p("$.user.n.deeper")), None);
        assert_eq!(get(&d, &p("$.user[0]")), None);
    }

    #[test]
    fn set_creates_leaf_but_never_intermediates() {
        let mut d = json!({"a": {"b": 1}});
        assert_eq!(set(&mut d, &p("$.a.b"), json!(2)), SetOutcome::Set);
        assert_eq!(get(&d, &p("$.a.b")), Some(&json!(2)));
        assert_eq!(set(&mut d, &p("$.a.c"), json!(9)), SetOutcome::Created);
        // Missing intermediate: refused, document untouched.
        assert_eq!(
            set(&mut d, &p("$.x.y"), json!(1)),
            SetOutcome::MissingParent
        );
        assert_eq!(get(&d, &p("$.x")), None);
        // Root replace.
        assert_eq!(set(&mut d, &p("$"), json!([1])), SetOutcome::Set);
        assert_eq!(d, json!([1]));
    }

    #[test]
    fn set_at_array_end_appends_and_past_end_refuses() {
        let mut d = json!({"a": [1, 2]});
        assert_eq!(set(&mut d, &p("$.a[2]"), json!(3)), SetOutcome::Created);
        assert_eq!(d, json!({"a": [1, 2, 3]}));
        assert_eq!(
            set(&mut d, &p("$.a[9]"), json!(0)),
            SetOutcome::ShapeMismatch
        );
        assert_eq!(d, json!({"a": [1, 2, 3]}), "refused write left no hole");
    }

    #[test]
    fn remove_drops_members_and_elements() {
        let mut d = json!({"a": {"b": 1, "c": 2}, "arr": [1, 2, 3]});
        assert!(remove(&mut d, &p("$.a.b")));
        assert_eq!(get(&d, &p("$.a.b")), None);
        assert!(remove(&mut d, &p("$.arr[1]")));
        assert_eq!(get(&d, &p("$.arr")), Some(&json!([1, 3])));
        assert!(!remove(&mut d, &p("$.nope")));
        assert!(!remove(&mut d, &p("$")), "root is a whole-key DEL");
    }

    #[test]
    fn type_names_match_the_redis_vocabulary() {
        assert_eq!(type_name(&json!(null)), "null");
        assert_eq!(type_name(&json!(true)), "boolean");
        assert_eq!(type_name(&json!(3)), "integer");
        assert_eq!(type_name(&json!(3.5)), "number");
        assert_eq!(type_name(&json!("s")), "string");
        assert_eq!(type_name(&json!([])), "array");
        assert_eq!(type_name(&json!({})), "object");
    }
}
