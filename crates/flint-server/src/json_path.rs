// SPDX-License-Identifier: Elastic-2.0
//! The JSONPath subset Flint's JSON commands accept, and the resolution
//! primitives built on it.
//!
//! A DEFINITE path names at most one location: `$` (the root), object member
//! steps, and array index steps, in any mix — `$.user.tags[0]`,
//! `$["odd key"].n`, and the legacy dot form `user.tags[0]` (Redis accepts
//! both; a path not starting with `$` is treated as rooted). Negative array
//! indexes count from the end, like Redis. These resolve through [`get`],
//! [`get_mut`], [`set`] and [`remove`], exactly as before ADR-0054.
//!
//! An INDEFINITE path may name many (ADR-0054): wildcards (`$.a[*]`, `$.*`),
//! recursive descent (`$..a`), unions (`$['a','b']`, `$[0,2]`), slices
//! (`$[0:2]`, `$[::2]`) and filters (`$.items[?(@.price < 10 && @.stock)]`).
//! These parse to [`Sel`]ectors, and [`select`] turns them into the concrete
//! locations they name, in document order; the command layer then acts on
//! each. Only the `$` dialect takes them: a legacy path with one is still
//! [`PathError::Unsupported`]. RedisJSON's legacy dialect answers the first
//! match for a read and acts on all of them for a write; Flint refuses
//! rather than take on those two contracts. Regex filters (`=~`), negative
//! slice steps and paths inside filters that are not themselves definite
//! are unsupported too.
//!
//! The REPLY side was adopted before multi-match existed (see [`Mode`]), so
//! every `$` reply was already a container and multi-match changed no
//! reply type.

use serde_json::Value as J;

/// One resolution step.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
///   CONTAINER of matches, one element per match, and a path that matches
///   nothing is an empty container, not an error.
///
/// Wrapping from the start is what made multi-match (ADR-0054) additive:
/// `$..a` returns two elements where `$.a` returns one, and no reply type
/// changed. Shipping scalars would have made it a break.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    #[default]
    Legacy,
    JsonPath,
}

/// A parsed path: the steps from the document root (empty = the root), plus
/// the dialect it was written in. `multi` is `Some` for an INDEFINITE path
/// (ADR-0054), whose `steps` are then empty and unused.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Path {
    pub steps: Vec<Step>,
    pub mode: Mode,
    pub multi: Option<Vec<Sel>>,
}

impl Path {
    /// A path built internally (parent walks), where the dialect is
    /// irrelevant because nothing is replied from it.
    pub(crate) fn internal(steps: Vec<Step>) -> Self {
        Self {
            steps,
            mode: Mode::Legacy,
            multi: None,
        }
    }

    /// An indefinite path's selectors, or `None` for a definite path.
    pub fn selectors(&self) -> Option<&[Sel]> {
        self.multi.as_deref()
    }

    pub fn is_root(&self) -> bool {
        self.steps.is_empty() && self.multi.is_none()
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
    /// Valid JSONPath, outside the supported subset: a multi-match path in
    /// the legacy dialect, a regex filter, a negative slice step.
    Unsupported,
}

/// One selector of an INDEFINITE path (ADR-0054).
#[derive(Debug, Clone, PartialEq)]
pub enum Sel {
    /// `.name`, `['name']`.
    Key(String),
    /// `[i]`; negative counts from the end.
    Index(i64),
    /// `.*`, `[*]`: every member of an object, every element of an array.
    Wild,
    /// `['a','b']`, `[0,2]`: each listed member or element, in the order
    /// listed (duplicates kept, as RFC 9535 has it).
    Union(Vec<Step>),
    /// `[start:end:step]`, Python's bounds, a positive step.
    Slice {
        start: Option<i64>,
        end: Option<i64>,
        step: i64,
    },
    /// `[?(expr)]`: the elements of an array, or the member values of an
    /// object, for which the expression holds.
    Filter(Box<Expr>),
    /// `..sel`: `sel` applied to the node and to every node beneath it,
    /// in pre-order.
    Descend(Box<Sel>),
}

/// A filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    /// `?(@.x)`: the path resolves.
    Exists(Operand),
    Cmp(Operand, CmpOp, Operand),
}

/// One side of a filter comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// `@...`, from the candidate.
    Rel(Vec<Step>),
    /// `$...`, from the document root.
    Abs(Vec<Step>),
    Lit(J),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// How deeply filter expressions may nest. A path is a client's input, and
/// a parser that recursed without bound on `((((...` would be a stack
/// overflow on request.
const MAX_FILTER_DEPTH: usize = 32;

/// Parse a path in either dialect. An empty path means the LEGACY root,
/// because a caller who passed no path at all (`JSON.GET key`) wants the
/// document itself, not a one-element container holding it.
///
/// One scanner reads both dialects. It replaced a substring pre-scan that
/// refused anything containing `..`, `*`, `?` or `@`, which also refused a
/// quoted key such as `$["a*b"]`.
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
            multi: None,
        });
    }
    let body = s.strip_prefix('$').unwrap_or(s);
    let sels = Scanner::new(body).selectors()?;
    if sels
        .iter()
        .all(|x| matches!(x, Sel::Key(_) | Sel::Index(_)))
    {
        let mut steps = Vec::with_capacity(sels.len());
        for x in sels {
            match x {
                Sel::Key(k) => steps.push(Step::Key(k)),
                Sel::Index(i) => steps.push(Step::Index(i)),
                _ => return Err(PathError::Malformed),
            }
        }
        return Ok(Path {
            steps,
            mode,
            multi: None,
        });
    }
    if mode == Mode::Legacy {
        return Err(PathError::Unsupported);
    }
    Ok(Path {
        steps: Vec::new(),
        mode,
        multi: Some(sels),
    })
}

/// The path scanner. Byte-oriented: every token it recognises is ASCII, and
/// names and quoted keys are sliced back out of the source, so UTF-8 keys
/// survive untouched.
struct Scanner<'a> {
    src: &'a str,
    b: &'a [u8],
    i: usize,
}

/// What ends a name after `.`, at the top level and inside a filter.
const NAME_END: &[u8] = b".[";
const FILTER_NAME_END: &[u8] = b".[ )=!<>&|,";
/// What ends a literal inside a filter: as above, but a number may hold a
/// `.` (`@.price < 9.99`).
const LITERAL_END: &[u8] = b" )=!<>&|,]";

enum Item {
    Key(String),
    Index(i64),
    Slice(Option<i64>, Option<i64>, i64),
}

impl<'a> Scanner<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            b: src.as_bytes(),
            i: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn eat_str(&mut self, t: &str) -> bool {
        if self.b[self.i..].starts_with(t.as_bytes()) {
            self.i += t.len();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, c: u8) -> Result<(), PathError> {
        if self.eat(c) {
            Ok(())
        } else {
            Err(PathError::Malformed)
        }
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t')) {
            self.i += 1;
        }
    }

    fn selectors(&mut self) -> Result<Vec<Sel>, PathError> {
        let mut out = Vec::new();
        while self.i < self.b.len() {
            if self.eat_str("..") {
                let inner = if self.peek() == Some(b'[') {
                    self.bracket()?
                } else if self.eat(b'*') {
                    Sel::Wild
                } else {
                    Sel::Key(self.name(NAME_END)?)
                };
                out.push(Sel::Descend(Box::new(inner)));
            } else if self.eat(b'.') {
                if self.eat(b'*') {
                    out.push(Sel::Wild);
                } else {
                    out.push(Sel::Key(self.name(NAME_END)?));
                }
            } else if self.peek() == Some(b'[') {
                out.push(self.bracket()?);
            } else if out.is_empty() {
                // A leading segment with no dot: legacy `user.name`, and
                // `$name`, which this parser has always read as `$.name`.
                out.push(Sel::Key(self.name(NAME_END)?));
            } else {
                return Err(PathError::Malformed);
            }
        }
        Ok(out)
    }

    fn name(&mut self, end: &[u8]) -> Result<String, PathError> {
        let start = self.i;
        while let Some(c) = self.peek() {
            if end.contains(&c) {
                break;
            }
            self.i += 1;
        }
        if self.i == start {
            return Err(PathError::Malformed);
        }
        Ok(self.src[start..self.i].to_string())
    }

    /// A quoted string, single or double quotes, with backslash escapes for
    /// the quote and the backslash.
    fn quoted(&mut self) -> Result<String, PathError> {
        let q = self.peek().ok_or(PathError::Malformed)?;
        self.i += 1;
        let mut out = String::new();
        let mut run = self.i;
        loop {
            match self.peek() {
                None => return Err(PathError::Malformed),
                Some(c) if c == q => {
                    out.push_str(&self.src[run..self.i]);
                    self.i += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    out.push_str(&self.src[run..self.i]);
                    self.i += 1;
                    match self.peek() {
                        Some(c @ (b'\\' | b'\'' | b'"')) => out.push(c as char),
                        _ => return Err(PathError::Malformed),
                    }
                    self.i += 1;
                    run = self.i;
                }
                Some(_) => self.i += 1,
            }
        }
    }

    fn int(&mut self) -> Result<Option<i64>, PathError> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        match &self.src[start..self.i] {
            "" => Ok(None),
            "-" => Err(PathError::Malformed),
            t => t.parse().map(Some).map_err(|_| PathError::Malformed),
        }
    }

    fn item(&mut self) -> Result<Item, PathError> {
        if matches!(self.peek(), Some(b'"' | b'\'')) {
            let k = self.quoted()?;
            if k.is_empty() {
                return Err(PathError::Malformed);
            }
            return Ok(Item::Key(k));
        }
        let start = self.int()?;
        self.ws();
        if !self.eat(b':') {
            return start.map(Item::Index).ok_or(PathError::Malformed);
        }
        self.ws();
        let end = self.int()?;
        self.ws();
        let step = if self.eat(b':') {
            self.ws();
            self.int()?.unwrap_or(1)
        } else {
            1
        };
        match step {
            0 => Err(PathError::Malformed),
            s if s < 0 => Err(PathError::Unsupported),
            s => Ok(Item::Slice(start, end, s)),
        }
    }

    fn bracket(&mut self) -> Result<Sel, PathError> {
        self.expect(b'[')?;
        self.ws();
        if self.eat(b'*') {
            self.ws();
            self.expect(b']')?;
            return Ok(Sel::Wild);
        }
        if self.eat(b'?') {
            self.ws();
            self.expect(b'(')?;
            let e = self.expr(0)?;
            self.ws();
            self.expect(b')')?;
            self.ws();
            self.expect(b']')?;
            return Ok(Sel::Filter(Box::new(e)));
        }
        let mut items = Vec::new();
        loop {
            self.ws();
            items.push(self.item()?);
            self.ws();
            if self.eat(b',') {
                continue;
            }
            self.expect(b']')?;
            break;
        }
        if items.len() == 1 {
            return Ok(match items.pop() {
                Some(Item::Key(k)) => Sel::Key(k),
                Some(Item::Index(i)) => Sel::Index(i),
                Some(Item::Slice(a, b, c)) => Sel::Slice {
                    start: a,
                    end: b,
                    step: c,
                },
                None => return Err(PathError::Malformed),
            });
        }
        let mut steps = Vec::with_capacity(items.len());
        for it in items {
            match it {
                Item::Key(k) => steps.push(Step::Key(k)),
                Item::Index(i) => steps.push(Step::Index(i)),
                // A slice inside a union is valid RFC 9535 and rare in
                // practice; refused rather than half-honored.
                Item::Slice(..) => return Err(PathError::Unsupported),
            }
        }
        Ok(Sel::Union(steps))
    }

    fn expr(&mut self, depth: usize) -> Result<Expr, PathError> {
        if depth > MAX_FILTER_DEPTH {
            return Err(PathError::Malformed);
        }
        let mut l = self.and(depth)?;
        loop {
            self.ws();
            if self.eat_str("||") {
                let r = self.and(depth)?;
                l = Expr::Or(Box::new(l), Box::new(r));
            } else {
                return Ok(l);
            }
        }
    }

    fn and(&mut self, depth: usize) -> Result<Expr, PathError> {
        let mut l = self.unary(depth)?;
        loop {
            self.ws();
            if self.eat_str("&&") {
                let r = self.unary(depth)?;
                l = Expr::And(Box::new(l), Box::new(r));
            } else {
                return Ok(l);
            }
        }
    }

    fn unary(&mut self, depth: usize) -> Result<Expr, PathError> {
        self.ws();
        if self.peek() == Some(b'!') && self.b.get(self.i + 1) != Some(&b'=') {
            self.i += 1;
            if depth + 1 > MAX_FILTER_DEPTH {
                return Err(PathError::Malformed);
            }
            return Ok(Expr::Not(Box::new(self.unary(depth + 1)?)));
        }
        if self.eat(b'(') {
            let e = self.expr(depth + 1)?;
            self.ws();
            self.expect(b')')?;
            return Ok(e);
        }
        let l = self.operand()?;
        self.ws();
        let op = if self.eat_str("==") {
            CmpOp::Eq
        } else if self.eat_str("!=") {
            CmpOp::Ne
        } else if self.eat_str("<=") {
            CmpOp::Le
        } else if self.eat_str(">=") {
            CmpOp::Ge
        } else if self.eat_str("=~") {
            return Err(PathError::Unsupported);
        } else if self.eat(b'<') {
            CmpOp::Lt
        } else if self.eat(b'>') {
            CmpOp::Gt
        } else if self.peek() == Some(b'=') {
            return Err(PathError::Malformed);
        } else {
            return match l {
                // A bare literal is not a test: `?(1)` says nothing.
                Operand::Lit(_) => Err(PathError::Malformed),
                o => Ok(Expr::Exists(o)),
            };
        };
        self.ws();
        let r = self.operand()?;
        Ok(Expr::Cmp(l, op, r))
    }

    fn operand(&mut self) -> Result<Operand, PathError> {
        self.ws();
        match self.peek() {
            Some(b'@') => {
                self.i += 1;
                Ok(Operand::Rel(self.rel_steps()?))
            }
            Some(b'$') => {
                self.i += 1;
                Ok(Operand::Abs(self.rel_steps()?))
            }
            Some(b'"' | b'\'') => Ok(Operand::Lit(J::String(self.quoted()?))),
            _ => {
                let start = self.i;
                while let Some(c) = self.peek() {
                    if LITERAL_END.contains(&c) {
                        break;
                    }
                    self.i += 1;
                }
                let tok = &self.src[start..self.i];
                match tok {
                    "true" => Ok(Operand::Lit(J::Bool(true))),
                    "false" => Ok(Operand::Lit(J::Bool(false))),
                    "null" => Ok(Operand::Lit(J::Null)),
                    t => match serde_json::from_str::<J>(t) {
                        Ok(n @ J::Number(_)) => Ok(Operand::Lit(n)),
                        _ => Err(PathError::Malformed),
                    },
                }
            }
        }
    }

    /// The definite steps after `@` or `$` inside a filter.
    fn rel_steps(&mut self) -> Result<Vec<Step>, PathError> {
        let mut steps = Vec::new();
        loop {
            if self.b[self.i..].starts_with(b"..") {
                return Err(PathError::Unsupported);
            }
            if self.eat(b'.') {
                if self.peek() == Some(b'*') {
                    return Err(PathError::Unsupported);
                }
                steps.push(Step::Key(self.name(FILTER_NAME_END)?));
            } else if self.peek() == Some(b'[') {
                self.i += 1;
                self.ws();
                match self.item()? {
                    Item::Key(k) => steps.push(Step::Key(k)),
                    Item::Index(i) => steps.push(Step::Index(i)),
                    Item::Slice(..) => return Err(PathError::Unsupported),
                }
                self.ws();
                self.expect(b']')?;
            } else {
                return Ok(steps);
            }
        }
    }
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

/// The concrete locations a path names, in document order, each as the
/// definite steps from the root (indexes non-negative). A definite path
/// yields at most one; an indefinite one (ADR-0054) any number. Feed a
/// location back through `Path::internal` to [`get`], [`get_mut`] or
/// [`remove`].
pub fn select(doc: &J, path: &Path) -> Vec<Vec<Step>> {
    let Some(sels) = path.selectors() else {
        return get(doc, path)
            .map(|_| path.steps.clone())
            .into_iter()
            .collect();
    };
    select_sels(doc, sels)
}

/// [`select`] over a selector list: the indefinite core, also used for a
/// path's parents (`JSON.SET` adding a key under each match).
pub fn select_sels(doc: &J, sels: &[Sel]) -> Vec<Vec<Step>> {
    let mut cur: Vec<(Vec<Step>, &J)> = vec![(Vec::new(), doc)];
    for sel in sels {
        let mut next = Vec::new();
        for (p, v) in &cur {
            apply(doc, sel, p, v, &mut next);
        }
        cur = next;
    }
    cur.into_iter().map(|(p, _)| p).collect()
}

fn child<'v>(p: &[Step], s: Step, v: &'v J, out: &mut Vec<(Vec<Step>, &'v J)>) {
    let mut q = Vec::with_capacity(p.len() + 1);
    q.extend_from_slice(p);
    q.push(s);
    out.push((q, v));
}

fn apply<'v>(root: &'v J, sel: &Sel, p: &[Step], v: &'v J, out: &mut Vec<(Vec<Step>, &'v J)>) {
    match sel {
        Sel::Key(k) => {
            if let J::Object(m) = v
                && let Some(c) = m.get(k)
            {
                child(p, Step::Key(k.clone()), c, out);
            }
        }
        Sel::Index(i) => {
            if let J::Array(a) = v
                && let Some(ix) = resolve_index(*i, a.len())
            {
                child(p, Step::Index(ix as i64), &a[ix], out);
            }
        }
        Sel::Wild => match v {
            J::Object(m) => {
                for (k, c) in m {
                    child(p, Step::Key(k.clone()), c, out);
                }
            }
            J::Array(a) => {
                for (ix, c) in a.iter().enumerate() {
                    child(p, Step::Index(ix as i64), c, out);
                }
            }
            _ => {}
        },
        Sel::Union(steps) => {
            for s in steps {
                let one = match s {
                    Step::Key(k) => Sel::Key(k.clone()),
                    Step::Index(i) => Sel::Index(*i),
                };
                apply(root, &one, p, v, out);
            }
        }
        Sel::Slice { start, end, step } => {
            if let J::Array(a) = v {
                let n = a.len() as i64;
                let clamp = |x: i64| if x < 0 { (x + n).max(0) } else { x.min(n) };
                let lo = start.map_or(0, clamp);
                let hi = end.map_or(n, clamp);
                let mut ix = lo;
                while ix < hi {
                    child(p, Step::Index(ix), &a[ix as usize], out);
                    ix += step;
                }
            }
        }
        Sel::Filter(e) => match v {
            J::Array(a) => {
                for (ix, c) in a.iter().enumerate() {
                    if holds(root, e, c) {
                        child(p, Step::Index(ix as i64), c, out);
                    }
                }
            }
            J::Object(m) => {
                for (k, c) in m {
                    if holds(root, e, c) {
                        child(p, Step::Key(k.clone()), c, out);
                    }
                }
            }
            _ => {}
        },
        Sel::Descend(inner) => descend(root, inner, p, v, out),
    }
}

/// `..inner`: the node itself, then every node beneath it, in pre-order.
fn descend<'v>(root: &'v J, inner: &Sel, p: &[Step], v: &'v J, out: &mut Vec<(Vec<Step>, &'v J)>) {
    apply(root, inner, p, v, out);
    match v {
        J::Object(m) => {
            for (k, c) in m {
                let mut q = p.to_vec();
                q.push(Step::Key(k.clone()));
                descend(root, inner, &q, c, out);
            }
        }
        J::Array(a) => {
            for (ix, c) in a.iter().enumerate() {
                let mut q = p.to_vec();
                q.push(Step::Index(ix as i64));
                descend(root, inner, &q, c, out);
            }
        }
        _ => {}
    }
}

fn holds(root: &J, e: &Expr, cur: &J) -> bool {
    match e {
        Expr::Or(a, b) => holds(root, a, cur) || holds(root, b, cur),
        Expr::And(a, b) => holds(root, a, cur) && holds(root, b, cur),
        Expr::Not(x) => !holds(root, x, cur),
        Expr::Exists(o) => resolve(root, o, cur).is_some(),
        Expr::Cmp(l, op, r) => compare(resolve(root, l, cur), *op, resolve(root, r, cur)),
    }
}

fn resolve<'a>(root: &'a J, o: &'a Operand, cur: &'a J) -> Option<&'a J> {
    match o {
        Operand::Rel(steps) => walk(cur, steps),
        Operand::Abs(steps) => walk(root, steps),
        Operand::Lit(v) => Some(v),
    }
}

fn walk<'a>(mut cur: &'a J, steps: &[Step]) -> Option<&'a J> {
    for step in steps {
        cur = match (step, cur) {
            (Step::Key(k), J::Object(m)) => m.get(k)?,
            (Step::Index(i), J::Array(a)) => a.get(resolve_index(*i, a.len())?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// RedisJSON's comparison: `==` holds for equal values (numbers by value,
/// so 1 == 1.0) and never involving an absent operand, two absent ones
/// included (RFC 9535 calls those equal; RedisJSON v8.2.8 does not, and it
/// is the contract here). `!=` is its negation. Ordering holds only between
/// two numbers or two strings.
fn compare(l: Option<&J>, op: CmpOp, r: Option<&J>) -> bool {
    match op {
        CmpOp::Eq => same(l, r),
        CmpOp::Ne => !same(l, r),
        _ => {
            let ord = match (l, r) {
                (Some(J::Number(a)), Some(J::Number(b))) => match (a.as_f64(), b.as_f64()) {
                    (Some(x), Some(y)) => x.partial_cmp(&y),
                    _ => None,
                },
                (Some(J::String(a)), Some(J::String(b))) => Some(a.cmp(b)),
                _ => None,
            };
            match (ord, op) {
                (Some(o), CmpOp::Lt) => o.is_lt(),
                (Some(o), CmpOp::Le) => o.is_le(),
                (Some(o), CmpOp::Gt) => o.is_gt(),
                (Some(o), CmpOp::Ge) => o.is_ge(),
                _ => false,
            }
        }
    }
}

fn same(l: Option<&J>, r: Option<&J>) -> bool {
    match (l, r) {
        (Some(a), Some(b)) => value_eq(a, b),
        _ => false,
    }
}

fn value_eq(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Number(x), J::Number(y)) => x.as_f64() == y.as_f64(),
        (J::Array(x), J::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| value_eq(p, q))
        }
        (J::Object(x), J::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| value_eq(v, w)))
        }
        _ => a == b,
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

    /// ADR-0054: the multi-match forms parse in the `$` dialect, and stay
    /// refused in the legacy one, which answers a single value.
    #[test]
    fn multimatch_parses_in_the_dollar_dialect_and_not_the_legacy_one() {
        for s in [
            "$..a",
            "$.a[*]",
            "$.*",
            "$.a[0:2]",
            "$.a[::2]",
            "$.a[-2:]",
            "$['a','b']",
            "$.a[0,2]",
            "$.a[?(@.x>1)]",
            "$.a..b",
            "$..[0]",
            "$..*",
        ] {
            let path = p(s);
            assert!(path.selectors().is_some(), "{s} should be indefinite");
            assert!(!path.is_root(), "{s}");
            assert!(path.is_jsonpath(), "{s}");
        }
        for s in ["..a", "a[*]", ".*", "a[0:2]", "a[?(@.x)]"] {
            assert_eq!(parse(s), Err(PathError::Unsupported), "legacy {s}");
        }
    }

    #[test]
    fn what_stays_unsupported_and_what_is_malformed() {
        for s in [
            "$.a[?(@.n =~ 'x.*')]",
            "$.a[::-1]",
            "$.a[0,1:3]",
            "$.a[?(@..x)]",
            "$.a[?(@.*)]",
        ] {
            assert_eq!(parse(s), Err(PathError::Unsupported), "{s}");
        }
        for s in [
            "$.a[",
            "$.a[x]",
            "$..",
            "$.a[0:2:0]",
            "$.a[?(@.x)",
            "$.a[?(1)]",
            "$.a[?(@.x = 1)]",
            r#"$[""]"#,
            "$.a[-]",
        ] {
            assert_eq!(parse(s), Err(PathError::Malformed), "{s}");
        }
    }

    #[test]
    fn quoted_keys_may_hold_what_the_old_prescan_refused() {
        assert_eq!(p(r#"$["a*b"]"#).steps, vec![Step::Key("a*b".into())]);
        assert_eq!(p(r#"$['a]b']"#).steps, vec![Step::Key("a]b".into())]);
        assert_eq!(p(r#"$["x?@..y"]"#).steps, vec![Step::Key("x?@..y".into())]);
        assert_eq!(p(r#"$['it\'s']"#).steps, vec![Step::Key("it's".into())]);
        assert!(
            p(r#"$["a*b"]"#).selectors().is_none(),
            "a quoted key is definite"
        );
    }

    #[test]
    fn a_filter_nested_without_bound_is_refused_not_recursed() {
        let deep = format!("$.a[?({}@.x{})]", "(".repeat(40), ")".repeat(40));
        assert_eq!(parse(&deep), Err(PathError::Malformed));
        let fine = format!("$.a[?({}@.x{})]", "(".repeat(8), ")".repeat(8));
        assert!(parse(&fine).is_ok());
    }

    fn sel(d: &J, s: &str) -> Vec<J> {
        let path = p(s);
        select(d, &path)
            .into_iter()
            .map(|loc| {
                get(d, &Path::internal(loc))
                    .cloned()
                    .expect("a selected location resolves")
            })
            .collect()
    }

    #[test]
    fn select_answers_each_form_in_document_order() {
        let d = json!({
            "a": 1,
            "b": {"a": 2, "c": [{"a": 3}, {"x": 4}]},
            "arr": [10, 20, 30, 40, 50],
        });
        assert_eq!(sel(&d, "$..a"), vec![json!(1), json!(2), json!(3)]);
        assert_eq!(sel(&d, "$.b.*").len(), 2);
        assert_eq!(
            sel(&d, "$.arr[*]"),
            vec![json!(10), json!(20), json!(30), json!(40), json!(50)]
        );
        assert_eq!(sel(&d, "$.arr[1:3]"), vec![json!(20), json!(30)]);
        assert_eq!(sel(&d, "$.arr[::2]"), vec![json!(10), json!(30), json!(50)]);
        assert_eq!(sel(&d, "$.arr[-2:]"), vec![json!(40), json!(50)]);
        assert_eq!(
            sel(&d, "$.arr[4,0,4]"),
            vec![json!(50), json!(10), json!(50)]
        );
        assert_eq!(sel(&d, "$['a','missing','arr']").len(), 2);
        assert_eq!(sel(&d, "$..c[*].a"), vec![json!(3)]);
        assert!(sel(&d, "$.missing[*]").is_empty());
        assert!(
            sel(&d, "$.a[*]").is_empty(),
            "a wildcard on a scalar selects nothing"
        );
        // A definite path selects its one location through the same door.
        assert_eq!(sel(&d, "$.b.a"), vec![json!(2)]);
        assert!(sel(&d, "$.b.zz").is_empty());
    }

    #[test]
    fn filters_compare_test_existence_and_combine() {
        let d = json!({
            "limit": 25,
            "items": [
                {"name": "pen", "price": 5, "stock": true},
                {"name": "book", "price": 30},
                {"name": "ink", "price": 25.0, "stock": false},
                {"name": "cap"},
            ],
        });
        let names = |s: &str| -> Vec<J> {
            let path = p(&format!("{s}.name"));
            select(&d, &path)
                .into_iter()
                .filter_map(|loc| get(&d, &Path::internal(loc)).cloned())
                .collect()
        };
        assert_eq!(names("$.items[?(@.price < 10)]"), vec![json!("pen")]);
        assert_eq!(names("$.items[?(@.price < 9.99)]"), vec![json!("pen")]);
        assert_eq!(names("$.items[?(@.price == 25.0)]"), vec![json!("ink")]);
        assert_eq!(names("$.items[?(@.price > -1.5e1)]").len(), 3);
        assert_eq!(
            names("$.items[?(@.price >= 25)]"),
            vec![json!("book"), json!("ink")]
        );
        // 25 == 25.0: numbers compare by value.
        assert_eq!(names("$.items[?(@.price == 25)]"), vec![json!("ink")]);
        assert_eq!(
            names("$.items[?(@.stock)]"),
            vec![json!("pen"), json!("ink")]
        );
        assert_eq!(names("$.items[?(!@.price)]"), vec![json!("cap")]);
        assert_eq!(
            names("$.items[?(@.stock == true || @.price > 29)]"),
            vec![json!("pen"), json!("book")]
        );
        assert_eq!(
            names("$.items[?(@.price > 1 && @.stock == false)]"),
            vec![json!("ink")]
        );
        assert_eq!(names("$.items[?(@.name == 'ink')]"), vec![json!("ink")]);
        assert_eq!(
            names(r#"$.items[?(@.name > "c")]"#),
            vec![json!("pen"), json!("ink"), json!("cap")]
        );
        // `$` inside a filter reads from the document root.
        assert_eq!(names("$.items[?(@.price == $.limit)]"), vec![json!("ink")]);
        // Ordering never holds across types, nor against an absent value.
        assert!(names("$.items[?(@.price > 'a')]").is_empty());
        assert!(names("$.items[?(@.missing < 1)]").is_empty());
        // An absent operand equals nothing, not even another absent one
        // (RedisJSON, not RFC 9535), so `!=` holds there.
        assert!(names("$.items[?(@.nope == @.nada)]").is_empty());
        assert_eq!(names("$.items[?(@.nope != @.nada)]").len(), 4);
        // A filter on an object tests its member values.
        let o = json!({"x": {"v": 1}, "y": {"v": 2}, "z": 3});
        assert_eq!(sel(&o, "$[?(@.v > 1)]"), vec![json!({"v": 2})]);
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
