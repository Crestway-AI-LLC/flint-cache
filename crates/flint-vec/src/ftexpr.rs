// SPDX-License-Identifier: Elastic-2.0
//! `FT.AGGREGATE`'s expressions, in `APPLY` and `FILTER` (ADR-0064): the
//! language RediSearch's `aggregate/expr` reads, how it evaluates one over a
//! row, its functions, and how it writes a number. Written from RediSearch's
//! rules, not its code.

use std::cmp::Ordering;

/// A value in an aggregation's row, as RediSearch's `RSValue` holds one.
#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    Num(f64),
    Str(Box<[u8]>),
    Arr(Vec<Datum>),
}

impl Datum {
    pub fn str(s: impl AsRef<[u8]>) -> Self {
        Datum::Str(s.as_ref().into())
    }

    /// As a number: a number, or a string that reads whole as one, as Rust
    /// reads it (RediSearch's `str_to_float`): no spaces, no hex, `inf` and
    /// `nan` allowed.
    pub fn to_number(&self) -> Option<f64> {
        match self {
            Datum::Num(x) => Some(*x),
            Datum::Str(s) => str_number(s),
            Datum::Null | Datum::Arr(_) => None,
        }
    }

    /// Whether `FILTER` keeps the row: a number other than 0 (NaN too), a
    /// string or array that is not empty.
    pub fn truthy(&self) -> bool {
        match self {
            Datum::Null => false,
            Datum::Num(x) => *x != 0.0,
            Datum::Str(s) => !s.is_empty(),
            Datum::Arr(a) => !a.is_empty(),
        }
    }

    /// The text a reply or `to_str` gives it: a number as
    /// [`num_to_str`] writes it, null as nothing.
    pub fn text(&self) -> Vec<u8> {
        match self {
            Datum::Null => Vec::new(),
            Datum::Num(x) => num_to_str(*x).into_bytes(),
            Datum::Str(s) => s.to_vec(),
            Datum::Arr(a) => {
                let parts: Vec<Vec<u8>> = a.iter().map(Datum::text).collect();
                parts.join(&b","[..])
            }
        }
    }
}

fn str_number(s: &[u8]) -> Option<f64> {
    std::str::from_utf8(s).ok()?.parse::<f64>().ok()
}

/// A number as RediSearch writes one in a reply (`num_to_str`): a whole
/// number in `i64`'s range as an integer, anything else as C's `%.12g`.
pub fn num_to_str(x: f64) -> String {
    if x.fract() == 0.0 && x >= i64::MIN as f64 && x < i64::MAX as f64 {
        return (x as i64).to_string();
    }
    fmt_g(x, 12)
}

/// C's `%.<p>g`: `p` significant digits, in exponent form when the
/// exponent is below -4 or at least `p`, trailing zeros dropped.
fn fmt_g(x: f64, p: usize) -> String {
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.into();
    }
    let e = format!("{:.*e}", p - 1, x);
    let Some((mant, exp)) = e.split_once('e') else {
        return e;
    };
    let exp: i32 = exp.parse().unwrap_or(0);
    let strip = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s.to_string()
        }
    };
    if exp < -4 || exp >= p as i32 {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", strip(mant), exp.unsigned_abs())
    } else {
        strip(&format!("{:.*}", (p as i32 - 1 - exp) as usize, x))
    }
}

/// What an expression reads its properties from: one row of a pipeline.
pub trait Row {
    /// The row's value under `name`; `None` when it has none.
    fn get(&self, name: &str) -> Option<&Datum>;
    /// The query's terms this row's document matched, for `matched_terms`.
    fn matched_terms(&self) -> Vec<Box<[u8]>> {
        Vec::new()
    }
}

/// An arithmetic operator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Arith {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

/// A comparison.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A parsed expression.
#[derive(Clone, Debug, PartialEq)]
pub enum Ast {
    Lit(Datum),
    Prop(Box<str>),
    Arith(Arith, Box<Ast>, Box<Ast>),
    Cmp(Cmp, Box<Ast>, Box<Ast>),
    And(Box<Ast>, Box<Ast>),
    Or(Box<Ast>, Box<Ast>),
    Not(Box<Ast>),
    Call(Func, Vec<Ast>),
}

/// RediSearch's functions, by the name an expression calls.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Func {
    Log,
    Log2,
    Exp,
    Floor,
    Ceil,
    Abs,
    Sqrt,
    Lower,
    Upper,
    Substr,
    Format,
    Split,
    MatchedTerms,
    ToNumber,
    ToStr,
    Exists,
    Case,
    StartsWith,
    Contains,
    Strlen,
    Timefmt,
    Parsetime,
    Hour,
    Minute,
    Day,
    Month,
    Year,
    MonthOfYear,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    GeoDistance,
}

/// Each function's name and how many arguments it takes, at least and at
/// most (`usize::MAX`: no limit).
const FUNCS: &[(&str, Func, usize, usize)] = &[
    ("log", Func::Log, 1, 1),
    ("log2", Func::Log2, 1, 1),
    ("exp", Func::Exp, 1, 1),
    ("floor", Func::Floor, 1, 1),
    ("ceil", Func::Ceil, 1, 1),
    ("abs", Func::Abs, 1, 1),
    ("sqrt", Func::Sqrt, 1, 1),
    ("lower", Func::Lower, 1, 1),
    ("upper", Func::Upper, 1, 1),
    ("substr", Func::Substr, 3, 3),
    ("format", Func::Format, 1, usize::MAX),
    ("split", Func::Split, 1, 3),
    ("matched_terms", Func::MatchedTerms, 0, 1),
    ("to_number", Func::ToNumber, 1, 1),
    ("to_str", Func::ToStr, 1, 1),
    ("exists", Func::Exists, 1, 1),
    ("case", Func::Case, 3, 3),
    ("startswith", Func::StartsWith, 2, 2),
    ("contains", Func::Contains, 2, 2),
    ("strlen", Func::Strlen, 1, 1),
    ("timefmt", Func::Timefmt, 1, 2),
    ("parsetime", Func::Parsetime, 2, 2),
    ("hour", Func::Hour, 1, 1),
    ("minute", Func::Minute, 1, 1),
    ("day", Func::Day, 1, 1),
    ("month", Func::Month, 1, 1),
    ("year", Func::Year, 1, 1),
    ("monthofyear", Func::MonthOfYear, 1, 1),
    ("dayofmonth", Func::DayOfMonth, 1, 1),
    ("dayofweek", Func::DayOfWeek, 1, 1),
    ("dayofyear", Func::DayOfYear, 1, 1),
    ("geodistance", Func::GeoDistance, 2, 4),
];

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Num(f64),
    Str(Vec<u8>),
    Prop(String),
    Sym(String),
    Null,
    Open,
    Close,
    Comma,
    Op(&'static str),
    End,
}

struct Lexed {
    tok: Tok,
    at: usize,
    text: String,
}

const OPS: &[&str] = &[
    "==", "!=", "<=", ">=", "&&", "||", "<", ">", "!", "+", "-", "*", "/", "%", "^",
];

fn syntax(at: usize, near: &str) -> String {
    format!("Syntax error at offset {at} near '{near}'")
}

/// The number that starts at `b[i..]`, as RediSearch's lexer reads one: an
/// optional sign, digits, an optional fraction and exponent, or `inf`.
fn lex_number(b: &[u8], i: usize) -> Option<usize> {
    let mut j = i;
    if matches!(b.get(j), Some(b'+' | b'-')) {
        j += 1;
    }
    if b[j..].starts_with(b"inf") {
        let end = j + 3;
        let word = b
            .get(end)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_');
        return (!word || j > i).then_some(end);
    }
    let digits = |k: usize| b[k..].iter().take_while(|c| c.is_ascii_digit()).count();
    let d = digits(j);
    if d == 0 {
        return None;
    }
    j += d;
    if b.get(j) == Some(&b'.') && digits(j + 1) > 0 {
        j += 1 + digits(j + 1);
    }
    if matches!(b.get(j), Some(b'e' | b'E')) {
        let mut k = j + 1;
        if matches!(b.get(k), Some(b'+' | b'-')) {
            k += 1;
        }
        if digits(k) > 0 {
            j = k + digits(k);
        }
    }
    Some(j)
}

fn lex(src: &str) -> Result<Vec<Lexed>, String> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() || c == b'$' {
            // RediSearch's lexer passes over `$` without a token.
            i += 1;
            continue;
        }
        let start = i;
        let tok = if let Some(end) = lex_number(b, i) {
            let t = &src[i..end];
            i = end;
            let x = if t.trim_start_matches(['+', '-']) == "inf" {
                if t.starts_with('-') {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            } else {
                t.parse::<f64>().map_err(|_| syntax(start, t))?
            };
            Tok::Num(x)
        } else if c == b'\'' || c == b'"' {
            let mut j = i + 1;
            let mut raw = Vec::new();
            loop {
                match b.get(j) {
                    None => return Err(syntax(start, &src[start..])),
                    Some(&q) if q == c => break,
                    Some(b'\\') if j + 1 < b.len() => {
                        // A backslash before punctuation or a space is
                        // dropped; before anything else it is kept.
                        let n = b[j + 1];
                        if !(n.is_ascii_punctuation() || n.is_ascii_whitespace()) {
                            raw.push(b'\\');
                        }
                        raw.push(n);
                        j += 2;
                    }
                    Some(&x) => {
                        raw.push(x);
                        j += 1;
                    }
                }
            }
            i = j + 1;
            Tok::Str(raw)
        } else if c == b'@' {
            let mut j = i + 1;
            let mut name = Vec::new();
            while let Some(&x) = b.get(j) {
                if x == b'\\' && j + 1 < b.len() {
                    name.push(b[j + 1]);
                    j += 2;
                } else if x == b'_' || !(x.is_ascii_punctuation() || x.is_ascii_whitespace()) {
                    name.push(x);
                    j += 1;
                } else {
                    break;
                }
            }
            if name.is_empty() {
                return Err(syntax(start, "@"));
            }
            i = j;
            Tok::Prop(String::from_utf8_lossy(&name).into_owned())
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let end = i + b[i..]
                .iter()
                .take_while(|x| x.is_ascii_alphanumeric() || **x == b'_')
                .count();
            let w = &src[i..end];
            i = end;
            if w == "NULL" {
                Tok::Null
            } else {
                Tok::Sym(w.to_string())
            }
        } else if c == b'(' {
            i += 1;
            Tok::Open
        } else if c == b')' {
            i += 1;
            Tok::Close
        } else if c == b',' {
            i += 1;
            Tok::Comma
        } else if let Some(op) = OPS.iter().find(|op| b[i..].starts_with(op.as_bytes())) {
            i += op.len();
            Tok::Op(op)
        } else {
            let end = src[i..]
                .chars()
                .next()
                .map_or(i + 1, |ch| i + ch.len_utf8());
            return Err(syntax(start, &src[start..end]));
        };
        out.push(Lexed {
            tok,
            at: start,
            text: src[start..i].to_string(),
        });
    }
    out.push(Lexed {
        tok: Tok::End,
        at: src.len(),
        text: String::new(),
    });
    Ok(out)
}

struct Parser {
    toks: Vec<Lexed>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.at].tok
    }

    fn bump(&mut self) -> &Lexed {
        let t = &self.toks[self.at];
        if self.at + 1 < self.toks.len() {
            self.at += 1;
        }
        t
    }

    fn err(&self) -> String {
        let t = &self.toks[self.at];
        syntax(t.at, &t.text)
    }

    fn op(&self) -> Option<&'static str> {
        match self.peek() {
            Tok::Op(o) => Some(o),
            _ => None,
        }
    }

    fn or(&mut self) -> Result<Ast, String> {
        let mut l = self.and()?;
        while self.op() == Some("||") {
            self.bump();
            l = Ast::Or(Box::new(l), Box::new(self.and()?));
        }
        Ok(l)
    }

    fn and(&mut self) -> Result<Ast, String> {
        let mut l = self.cmp()?;
        while self.op() == Some("&&") {
            self.bump();
            l = Ast::And(Box::new(l), Box::new(self.cmp()?));
        }
        Ok(l)
    }

    /// One comparison at most: they do not chain.
    fn cmp(&mut self) -> Result<Ast, String> {
        let l = self.add()?;
        let c = match self.op() {
            Some("==") => Cmp::Eq,
            Some("!=") => Cmp::Ne,
            Some("<") => Cmp::Lt,
            Some("<=") => Cmp::Le,
            Some(">") => Cmp::Gt,
            Some(">=") => Cmp::Ge,
            _ => return Ok(l),
        };
        self.bump();
        let r = self.add()?;
        if matches!(self.op(), Some("==" | "!=" | "<" | "<=" | ">" | ">=")) {
            return Err(self.err());
        }
        Ok(Ast::Cmp(c, Box::new(l), Box::new(r)))
    }

    fn add(&mut self) -> Result<Ast, String> {
        let mut l = self.mul()?;
        loop {
            let a = match self.op() {
                Some("+") => Arith::Add,
                Some("-") => Arith::Sub,
                _ => return Ok(l),
            };
            self.bump();
            l = Ast::Arith(a, Box::new(l), Box::new(self.mul()?));
        }
    }

    fn mul(&mut self) -> Result<Ast, String> {
        let mut l = self.pow()?;
        loop {
            let a = match self.op() {
                Some("*") => Arith::Mul,
                Some("/") => Arith::Div,
                Some("%") => Arith::Mod,
                _ => return Ok(l),
            };
            self.bump();
            l = Ast::Arith(a, Box::new(l), Box::new(self.pow()?));
        }
    }

    /// `^` binds to its right.
    fn pow(&mut self) -> Result<Ast, String> {
        let l = self.primary()?;
        if self.op() == Some("^") {
            self.bump();
            return Ok(Ast::Arith(Arith::Pow, Box::new(l), Box::new(self.pow()?)));
        }
        Ok(l)
    }

    fn primary(&mut self) -> Result<Ast, String> {
        let t = self.bump();
        let (tok, at, text) = (t.tok.clone(), t.at, t.text.clone());
        match tok {
            Tok::Num(x) => Ok(Ast::Lit(Datum::Num(x))),
            Tok::Str(s) => Ok(Ast::Lit(Datum::Str(s.into()))),
            Tok::Prop(p) => Ok(Ast::Prop(p.into())),
            Tok::Null => Ok(Ast::Lit(Datum::Null)),
            // `!` binds looser than a comparison and tighter than `&&`, so
            // its operand is one comparison: `!@a == 1` is `!(@a == 1)`.
            Tok::Op("!") => Ok(Ast::Not(Box::new(self.cmp()?))),
            Tok::Open => {
                let e = self.or()?;
                if *self.peek() != Tok::Close {
                    return Err(self.err());
                }
                self.bump();
                Ok(e)
            }
            Tok::Sym(name) => {
                if *self.peek() != Tok::Open {
                    return Err(format!("Unknown symbol '{name}'"));
                }
                let lower = name.to_ascii_lowercase();
                let Some(&(fname, f, min, max)) = FUNCS.iter().find(|(n, ..)| *n == lower) else {
                    return Err(format!("Unknown function name '{name}'"));
                };
                self.bump();
                let mut args = Vec::new();
                if *self.peek() != Tok::Close {
                    loop {
                        args.push(self.or()?);
                        match self.peek() {
                            Tok::Comma => {
                                self.bump();
                            }
                            Tok::Close => break,
                            _ => return Err(self.err()),
                        }
                    }
                }
                self.bump();
                let n = args.len();
                if n < min || n > max {
                    return Err(if min == max {
                        format!("Function '{fname}' expects {min} arguments, but got {n}")
                    } else if max == usize::MAX {
                        format!("Function '{fname}' expects at least {min} arguments, but got {n}")
                    } else {
                        format!(
                            "Function '{fname}' expects between {min} and {max} arguments, but got {n}"
                        )
                    });
                }
                Ok(Ast::Call(f, args))
            }
            _ => Err(syntax(at, &text)),
        }
    }
}

/// `src` as an expression, or RediSearch's words for why it is not one.
pub fn parse(src: &str) -> Result<Ast, String> {
    let mut p = Parser {
        toks: lex(src)?,
        at: 0,
    };
    let e = p.or()?;
    if *p.peek() != Tok::End {
        return Err(p.err());
    }
    Ok(e)
}

/// The words an arithmetic operator's operand that is not a number fails
/// its query with.
const NOT_NUMERIC: &str = "Could not convert value to a number";

fn missing(name: &str) -> String {
    format!(
        "Could not find the value for a parameter name, consider using EXISTS if applicable for {name}"
    )
}

/// Equality as `==` and `!=` test it: a number and a numeric string as
/// numbers, NaN equal to anything, null equal only to null.
fn equal(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Num(x), Datum::Num(y)) => x == y || x.is_nan() || y.is_nan(),
        (Datum::Str(x), Datum::Str(y)) => x == y,
        (Datum::Num(x), Datum::Str(s)) | (Datum::Str(s), Datum::Num(x)) => {
            str_number(s).is_some_and(|y| *x == y || x.is_nan() || y.is_nan())
        }
        (Datum::Null, Datum::Null) => true,
        (Datum::Arr(x), Datum::Arr(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| equal(a, b))
        }
        _ => false,
    }
}

/// The order `<` and its kin test: numbers as numbers, strings as bytes, a
/// number against a string that is not one an error, null first, an array
/// by its first element. NaN is equal to anything.
pub fn order(a: &Datum, b: &Datum) -> Result<Ordering, String> {
    Ok(match (a, b) {
        (Datum::Num(x), Datum::Num(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Datum::Str(x), Datum::Str(y)) => x.cmp(y),
        (Datum::Num(x), Datum::Str(s)) => {
            let y = str_number(s).ok_or("Error converting string")?;
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
        (Datum::Str(s), Datum::Num(y)) => {
            let x = str_number(s).ok_or("Error converting string")?;
            x.partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Less,
        (_, Datum::Null) => Ordering::Greater,
        (Datum::Arr(x), Datum::Arr(y)) => match (x.first(), y.first()) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(p), Some(q)) => order(p, q)?,
        },
        (Datum::Arr(x), other) => match x.first() {
            None => Ordering::Less,
            Some(p) => order(p, other)?,
        },
        (other, Datum::Arr(y)) => match y.first() {
            None => Ordering::Greater,
            Some(q) => order(other, q)?,
        },
    })
}

fn bool_num(b: bool) -> Datum {
    Datum::Num(if b { 1.0 } else { 0.0 })
}

impl Ast {
    /// Every property the expression reads, for the pipeline to check
    /// before it runs.
    pub fn props(&self) -> Vec<&str> {
        let mut out = Vec::new();
        self.walk(&mut out);
        out
    }

    fn walk<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            Ast::Lit(_) => {}
            Ast::Prop(p) => out.push(p),
            Ast::Arith(_, l, r) | Ast::Cmp(_, l, r) | Ast::And(l, r) | Ast::Or(l, r) => {
                l.walk(out);
                r.walk(out);
            }
            Ast::Not(e) => e.walk(out),
            Ast::Call(_, args) => args.iter().for_each(|a| a.walk(out)),
        }
    }

    /// The expression's value over `row`, or the error that fails the
    /// query.
    pub fn eval(&self, row: &dyn Row) -> Result<Datum, String> {
        match self {
            Ast::Lit(d) => Ok(d.clone()),
            Ast::Prop(p) => row.get(p).cloned().ok_or_else(|| missing(p)),
            Ast::Arith(op, l, r) => {
                let (l, r) = (l.eval(row)?, r.eval(row)?);
                let (Some(x), Some(y)) = (l.to_number(), r.to_number()) else {
                    return Err(NOT_NUMERIC.into());
                };
                Ok(Datum::Num(match op {
                    Arith::Add => x + y,
                    Arith::Sub => x - y,
                    Arith::Mul => x * y,
                    Arith::Div => x / y,
                    Arith::Mod => x % y,
                    Arith::Pow => x.powf(y),
                }))
            }
            Ast::Cmp(c, l, r) => {
                let (l, r) = (l.eval(row)?, r.eval(row)?);
                Ok(bool_num(match c {
                    Cmp::Eq => equal(&l, &r),
                    Cmp::Ne => !equal(&l, &r),
                    Cmp::Lt => order(&l, &r)? == Ordering::Less,
                    Cmp::Le => order(&l, &r)? != Ordering::Greater,
                    Cmp::Gt => order(&l, &r)? == Ordering::Greater,
                    Cmp::Ge => order(&l, &r)? != Ordering::Less,
                }))
            }
            Ast::And(l, r) => Ok(bool_num(l.eval(row)?.truthy() && r.eval(row)?.truthy())),
            Ast::Or(l, r) => Ok(bool_num(l.eval(row)?.truthy() || r.eval(row)?.truthy())),
            Ast::Not(e) => Ok(bool_num(!e.eval(row)?.truthy())),
            Ast::Call(f, args) => call(*f, args, row),
        }
    }
}

/// A function's string argument, or RediSearch's words for its type.
fn want_str<'a>(d: &'a Datum, i: usize, name: &str) -> Result<&'a [u8], String> {
    match d {
        Datum::Str(s) => Ok(s),
        _ => Err(format!(
            "Invalid type for argument {i} in function '{name}': expected a string"
        )),
    }
}

fn call(f: Func, args: &[Ast], row: &dyn Row) -> Result<Datum, String> {
    // `exists` asks whether its property has a value, and `case` evaluates
    // only the branch it takes: neither fails on a missing value it skips.
    match f {
        Func::Exists => {
            let has = match &args[0] {
                Ast::Prop(p) => row.get(p).is_some_and(|d| *d != Datum::Null),
                e => e.eval(row)? != Datum::Null,
            };
            return Ok(bool_num(has));
        }
        Func::Case => {
            let pick = if args[0].eval(row)?.truthy() { 1 } else { 2 };
            return args[pick].eval(row);
        }
        Func::MatchedTerms => {
            let max = match args.first() {
                Some(a) => a.eval(row)?.to_number().unwrap_or(100.0).clamp(0.0, 100.0) as usize,
                None => 100,
            };
            let mut terms = row.matched_terms();
            terms.truncate(max);
            return Ok(Datum::Arr(terms.into_iter().map(Datum::Str).collect()));
        }
        _ => {}
    }
    let v: Vec<Datum> = args.iter().map(|a| a.eval(row)).collect::<Result<_, _>>()?;
    let num = |i: usize| v[i].to_number();
    let math = |g: fn(f64) -> f64| Datum::Num(num(0).map_or(f64::NAN, g));
    Ok(match f {
        Func::Log => math(f64::ln),
        Func::Log2 => math(f64::log2),
        Func::Exp => math(f64::exp),
        Func::Floor => math(f64::floor),
        Func::Ceil => math(f64::ceil),
        Func::Abs => math(f64::abs),
        Func::Sqrt => math(f64::sqrt),
        Func::Lower | Func::Upper => match &v[0] {
            Datum::Str(s) => Datum::Str(if f == Func::Lower {
                s.to_ascii_lowercase().into()
            } else {
                s.to_ascii_uppercase().into()
            }),
            _ => Datum::Null,
        },
        Func::Substr => {
            let s = match &v[0] {
                Datum::Str(s) => s,
                _ => return Err("Invalid type for substr. Expected string".into()),
            };
            let sz = s.len() as i64;
            let (Some(off), Some(len)) = (num(1), num(2)) else {
                return Err("Invalid type for substr. Expected number".into());
            };
            let mut off = off as i64;
            if off < 0 {
                off += sz;
            }
            let off = off.clamp(0, sz);
            let mut len = len as i64;
            if len < 0 {
                len += sz - off;
            }
            let len = len.clamp(0, sz - off);
            Datum::str(&s[off as usize..(off + len) as usize])
        }
        Func::Format => {
            let Datum::Str(fmt) = &v[0] else {
                return Err("Bad format string!".into());
            };
            let mut out = Vec::new();
            let mut next = 1;
            let mut i = 0;
            while i < fmt.len() {
                if fmt[i] != b'%' {
                    out.push(fmt[i]);
                    i += 1;
                    continue;
                }
                match fmt.get(i + 1) {
                    Some(b'%') => out.push(b'%'),
                    Some(b's') => {
                        let a = v.get(next).ok_or("Not enough arguments for format")?;
                        next += 1;
                        if *a == Datum::Null {
                            out.extend_from_slice(b"(null)");
                        } else {
                            out.extend(a.text());
                        }
                    }
                    _ => return Err("Unknown format specifier passed".into()),
                }
                i += 2;
            }
            Datum::Str(out.into())
        }
        Func::Split => {
            let s = want_str(&v[0], 0, "split")?;
            let seps: &[u8] = match v.get(1) {
                Some(d) => want_str(d, 1, "split")?,
                None => b",",
            };
            let strip: &[u8] = match v.get(2) {
                Some(d) => want_str(d, 2, "split")?,
                None => b" ",
            };
            let parts: Vec<Datum> = s
                .split(|c| seps.contains(c))
                .map(|t| {
                    let a = t.iter().position(|c| !strip.contains(c)).unwrap_or(t.len());
                    let b = t
                        .iter()
                        .rposition(|c| !strip.contains(c))
                        .map_or(a, |p| p + 1);
                    &t[a..b.max(a)]
                })
                .filter(|t| !t.is_empty())
                .take(1024)
                .map(Datum::str)
                .collect();
            Datum::Arr(parts)
        }
        Func::ToNumber => match &v[0] {
            Datum::Num(x) => Datum::Num(*x),
            d => match d.to_number() {
                Some(x) => Datum::Num(x),
                None => {
                    return Err(format!(
                        "to_number: cannot convert string '{}'",
                        String::from_utf8_lossy(&d.text())
                    ));
                }
            },
        },
        Func::ToStr => Datum::Str(v[0].text().into()),
        Func::StartsWith => {
            let s = want_str(&v[0], 0, "startswith")?;
            let p = want_str(&v[1], 1, "startswith")?;
            bool_num(s.starts_with(p))
        }
        Func::Contains => {
            let s = want_str(&v[0], 0, "contains")?;
            let n = want_str(&v[1], 1, "contains")?;
            Datum::Num(if n.is_empty() {
                (s.len() + 1) as f64
            } else {
                s.windows(n.len()).filter(|w| *w == n).count() as f64
            })
        }
        Func::Strlen => Datum::Num(want_str(&v[0], 0, "strlen")?.len() as f64),
        Func::Timefmt => {
            let fmt: &[u8] = match v.get(1) {
                Some(d) => want_str(d, 1, "timefmt")?,
                None => b"%FT%TZ",
            };
            match num(0) {
                Some(t) => match strftime(fmt, t as i64)? {
                    Some(s) => Datum::Str(s.into()),
                    None => Datum::Null,
                },
                None => Datum::Null,
            }
        }
        Func::Parsetime => {
            let s = want_str(&v[0], 0, "parsetime")?;
            let fmt = want_str(&v[1], 1, "parsetime")?;
            match strptime(s, fmt)? {
                Some(t) => Datum::Num(t as f64),
                None => Datum::Null,
            }
        }
        Func::Hour
        | Func::Minute
        | Func::Day
        | Func::Month
        | Func::Year
        | Func::MonthOfYear
        | Func::DayOfMonth
        | Func::DayOfWeek
        | Func::DayOfYear => match num(0) {
            Some(t) if t >= 0.0 => Datum::Num(date_part(f, t as i64) as f64),
            _ => Datum::Null,
        },
        Func::GeoDistance => Datum::Num(geodistance(&v)),
        Func::Exists | Func::Case | Func::MatchedTerms => Datum::Null,
    })
}

/// `geodistance(a, b)`: two points as `"lon,lat"` strings, a string and a
/// longitude and latitude in either order, or four numbers; metres, to two
/// decimals, by the same haversine as `GEO` fields (ADR-0062). NaN when a
/// point does not read.
fn geodistance(v: &[Datum]) -> f64 {
    let point = |d: &Datum| match d {
        Datum::Str(s) => crate::geo::parse_point(s).ok(),
        _ => None,
    };
    let pair = |a: &Datum, b: &Datum| Some((a.to_number()?, b.to_number()?));
    let pts = match v {
        [a, b] => point(a).zip(point(b)),
        [a, b, c] => match point(a) {
            Some(p) => pair(b, c).map(|q| (p, q)),
            None => pair(a, b).zip(point(c)),
        },
        [a, b, c, d] => pair(a, b).zip(pair(c, d)),
        _ => None,
    };
    match pts {
        Some(((lon1, lat1), (lon2, lat2))) => {
            (crate::geo::distance(lon1, lat1, lon2, lat2) * 100.0).round() / 100.0
        }
        None => f64::NAN,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date, any month or day
/// counted past its end, as `timegm` normalizes them.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y + (m - 1).div_euclid(12);
    let m = (m - 1).rem_euclid(12) + 1;
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468 + (d - 1)
}

/// The date of a day since 1970-01-01: year, month 1-12, day 1-31.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + if m <= 2 { 1 } else { 0 }, m, d)
}

/// A UTC time broken into its fields, as `gmtime` breaks it.
struct Tm {
    year: i64,
    mon: i64,
    mday: i64,
    hour: i64,
    min: i64,
    sec: i64,
    wday: i64,
    yday: i64,
}

fn gmtime(t: i64) -> Tm {
    let days = t.div_euclid(86_400);
    let secs = t.rem_euclid(86_400);
    let (year, mon, mday) = civil_from_days(days);
    Tm {
        year,
        mon,
        mday,
        hour: secs / 3600,
        min: secs % 3600 / 60,
        sec: secs % 60,
        wday: (days + 4).rem_euclid(7),
        yday: days - days_from_civil(year, 1, 1),
    }
}

fn date_part(f: Func, t: i64) -> i64 {
    let tm = gmtime(t);
    match f {
        Func::Minute => t - t % 60,
        Func::Hour => t - t % 3600,
        Func::Day => t - t % 86_400,
        Func::Month => days_from_civil(tm.year, tm.mon, 1) * 86_400,
        Func::Year => tm.year,
        Func::MonthOfYear => tm.mon - 1,
        Func::DayOfMonth => tm.mday,
        Func::DayOfWeek => tm.wday,
        _ => tm.yday,
    }
}

const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

fn unsupported(c: u8) -> String {
    format!(
        "the time format specifier %{} is not supported: the C locale's POSIX specifiers only (ADR-0064)",
        c as char
    )
}

/// `strftime` in the C locale over `t`, UTC, as `timefmt` writes it; an
/// unknown specifier is refused by name (ADR-0064, sub-decision 2).
fn strftime(fmt: &[u8], t: i64) -> Result<Option<Vec<u8>>, String> {
    let tm = gmtime(t);
    let mut out = Vec::new();
    let mut i = 0;
    while i < fmt.len() {
        if fmt[i] != b'%' {
            out.push(fmt[i]);
            i += 1;
            continue;
        }
        let Some(&c) = fmt.get(i + 1) else {
            out.push(b'%');
            break;
        };
        i += 2;
        let h12 = if tm.hour % 12 == 0 { 12 } else { tm.hour % 12 };
        // ISO 8601 week and its year, for %G, %g and %V.
        let iso = || {
            let wd = (tm.wday + 6) % 7;
            let thursday = tm.yday - wd + 3;
            let (mut y, mut d) = (tm.year, thursday);
            if d < 0 {
                y -= 1;
                d += days_from_civil(y + 1, 1, 1) - days_from_civil(y, 1, 1);
            } else {
                let len = days_from_civil(y + 1, 1, 1) - days_from_civil(y, 1, 1);
                if d >= len {
                    y += 1;
                    d -= len;
                }
            }
            (y, d / 7 + 1)
        };
        let s = match c {
            b'a' => DAYS[tm.wday as usize][..3].to_string(),
            b'A' => DAYS[tm.wday as usize].to_string(),
            b'b' | b'h' => MONTHS[(tm.mon - 1) as usize][..3].to_string(),
            b'B' => MONTHS[(tm.mon - 1) as usize].to_string(),
            b'c' => format!(
                "{} {} {:2} {:02}:{:02}:{:02} {}",
                &DAYS[tm.wday as usize][..3],
                &MONTHS[(tm.mon - 1) as usize][..3],
                tm.mday,
                tm.hour,
                tm.min,
                tm.sec,
                tm.year
            ),
            b'C' => format!("{:02}", tm.year.div_euclid(100)),
            b'd' => format!("{:02}", tm.mday),
            b'D' => format!(
                "{:02}/{:02}/{:02}",
                tm.mon,
                tm.mday,
                tm.year.rem_euclid(100)
            ),
            b'e' => format!("{:2}", tm.mday),
            b'F' => format!("{}-{:02}-{:02}", tm.year, tm.mon, tm.mday),
            b'g' => format!("{:02}", iso().0.rem_euclid(100)),
            b'G' => iso().0.to_string(),
            b'H' => format!("{:02}", tm.hour),
            b'I' => format!("{h12:02}"),
            b'j' => format!("{:03}", tm.yday + 1),
            b'm' => format!("{:02}", tm.mon),
            b'M' => format!("{:02}", tm.min),
            b'n' => "\n".into(),
            b'p' => if tm.hour < 12 { "AM" } else { "PM" }.into(),
            b'r' => format!(
                "{h12:02}:{:02}:{:02} {}",
                tm.min,
                tm.sec,
                if tm.hour < 12 { "AM" } else { "PM" }
            ),
            b'R' => format!("{:02}:{:02}", tm.hour, tm.min),
            b'S' => format!("{:02}", tm.sec),
            b't' => "\t".into(),
            b'T' => format!("{:02}:{:02}:{:02}", tm.hour, tm.min, tm.sec),
            b'u' => (if tm.wday == 0 { 7 } else { tm.wday }).to_string(),
            b'U' => format!("{:02}", (tm.yday + 7 - tm.wday) / 7),
            b'V' => format!("{:02}", iso().1),
            b'w' => tm.wday.to_string(),
            b'W' => format!("{:02}", (tm.yday + 7 - (tm.wday + 6) % 7) / 7),
            b'x' => format!(
                "{:02}/{:02}/{:02}",
                tm.mon,
                tm.mday,
                tm.year.rem_euclid(100)
            ),
            b'X' => format!("{:02}:{:02}:{:02}", tm.hour, tm.min, tm.sec),
            b'y' => format!("{:02}", tm.year.rem_euclid(100)),
            b'Y' => tm.year.to_string(),
            b'z' => "+0000".into(),
            b'Z' => "GMT".into(),
            b'%' => "%".into(),
            other => return Err(unsupported(other)),
        };
        out.extend_from_slice(s.as_bytes());
    }
    Ok(Some(out))
}

/// `strptime` in the C locale, then `timegm`, as `parsetime` reads a time:
/// a field the format does not set is 0, so a missing day is the last of
/// the month before, as `timegm` normalizes it. `None` when `s` does not
/// match; what follows the match is ignored.
fn strptime(s: &[u8], fmt: &[u8]) -> Result<Option<i64>, String> {
    let (mut year, mut mon, mut mday, mut hour, mut min, mut sec) = (1900i64, 0i64, 0i64, 0, 0, 0);
    let mut pm: Option<bool> = None;
    // glibc applies `%p` to an hour read by `%I` only.
    let mut have_i = false;
    let mut century: Option<i64> = None;
    let mut yy: Option<i64> = None;
    let mut yday: Option<i64> = None;
    let mut epoch: Option<i64> = None;
    let mut at = 0usize;
    let skip_ws = |at: &mut usize| {
        while s.get(*at).is_some_and(u8::is_ascii_whitespace) {
            *at += 1;
        }
    };
    // Up to `max` digits, after any spaces, as glibc reads a number.
    let digits = |at: &mut usize, max: usize, signed: bool| -> Option<i64> {
        skip_ws(at);
        let mut neg = false;
        if signed && matches!(s.get(*at), Some(b'+' | b'-')) {
            neg = s[*at] == b'-';
            *at += 1;
        }
        let start = *at;
        while *at - start < max && s.get(*at).is_some_and(u8::is_ascii_digit) {
            *at += 1;
        }
        if *at == start {
            return None;
        }
        let n: i64 = std::str::from_utf8(&s[start..*at]).ok()?.parse().ok()?;
        Some(if neg { -n } else { n })
    };
    let name = |at: &mut usize, names: &[&str]| -> Option<i64> {
        skip_ws(at);
        let rest = &s[*at..];
        for (i, n) in names.iter().enumerate() {
            for cand in [n.as_bytes(), &n.as_bytes()[..3]] {
                if rest.len() >= cand.len() && rest[..cand.len()].eq_ignore_ascii_case(cand) {
                    *at += cand.len();
                    return Some(i as i64);
                }
            }
        }
        None
    };
    // The format, with the composite specifiers spelled out.
    let mut f = Vec::new();
    let mut i = 0;
    while i < fmt.len() {
        if fmt[i] == b'%' && i + 1 < fmt.len() {
            let exp: &[u8] = match fmt[i + 1] {
                b'D' | b'x' => b"%m/%d/%y",
                b'F' => b"%Y-%m-%d",
                b'R' => b"%H:%M",
                b'T' | b'X' => b"%H:%M:%S",
                b'r' => b"%I:%M:%S %p",
                b'c' => b"%a %b %e %H:%M:%S %Y",
                _ => &fmt[i..i + 2],
            };
            f.extend_from_slice(exp);
            i += 2;
        } else {
            f.push(fmt[i]);
            i += 1;
        }
    }
    let mut i = 0;
    while i < f.len() {
        let c = f[i];
        if c.is_ascii_whitespace() {
            skip_ws(&mut at);
            i += 1;
            continue;
        }
        if c != b'%' || i + 1 >= f.len() {
            if s.get(at) != Some(&c) {
                return Ok(None);
            }
            at += 1;
            i += 1;
            continue;
        }
        let spec = f[i + 1];
        i += 2;
        let ok = match spec {
            b'%' => {
                skip_ws(&mut at);
                let hit = s.get(at) == Some(&b'%');
                at += 1;
                hit
            }
            b'n' | b't' => {
                skip_ws(&mut at);
                true
            }
            b'a' | b'A' => name(&mut at, &DAYS).is_some(),
            b'b' | b'B' | b'h' => name(&mut at, &MONTHS).map(|m| mon = m).is_some(),
            b'C' => digits(&mut at, 2, false)
                .map(|n| century = Some(n))
                .is_some(),
            b'd' | b'e' => digits(&mut at, 2, false)
                .filter(|n| (1..=31).contains(n))
                .map(|n| mday = n)
                .is_some(),
            b'H' => digits(&mut at, 2, false)
                .filter(|n| *n <= 23)
                .map(|n| hour = n)
                .is_some(),
            b'I' => digits(&mut at, 2, false)
                .filter(|n| (1..=12).contains(n))
                .map(|n| {
                    hour = n % 12;
                    have_i = true;
                })
                .is_some(),
            b'j' => digits(&mut at, 3, false)
                .filter(|n| (1..=366).contains(n))
                .map(|n| yday = Some(n - 1))
                .is_some(),
            b'm' => digits(&mut at, 2, false)
                .filter(|n| (1..=12).contains(n))
                .map(|n| mon = n - 1)
                .is_some(),
            b'M' => digits(&mut at, 2, false)
                .filter(|n| *n <= 59)
                .map(|n| min = n)
                .is_some(),
            b'p' => {
                skip_ws(&mut at);
                let rest = &s[at..];
                if rest.len() >= 2 && rest[..2].eq_ignore_ascii_case(b"AM") {
                    pm = Some(false);
                    at += 2;
                    true
                } else if rest.len() >= 2 && rest[..2].eq_ignore_ascii_case(b"PM") {
                    pm = Some(true);
                    at += 2;
                    true
                } else {
                    false
                }
            }
            b'S' => digits(&mut at, 2, false)
                .filter(|n| *n <= 61)
                .map(|n| sec = n)
                .is_some(),
            b's' => digits(&mut at, 20, true).map(|n| epoch = Some(n)).is_some(),
            b'y' => digits(&mut at, 2, false).map(|n| yy = Some(n)).is_some(),
            b'Y' => digits(&mut at, 4, false).map(|n| year = n).is_some(),
            b'z' | b'Z' => {
                return Err(unsupported(spec));
            }
            other => return Err(unsupported(other)),
        };
        if !ok {
            return Ok(None);
        }
    }
    if let Some(t) = epoch {
        return Ok(Some(t));
    }
    if let Some(y) = yy {
        year = match century {
            Some(c) => c * 100 + y,
            None if y < 69 => 2000 + y,
            None => 1900 + y,
        };
    } else if let Some(c) = century {
        year = c * 100;
    }
    if have_i && pm == Some(true) {
        hour += 12;
    }
    let days = match yday {
        Some(d) if mday == 0 && mon == 0 => days_from_civil(year, 1, 1) + d,
        _ => days_from_civil(year, mon + 1, mday),
    };
    Ok(Some(days * 86_400 + hour * 3600 + min * 60 + sec))
}

#[cfg(test)]
mod tests;
