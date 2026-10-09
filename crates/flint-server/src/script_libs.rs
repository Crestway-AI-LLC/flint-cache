// SPDX-License-Identifier: Elastic-2.0
//! The Lua libraries Redis gives scripts beside the standard ones (ADR-0052
//! D3): `cjson`, `cmsgpack`, `bit` and `struct`, as Valkey 9.1 bundles them
//! (lua-cjson 2.1, lua-cmsgpack, LuaBitOp and lua-struct), byte for byte.
//! BullMQ's scripts unpack their options with `cmsgpack` and keep job data
//! as `cjson`.
//!
//! Each function is Rust behind a small C function, the trampoline, which is
//! what a script calls. It does what only a C function can, as the
//! libraries' own C does:
//!
//! - raise an error after the caller's position (`luaL_where(L, 1)`), which
//!   a Lua wrapper loses when a script tail-calls it (`return
//!   cjson.decode(s)`), and name the function as the script called it
//!   (`luaL_argerror`);
//! - take any number of arguments and give any number of results through one
//!   table. Rust holds an mlua reference for each Lua string or table in
//!   hand, and mlua has fewer than 8,000 before it panics, where Valkey
//!   answers.
//!
//! The nested formats are walked with stacks of their own, not recursion:
//! Valkey decodes MessagePack 3,999 deep, past what a connection thread's
//! stack holds. The Lua stack slots Valkey's C would use are counted, so a
//! call fails at the depth and count Valkey's does, with its message.

use std::ffi::c_int;

use mlua::{Function, Lua, MultiValue, Table, Value as LuaValue, ffi};

/// Lua 5.1's `LUAI_MAXCSTACK`: the slots a C function may use before
/// `luaL_checkstack` raises "stack overflow".
const MAX_C_STACK: usize = 8000;

/// What a library function answers.
enum Reply {
    /// A few results, returned as they are.
    Values(Vec<LuaValue>),
    /// Results in a table, for a function that can answer thousands
    /// (`cmsgpack.unpack`, `struct.unpack`): the trampoline puts them on
    /// the Lua stack.
    Spread(Table, usize),
}

/// A library error, raised as its C original raises it.
enum Fail {
    /// `luaL_error`: the message after the caller's position.
    Where(String),
    /// `luaL_argerror(L, n, message)`.
    Arg(usize, String),
    /// An error Lua raises inside a C function, as `lua_settable` raises
    /// "table index is nil": no position.
    Bare(String),
    /// mlua's own (out of memory), raised as it is.
    Lua(mlua::Error),
}

impl From<String> for Fail {
    fn from(s: String) -> Self {
        Fail::Where(s)
    }
}

impl From<&str> for Fail {
    fn from(s: &str) -> Self {
        Fail::Where(s.into())
    }
}

impl From<mlua::Error> for Fail {
    fn from(e: mlua::Error) -> Self {
        Fail::Lua(e)
    }
}

type Answer = Result<Reply, Fail>;

fn one(v: LuaValue) -> Answer {
    Ok(Reply::Values(vec![v]))
}

/// A call's arguments, which the trampoline packs into a table.
struct Args {
    t: Table,
    n: usize,
}

impl Args {
    /// Argument `i`, from 1; `None` past the last (`lua_isnone`).
    fn get(&self, i: usize) -> Result<Option<LuaValue>, Fail> {
        if i == 0 || i > self.n {
            return Ok(None);
        }
        Ok(Some(self.t.raw_get(i)?))
    }
}

// How an answer reaches the trampoline: a code, then what it says.
/// The results follow.
const VALUES: i64 = 0;
/// A table follows, then how many results it holds.
const SPREAD: i64 = 1;
/// A message follows, to raise after the caller's position.
const RAISE_WHERE: i64 = 2;
/// An argument's number follows, then a message, for `luaL_argerror`.
const RAISE_ARG: i64 = 3;
/// A message follows, to raise as it is.
const RAISE_BARE: i64 = 4;

fn answer(lua: &Lua, a: Answer) -> mlua::Result<MultiValue> {
    let code = |c: i64| LuaValue::Number(c as f64);
    let mut out = MultiValue::new();
    match a {
        Ok(Reply::Values(v)) => {
            out.push_back(code(VALUES));
            out.extend(v);
        }
        Ok(Reply::Spread(t, n)) => {
            out.push_back(code(SPREAD));
            out.push_back(LuaValue::Table(t));
            out.push_back(LuaValue::Number(n as f64));
        }
        Err(Fail::Where(m)) => {
            out.push_back(code(RAISE_WHERE));
            out.push_back(LuaValue::String(lua.create_string(&m)?));
        }
        Err(Fail::Arg(n, m)) => {
            out.push_back(code(RAISE_ARG));
            out.push_back(LuaValue::Number(n as f64));
            out.push_back(LuaValue::String(lua.create_string(&m)?));
        }
        Err(Fail::Bare(m)) => {
            out.push_back(code(RAISE_BARE));
            out.push_back(LuaValue::String(lua.create_string(&m)?));
        }
        Err(Fail::Lua(e)) => return Err(e),
    }
    Ok(out)
}

/// Packs a C function's arguments into a table and calls the Rust function
/// (upvalue 1) with it and their count, leaving its results on the stack.
///
/// This and the C functions below keep no Rust value with a destructor in
/// their frames, so the errors Lua raises through them (a `longjmp`) skip
/// nothing.
unsafe extern "C-unwind" fn call_packed(state: *mut ffi::lua_State) {
    unsafe {
        let n = ffi::lua_gettop(state);
        ffi::lua_createtable(state, n, 0);
        ffi::lua_insert(state, 1);
        for i in (1..=n).rev() {
            ffi::lua_rawseti(state, 1, i as ffi::lua_Integer);
        }
        ffi::lua_pushvalue(state, ffi::lua_upvalueindex(1));
        ffi::lua_insert(state, 1);
        ffi::lua_pushinteger(state, n as ffi::lua_Integer);
        // function, arguments, count
        ffi::lua_call(state, 2, ffi::LUA_MULTRET);
    }
}

/// A function that takes any number of arguments (`redis.call`): it
/// returns what the Rust function returns.
unsafe extern "C-unwind" fn packing(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        call_packed(state);
        ffi::lua_gettop(state)
    }
}

/// What a script calls for a library function: it does what the answer's
/// code says.
unsafe extern "C-unwind" fn trampoline(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        call_packed(state);
        match ffi::lua_tointeger(state, 1) as i64 {
            VALUES => ffi::lua_gettop(state) - 1,
            SPREAD => {
                let count = ffi::lua_tointeger(state, 3) as c_int;
                ffi::lua_settop(state, 2);
                ffi::lua_replace(state, 1);
                // Lua 5.1's own check: mlua's `luaL_checkstack` is 5.3's,
                // which asks for 20 slots more.
                ffi::luaL_checkstack_(state, count, c"too many results".as_ptr());
                for i in 1..=count {
                    ffi::lua_rawgeti(state, 1, i as ffi::lua_Integer);
                }
                count
            }
            RAISE_WHERE => {
                ffi::luaL_where(state, 1);
                ffi::lua_pushvalue(state, 2);
                ffi::lua_concat(state, 2);
                ffi::lua_error(state)
            }
            RAISE_ARG => {
                let narg = ffi::lua_tointeger(state, 2) as c_int;
                ffi::luaL_argerror(
                    state,
                    narg,
                    ffi::lua_tolstring(state, 3, std::ptr::null_mut()),
                )
            }
            _ => {
                ffi::lua_pushvalue(state, 2);
                ffi::lua_error(state)
            }
        }
    }
}

/// `inner`, which takes a table of arguments and their count, behind the C
/// function `outer`.
fn c_closure(lua: &Lua, inner: Function, outer: ffi::lua_CFunction) -> mlua::Result<Function> {
    // SAFETY: the closure pushes one C closure over the one value on the
    // stack, the function it calls.
    unsafe {
        lua.exec_raw::<Function>(inner, |state| {
            ffi::lua_pushcclosure(state, outer, 1);
        })
    }
}

/// A library function: `f` behind the trampoline.
fn lib_fn<F>(lua: &Lua, f: F) -> mlua::Result<Function>
where
    F: Fn(&Lua, &Args) -> Answer + 'static,
{
    let inner = lua
        .create_function(move |lua, (t, n): (Table, usize)| answer(lua, f(lua, &Args { t, n })))?;
    c_closure(lua, inner, trampoline)
}

/// `inner`, which takes a table of arguments and their count, as a function
/// that takes them as they are: Rust then holds one reference for them, not
/// one each, of which mlua has fewer than 8,000.
pub(crate) fn packed(lua: &Lua, inner: Function) -> mlua::Result<Function> {
    c_closure(lua, inner, packing)
}

/// Install the functions as `__flint_libs` and build the libraries from
/// them; the prelude then makes them read-only.
pub fn install(lua: &Lua) -> mlua::Result<()> {
    let raw = lua.create_table()?;
    let add = |name: &str, f: Function| raw.raw_set(name, f);
    // lua-cjson walks an object with `lua_next`.
    let next: Function = lua.globals().raw_get("next")?;
    add(
        "cjson_encode",
        lib_fn(lua, move |lua, a| cjson_encode(lua, a, &next))?,
    )?;
    add("cjson_decode", lib_fn(lua, cjson_decode)?)?;
    add("cmsgpack_pack", lib_fn(lua, cmsgpack_pack)?)?;
    add(
        "cmsgpack_unpack",
        lib_fn(lua, |lua, a| cmsgpack_unpack(lua, a, Unpack::All))?,
    )?;
    add(
        "cmsgpack_unpack_one",
        lib_fn(lua, |lua, a| cmsgpack_unpack(lua, a, Unpack::One))?,
    )?;
    add(
        "cmsgpack_unpack_limit",
        lib_fn(lua, |lua, a| cmsgpack_unpack(lua, a, Unpack::Limit))?,
    )?;
    add("bit_band", lib_fn(lua, |_, a| bit_fold(a, BitOp::And))?)?;
    add("bit_bor", lib_fn(lua, |_, a| bit_fold(a, BitOp::Or))?)?;
    add("bit_bxor", lib_fn(lua, |_, a| bit_fold(a, BitOp::Xor))?)?;
    add("bit_tobit", lib_fn(lua, |_, a| bit_unary(a, |x| x))?)?;
    add("bit_bnot", lib_fn(lua, |_, a| bit_unary(a, |x| !x))?)?;
    add(
        "bit_bswap",
        lib_fn(lua, |_, a| bit_unary(a, |x| x.swap_bytes()))?,
    )?;
    add(
        "bit_lshift",
        lib_fn(lua, |_, a| bit_shift(a, |x, n| x.wrapping_shl(n)))?,
    )?;
    add(
        "bit_rshift",
        lib_fn(lua, |_, a| bit_shift(a, |x, n| ((x as u32) >> n) as i32))?,
    )?;
    add(
        "bit_arshift",
        lib_fn(lua, |_, a| bit_shift(a, |x, n| x >> n))?,
    )?;
    add(
        "bit_rol",
        lib_fn(lua, |_, a| {
            bit_shift(a, |x, n| (x as u32).rotate_left(n) as i32)
        })?,
    )?;
    add(
        "bit_ror",
        lib_fn(lua, |_, a| {
            bit_shift(a, |x, n| (x as u32).rotate_right(n) as i32)
        })?,
    )?;
    add("bit_tohex", lib_fn(lua, bit_tohex)?)?;
    add("struct_pack", lib_fn(lua, struct_pack)?)?;
    add("struct_unpack", lib_fn(lua, struct_unpack)?)?;
    add("struct_size", lib_fn(lua, struct_size)?)?;
    raw.raw_set(
        "null",
        LuaValue::LightUserData(mlua::LightUserData(std::ptr::null_mut())),
    )?;
    lua.globals().raw_set("__flint_libs", raw)?;
    lua.load(LIBS_PRELUDE)
        .set_name("=flint-libs")
        .call::<()>(())?;
    Ok(())
}

/// Builds the four libraries from the functions, then drops their table
/// from the globals.
const LIBS_PRELUDE: &str = r##"
local raw = __flint_libs
rawset(_G, "__flint_libs", nil)
local error, select, unpack = error, select, unpack
-- The configuration functions answer lua-cjson's defaults; Flint does not
-- let a script change them, as they would outlive it on a shared state.
local function setting(...)
  local defaults = { ... }
  return function(...)
    if select("#", ...) > 0 then
      error("Flint's cjson settings are fixed (ADR-0052)", 2)
    end
    return unpack(defaults)
  end
end
cjson = {
  encode = raw.cjson_encode,
  decode = raw.cjson_decode,
  null = raw.null,
  _NAME = "cjson",
  _VERSION = "2.1.0",
  encode_sparse_array = setting(false, 2, 10),
  encode_max_depth = setting(1000),
  decode_max_depth = setting(1000),
  encode_number_precision = setting(14),
  encode_keep_buffer = setting(true),
  encode_invalid_numbers = setting(false),
  decode_invalid_numbers = setting(true),
}
cmsgpack = {
  pack = raw.cmsgpack_pack,
  unpack = raw.cmsgpack_unpack,
  unpack_one = raw.cmsgpack_unpack_one,
  unpack_limit = raw.cmsgpack_unpack_limit,
  _NAME = "cmsgpack",
  _VERSION = "lua-cmsgpack 0.4.0",
}
bit = {}
for _, name in ipairs({ "tobit", "bnot", "band", "bor", "bxor", "lshift", "rshift",
                        "arshift", "rol", "ror", "bswap", "tohex" }) do
  bit[name] = raw["bit_" .. name]
end
struct = {
  pack = raw.struct_pack,
  unpack = raw.struct_unpack,
  size = raw.struct_size,
}
"##;

/// `lua_tonumber` of a string: a number C's `strtod` reads whole, spaces
/// around it allowed.
fn str_num(b: &[u8]) -> Option<f64> {
    strtod_full(b.trim_ascii())
}

fn num(v: &LuaValue) -> Option<f64> {
    match v {
        LuaValue::Number(n) => Some(*n),
        LuaValue::Integer(i) => Some(*i as f64),
        // luaL_checknumber takes a string that reads as a number.
        LuaValue::String(s) => str_num(&s.as_bytes()),
        _ => None,
    }
}

/// `lua_typename`, for the libraries' error messages.
fn type_name(v: &LuaValue) -> &'static str {
    match v {
        LuaValue::Nil => "nil",
        LuaValue::Boolean(_) => "boolean",
        LuaValue::LightUserData(_) | LuaValue::UserData(_) => "userdata",
        LuaValue::Number(_) | LuaValue::Integer(_) => "number",
        LuaValue::String(_) => "string",
        LuaValue::Table(_) => "table",
        LuaValue::Function(_) => "function",
        LuaValue::Thread(_) => "thread",
        _ => "userdata",
    }
}

/// `luaL_argerror`: the trampoline names the function as it was called.
fn bad_arg(n: usize, msg: &str) -> Fail {
    Fail::Arg(n, msg.into())
}

fn check_number(args: &Args, n: usize) -> Result<f64, Fail> {
    match args.get(n)? {
        None => Err(bad_arg(n, "number expected, got no value")),
        Some(v) => {
            num(&v).ok_or_else(|| bad_arg(n, &format!("number expected, got {}", type_name(&v))))
        }
    }
}

fn check_string(args: &Args, n: usize) -> Result<Vec<u8>, Fail> {
    match args.get(n)? {
        Some(LuaValue::String(s)) => Ok(s.as_bytes().to_vec()),
        Some(v @ (LuaValue::Number(_) | LuaValue::Integer(_))) => {
            Ok(fmt_g14(num(&v).unwrap_or(0.0)).into_bytes())
        }
        None => Err(bad_arg(n, "string expected, got no value")),
        Some(v) => Err(bad_arg(
            n,
            &format!("string expected, got {}", type_name(&v)),
        )),
    }
}

/// C's `%.14g`, which Lua 5.1 and lua-cjson spell numbers with.
pub(crate) fn fmt_g14(x: f64) -> String {
    fmt_g(x, 14)
}

fn fmt_g(x: f64, p: usize) -> String {
    if x.is_nan() {
        return if x.is_sign_negative() { "-nan" } else { "nan" }.into();
    }
    if x.is_infinite() {
        return if x < 0.0 { "-inf" } else { "inf" }.into();
    }
    if x == 0.0 {
        return if x.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let sci = format!("{:.*e}", p - 1, x);
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if exp < -4 || exp >= p as i32 {
        let mant = trim_zeros(mant);
        format!("{mant}e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs())
    } else {
        let decimals = (p as i32 - 1 - exp).max(0) as usize;
        trim_zeros(&format!("{x:.decimals$}")).to_string()
    }
}

fn trim_zeros(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}

/// C's `strtod` over the longest prefix it accepts: answers the value and
/// how many bytes it read (0: none).
fn strtod(s: &[u8]) -> (f64, usize) {
    let mut i = 0;
    let neg = match s.first() {
        Some(b'-') => {
            i = 1;
            true
        }
        Some(b'+') => {
            i = 1;
            false
        }
        _ => false,
    };
    let sign = |v: f64| if neg { -v } else { v };
    let rest = &s[i..];
    let lower: Vec<u8> = rest.iter().take(8).map(u8::to_ascii_lowercase).collect();
    if lower.starts_with(b"infinity") {
        return (sign(f64::INFINITY), i + 8);
    }
    if lower.starts_with(b"inf") {
        return (sign(f64::INFINITY), i + 3);
    }
    if lower.starts_with(b"nan") {
        let mut j = i + 3;
        if s.get(j) == Some(&b'(')
            && let Some(close) = s[j..].iter().position(|&c| c == b')')
            && s[j + 1..j + close]
                .iter()
                .all(|c| c.is_ascii_alphanumeric() || *c == b'_')
        {
            j += close + 1;
        }
        return (f64::NAN, j);
    }
    if lower.starts_with(b"0x") {
        let mut j = i + 2;
        let mut mant = 0f64;
        let mut digits = 0;
        let mut exp: i32 = 0;
        while let Some(d) = s.get(j).and_then(|c| (*c as char).to_digit(16)) {
            mant = mant * 16.0 + d as f64;
            digits += 1;
            j += 1;
        }
        if s.get(j) == Some(&b'.') {
            let mut k = j + 1;
            while let Some(d) = s.get(k).and_then(|c| (*c as char).to_digit(16)) {
                mant = mant * 16.0 + d as f64;
                exp -= 4;
                digits += 1;
                k += 1;
            }
            if digits > 0 {
                j = k;
            }
        }
        if digits == 0 {
            // "0x" with no digits: strtod reads the "0".
            return (sign(0.0), i + 1);
        }
        if matches!(s.get(j), Some(b'p' | b'P')) {
            let mut k = j + 1;
            let eneg = match s.get(k) {
                Some(b'-') => {
                    k += 1;
                    true
                }
                Some(b'+') => {
                    k += 1;
                    false
                }
                _ => false,
            };
            let start = k;
            let mut e: i32 = 0;
            while let Some(d) = s.get(k).filter(|c| c.is_ascii_digit()) {
                e = e.saturating_mul(10).saturating_add((d - b'0') as i32);
                k += 1;
            }
            if k > start {
                exp = exp.saturating_add(if eneg { -e } else { e });
                j = k;
            }
        }
        return (sign(mant * 2f64.powi(exp)), j);
    }
    let mut j = i;
    let int_start = j;
    while s.get(j).is_some_and(u8::is_ascii_digit) {
        j += 1;
    }
    let mut digits = j - int_start;
    if s.get(j) == Some(&b'.') {
        let mut k = j + 1;
        while s.get(k).is_some_and(u8::is_ascii_digit) {
            k += 1;
        }
        digits += k - j - 1;
        if digits > 0 {
            j = k;
        }
    }
    if digits == 0 {
        return (0.0, 0);
    }
    if matches!(s.get(j), Some(b'e' | b'E')) {
        let mut k = j + 1;
        if matches!(s.get(k), Some(b'-' | b'+')) {
            k += 1;
        }
        let start = k;
        while s.get(k).is_some_and(u8::is_ascii_digit) {
            k += 1;
        }
        if k > start {
            j = k;
        }
    }
    let text = std::str::from_utf8(&s[i..j]).unwrap_or("0");
    let text = if text.starts_with('.') {
        format!("0{text}")
    } else if text.ends_with('.') || text.contains(".e") || text.contains(".E") {
        text.replacen('.', ".0", 1)
    } else {
        text.to_string()
    };
    (sign(text.parse::<f64>().unwrap_or(0.0)), j)
}

/// `strtod` that must read the whole of `s`.
fn strtod_full(s: &[u8]) -> Option<f64> {
    let (v, used) = strtod(s);
    (used > 0 && used == s.len()).then_some(v)
}

// ---------------------------------------------------------------- cjson ----

const ENCODE_MAX_DEPTH: usize = 1000;
const DECODE_MAX_DEPTH: usize = 1000;

fn cjson_encode(lua: &Lua, args: &Args, next: &Function) -> Answer {
    if args.n != 1 {
        return Err(bad_arg(1, "expected 1 argument"));
    }
    let root = args.get(1)?.unwrap_or(LuaValue::Nil);
    let out = json_encode(root, next)?;
    one(LuaValue::String(lua.create_string(&out)?))
}

fn encode_exception(v: &LuaValue, reason: &str) -> String {
    format!("Cannot serialise {}: {reason}", type_name(v))
}

/// A table being written: an array by index, an object by `next`.
enum Writing {
    Array { t: Table, len: usize, at: usize },
    Object { t: Table, key: LuaValue },
}

/// lua-cjson's `json_append_data`, with the tables open at once on a stack
/// of their own: at most 1,000, each an mlua reference with its key's.
fn json_encode(root: LuaValue, next: &Function) -> Result<Vec<u8>, Fail> {
    let mut out = Vec::new();
    let mut open: Vec<Writing> = Vec::new();
    let mut value = Some(root);
    loop {
        match value.take() {
            Some(LuaValue::Table(t)) => {
                let depth = open.len() + 1;
                if depth > ENCODE_MAX_DEPTH {
                    return Err(format!("Cannot serialise, excessive nesting ({depth})").into());
                }
                match array_length(&t)? {
                    Some(len) if len > 0 => {
                        out.push(b'[');
                        open.push(Writing::Array { t, len, at: 0 });
                    }
                    _ => {
                        out.push(b'{');
                        open.push(Writing::Object {
                            t,
                            key: LuaValue::Nil,
                        });
                    }
                }
            }
            Some(v) => json_scalar(&v, &mut out)?,
            None => {}
        }
        let Some(top) = open.last_mut() else {
            return Ok(out);
        };
        match top {
            Writing::Array { t, len, at } => {
                if *at == *len {
                    out.push(b']');
                    open.pop();
                    continue;
                }
                if *at > 0 {
                    out.push(b',');
                }
                *at += 1;
                value = Some(t.raw_get(*at)?);
            }
            Writing::Object { t, key } => {
                let (k, v): (LuaValue, LuaValue) = next.call((t.clone(), key.clone()))?;
                if k.is_nil() {
                    out.push(b'}');
                    open.pop();
                    continue;
                }
                if !key.is_nil() {
                    out.push(b',');
                }
                match &k {
                    LuaValue::String(s) => json_string(&s.as_bytes(), &mut out),
                    LuaValue::Number(_) | LuaValue::Integer(_) => {
                        out.push(b'"');
                        json_number(&k, &mut out)?;
                        out.push(b'"');
                    }
                    other => {
                        return Err(encode_exception(
                            other,
                            "table key must be a number or string",
                        )
                        .into());
                    }
                }
                out.push(b':');
                *key = k;
                value = Some(v);
            }
        }
    }
}

fn json_scalar(v: &LuaValue, out: &mut Vec<u8>) -> Result<(), Fail> {
    match v {
        LuaValue::String(s) => json_string(&s.as_bytes(), out),
        LuaValue::Number(_) | LuaValue::Integer(_) => json_number(v, out)?,
        LuaValue::Boolean(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        LuaValue::Nil => out.extend_from_slice(b"null"),
        LuaValue::LightUserData(p) if p.0.is_null() => out.extend_from_slice(b"null"),
        other => return Err(encode_exception(other, "type not supported").into()),
    }
    Ok(())
}

fn json_number(v: &LuaValue, out: &mut Vec<u8>) -> Result<(), Fail> {
    let n = num(v).unwrap_or(0.0);
    if !n.is_finite() {
        return Err(encode_exception(v, "must not be NaN or Inf").into());
    }
    out.extend_from_slice(fmt_g14(n).as_bytes());
    Ok(())
}

/// lua-cjson's `lua_array_length`: the largest index when every key is a
/// whole number of at least 1 and the table is not excessively sparse;
/// `None` for an object.
fn array_length(t: &Table) -> Result<Option<usize>, Fail> {
    let (mut max, mut items) = (0f64, 0f64);
    for pair in t.pairs::<LuaValue, LuaValue>() {
        let (k, _) = pair?;
        match num_key(&k) {
            Some(k) if k != 0.0 && k.floor() == k && k >= 1.0 => {
                max = max.max(k);
                items += 1.0;
            }
            _ => return Ok(None),
        }
    }
    if max > items * 2.0 && max > 10.0 {
        return Err("Cannot serialise table: excessively sparse array".into());
    }
    Ok(Some(max as usize))
}

fn num_key(k: &LuaValue) -> Option<f64> {
    match k {
        LuaValue::Number(n) => Some(*n),
        LuaValue::Integer(i) => Some(*i as f64),
        _ => None,
    }
}

fn json_string(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'/' => out.extend_from_slice(b"\\/"),
            0x08 => out.extend_from_slice(b"\\b"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'\r' => out.extend_from_slice(b"\\r"),
            0..=0x1f | 0x7f => out.extend_from_slice(format!("\\u{c:04x}").as_bytes()),
            _ => out.push(c),
        }
    }
    out.push(b'"');
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Tok {
    ObjBegin,
    ObjEnd,
    ArrBegin,
    ArrEnd,
    Str,
    Num,
    Bool,
    Null,
    Colon,
    Comma,
    End,
    Error,
}

impl Tok {
    fn name(self) -> &'static str {
        match self {
            Tok::ObjBegin => "T_OBJ_BEGIN",
            Tok::ObjEnd => "T_OBJ_END",
            Tok::ArrBegin => "T_ARR_BEGIN",
            Tok::ArrEnd => "T_ARR_END",
            Tok::Str => "T_STRING",
            Tok::Num => "T_NUMBER",
            Tok::Bool => "T_BOOLEAN",
            Tok::Null => "T_NULL",
            Tok::Colon => "T_COLON",
            Tok::Comma => "T_COMMA",
            Tok::End => "T_END",
            Tok::Error => "T_ERROR",
        }
    }
}

struct Token {
    kind: Tok,
    index: usize,
    string: Vec<u8>,
    number: f64,
    boolean: bool,
    error: &'static str,
}

struct Json<'a> {
    data: &'a [u8],
    ptr: usize,
}

impl Json<'_> {
    /// The byte at `i`, or 0 past the end: lua-cjson reads a C string.
    fn at(&self, i: usize) -> u8 {
        self.data.get(i).copied().unwrap_or(0)
    }

    fn next(&mut self) -> Token {
        while matches!(self.at(self.ptr), b' ' | b'\t' | b'\n' | b'\r') {
            self.ptr += 1;
        }
        let mut t = Token {
            kind: Tok::Error,
            index: self.ptr,
            string: Vec::new(),
            number: 0.0,
            boolean: false,
            error: "",
        };
        let ch = self.at(self.ptr);
        let single = match ch {
            0 => Some(Tok::End),
            b'{' => Some(Tok::ObjBegin),
            b'}' => Some(Tok::ObjEnd),
            b'[' => Some(Tok::ArrBegin),
            b']' => Some(Tok::ArrEnd),
            b':' => Some(Tok::Colon),
            b',' => Some(Tok::Comma),
            _ => None,
        };
        if let Some(kind) = single {
            t.kind = kind;
            if kind != Tok::End {
                self.ptr += 1;
            }
            return t;
        }
        let rest = &self.data[self.ptr.min(self.data.len())..];
        if ch == b'"' {
            self.string(&mut t);
        } else if ch == b'-' || ch.is_ascii_digit() {
            self.number(&mut t);
        } else if rest.starts_with(b"true") {
            t.kind = Tok::Bool;
            t.boolean = true;
            self.ptr += 4;
        } else if rest.starts_with(b"false") {
            t.kind = Tok::Bool;
            self.ptr += 5;
        } else if rest.starts_with(b"null") {
            t.kind = Tok::Null;
            self.ptr += 4;
        } else if self.invalid_number() {
            self.number(&mut t);
        } else {
            self.fail(&mut t, "invalid token");
        }
        t
    }

    fn fail(&self, t: &mut Token, msg: &'static str) {
        t.kind = Tok::Error;
        t.error = msg;
        t.index = self.ptr;
    }

    /// lua-cjson's `json_is_invalid_number`: what strtod reads but JSON does
    /// not allow, which this build decodes anyway (decode_invalid_numbers).
    fn invalid_number(&self) -> bool {
        let mut p = self.ptr;
        if self.at(p) == b'+' {
            return true;
        }
        if self.at(p) == b'-' {
            p += 1;
        }
        if self.at(p) == b'0' {
            let c2 = self.at(p + 1);
            return (c2 | 0x20) == b'x' || c2.is_ascii_digit();
        } else if self.at(p) <= b'9' {
            return false;
        }
        let word: Vec<u8> = (0..3)
            .map(|i| self.at(p + i).to_ascii_lowercase())
            .collect();
        word == b"inf" || word == b"nan"
    }

    fn number(&mut self, t: &mut Token) {
        let end = self
            .data
            .iter()
            .skip(self.ptr)
            .position(|&c| c == 0)
            .map_or(self.data.len(), |n| self.ptr + n);
        let (v, used) = strtod(&self.data[self.ptr..end]);
        if used == 0 {
            self.fail(t, "invalid number");
            return;
        }
        t.kind = Tok::Num;
        t.number = v;
        self.ptr += used;
    }

    fn string(&mut self, t: &mut Token) {
        self.ptr += 1;
        let mut out = Vec::new();
        loop {
            let ch = self.at(self.ptr);
            if ch == b'"' {
                break;
            }
            if ch == 0 {
                self.fail(t, "unexpected end of string");
                return;
            }
            if ch == b'\\' {
                let esc = self.at(self.ptr + 1);
                let mapped = match esc {
                    b'"' => b'"',
                    b'\\' => b'\\',
                    b'/' => b'/',
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'u' => {
                        if self.unicode_escape(&mut out) {
                            continue;
                        }
                        self.fail(t, "invalid unicode escape code");
                        return;
                    }
                    _ => {
                        self.fail(t, "invalid escape code");
                        return;
                    }
                };
                self.ptr += 1;
                out.push(mapped);
            } else {
                out.push(ch);
            }
            self.ptr += 1;
        }
        self.ptr += 1;
        t.kind = Tok::Str;
        t.string = out;
    }

    fn hex4(&self, at: usize) -> Option<u32> {
        let mut v = 0;
        for i in 0..4 {
            v = v * 16 + (self.at(at + i) as char).to_digit(16)?;
        }
        Some(v)
    }

    /// `\uXXXX` at `ptr`, with a surrogate pair's second half; appends UTF-8
    /// and moves past it. False for an invalid escape.
    fn unicode_escape(&mut self, out: &mut Vec<u8>) -> bool {
        let Some(mut cp) = self.hex4(self.ptr + 2) else {
            return false;
        };
        let mut len = 6;
        if (0xDC00..=0xDFFF).contains(&cp) {
            return false;
        }
        if (0xD800..=0xDBFF).contains(&cp) {
            if self.at(self.ptr + 6) != b'\\' || self.at(self.ptr + 7) != b'u' {
                return false;
            }
            let Some(low) = self.hex4(self.ptr + 8) else {
                return false;
            };
            if !(0xDC00..=0xDFFF).contains(&low) {
                return false;
            }
            cp = (((cp & 0x3FF) << 10) | (low & 0x3FF)) + 0x10000;
            len = 12;
        }
        let mut buf = [0u8; 4];
        match char::from_u32(cp) {
            Some(c) => out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes()),
            None => return false,
        }
        self.ptr += len;
        true
    }
}

fn parse_error(expected: &str, t: &Token) -> String {
    let found = if t.kind == Tok::Error {
        t.error
    } else {
        t.kind.name()
    };
    format!(
        "Expected {expected} but found {found} at character {}",
        t.index + 1
    )
}

fn cjson_decode(lua: &Lua, args: &Args) -> Answer {
    if args.n != 1 {
        return Err(bad_arg(1, "expected 1 argument"));
    }
    let data = check_string(args, 1)?;
    // lua-cjson: only the first character is sure to be ASCII, and that is
    // enough to tell UTF-16 or UTF-32.
    if data.len() >= 2 && (data[0] == 0 || data[1] == 0) {
        return Err("JSON parser does not support UTF-16 or UTF-32".into());
    }
    let mut json = Json {
        data: &data,
        ptr: 0,
    };
    let v = json_decode(lua, &mut json)?;
    let t = json.next();
    if t.kind != Tok::End {
        return Err(parse_error("the end", &t).into());
    }
    one(v)
}

/// A container being read.
enum Reading {
    Array { t: Table, at: usize },
    Object { t: Table, key: Vec<u8> },
}

impl Reading {
    fn into_table(self) -> Table {
        match self {
            Reading::Array { t, .. } | Reading::Object { t, .. } => t,
        }
    }
}

/// lua-cjson's `json_process_value`, with the containers open at once on a
/// stack of their own: at most 1,000, each an mlua reference.
fn json_decode(lua: &Lua, json: &mut Json) -> Result<LuaValue, Fail> {
    let mut open: Vec<Reading> = Vec::new();
    let mut t = json.next();
    loop {
        // `t` starts a value.
        let mut value = match t.kind {
            Tok::Str => LuaValue::String(lua.create_string(&t.string)?),
            Tok::Num => LuaValue::Number(t.number),
            Tok::Bool => LuaValue::Boolean(t.boolean),
            Tok::Null => LuaValue::LightUserData(mlua::LightUserData(std::ptr::null_mut())),
            Tok::ObjBegin | Tok::ArrBegin => {
                let depth = open.len() + 1;
                if depth > DECODE_MAX_DEPTH {
                    return Err(format!(
                        "Found too many nested data structures ({depth}) at character {}",
                        json.ptr
                    )
                    .into());
                }
                let table = lua.create_table()?;
                let object = t.kind == Tok::ObjBegin;
                let close = if object { Tok::ObjEnd } else { Tok::ArrEnd };
                t = json.next();
                if t.kind == close {
                    LuaValue::Table(table)
                } else if object {
                    let key = object_key(json, &t)?;
                    open.push(Reading::Object { t: table, key });
                    t = json.next();
                    continue;
                } else {
                    open.push(Reading::Array { t: table, at: 1 });
                    continue;
                }
            }
            _ => return Err(parse_error("value", &t).into()),
        };
        // `value` is whole: it goes into the innermost container, which
        // then closes, and goes into its own, or goes on.
        loop {
            let object = match open.last_mut() {
                None => return Ok(value),
                Some(Reading::Array { t: table, at }) => {
                    table.raw_set(*at, value)?;
                    *at += 1;
                    false
                }
                Some(Reading::Object { t: table, key }) => {
                    table.raw_set(lua.create_string(&*key)?, value)?;
                    true
                }
            };
            let close = if object { Tok::ObjEnd } else { Tok::ArrEnd };
            t = json.next();
            if t.kind == close {
                let Some(done) = open.pop() else {
                    unreachable!("a container was open");
                };
                value = LuaValue::Table(done.into_table());
                continue;
            }
            if t.kind != Tok::Comma {
                let expected = if object {
                    "comma or object end"
                } else {
                    "comma or array end"
                };
                return Err(parse_error(expected, &t).into());
            }
            t = json.next();
            if object {
                let next_key = object_key(json, &t)?;
                if let Some(Reading::Object { key, .. }) = open.last_mut() {
                    *key = next_key;
                }
                t = json.next();
            }
            break;
        }
    }
}

/// An object's key and the colon after it; `t` is the key's token.
fn object_key(json: &mut Json, t: &Token) -> Result<Vec<u8>, Fail> {
    if t.kind != Tok::Str {
        return Err(parse_error("object key string", t).into());
    }
    let colon = json.next();
    if colon.kind != Tok::Colon {
        return Err(parse_error("colon", &colon).into());
    }
    Ok(t.string.clone())
}

// ------------------------------------------------------------- cmsgpack ----

const MSGPACK_MAX_NESTING: usize = 16;

fn cmsgpack_pack(lua: &Lua, args: &Args) -> Answer {
    if args.n == 0 {
        return Err(bad_arg(0, "MessagePack pack needs input."));
    }
    // lua-cmsgpack asks the Lua stack for room for every argument again.
    if args.n * 2 > MAX_C_STACK {
        return Err(bad_arg(0, "Too many arguments for MessagePack pack."));
    }
    let mut out = Vec::new();
    for i in 1..=args.n {
        mp_encode(&args.get(i)?.unwrap_or(LuaValue::Nil), 0, &mut out)?;
    }
    one(LuaValue::String(lua.create_string(&out)?))
}

/// lua-cmsgpack's `mp_encode_lua_type`. It recurses, but no deeper than
/// the nesting limit, and walks a map twice, as the C does, rather than
/// hold its pairs.
fn mp_encode(v: &LuaValue, level: usize, out: &mut Vec<u8>) -> Result<(), Fail> {
    match v {
        LuaValue::String(s) => {
            let b = s.as_bytes();
            let len = b.len();
            if len < 32 {
                out.push(0xa0 | len as u8);
            } else if len <= 0xff {
                out.extend_from_slice(&[0xd9, len as u8]);
            } else if len <= 0xffff {
                out.push(0xda);
                out.extend_from_slice(&(len as u16).to_be_bytes());
            } else {
                out.push(0xdb);
                out.extend_from_slice(&(len as u32).to_be_bytes());
            }
            out.extend_from_slice(&b);
        }
        LuaValue::Boolean(b) => out.push(if *b { 0xc3 } else { 0xc2 }),
        LuaValue::Number(_) | LuaValue::Integer(_) => {
            let n = num(v).unwrap_or(0.0);
            if !n.is_infinite() && (n as i64) as f64 == n {
                mp_int(n as i64, out);
            } else {
                let f = n as f32;
                if f as f64 == n {
                    out.push(0xca);
                    out.extend_from_slice(&f.to_be_bytes());
                } else {
                    out.push(0xcb);
                    out.extend_from_slice(&n.to_be_bytes());
                }
            }
        }
        LuaValue::Table(t) if level < MSGPACK_MAX_NESTING => {
            if mp_is_array(t)? {
                let len = t.raw_len();
                mp_header(len, 0x90, 0xdc, 0xdd, out);
                for i in 1..=len {
                    let item: LuaValue = t.raw_get(i)?;
                    mp_encode(&item, level + 1, out)?;
                }
            } else {
                let mut len = 0;
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    pair?;
                    len += 1;
                }
                mp_header(len, 0x80, 0xde, 0xdf, out);
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    let (k, val) = pair?;
                    mp_encode(&k, level + 1, out)?;
                    mp_encode(&val, level + 1, out)?;
                }
            }
        }
        // Past the nesting limit a table is nil, as is any type msgpack
        // has no word for.
        _ => out.push(0xc0),
    }
    Ok(())
}

fn mp_int(i: i64, out: &mut Vec<u8>) {
    if i >= 0 {
        if i <= 127 {
            out.push(i as u8);
        } else if i <= 0xff {
            out.extend_from_slice(&[0xcc, i as u8]);
        } else if i <= 0xffff {
            out.push(0xcd);
            out.extend_from_slice(&(i as u16).to_be_bytes());
        } else if i <= 0xffff_ffff {
            out.push(0xce);
            out.extend_from_slice(&(i as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&(i as u64).to_be_bytes());
        }
    } else if i >= -32 {
        out.push(i as i8 as u8);
    } else if i >= -128 {
        out.extend_from_slice(&[0xd0, i as i8 as u8]);
    } else if i >= -32768 {
        out.push(0xd1);
        out.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= -2_147_483_648 {
        out.push(0xd2);
        out.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&i.to_be_bytes());
    }
}

fn mp_header(len: usize, fix: u8, b16: u8, b32: u8, out: &mut Vec<u8>) {
    if len < 16 {
        out.push(fix | len as u8);
    } else if len <= 0xffff {
        out.push(b16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(b32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// lua-cmsgpack's `table_is_an_array`: every key a whole number of at
/// least 1, and as many keys as the largest.
fn mp_is_array(t: &Table) -> Result<bool, Fail> {
    let (mut count, mut max) = (0f64, 0f64);
    for pair in t.pairs::<LuaValue, LuaValue>() {
        let (k, _) = pair?;
        match num_key(&k) {
            Some(n) if n > 0.0 && (n as i32) as f64 == n => {
                max = max.max(n);
                count += 1.0;
            }
            _ => return Ok(false),
        }
    }
    Ok(max == count)
}

#[derive(Clone, Copy)]
enum Unpack {
    All,
    One,
    Limit,
}

enum MpErr {
    Eof,
    BadFormat,
    Fail(Fail),
}

impl From<Fail> for MpErr {
    fn from(f: Fail) -> Self {
        MpErr::Fail(f)
    }
}

impl From<mlua::Error> for MpErr {
    fn from(e: mlua::Error) -> Self {
        MpErr::Fail(Fail::Lua(e))
    }
}

struct Cur<'a> {
    b: &'a [u8],
    at: usize,
}

impl Cur<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], MpErr> {
        let s = self.b.get(self.at..self.at + n).ok_or(MpErr::Eof)?;
        self.at += n;
        Ok(s)
    }
    fn be(&mut self, n: usize) -> Result<u64, MpErr> {
        Ok(self.take(n)?.iter().fold(0u64, |a, &x| (a << 8) | x as u64))
    }
}

/// The Lua stack slots lua-cmsgpack's C would hold, counted so that a call
/// fails where Valkey's does: `luaL_checkstack` refuses a slot past
/// `MAX_C_STACK`.
struct Slots(usize);

impl Slots {
    fn check(&self, why: &str) -> Result<(), Fail> {
        if self.0 + 1 > MAX_C_STACK {
            return Err(format!("stack overflow ({why})").into());
        }
        Ok(())
    }
}

const TOO_MANY_VALUES: &str =
    "too many return values at once; use unpack_one or unpack_limit instead.";

fn cmsgpack_unpack(lua: &Lua, args: &Args, how: Unpack) -> Answer {
    let s = check_string(args, 1)?;
    let opt_int = |i: usize| -> Result<i64, Fail> {
        match args.get(i)? {
            None | Some(LuaValue::Nil) => Ok(0),
            Some(_) => Ok(check_number(args, i)? as i64),
        }
    };
    let (limit, offset) = match how {
        Unpack::All => (0, 0),
        Unpack::One => (1, opt_int(2)?),
        Unpack::Limit => (check_number(args, 2)? as i64, opt_int(3)?),
    };
    let decode_all = limit == 0 && offset == 0;
    if offset < 0 || limit < 0 {
        return Err(format!(
            "Invalid request to unpack with offset of {offset} and limit of {}.",
            s.len()
        )
        .into());
    }
    if offset as usize > s.len() {
        return Err(format!(
            "Start offset {offset} greater than input length {}.",
            s.len()
        )
        .into());
    }
    let limit = if decode_all { i64::MAX } else { limit };
    // `unpack` keeps all its arguments on the stack; `unpack_one` and
    // `unpack_limit` pop all but the first.
    let mut slots = Slots(match how {
        Unpack::All => args.n,
        Unpack::One | Unpack::Limit => 1,
    });
    let results = lua.create_table()?;
    // Without decode_all the first result is the next offset, set last.
    let mut count = usize::from(!decode_all);
    let held = lua.create_table()?;
    let mut cur = Cur {
        b: &s,
        at: offset as usize,
    };
    let mut cnt = 0;
    while cur.at < s.len() && cnt < limit {
        match mp_decode(lua, &mut cur, &mut slots, &held) {
            Ok(v) => {
                count += 1;
                results.raw_set(count, v)?;
            }
            Err(MpErr::Eof) => return Err("Missing bytes in input.".into()),
            Err(MpErr::BadFormat) => return Err("Bad data format in input.".into()),
            Err(MpErr::Fail(f)) => return Err(f),
        }
        cnt += 1;
    }
    if !decode_all {
        slots.check("in function mp_unpack_full")?;
        let next = if cur.at == s.len() {
            -1.0
        } else {
            cur.at as f64
        };
        results.raw_set(1, next)?;
    }
    Ok(Reply::Spread(results, count))
}

/// A MessagePack container being filled.
enum Filling {
    Array {
        t: Table,
        left: u64,
        at: u64,
    },
    /// `keyed`: its key is read, in `held`, and its value is next.
    Map {
        t: Table,
        left: u64,
        keyed: bool,
    },
}

/// lua-cmsgpack's `mp_decode_to_lua_type`: one whole value, with the
/// containers open at once on a stack of their own. A container's table is
/// an mlua reference, at most 3,999 of them before the slots run out; a
/// map's key waits in `held`, as it may be a table too.
fn mp_decode(lua: &Lua, c: &mut Cur, slots: &mut Slots, held: &Table) -> Result<LuaValue, MpErr> {
    let mut open: Vec<Filling> = Vec::new();
    loop {
        // One value: a scalar, or a container's header.
        if c.at >= c.b.len() {
            return Err(MpErr::Eof);
        }
        slots.check(TOO_MANY_VALUES)?;
        // The value's slot, or its table's.
        slots.0 += 1;
        let t = c.take(1)?[0];
        let n = |x: f64| LuaValue::Number(x);
        let mut value = match t {
            0x00..=0x7f => n(t as f64),
            0xe0..=0xff => n(t as i8 as f64),
            0xc0 => LuaValue::Nil,
            0xc2 => LuaValue::Boolean(false),
            0xc3 => LuaValue::Boolean(true),
            0xcc => n(c.be(1)? as f64),
            0xcd => n(c.be(2)? as f64),
            0xce => n(c.be(4)? as f64),
            // lua-cmsgpack pushes a uint64 as Lua's signed integer.
            0xcf => n(c.be(8)? as i64 as f64),
            0xd0 => n(c.be(1)? as u8 as i8 as f64),
            0xd1 => n(c.be(2)? as u16 as i16 as f64),
            0xd2 => n(c.be(4)? as u32 as i32 as f64),
            0xd3 => n(c.be(8)? as i64 as f64),
            0xca => n(f32::from_bits(c.be(4)? as u32) as f64),
            0xcb => n(f64::from_bits(c.be(8)?)),
            0xa0..=0xbf | 0xd9 | 0xda | 0xdb => {
                let len = match t {
                    0xd9 => c.be(1)?,
                    0xda => c.be(2)?,
                    0xdb => c.be(4)?,
                    _ => (t & 0x1f) as u64,
                } as usize;
                LuaValue::String(lua.create_string(c.take(len)?)?)
            }
            0x90..=0x9f | 0xdc | 0xdd => {
                let left = match t {
                    0xdc => c.be(2)?,
                    0xdd => c.be(4)?,
                    _ => (t & 0x0f) as u64,
                };
                let table = lua.create_table()?;
                slots.check("in function mp_decode_to_lua_array")?;
                if left == 0 {
                    LuaValue::Table(table)
                } else {
                    open.push(Filling::Array {
                        t: table,
                        left,
                        at: 1,
                    });
                    // The first index.
                    slots.0 += 1;
                    continue;
                }
            }
            0x80..=0x8f | 0xde | 0xdf => {
                let left = match t {
                    0xde => c.be(2)?,
                    0xdf => c.be(4)?,
                    _ => (t & 0x0f) as u64,
                };
                let table = lua.create_table()?;
                if left == 0 {
                    LuaValue::Table(table)
                } else {
                    open.push(Filling::Map {
                        t: table,
                        left,
                        keyed: false,
                    });
                    continue;
                }
            }
            _ => return Err(MpErr::BadFormat),
        };
        // `value` is whole: it goes into the innermost container, which
        // then closes, and goes into its own, or goes on.
        loop {
            let level = open.len();
            let Some(top) = open.last_mut() else {
                return Ok(value);
            };
            let left = match top {
                Filling::Array { t, left, at } => {
                    t.raw_set(*at, value)?;
                    *at += 1;
                    *left -= 1;
                    *left
                }
                Filling::Map { keyed, .. } if !*keyed => {
                    held.raw_set(level, value)?;
                    *keyed = true;
                    break;
                }
                Filling::Map { t, left, keyed } => {
                    // Lua's own error, raised in a C function: no position.
                    let key: LuaValue = held.raw_get(level)?;
                    match key {
                        LuaValue::Nil => return Err(Fail::Bare("table index is nil".into()).into()),
                        LuaValue::Number(k) if k.is_nan() => {
                            return Err(Fail::Bare("table index is NaN".into()).into());
                        }
                        _ => {}
                    }
                    t.raw_set(key, value)?;
                    held.raw_set(level, LuaValue::Nil)?;
                    *keyed = false;
                    *left -= 1;
                    *left
                }
            };
            // `lua_settable` pops the key and the value.
            slots.0 -= 2;
            if left > 0 {
                if matches!(top, Filling::Array { .. }) {
                    // The next index.
                    slots.0 += 1;
                }
                break;
            }
            let Some(Filling::Array { t, .. } | Filling::Map { t, .. }) = open.pop() else {
                unreachable!("a container was open");
            };
            value = LuaValue::Table(t);
        }
    }
}

// ------------------------------------------------------------------ bit ----

#[derive(Clone, Copy)]
enum BitOp {
    And,
    Or,
    Xor,
}

/// LuaBitOp's `tobit`: the number rounded, to nearest even, and wrapped to
/// a signed 32-bit integer, by its own trick.
fn tobit(x: f64) -> i32 {
    (x + 6_755_399_441_055_744.0).to_bits() as u32 as i32
}

fn bit_arg(args: &Args, n: usize) -> Result<i32, Fail> {
    Ok(tobit(check_number(args, n)?))
}

fn bit_ret(x: i32) -> Answer {
    one(LuaValue::Number(x as f64))
}

/// LuaBitOp's `band`, `bor` and `bxor`, which read their arguments from
/// the last back to the second.
fn bit_fold(args: &Args, op: BitOp) -> Answer {
    let mut acc = bit_arg(args, 1)?;
    for i in (2..=args.n).rev() {
        let x = bit_arg(args, i)?;
        acc = match op {
            BitOp::And => acc & x,
            BitOp::Or => acc | x,
            BitOp::Xor => acc ^ x,
        };
    }
    bit_ret(acc)
}

fn bit_unary(args: &Args, f: fn(i32) -> i32) -> Answer {
    bit_ret(f(bit_arg(args, 1)?))
}

fn bit_shift(args: &Args, f: fn(i32, u32) -> i32) -> Answer {
    let x = bit_arg(args, 1)?;
    let n = (bit_arg(args, 2)? as u32) & 31;
    bit_ret(f(x, n))
}

fn bit_tohex(lua: &Lua, args: &Args) -> Answer {
    let mut b = bit_arg(args, 1)? as u32;
    let mut n = match args.get(2)? {
        None => 8,
        Some(_) => bit_arg(args, 2)?,
    };
    let digits: &[u8] = if n < 0 {
        n = n.wrapping_neg();
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    // -2^31 stays negative, as in C; it gives no digits here.
    let n = n.clamp(0, 8) as usize;
    let mut buf = vec![0u8; n];
    for i in (0..n).rev() {
        buf[i] = digits[(b & 15) as usize];
        b >>= 4;
    }
    one(LuaValue::String(lua.create_string(&buf)?))
}

// --------------------------------------------------------------- struct ----

const MAXINTSIZE: usize = 32;
const MAXALIGN: usize = 8;

struct Header {
    big: bool,
    align: usize,
}

fn getnum(fmt: &[u8], at: &mut usize, df: usize) -> Result<usize, String> {
    if !fmt.get(*at).is_some_and(u8::is_ascii_digit) {
        return Ok(df);
    }
    let mut a: usize = 0;
    while let Some(d) = fmt.get(*at).filter(|c| c.is_ascii_digit()) {
        a = a
            .checked_mul(10)
            .and_then(|a| a.checked_add((d - b'0') as usize))
            .filter(|a| *a <= i32::MAX as usize)
            .ok_or("integral size overflow")?;
        *at += 1;
    }
    Ok(a)
}

fn optsize(opt: u8, fmt: &[u8], at: &mut usize) -> Result<usize, String> {
    Ok(match opt {
        b'B' | b'b' | b'x' => 1,
        b'H' | b'h' => 2,
        b'L' | b'l' | b'T' | b'd' => 8,
        b'f' => 4,
        b'c' => getnum(fmt, at, 1)?,
        b'i' | b'I' => {
            let sz = getnum(fmt, at, 4)?;
            if sz > MAXINTSIZE {
                return Err(format!(
                    "integral size {sz} is larger than limit of {MAXINTSIZE}"
                ));
            }
            sz
        }
        _ => 0,
    })
}

fn gettoalign(len: usize, h: &Header, opt: u8, size: usize) -> usize {
    if size == 0 || opt == b'c' {
        return 0;
    }
    let size = size.min(h.align);
    (size - (len & (size - 1))) & (size - 1)
}

fn controloptions(opt: u8, fmt: &[u8], at: &mut usize, h: &mut Header) -> Result<(), Fail> {
    match opt {
        b' ' => {}
        b'>' => h.big = true,
        b'<' | b'=' => h.big = false,
        b'!' => {
            let a = getnum(fmt, at, MAXALIGN)?;
            if !a.is_power_of_two() {
                return Err(format!("alignment {a} is not a power of 2").into());
            }
            h.align = a;
        }
        _ => {
            return Err(bad_arg(
                1,
                &format!("invalid format option '{}'", opt as char),
            ));
        }
    }
    Ok(())
}

fn struct_pack(lua: &Lua, args: &Args) -> Answer {
    let fmt = check_string(args, 1)?;
    // lua-struct pushes a nil after its arguments, so the first one
    // missing is nil, not none.
    let args = &Args {
        t: args.t.clone(),
        n: args.n + 1,
    };
    let mut h = Header {
        big: false,
        align: 1,
    };
    let mut arg = 2;
    let mut out = Vec::new();
    let mut total = 0usize;
    let mut at = 0;
    while at < fmt.len() {
        let opt = fmt[at];
        at += 1;
        let mut size = optsize(opt, &fmt, &mut at)?;
        let pad = gettoalign(total, &h, opt, size);
        total += pad;
        out.extend(std::iter::repeat_n(0u8, pad));
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let n = check_number(args, arg)?;
                arg += 1;
                let v: u64 = if n < 0.0 { n as i64 as u64 } else { n as u64 };
                let bytes = v.to_le_bytes();
                let mut chunk: Vec<u8> = (0..size)
                    .map(|i| bytes.get(i).copied().unwrap_or(0))
                    .collect();
                if h.big {
                    chunk.reverse();
                }
                out.extend_from_slice(&chunk);
            }
            b'x' => out.push(0),
            b'f' => {
                let f = check_number(args, arg)? as f32;
                arg += 1;
                out.extend_from_slice(&if h.big {
                    f.to_be_bytes()
                } else {
                    f.to_le_bytes()
                });
            }
            b'd' => {
                let d = check_number(args, arg)?;
                arg += 1;
                out.extend_from_slice(&if h.big {
                    d.to_be_bytes()
                } else {
                    d.to_le_bytes()
                });
            }
            b'c' | b's' => {
                let s = check_string(args, arg)?;
                arg += 1;
                if size == 0 {
                    size = s.len();
                }
                if s.len() < size {
                    return Err(bad_arg(arg, "string too short"));
                }
                out.extend_from_slice(&s[..size]);
                if opt == b's' {
                    out.push(0);
                    size += 1;
                }
            }
            _ => controloptions(opt, &fmt, &mut at, &mut h)?,
        }
        total += size;
    }
    one(LuaValue::String(lua.create_string(&out)?))
}

/// A result `struct.unpack` has read: a number, or bytes of the data.
enum Item {
    Num(f64),
    Bytes(std::ops::Range<usize>),
}

fn struct_unpack(lua: &Lua, args: &Args) -> Answer {
    let fmt = check_string(args, 1)?;
    let data = check_string(args, 2)?;
    let ld = data.len();
    let start = match args.get(3)? {
        None | Some(LuaValue::Nil) => 1.0,
        Some(_) => check_number(args, 3)?,
    };
    let mut pos = (start as i64 - 1) as usize;
    if start < 1.0 || pos > ld {
        return Err(bad_arg(3, "offset must be 1 or greater"));
    }
    let mut h = Header {
        big: false,
        align: 1,
    };
    let mut items: Vec<Item> = Vec::new();
    let mut at = 0;
    while at < fmt.len() {
        let opt = fmt[at];
        at += 1;
        let mut size = optsize(opt, &fmt, &mut at)?;
        pos += gettoalign(pos, &h, opt, size);
        if size > ld.saturating_sub(pos) {
            return Err(bad_arg(2, "data string too short"));
        }
        // lua-struct asks the stack for an item and the next position, its
        // arguments still below them.
        if args.n + items.len() + 2 > MAX_C_STACK {
            return Err("stack overflow (too many results)".into());
        }
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let raw = &data[pos..pos + size];
                let mut l: u64 = 0;
                let ordered: Vec<u8> = if h.big {
                    raw.to_vec()
                } else {
                    raw.iter().rev().copied().collect()
                };
                for b in ordered {
                    l = (l << 8) | b as u64;
                }
                let v = if opt.is_ascii_lowercase()
                    && size > 0
                    && size < 8
                    && l & (1 << (size * 8 - 1)) != 0
                {
                    (l | (u64::MAX << (size * 8 - 1))) as i64 as f64
                } else if opt.is_ascii_lowercase() {
                    l as i64 as f64
                } else {
                    l as f64
                };
                items.push(Item::Num(v));
            }
            b'x' => {}
            b'f' => {
                let b: [u8; 4] = data[pos..pos + 4].try_into().unwrap_or_default();
                let f = if h.big {
                    f32::from_be_bytes(b)
                } else {
                    f32::from_le_bytes(b)
                };
                items.push(Item::Num(f as f64));
            }
            b'd' => {
                let b: [u8; 8] = data[pos..pos + 8].try_into().unwrap_or_default();
                let d = if h.big {
                    f64::from_be_bytes(b)
                } else {
                    f64::from_le_bytes(b)
                };
                items.push(Item::Num(d));
            }
            b'c' => {
                if size == 0 {
                    // The previous result is the size, if it reads as a number.
                    let prev = match items.pop() {
                        Some(Item::Num(n)) => Some(n),
                        Some(Item::Bytes(r)) => str_num(&data[r]),
                        None => None,
                    };
                    let Some(prev) = prev else {
                        return Err("format 'c0' needs a previous size".into());
                    };
                    size = prev as usize;
                    if size > ld.saturating_sub(pos) {
                        return Err(bad_arg(2, "data string too short"));
                    }
                }
                items.push(Item::Bytes(pos..pos + size));
            }
            b's' => {
                let Some(nul) = data[pos..].iter().position(|&c| c == 0) else {
                    return Err("unfinished string in data".into());
                };
                size = nul + 1;
                items.push(Item::Bytes(pos..pos + nul));
            }
            _ => controloptions(opt, &fmt, &mut at, &mut h)?,
        }
        pos += size;
    }
    let results = lua.create_table()?;
    for (i, item) in items.iter().enumerate() {
        let v = match item {
            Item::Num(n) => LuaValue::Number(*n),
            Item::Bytes(r) => LuaValue::String(lua.create_string(&data[r.clone()])?),
        };
        results.raw_set(i + 1, v)?;
    }
    results.raw_set(items.len() + 1, (pos + 1) as f64)?;
    Ok(Reply::Spread(results, items.len() + 1))
}

fn struct_size(_lua: &Lua, args: &Args) -> Answer {
    let fmt = check_string(args, 1)?;
    let mut h = Header {
        big: false,
        align: 1,
    };
    let mut pos = 0usize;
    let mut at = 0;
    while at < fmt.len() {
        let opt = fmt[at];
        at += 1;
        let size = optsize(opt, &fmt, &mut at)?;
        pos += gettoalign(pos, &h, opt, size);
        if opt == b's' {
            return Err(bad_arg(1, "option 's' has no fixed size"));
        } else if opt == b'c' && size == 0 {
            return Err(bad_arg(1, "option 'c0' has no fixed size"));
        }
        if !opt.is_ascii_alphanumeric() {
            controloptions(opt, &fmt, &mut at, &mut h)?;
        }
        pos += size;
    }
    one(LuaValue::Number(pos as f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn g14_spells_numbers_as_c_does() {
        for (x, want) in [
            (1.0, "1"),
            (0.1, "0.1"),
            (-0.0, "-0"),
            (1e20, "1e+20"),
            (1.2345678901234568, "1.2345678901235"),
            (9007199254740992.0, "9.007199254741e+15"),
            (123456789012345.0, "1.2345678901234e+14"),
            (1.0 / 3.0, "0.33333333333333"),
            (-1.5e-7, "-1.5e-07"),
            (1500.0, "1500"),
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (12345678901234.0, "12345678901234"),
            (1e14, "1e+14"),
        ] {
            assert_eq!(fmt_g14(x), want, "{x}");
        }
    }

    #[test]
    fn strtod_reads_the_prefix_c_reads() {
        assert_eq!(strtod(b"12x"), (12.0, 2));
        assert_eq!(strtod(b"1.5e3,"), (1500.0, 5));
        assert_eq!(strtod(b"1e"), (1.0, 1));
        assert_eq!(strtod(b"0x10]"), (16.0, 4));
        assert_eq!(strtod(b"-0"), (-0.0, 2));
        assert_eq!(strtod(b"+5"), (5.0, 2));
        assert_eq!(strtod(b"inf"), (f64::INFINITY, 3));
        assert_eq!(strtod(b"-Infinity"), (f64::NEG_INFINITY, 9));
        assert_eq!(strtod(b"-"), (0.0, 0));
        assert_eq!(strtod(b".5"), (0.5, 2));
        assert_eq!(strtod(b"5."), (5.0, 2));
        assert!(strtod(b"1e400").0.is_infinite());
    }

    /// A state with the libraries; its chunks are named as scripts are.
    fn libs() -> Lua {
        let lua = Lua::new();
        install(&lua).expect("install");
        lua
    }

    /// What a chunk returns, or the message it raised, without the
    /// traceback mlua appends.
    fn run(lua: &Lua, src: &str) -> Result<LuaValue, String> {
        lua.load(src)
            .set_name("@user_script")
            .eval::<LuaValue>()
            .map_err(|e| match e {
                mlua::Error::RuntimeError(m) => match m.split_once("\nstack traceback:") {
                    Some((message, _)) => message.to_string(),
                    None => m,
                },
                other => format!("not a Lua error: {other}"),
            })
    }

    fn number(v: Result<LuaValue, String>) -> f64 {
        match v {
            Ok(LuaValue::Number(n)) => n,
            Ok(LuaValue::Integer(i)) => i as f64,
            other => panic!("not a number: {other:?}"),
        }
    }

    #[test]
    fn errors_name_the_scripts_line_even_from_a_tail_call() {
        let lua = libs();
        let decode = "Expected comma or array end but found T_END at character 3";
        assert_eq!(
            run(&lua, "return cjson.decode('[1')"),
            Err(format!("user_script:1: {decode}"))
        );
        assert_eq!(
            run(&lua, "local x = 1\nlocal y = cjson.decode('[1')\nreturn y"),
            Err(format!("user_script:2: {decode}"))
        );
        // Called by pcall, a C function, it has no line to name.
        let caught = run(&lua, "local ok, e = pcall(cjson.decode, '[1') return e");
        assert_eq!(
            caught.map(|v| format!("{v:?}")),
            Ok(format!("String({decode:?})"))
        );
    }

    #[test]
    fn argument_errors_name_the_function_as_it_was_called() {
        let lua = libs();
        assert_eq!(
            run(&lua, "local d = cjson.decode return d({})"),
            Err("user_script:1: bad argument #1 to 'd' (string expected, got table)".into())
        );
        assert_eq!(
            run(&lua, "return cjson:decode('1')"),
            Err("user_script:1: calling 'decode' on bad self (expected 1 argument)".into())
        );
        // LuaBitOp reads the arguments from the last back.
        assert_eq!(
            run(&lua, "return bit.bor(1, 'x', 'y')"),
            Err("user_script:1: bad argument #3 to 'bor' (number expected, got string)".into())
        );
    }

    #[test]
    fn nesting_fails_where_valkey_runs_out_of_stack() {
        let lua = libs();
        let arrays = |n| format!("return type(cmsgpack.unpack(string.rep('\\145', {n}) .. '\\1'))");
        assert_eq!(
            run(&lua, &arrays(3999)).map(|v| format!("{v:?}")),
            Ok("String(\"table\")".into())
        );
        assert_eq!(
            run(&lua, &arrays(4000)),
            Err("user_script:1: stack overflow (in function mp_decode_to_lua_array)".into())
        );
        assert_eq!(
            run(&lua, "local t = {} t[1] = t return cjson.encode(t)"),
            Err("user_script:1: Cannot serialise, excessive nesting (1001)".into())
        );
        assert_eq!(
            run(
                &lua,
                "return cjson.decode(string.rep('[', 1001) .. string.rep(']', 1001))"
            ),
            Err(
                "user_script:1: Found too many nested data structures (1001) at character 1001"
                    .into()
            )
        );
        // Lua's own error, raised inside the C function: no position.
        assert_eq!(
            run(&lua, "return cmsgpack.unpack('\\129\\192\\1')"),
            Err("table index is nil".into())
        );
    }

    #[test]
    fn thousands_of_values_cross_without_a_reference_each() {
        let lua = libs();
        let pack = "local t = {} for i=1,9000 do t['k'..i]=i end return #cmsgpack.pack(t)";
        assert_eq!(number(run(&lua, pack)), 79514.0);
        let unpack = |n| format!("local t = {{cmsgpack.unpack(string.rep('\\1', {n}))}} return #t");
        assert_eq!(number(run(&lua, &unpack(7999))), 7999.0);
        assert_eq!(
            run(&lua, &unpack(8000)),
            Err("user_script:1: stack overflow (too many return values at once; use unpack_one or unpack_limit instead.)".into())
        );
        let strings =
            "return select('#', struct.unpack(string.rep('c1', 7997), string.rep('x', 9000)))";
        assert_eq!(number(run(&lua, strings)), 7998.0);
    }

    #[test]
    fn tobit_rounds_to_even_and_wraps() {
        assert_eq!(tobit(1.5), 2);
        assert_eq!(tobit(2.5), 2);
        assert_eq!(tobit(4294967297.0), 1);
        assert_eq!(tobit(-1.0), -1);
        assert_eq!(tobit(2147483648.0), -2147483648);
    }
}
