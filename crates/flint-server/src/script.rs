// SPDX-License-Identifier: Elastic-2.0
//! Lua scripting: `EVAL`, `EVALSHA` and `SCRIPT` (ADR-0051).
//!
//! A script runs in an embedded PUC-Rio Lua 5.1, the interpreter Redis and
//! Valkey embed, so it means what it means there, number formatting
//! included. What differs is the frame around it:
//!
//! - **A sandboxed state, one per namespace per thread.** Only the base,
//!   `table`, `string` and `math` libraries load. The loaders (`load`,
//!   `loadstring`, `dofile`, `loadfile`, `require`), `setfenv`/`getfenv`,
//!   `newproxy` and `print` are removed, and the script text compiles as TEXT
//!   only, so bytecode (the historical escape route) cannot load. A state
//!   serves one namespace, so nothing crosses a tenant boundary; within one,
//!   globals, the libraries and `redis` are read-only (as Redis 7 makes
//!   them), so a script cannot change what the next one sees. See `Engine`.
//! - **A time limit and a memory limit.** An instruction-count hook aborts a
//!   script past its time limit, and once it has fired, `pcall`, `xpcall` and
//!   `coroutine.resume` re-raise instead of catching, so a script cannot
//!   swallow its own abort. The memory limit is the allocator's.
//! - **Declared keys only.** Every `redis.call` runs through the caller's
//!   `call`, which refuses a command touching any key outside `KEYS`
//!   (`KeyGuard`).
//! - **All or nothing.** The caller buffers the script's writes and commits
//!   them only when [`Outcome::commit`] is set: an uncaught error, the time
//!   limit or the memory limit discards them. Redis keeps the writes a
//!   failing script made before it failed; Flint does not (ADR-0051).
//!
//! The replies and errors are Valkey's, captured off the wire from Valkey
//! 9.1 rather than recalled; the conformance corpus runs the same scripts
//! against the Valkey oracle.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use flint_resp::{Value, fmt_double};
use flint_slot::slot_for_key;
use flint_storage::Kv;
use mlua::chunk::ChunkMode;
use mlua::{
    Function, HookTriggers, Lua, LuaOptions, LuaString, MultiValue, StdLib, Table,
    Value as LuaValue, VmState,
};

/// The time and memory a script may use (`commands::Limits` carries them).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScriptLimits {
    pub time: Duration,
    pub memory_bytes: usize,
}

/// 50 ms, as ADR-0051 proposed: a script holds its keys' write locks for
/// its whole run, and the libraries measured run in microseconds.
pub const DEFAULT_TIME_LIMIT: Duration = Duration::from_millis(50);
/// 64 MiB of Lua heap per call.
pub const DEFAULT_MEMORY_LIMIT: usize = 64 << 20;

impl Default for ScriptLimits {
    fn default() -> Self {
        Self {
            time: DEFAULT_TIME_LIMIT,
            memory_bytes: DEFAULT_MEMORY_LIMIT,
        }
    }
}

/// How often, in VM instructions, the hook looks at the clock. A few tens of
/// microseconds of Lua between looks; the look itself is one `Instant` read.
const HOOK_EVERY: u32 = 10_000;

/// A script's result, and whether its writes may be committed.
#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub reply: Value,
    /// True when the script ran to its end, including when it returned an
    /// error reply (`redis.error_reply`): that is a value, not a failure.
    pub commit: bool,
    /// True when the text compiled, which is when Redis caches it.
    pub compiled: bool,
    /// True when a command answered `Abandon`: the script was stopped where
    /// it stood, nothing it wrote may be kept, and the caller runs it again
    /// from the start (ADR-0052).
    pub abandoned: bool,
}

/// What `call` answers instead of a reply when the script must not go on:
/// a command reached a key the caller's lock does not cover (ADR-0052). The
/// script is stopped as the time limit stops one, so no `pcall` can catch
/// it, and its state stays reusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abandon;

/// What runs one command for `redis.call` and `redis.pcall`: its reply, or
/// `Abandon`. It owns the key guard and the per-call buffer.
pub type Call<'a> = dyn Fn(&[Vec<u8>]) -> Result<Value, Abandon> + 'a;

/// Commands a script may not call, with Valkey's reply. Transactions, the
/// scripting commands themselves (no recursion), and connection state.
fn denied(upper: &[u8]) -> bool {
    matches!(
        upper,
        b"EVAL"
            | b"EVALSHA"
            | b"EVAL_RO"
            | b"EVALSHA_RO"
            | b"SCRIPT"
            | b"FCALL"
            | b"FCALL_RO"
            | b"FUNCTION"
            | b"MULTI"
            | b"EXEC"
            | b"DISCARD"
            | b"WATCH"
            | b"UNWATCH"
            | b"SUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"SSUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"SUNSUBSCRIBE"
            | b"AUTH"
            | b"HELLO"
            | b"CLIENT"
            | b"SELECT"
            | b"QUIT"
            | b"RESET"
            | b"MONITOR"
            | b"SHUTDOWN"
    )
}

/// Compile `text` as Redis does for `SCRIPT LOAD` and `EVAL`, without
/// running it: `Err` carries the reply for a script that does not compile.
pub fn compile_check(text: &[u8]) -> Result<(), Value> {
    let lua = sandbox(DEFAULT_MEMORY_LIMIT).map_err(internal)?;
    compile(&lua, text).map(|_| ())
}

fn compile(lua: &Lua, text: &[u8]) -> Result<Function, Value> {
    lua.load(text)
        .set_name("@user_script")
        .set_mode(ChunkMode::Text)
        .into_function()
        .map_err(|e| {
            let msg = match &e {
                mlua::Error::SyntaxError { message, .. } => message.clone(),
                other => other.to_string(),
            };
            Value::Error(format!("ERR Error compiling script (new function): {msg}"))
        })
}

fn internal(e: mlua::Error) -> Value {
    Value::Error(format!("ERR the script engine failed: {e}"))
}

/// A state with the sandboxed library set and nothing else.
fn sandbox(memory_bytes: usize) -> mlua::Result<Lua> {
    let lua = Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        LuaOptions::new(),
    )?;
    lua.set_memory_limit(memory_bytes)?;
    let g = lua.globals();
    for name in [
        "dofile",
        "loadfile",
        "load",
        "loadstring",
        "require",
        "module",
        "setfenv",
        "getfenv",
        "newproxy",
        "print",
        "package",
        "io",
        "os",
        "debug",
    ] {
        g.raw_set(name, LuaValue::Nil)?;
    }
    Ok(lua)
}

/// A prepared Lua state: the sandbox, the read-only environment and the
/// handlers, built once and reused for one namespace's scripts on one
/// thread. Building one costs ~60 us (the libraries, then the prelude);
/// reusing it costs what the script does.
///
/// Reuse is confined so nothing crosses a tenant boundary: a state belongs
/// to one namespace, and a pool is per thread. Within a namespace, one
/// script cannot change what the next sees: globals are read-only, the
/// libraries and `redis` are read-only proxies, `rawset` refuses them, the
/// string metatable is hidden, and KEYS, ARGV, `redis.call` and
/// `redis.pcall` are replaced for every call. A state that hit a limit is
/// dropped, never reused.
struct Engine {
    lua: Lua,
    ns: Vec<u8>,
    memory_bytes: usize,
    /// The table behind the read-only `redis`, where each call's
    /// `redis.call` and `redis.pcall` are put.
    redis: Table,
    xpcall: Function,
    handler: Function,
    failure: Rc<RefCell<Option<String>>>,
    pending: Rc<RefCell<Option<String>>>,
    dead: Rc<Cell<bool>>,
    /// Set when a command answered `Abandon`. Stops the script as `dead`
    /// does, but the state is sound: the stop is an ordinary Lua error.
    abandon: Rc<Cell<bool>>,
    compiled: HashMap<String, Function>,
    uses: u32,
}

/// States per thread; past it the least recently used goes.
const ENGINES_PER_THREAD: usize = 8;
/// Scripts per state before it is rebuilt, which bounds anything a state
/// could accumulate.
const USES_PER_ENGINE: u32 = 100_000;
/// Compiled scripts kept per state.
const COMPILED_PER_ENGINE: usize = 256;

thread_local! {
    static ENGINES: RefCell<Vec<Engine>> = const { RefCell::new(Vec::new()) };
}

impl Engine {
    fn build(ns: &[u8], memory_bytes: usize) -> mlua::Result<Engine> {
        let lua = sandbox(memory_bytes)?;
        let g = lua.globals();
        let xpcall: Function = g.raw_get("xpcall")?;
        let tostring: Function = g.raw_get("tostring")?;
        let failure: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        // A failed `redis.call`'s error. `redis.call` is a Rust function, as
        // it is a C function in Redis, so a script that tail-calls it keeps
        // its own frame and its line; the error a Rust function raises is
        // not a Lua value the script could inspect, so its text travels
        // here, to the handler (uncaught) or to the `pcall` wrapper (caught).
        let pending: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let dead = Rc::new(Cell::new(false));
        let abandon = Rc::new(Cell::new(false));

        // The error handler: Valkey's shape. It runs where the error was
        // raised, so the stack still shows the script's line. A string error
        // is the script's own (`error('x')`, or Lua's): "ERR " plus the
        // message. A table with `err` is `error({err=...})`: its text as is.
        let handler = {
            let failure = Rc::clone(&failure);
            let pending = Rc::clone(&pending);
            lua.create_function(move |lua, err: LuaValue| {
                let line = script_line(lua);
                let raised = pending.borrow_mut().take();
                let text = match &err {
                    _ if raised.is_some() => raised.unwrap_or_default(),
                    // Valkey: an `err` Lua can read as a string is the text
                    // (a number included); a table without one is "unknown".
                    LuaValue::Table(t) => match t.raw_get::<LuaValue>("err")? {
                        LuaValue::String(s) => s.to_string_lossy(),
                        e @ (LuaValue::Number(_) | LuaValue::Integer(_)) => {
                            tostring.call::<String>(e)?
                        }
                        _ => "ERR unknown error".to_string(),
                    },
                    LuaValue::String(s) => format!("ERR {}", s.to_string_lossy()),
                    other => format!("ERR {}", tostring.call::<String>(other.clone())?),
                };
                *failure.borrow_mut() = Some(format!("{text} script: on @user_script:{line}."));
                Ok(())
            })?
        };
        let take_pending = {
            let pending = Rc::clone(&pending);
            lua.create_function(move |_, ()| Ok(pending.borrow_mut().take()))?
        };
        let is_dead = {
            let dead = Rc::clone(&dead);
            let abandon = Rc::clone(&abandon);
            lua.create_function(move |_, ()| Ok(dead.get() || abandon.get()))?
        };

        let redis = lua.create_table()?;
        for (name, v) in [
            ("LOG_DEBUG", 0),
            ("LOG_VERBOSE", 1),
            ("LOG_NOTICE", 2),
            ("LOG_WARNING", 3),
            ("REPL_NONE", 0),
            ("REPL_AOF", 1),
            ("REPL_SLAVE", 2),
            ("REPL_REPLICA", 2),
            ("REPL_ALL", 3),
        ] {
            redis.raw_set(name, v)?;
        }
        redis.raw_set(
            "sha1hex",
            lua.create_function(|lua, s: LuaString| {
                lua.create_string(flint_tls::sha1_hex(&s.as_bytes()[..]))
            })?,
        )?;
        g.raw_set("redis", redis.clone())?;
        lua.load(PRELUDE)
            .set_name("=flint")
            .set_mode(ChunkMode::Text)
            .call::<()>((take_pending, is_dead))?;
        Ok(Engine {
            lua,
            ns: ns.to_vec(),
            memory_bytes,
            redis,
            xpcall,
            handler,
            failure,
            pending,
            dead,
            abandon,
            compiled: HashMap::new(),
            uses: 0,
        })
    }

    /// The compiled script, from this state's cache or freshly.
    fn function(&mut self, sha: &str, text: &[u8]) -> Result<Function, Value> {
        if let Some(f) = self.compiled.get(sha) {
            return Ok(f.clone());
        }
        let f = compile(&self.lua, text)?;
        if self.compiled.len() >= COMPILED_PER_ENGINE {
            self.compiled.clear();
        }
        self.compiled.insert(sha.to_string(), f.clone());
        Ok(f)
    }
}

/// Run a script for `ns`. `sha` is the SHA1 of `text`. `call` executes one
/// command for `redis.call` and `redis.pcall` and answers its reply; it owns
/// the key guard and the per-call buffer. Nothing here touches the store.
pub fn run(
    ns: &[u8],
    sha: &str,
    text: &[u8],
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
    limits: ScriptLimits,
    call: &Call<'_>,
) -> Outcome {
    let taken = ENGINES.with(|e| {
        let mut e = e.borrow_mut();
        e.iter()
            .position(|x| x.ns == ns && x.memory_bytes == limits.memory_bytes)
            .map(|i| e.remove(i))
    });
    let mut engine = match taken {
        Some(e) => e,
        None => match Engine::build(ns, limits.memory_bytes) {
            Ok(e) => e,
            Err(e) => {
                return Outcome {
                    reply: internal(e),
                    commit: false,
                    compiled: false,
                    abandoned: false,
                };
            }
        },
    };
    let user = match engine.function(sha, text) {
        Ok(f) => f,
        Err(reply) => {
            give_back(engine);
            return Outcome {
                reply,
                commit: false,
                compiled: false,
                abandoned: false,
            };
        }
    };
    let out = match execute(&engine, user, keys, argv, limits, call) {
        Ok(o) => o,
        Err(e) => Outcome {
            reply: internal(e),
            commit: false,
            compiled: true,
            abandoned: false,
        },
    };
    engine.uses += 1;
    // A state that stopped a script at a limit, or failed inside the
    // engine, is not trusted again.
    let engine_failed =
        matches!(&out.reply, Value::Error(e) if e.starts_with("ERR the script engine failed"));
    if !engine.dead.get() && !engine_failed && engine.uses < USES_PER_ENGINE {
        give_back(engine);
    }
    out
}

fn give_back(engine: Engine) {
    ENGINES.with(|e| {
        let mut e = e.borrow_mut();
        if e.len() >= ENGINES_PER_THREAD {
            e.remove(0);
        }
        e.push(engine);
    });
}

/// One call on a prepared state.
fn execute(
    engine: &Engine,
    user: Function,
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
    limits: ScriptLimits,
    call: &Call<'_>,
) -> mlua::Result<Outcome> {
    let lua = &engine.lua;
    engine.failure.borrow_mut().take();
    engine.pending.borrow_mut().take();
    engine.dead.set(false);
    engine.abandon.set(false);
    // A script may have stopped the collector; garbage from earlier scripts
    // must not count against this one's memory, and Lua 5.1 collects
    // nothing on its own when an allocation fails.
    lua.gc_restart();
    if lua.used_memory() > limits.memory_bytes / 2 {
        lua.gc_collect()?;
    }

    let g = lua.globals();
    let key_table = lua.create_table_with_capacity(keys.len(), 0)?;
    for (i, k) in keys.iter().enumerate() {
        key_table.raw_set(i + 1, lua.create_string(k)?)?;
    }
    let argv_table = lua.create_table_with_capacity(argv.len(), 0)?;
    for (i, a) in argv.iter().enumerate() {
        argv_table.raw_set(i + 1, lua.create_string(a)?)?;
    }
    g.raw_set("KEYS", key_table)?;
    g.raw_set("ARGV", argv_table)?;

    // The time limit. Once it fires it keeps firing, and `dead` tells the
    // wrappers in the prelude to re-raise rather than catch.
    //
    // The GLOBAL hook, not `set_hook`: Lua 5.1 copies a thread's hook into
    // each coroutine it creates, and mlua's per-thread hook looks its
    // callback up by thread, finds none for a coroutine the script made, and
    // removes itself -- so a coroutine ran unlimited. The global hook's
    // callback is the state's, whichever thread runs it.
    {
        let dead = Rc::clone(&engine.dead);
        let abandon = Rc::clone(&engine.abandon);
        let started = Instant::now();
        let limit = limits.time;
        lua.set_global_hook(
            HookTriggers::new().every_nth_instruction(HOOK_EVERY),
            move |_, _| {
                if abandon.get() {
                    Err(mlua::Error::runtime("the script was abandoned"))
                } else if dead.get() || started.elapsed() > limit {
                    dead.set(true);
                    Err(mlua::Error::runtime("the script's time limit"))
                } else {
                    Ok(VmState::Continue)
                }
            },
        )?;
    }

    let result = lua.scope(|scope| {
        let abandon = &engine.abandon;
        let rawcall =
            scope.create_function(|lua, args: MultiValue| redis_call(lua, args, call, abandon))?;
        let raising = {
            let pending = Rc::clone(&engine.pending);
            scope.create_function(move |lua, args: MultiValue| {
                let reply = redis_call(lua, args, call, abandon)?;
                if let LuaValue::Table(t) = &reply
                    && let LuaValue::String(e) = t.raw_get::<LuaValue>("err")?
                {
                    let text = e.to_string_lossy();
                    *pending.borrow_mut() = Some(text.clone());
                    return Err(mlua::Error::runtime(text));
                }
                Ok(reply)
            })?
        };
        engine.redis.raw_set("call", raising)?;
        engine.redis.raw_set("pcall", rawcall)?;
        let (ok, value): (bool, LuaValue) = engine.xpcall.call((user, engine.handler.clone()))?;
        Ok((ok, if ok { Some(to_reply(&value, 0)) } else { None }))
    });
    lua.remove_global_hook();
    engine.redis.raw_set("call", LuaValue::Nil)?;
    engine.redis.raw_set("pcall", LuaValue::Nil)?;
    let (ok, reply) = result?;
    // Before `ok`: a script that caught the stop with `pcall` and then
    // returned is still stopped, and its reply is not the one it would give.
    if engine.abandon.get() {
        return Ok(Outcome {
            reply: Value::Error("ERR the script was abandoned, to run again".into()),
            commit: false,
            compiled: true,
            abandoned: true,
        });
    }
    if ok {
        return Ok(Outcome {
            reply: reply.unwrap_or(Value::Bulk(None)),
            commit: true,
            compiled: true,
            abandoned: false,
        });
    }
    let reply = if engine.dead.get() {
        Value::Error(format!(
            "ERR the script ran past Flint's time limit of {} ms and was stopped; \
             nothing it wrote was kept",
            limits.time.as_millis()
        ))
    } else if let Some(text) = engine.failure.borrow_mut().take() {
        Value::Error(text)
    } else {
        // Lua calls no handler for a memory error; nothing else reaches
        // here without one.
        engine.dead.set(true);
        Value::Error(format!(
            "ERR the script ran past Flint's memory limit of {} MiB and was stopped; \
             nothing it wrote was kept",
            limits.memory_bytes >> 20
        ))
    };
    Ok(Outcome {
        reply,
        commit: false,
        compiled: true,
        abandoned: false,
    })
}

/// Lua, run once when a state is built, with `take_pending` and `is_dead`
/// as its arguments. Everything it defines closes over locals the script
/// cannot reach: the `debug` library is not loaded, so upvalues stay
/// private. `redis.call` and `redis.pcall` are not here: they borrow each
/// call's store, and are put into `redis` for the call alone.
const PRELUDE: &str = r#"
local take_pending, is_dead = ...
local error, type, rawget, tostring, pairs, ipairs = error, type, rawget, tostring, pairs, ipairs
local setmetatable, getmetatable = setmetatable, getmetatable
local rawpcall, rawxpcall, coresume, raw_rawset = pcall, xpcall, coroutine.resume, rawset

-- After the time limit fires, nothing may catch it.
local function live(...)
  if is_dead() then error("the script's time limit", 0) end
  return ...
end
-- A redis.call error caught by pcall is its message, as in Redis 7.
local function unwrap(ok, ...)
  if not ok then
    local raised = take_pending()
    if raised ~= nil then return false, raised end
    local e = ...
    if type(e) == "table" and type(rawget(e, "err")) == "string" then
      return false, rawget(e, "err")
    end
  end
  return ok, ...
end
local function discard(...)
  take_pending()
  return ...
end

redis.error_reply = function(msg)
  if type(msg) ~= "string" then error("wrong number or type of arguments", 2) end
  return { err = msg }
end
redis.status_reply = function(msg)
  if type(msg) ~= "string" then error("wrong number or type of arguments", 2) end
  return { ok = msg }
end
redis.log = function() end
redis.setresp = function(v)
  if v ~= 2 then error("Flint scripts reply in RESP2 only: redis.setresp(2)", 2) end
end
redis.set_repl = function() end
redis.replicate_commands = function() return true end
redis.breakpoint = function() return false end
redis.debug = function() end

-- Read-only views of the libraries and of redis: nothing one script does
-- to them survives into the next script on this state.
local protected = {}
local function readonly(t)
  local p = setmetatable({}, {
    __index = t,
    __newindex = function() error("Attempt to modify a readonly table", 2) end,
    __metatable = false,
  })
  protected[p] = true
  return p
end
local co = {}
for k, v in pairs(coroutine) do co[k] = v end
co.resume = function(...) return live(unwrap(coresume(...))) end
for _, name in ipairs({ "string", "table", "math", "redis" }) do
  raw_rawset(_G, name, readonly(_G[name]))
end
raw_rawset(_G, "coroutine", readonly(co))
raw_rawset(_G, "pcall", function(...) return live(unwrap(rawpcall(...))) end)
raw_rawset(_G, "xpcall", function(...) return live(discard(rawxpcall(...))) end)
raw_rawset(_G, "rawset", function(t, k, v)
  if protected[t] then error("Attempt to modify a readonly table", 2) end
  return raw_rawset(t, k, v)
end)
getmetatable("").__metatable = false
protected[_G] = true
setmetatable(_G, {
  __index = function(_, k)
    error("Script attempted to access nonexistent global variable '" .. tostring(k) .. "'", 2)
  end,
  __newindex = function()
    error("Attempt to modify a readonly table", 2)
  end,
  __metatable = false,
})
"#;

/// The innermost frame of the script's own code, as Valkey reports it.
fn script_line(lua: &Lua) -> String {
    for level in 1..64 {
        let found = lua.inspect_stack(level, |d| {
            let src = d.source();
            let mine = src.source.as_deref() == Some("@user_script");
            (mine, d.current_line())
        });
        match found {
            None => break,
            Some((true, Some(line))) => return line.to_string(),
            Some(_) => continue,
        }
    }
    "?".into()
}

/// `redis.pcall`: run one command and answer its reply as Lua. An error
/// comes back as `{err = ...}`, which `redis.call` raises.
fn redis_call(
    lua: &Lua,
    args: MultiValue,
    call: &Call<'_>,
    abandon: &Cell<bool>,
) -> mlua::Result<LuaValue> {
    if args.is_empty() {
        return err_table(
            lua,
            "ERR Please specify at least one argument for this call",
        );
    }
    let mut parts = Vec::with_capacity(args.len());
    for a in args {
        match a {
            LuaValue::String(s) => parts.push(s.as_bytes().to_vec()),
            LuaValue::Number(n) => parts.push(lua_number_arg(n)),
            // mlua hands an integral Lua 5.1 number over as an integer; it is
            // the same double, and spelled by the same rule.
            LuaValue::Integer(i) => parts.push(lua_number_arg(i as f64)),
            _ => return err_table(lua, "ERR Command arguments must be strings or integers"),
        }
    }
    if denied(&parts[0].to_ascii_uppercase()) {
        return err_table(lua, "ERR This command is not allowed from script");
    }
    let Ok(reply) = call(&parts) else {
        abandon.set(true);
        return Err(mlua::Error::runtime("the script was abandoned"));
    };
    let reply = match reply {
        // Our own texts (commands.rs), so matching them is matching our own
        // output: the script-side wording is Valkey's.
        Value::Error(e) if e.starts_with("ERR unknown command") => {
            Value::Error("ERR Unknown command called from script".into())
        }
        Value::Error(e) if e.starts_with("ERR wrong number of arguments") => {
            Value::Error("ERR Wrong number of args calling command from script".into())
        }
        other => other,
    };
    to_lua(lua, reply)
}

fn err_table(lua: &Lua, msg: &str) -> mlua::Result<LuaValue> {
    let t = lua.create_table()?;
    t.raw_set("err", msg)?;
    Ok(LuaValue::Table(t))
}

/// A command's reply as a script sees it: the RESP2 view, converted as
/// Redis converts it. A nil is `false`; a status is `{ok = ...}`; an error
/// is `{err = ...}`.
fn to_lua(lua: &Lua, v: Value) -> mlua::Result<LuaValue> {
    Ok(match v {
        Value::Simple(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", s)?;
            LuaValue::Table(t)
        }
        Value::Error(e) => {
            let t = lua.create_table()?;
            t.raw_set("err", e)?;
            LuaValue::Table(t)
        }
        Value::Integer(i) => LuaValue::Number(i as f64),
        Value::Bulk(Some(b)) => LuaValue::String(lua.create_string(b)?),
        Value::Bulk(None) | Value::Array(None) | Value::Null => LuaValue::Boolean(false),
        Value::Array(Some(items)) | Value::Set(items) => array(lua, items)?,
        Value::Double(d) => LuaValue::String(lua.create_string(fmt_double(d))?),
        Value::Map(pairs) => {
            let mut flat = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                flat.push(k);
                flat.push(v);
            }
            array(lua, flat)?
        }
        Value::ScorePairs(pairs) => {
            let mut flat = Vec::with_capacity(pairs.len() * 2);
            for (m, s) in pairs {
                flat.push(Value::Bulk(Some(m)));
                flat.push(Value::Bulk(Some(fmt_double(s))));
            }
            array(lua, flat)?
        }
        Value::Resp3Nested(inner) => to_lua(lua, *inner)?,
        Value::ByProto { resp2, .. } => to_lua(lua, *resp2)?,
    })
}

fn array(lua: &Lua, items: Vec<Value>) -> mlua::Result<LuaValue> {
    let t = lua.create_table_with_capacity(items.len(), 0)?;
    for (i, item) in items.into_iter().enumerate() {
        t.raw_set(i + 1, to_lua(lua, item)?)?;
    }
    Ok(LuaValue::Table(t))
}

/// Tables nest; a script can make one that contains itself.
const MAX_REPLY_DEPTH: usize = 1000;

/// A script's return value as a reply, as Redis converts it: a number
/// truncates to an integer, `true` is 1, `false` and nil are nil, a table
/// with `err` is an error and one with `ok` a status, and any other table
/// is an array of its elements from 1 up to the first nil.
fn to_reply(v: &LuaValue, depth: usize) -> Value {
    if depth > MAX_REPLY_DEPTH {
        return Value::Error("ERR reached lua stack limit".into());
    }
    match v {
        LuaValue::Boolean(true) => Value::Integer(1),
        LuaValue::Boolean(false) | LuaValue::Nil => Value::Bulk(None),
        LuaValue::Integer(i) => Value::Integer(*i),
        LuaValue::Number(n) => Value::Integer(*n as i64),
        LuaValue::String(s) => Value::Bulk(Some(s.as_bytes().to_vec())),
        LuaValue::Table(t) => table_reply(t, depth),
        _ => Value::Bulk(None),
    }
}

fn table_reply(t: &Table, depth: usize) -> Value {
    if let Ok(LuaValue::String(e)) = t.raw_get::<LuaValue>("err") {
        return Value::Error(e.to_string_lossy());
    }
    if let Ok(LuaValue::String(s)) = t.raw_get::<LuaValue>("ok") {
        return Value::Simple(s.to_string_lossy());
    }
    let mut out = Vec::new();
    for i in 1.. {
        match t.raw_get::<LuaValue>(i) {
            Ok(LuaValue::Nil) | Err(_) => break,
            Ok(item) => out.push(to_reply(&item, depth + 1)),
        }
    }
    Value::Array(Some(out))
}

/// A Lua number handed to `redis.call`, spelled as Valkey spells it. An
/// integral value within half of `i64`'s range is written as an integer
/// (Valkey's `double2ll`, so `-0.0` is `0` and `1e17` is all its digits);
/// anything else is the shortest digits that read back as the same double,
/// laid out by fpconv's rules (plain up to a point, then `e+N`/`e-N`).
/// Measured against Valkey 9.1: `0.1`, `60000`, `100000000000000000`,
/// `5e+18`, `1e+21`, `1e-7`, `0.3333333333333333`, `1.2345678912345679e+8`,
/// `-2.5e+300`, `nan`, `-inf`.
pub(crate) fn lua_number_arg(n: f64) -> Vec<u8> {
    const HALF: f64 = (i64::MAX / 2) as f64;
    if (-HALF..=HALF).contains(&n) && (n as i64) as f64 == n {
        return (n as i64).to_string().into_bytes();
    }
    if n.is_nan() {
        return b"nan".to_vec();
    }
    if n.is_infinite() {
        return if n > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        };
    }
    let neg = n < 0.0;
    // `{:e}` is Rust's shortest round-trip digits in scientific form:
    // "1.2345678912345679e8".
    let sci = format!("{:e}", n.abs());
    let (mantissa, exp) = sci.split_once('e').expect("{:e} always has an exponent");
    let digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    let exp10: i32 = exp.parse().expect("{:e}'s exponent is an integer");
    // fpconv's K: the power of ten of the LAST digit.
    let nd = digits.len() as i32;
    let k = exp10 - (nd - 1);
    let mut out = Vec::with_capacity(32);
    if neg {
        out.push(b'-');
    }
    let e = (k + nd - 1).abs();
    if k >= 0 && e < nd + 7 {
        out.extend_from_slice(&digits);
        out.extend(std::iter::repeat_n(b'0', k as usize));
        return out;
    }
    if k < 0 && (k > -7 || e < 4) {
        let offset = nd - k.abs();
        if offset <= 0 {
            out.extend_from_slice(b"0.");
            out.extend(std::iter::repeat_n(b'0', (-offset) as usize));
            out.extend_from_slice(&digits);
        } else {
            out.extend_from_slice(&digits[..offset as usize]);
            out.push(b'.');
            out.extend_from_slice(&digits[offset as usize..]);
        }
        return out;
    }
    let nd = digits.len().min(18 - neg as usize);
    out.push(digits[0]);
    if nd > 1 {
        out.push(b'.');
        out.extend_from_slice(&digits[1..nd]);
    }
    out.push(b'e');
    out.push(if k + digits.len() as i32 - 1 < 0 {
        b'-'
    } else {
        b'+'
    });
    out.extend_from_slice(e.to_string().as_bytes());
    out
}

/// The scripts a seat has seen, per namespace, for `EVALSHA` and `SCRIPT
/// EXISTS` (the bounds are `flint_commands::scripts`'). Through the proxy
/// this is not what makes `EVALSHA` work: the proxy keeps the texts and
/// forwards `EVALSHA` as `EVAL`, because a script loaded on one pair runs on
/// whichever pair owns its keys. This serves clients of a seat itself.
static CACHE: flint_commands::scripts::ScriptCache = flint_commands::scripts::ScriptCache::new();

/// Remember `text` under its SHA1 for `ns`.
pub fn remember(ns: &[u8], sha: &str, text: &[u8]) {
    CACHE.remember(ns, sha, text);
}

/// The text of a script `ns` has run or loaded.
pub fn lookup(ns: &[u8], sha: &str) -> Option<Vec<u8>> {
    CACHE.lookup(ns, sha)
}

/// `SCRIPT FLUSH`, for one namespace.
pub fn flush(ns: &[u8]) {
    CACHE.flush(ns);
}

/// Where a row key belongs: `(namespace, user key)` for the metadata,
/// subkey and zscore envelopes, which are every row a command writes.
/// `None` for anything else, which the guard refuses.
fn row_owner(k: &[u8]) -> Option<(&[u8], &[u8])> {
    let cf = *k.first()?;
    let ns_len = *k.get(1)? as usize;
    let ns = k.get(2..2 + ns_len)?;
    let rest = k.get(2 + ns_len + 2..)?;
    match cf {
        b'M' => Some((ns, rest)),
        b'S' | b'Z' => {
            let len = u16::from_be_bytes(rest.get(..2)?.try_into().ok()?) as usize;
            Some((ns, rest.get(2..2 + len)?))
        }
        _ => None,
    }
}

/// The store a script's commands see: every row they read or write must
/// belong to a key in the slot of the keys the script declared, which is
/// Redis Cluster's rule (ADR-0052; ADR-0051 required the declared keys
/// themselves). A command that reaches past them is recorded, and the caller
/// discards everything it did.
///
/// A declared key is covered by the write lock the caller took for the
/// script. An undeclared one in the same slot is not, unless the caller holds
/// the lock over every writer (`every_writer`): without it, a writer of that
/// key could interleave with the script, the race of BUG-0188. So without
/// it, reaching one is `Stray::NeedsEveryWriter`, which abandons the script
/// for the caller to run again under that lock.
///
/// The check is at the row, not the command, so it needs no table of which
/// argument of which command is a key: whatever a command touches is what
/// is checked, and a row it cannot attribute is refused.
pub struct KeyGuard<'a> {
    under: &'a dyn Kv,
    ns: &'a [u8],
    declared: &'a HashSet<Vec<u8>>,
    /// The declared keys' slot; `None` when the script declared none, and
    /// then it may touch no key at all.
    slot: Option<u16>,
    every_writer: bool,
    /// A placed tenant's namespace (ADR-0053): every key of it is on this
    /// seat's pair, so every slot counts as the declared keys' slot.
    whole: bool,
    strayed: Mutex<Option<Stray>>,
}

/// What a script's command reached past its keys for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stray {
    /// A key in another slot, or any key of a script that declared none.
    Key(Vec<u8>),
    /// A key in the declared keys' slot that they do not include, while the
    /// caller holds only their locks. Not a refusal: the script is abandoned
    /// and run again holding the lock over every writer.
    NeedsEveryWriter(Vec<u8>),
    /// Rows no one key owns: a command over the keyspace (`DBSIZE`, `SCAN`,
    /// `FLUSHALL`) or anything else a script has no business in.
    Keyspace,
}

impl Stray {
    /// The error a script's `redis.call` answers.
    pub fn refusal(&self) -> Value {
        match self {
            Stray::Key(k) => Value::Error(format!(
                "ERR Script attempted to access key '{}', which is not in the slot of its KEYS: \
                 a Flint script may touch only keys in the slot of the keys it is passed \
                 (ADR-0052)",
                String::from_utf8_lossy(k)
            )),
            // Reached only by a caller that cannot run the script again
            // (`Dispatcher::wants_every_writer`); `main` always can.
            Stray::NeedsEveryWriter(k) => Value::Error(format!(
                "ERR Script attempted to access key '{}', outside its KEYS, which needs the lock \
                 over every writer and could not take it (ADR-0052)",
                String::from_utf8_lossy(k)
            )),
            Stray::Keyspace => Value::Error(
                "ERR Script attempted a command over the whole keyspace: \
                 a Flint script may touch only keys in the slot of the keys it is passed \
                 (ADR-0052)"
                    .into(),
            ),
        }
    }
}

impl<'a> KeyGuard<'a> {
    /// `every_writer`: whether the caller holds the lock that excludes every
    /// writer, rather than the declared keys' own.
    pub fn new(
        under: &'a dyn Kv,
        ns: &'a [u8],
        declared: &'a HashSet<Vec<u8>>,
        every_writer: bool,
    ) -> Self {
        Self {
            under,
            ns,
            declared,
            slot: declared.iter().next().map(|k| slot_for_key(k)),
            every_writer,
            whole: false,
            strayed: Mutex::new(None),
        }
    }

    /// A placed tenant's script (ADR-0053): any key of the namespace, in any
    /// slot, is treated as a key in the declared keys' slot. Without the
    /// lock over every writer, reaching one still abandons the script to run
    /// again under it. A script that declares no keys still may touch none.
    pub fn whole(mut self, yes: bool) -> Self {
        self.whole = yes;
        self
    }

    /// Whether `key` counts as in the declared keys' slot.
    fn in_slot(&self, key: &[u8]) -> bool {
        self.slot.is_some() && (self.whole || Some(slot_for_key(key)) == self.slot)
    }

    /// The first thing a command reached that the script did not declare.
    pub fn strayed(&self) -> Option<Stray> {
        self.strayed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn allows(&self, row: &[u8]) -> bool {
        match row_owner(row) {
            Some((ns, key)) if ns == self.ns && self.declared.contains(key) => true,
            Some((ns, key))
                if ns == self.ns && !key.is_empty() && self.every_writer && self.in_slot(key) =>
            {
                true
            }
            other => {
                let stray = match other {
                    Some((ns, k)) if ns == self.ns && !k.is_empty() && self.in_slot(k) => {
                        Stray::NeedsEveryWriter(k.to_vec())
                    }
                    Some((_, k)) if !k.is_empty() => Stray::Key(k.to_vec()),
                    _ => Stray::Keyspace,
                };
                let mut s = self.strayed.lock().unwrap_or_else(|e| e.into_inner());
                s.get_or_insert(stray);
                false
            }
        }
    }

    /// A prefix scan is allowed when the prefix itself is a declared key's,
    /// and then shows only that key's rows: a metadata prefix is a byte
    /// prefix of the user key, so it also matches longer keys.
    fn scan_owner(&self, prefix: &[u8]) -> Option<Vec<u8>> {
        if self.allows(prefix) {
            row_owner(prefix).map(|(_, k)| k.to_vec())
        } else {
            None
        }
    }
}

impl Kv for KeyGuard<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.allows(key) {
            self.under.get(key)
        } else {
            None
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        if self.allows(key) {
            self.under.put(key, value);
        }
    }

    fn delete(&self, key: &[u8]) -> bool {
        self.allows(key) && self.under.delete(key)
    }

    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        let Some(owner) = self.scan_owner(prefix) else {
            return;
        };
        self.under.for_each_prefix(prefix, &mut |k, v| {
            if row_owner(k).is_some_and(|(_, key)| key == owner.as_slice()) {
                visit(k, v)
            } else {
                true
            }
        });
    }

    fn for_each_from(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        let Some(owner) = self.scan_owner(prefix) else {
            return;
        };
        self.under.for_each_from(prefix, start_after, &mut |k, v| {
            if row_owner(k).is_some_and(|(_, key)| key == owner.as_slice()) {
                visit(k, v)
            } else {
                true
            }
        });
    }

    fn clear(&self) {
        let mut s = self.strayed.lock().unwrap_or_else(|e| e.into_inner());
        s.get_or_insert(Stray::Keyspace);
    }
}

/// The measurement behind reusing states (ADR-0051's "As built"): run with
/// `cargo test --release -p flint-server --bin flint-server
/// where_a_script_call_spends_its_time -- --ignored --nocapture`.
#[cfg(test)]
mod cost_probe {
    use super::*;

    #[test]
    #[ignore]
    fn where_a_script_call_spends_its_time() {
        let n = 2000;
        let t = Instant::now();
        for _ in 0..n {
            let _ = Lua::new_with(
                StdLib::TABLE | StdLib::STRING | StdLib::MATH,
                LuaOptions::new(),
            )
            .expect("probe");
        }
        eprintln!("new_with:          {:?}/call", t.elapsed() / n);
        let t = Instant::now();
        for _ in 0..n {
            let _ = sandbox(DEFAULT_MEMORY_LIMIT).expect("probe");
        }
        eprintln!("sandbox:           {:?}/call", t.elapsed() / n);
        let lua = sandbox(DEFAULT_MEMORY_LIMIT).expect("probe");
        let t = Instant::now();
        for _ in 0..n {
            let _ = lua
                .load(PRELUDE)
                .set_mode(ChunkMode::Text)
                .into_function()
                .expect("probe");
        }
        eprintln!("compile prelude:   {:?}/call", t.elapsed() / n);
        let t = Instant::now();
        for _ in 0..n {
            let _ = compile(&lua, b"return redis.call('incr', KEYS[1])").expect("probe");
        }
        eprintln!("compile script:    {:?}/call", t.elapsed() / n);
        let call = |_: &[Vec<u8>]| Ok(Value::Integer(1));
        let text = b"return redis.call('incr', KEYS[1])";
        let sha = flint_tls::sha1_hex(text);
        let t = Instant::now();
        for _ in 0..n {
            let _ = run(
                b"ns",
                &sha,
                text,
                &[b"k".to_vec()],
                &[],
                ScriptLimits::default(),
                &call,
            );
        }
        eprintln!("whole run:         {:?}/call", t.elapsed() / n);
    }
}
