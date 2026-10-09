// SPDX-License-Identifier: Elastic-2.0
//! Pub/sub at the proxy (ADR-0052 D5).
//!
//! A tenant subscribes through the proxy and publishes on a seat. Every
//! pair's master holds every subscription of every proxy (the seat's broker,
//! `flint-server`'s `pubsub.rs`), so a `PUBLISH` reaches all of a channel's
//! subscribers from whichever pair it runs on: the pair its channel hashes
//! to, or the pair a transaction runs on.
//!
//! This proxy keeps one subscriber connection (`FLINTSUBSCRIBER`) to each
//! master, a link, however many clients subscribe. On it the proxy registers
//! how many of its clients hold each channel and pattern, and the seat sends
//! each message once for this proxy, which hands it to every client holding
//! it.
//!
//! - **Counts, reconciled.** A link does not relay each client's change. It
//!   sends the difference between the count this proxy holds now and the
//!   count it last registered there. A new connection starts from zero at the
//!   seat, so it registers everything again, and that is the whole of
//!   failover: a master that changes, restarts or cuts the link off gets this
//!   proxy's subscriptions back on the next connection. Messages published in
//!   between are lost, as they are when a Redis client reconnects: delivery
//!   is at most once.
//! - **A subscribe is answered once it is registered.** A client is told it
//!   is subscribed once every master reachable now holds the subscription,
//!   so a message published after the reply reaches it.
//! - **A slow client is cut off, not buffered without bound**: past
//!   [`CLIENT_LIMIT_BYTES`] queued, as Redis does with its default
//!   `client-output-buffer-limit pubsub 32mb`.
//! - **Links outlive their last subscription by [`LINK_IDLE`]**, so a client
//!   that subscribes for each result it awaits (Celery's result backend)
//!   does not dial every master each time.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

use crate::Topology;

/// What a client may queue before it is disconnected: Redis's default hard
/// limit for a pub/sub client.
pub(crate) const CLIENT_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// Clients cut off for falling [`CLIENT_LIMIT_BYTES`] behind,
/// `pubsub_clients_cut_total` in `PROXYSTATS`.
pub(crate) static CLIENTS_CUT_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Times a link was dialed again after its connection failed or was lost,
/// `pubsub_link_redials_total`. A seat that cut the link off for falling
/// behind is among them; that seat logs and counts the cut-off itself.
pub(crate) static LINK_REDIALS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Messages handed to clients' connections to write,
/// `pubsub_messages_total`: one per client a message reaches.
pub(crate) static MESSAGES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// How long a link stays open after this proxy's last subscription ends.
pub(crate) const LINK_IDLE: Duration = Duration::from_secs(60);

/// The longest a `SUBSCRIBE` waits for the masters to register it. Past it,
/// the client is answered anyway: the subscription stands, and a master that
/// has not registered it yet will as soon as its link does.
const SUBSCRIBE_WAIT: Duration = Duration::from_secs(2);

/// How often the links are checked against the masters, so a failover moves
/// them to the new master.
const SUPERVISE_EVERY: Duration = Duration::from_millis(250);

/// A link sends `PING` this often, and a link that has heard nothing for
/// [`LINK_SILENCE`] is closed and dialed again: a half-open connection
/// would otherwise hold subscriptions on a seat that no longer delivers.
const LINK_PING: Duration = Duration::from_secs(1);
const LINK_SILENCE: Duration = Duration::from_secs(5);
const LINK_DIAL_BUDGET: Duration = Duration::from_secs(3);
const LINK_BACKOFF_FIRST: Duration = Duration::from_millis(50);
const LINK_BACKOFF_MAX: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Kind {
    Channel,
    Pattern,
}

impl Kind {
    fn register(self) -> &'static [u8] {
        match self {
            Kind::Channel => b"FLINTSUB",
            Kind::Pattern => b"FLINTPSUB",
        }
    }

    fn subscribed(self) -> &'static [u8] {
        match self {
            Kind::Channel => b"subscribe",
            Kind::Pattern => b"psubscribe",
        }
    }

    fn unsubscribed(self) -> &'static [u8] {
        match self {
            Kind::Channel => b"unsubscribe",
            Kind::Pattern => b"punsubscribe",
        }
    }
}

/// Namespace, kind, and the channel or pattern.
type SubKey = (Vec<u8>, Kind, Vec<u8>);

/// A message on its way to clients, shared by every client that holds its
/// channel or pattern.
pub(crate) struct Message {
    pattern: Option<Vec<u8>>,
    channel: Vec<u8>,
    payload: Vec<u8>,
}

impl Message {
    fn size(&self) -> usize {
        self.pattern.as_ref().map_or(0, Vec::len) + self.channel.len() + self.payload.len()
    }

    /// `message <channel> <payload>`, or `pmessage <pattern> <channel>
    /// <payload>`: a push to a RESP3 client and an array to a RESP2 one.
    pub(crate) fn frame(&self) -> Value {
        let bulk = |b: &[u8]| Value::Bulk(Some(b.to_vec()));
        Value::Push(match &self.pattern {
            None => vec![bulk(b"message"), bulk(&self.channel), bulk(&self.payload)],
            Some(p) => vec![
                bulk(b"pmessage"),
                bulk(p),
                bulk(&self.channel),
                bulk(&self.payload),
            ],
        })
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// What one client has been sent nothing of yet.
#[derive(Default)]
pub(crate) struct Outbox {
    queue: Mutex<VecDeque<Arc<Message>>>,
    bytes: AtomicUsize,
    overflowed: AtomicBool,
    /// Signalled on each message, and when the client falls too far behind.
    pub(crate) ready: Notify,
}

impl Outbox {
    fn push(&self, m: Arc<Message>) {
        if self.overflowed.load(Ordering::Relaxed) {
            return;
        }
        let size = m.size();
        if self.bytes.fetch_add(size, Ordering::Relaxed) + size > CLIENT_LIMIT_BYTES {
            // Counted once, by whichever push got here first.
            if !self.overflowed.swap(true, Ordering::Relaxed) {
                CLIENTS_CUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            lock(&self.queue).clear();
        } else {
            lock(&self.queue).push_back(m);
        }
        self.ready.notify_one();
    }

    /// The client fell [`CLIENT_LIMIT_BYTES`] behind, and is to be closed.
    pub(crate) fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Relaxed)
    }

    /// Resolves once the client has fallen [`CLIENT_LIMIT_BYTES`] behind.
    pub(crate) async fn cut_off(&self) {
        loop {
            let ready = self.ready.notified();
            if self.overflowed() {
                return;
            }
            ready.await;
        }
    }

    pub(crate) fn drain(&self) -> Vec<Arc<Message>> {
        let taken: Vec<Arc<Message>> = lock(&self.queue).drain(..).collect();
        let size: usize = taken.iter().map(|m| m.size()).sum();
        self.bytes.fetch_sub(size, Ordering::Relaxed);
        MESSAGES_TOTAL.fetch_add(taken.len() as u64, Ordering::Relaxed);
        taken
    }
}

/// One master's link, as the hub sees it.
struct LinkState {
    /// Tells the link's task it is still the link to this address.
    id: u64,
    /// Subscriptions whose count changed since the link last registered.
    dirty: HashSet<SubKey>,
    /// Every change up to this sequence number is registered at the master.
    synced: u64,
    /// Its last connection failed or was lost: a subscribe does not wait for
    /// it, and it registers everything when it connects again.
    failing: bool,
    /// It holds a connection to the master now (`pubsub_links`).
    connected: bool,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct State {
    seq: u64,
    /// The clients holding each subscription, by client id.
    clients: HashMap<SubKey, HashMap<u64, Arc<Outbox>>>,
    links: HashMap<String, LinkState>,
    /// When a client last held a subscription, for [`LINK_IDLE`].
    last_used: Option<Instant>,
}

/// This proxy's subscriptions and its links. One per process ([`HUB`]).
#[derive(Default)]
pub(crate) struct Hub {
    state: Mutex<State>,
    /// Notified when a link registers changes or fails.
    progress: Notify,
    topo: OnceLock<Arc<Topology>>,
    rt: OnceLock<tokio::runtime::Handle>,
    next_id: AtomicU64,
}

pub(crate) static HUB: LazyLock<Hub> = LazyLock::new(Hub::default);

impl Hub {
    fn lock(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Run the links on a thread of their own, following `topo`'s masters.
    pub(crate) fn start(&'static self, topo: Arc<Topology>) -> std::io::Result<()> {
        if self.topo.set(topo).is_err() {
            return Ok(());
        }
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("pubsub-links".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
                let _ = tx.send(Ok(rt.handle().clone()));
                rt.block_on(async {
                    loop {
                        self.reconcile_links();
                        tokio::time::sleep(SUPERVISE_EVERY).await;
                    }
                });
            })?;
        let handle = rx
            .recv()
            .map_err(|_| std::io::Error::other("pub/sub link thread did not start"))??;
        let _ = self.rt.set(handle);
        Ok(())
    }

    /// Open a link to each master that has none, and close the links to
    /// addresses that are no longer masters, or every link once no client
    /// has subscribed for [`LINK_IDLE`].
    fn reconcile_links(&'static self) {
        let (Some(topo), Some(rt)) = (self.topo.get(), self.rt.get()) else {
            return;
        };
        let masters: HashSet<String> = topo.all_masters().into_iter().collect();
        let mut st = self.lock();
        let wanted =
            !st.clients.is_empty() || st.last_used.is_some_and(|t| t.elapsed() < LINK_IDLE);
        st.links.retain(|addr, l| {
            let keep = wanted && masters.contains(addr);
            if !keep {
                l.wake.notify_one();
            }
            keep
        });
        if !wanted {
            return;
        }
        for addr in masters {
            if st.links.contains_key(&addr) {
                continue;
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let wake = Arc::new(Notify::new());
            st.links.insert(
                addr.clone(),
                LinkState {
                    id,
                    dirty: HashSet::new(),
                    synced: 0,
                    failing: false,
                    connected: false,
                    wake: wake.clone(),
                },
            );
            rt.spawn(self.link(addr, id, wake));
        }
    }

    /// Clients `id` now holds `names`. Answers the sequence number to wait
    /// for ([`Hub::registered`]).
    fn add(
        &'static self,
        ns: &[u8],
        kind: Kind,
        names: &[Vec<u8>],
        id: u64,
        out: &Arc<Outbox>,
    ) -> u64 {
        let seq = {
            let mut st = self.lock();
            st.seq += 1;
            let st = &mut *st;
            for name in names {
                let key = (ns.to_vec(), kind, name.clone());
                st.clients
                    .entry(key.clone())
                    .or_default()
                    .insert(id, out.clone());
                for l in st.links.values_mut() {
                    l.dirty.insert(key.clone());
                }
            }
            st.last_used = Some(Instant::now());
            for l in st.links.values() {
                l.wake.notify_one();
            }
            st.seq
        };
        self.reconcile_links();
        seq
    }

    /// Client `id` no longer holds `names`. Nothing waits for the masters:
    /// a message they still send for it is not handed to this client.
    fn remove(&self, ns: &[u8], kind: Kind, names: &[Vec<u8>], id: u64) {
        let mut st = self.lock();
        st.seq += 1;
        let st = &mut *st;
        for name in names {
            let key = (ns.to_vec(), kind, name.clone());
            let Some(holders) = st.clients.get_mut(&key) else {
                continue;
            };
            holders.remove(&id);
            if holders.is_empty() {
                st.clients.remove(&key);
            }
            for l in st.links.values_mut() {
                l.dirty.insert(key.clone());
            }
        }
        st.last_used = Some(Instant::now());
        for l in st.links.values() {
            l.wake.notify_one();
        }
    }

    /// Wait until every master that can be reached holds the changes up to
    /// `seq`, or [`SUBSCRIBE_WAIT`] passes.
    async fn registered(&self, seq: u64) {
        let deadline = tokio::time::Instant::now() + SUBSCRIBE_WAIT;
        loop {
            let notified = self.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .lock()
                .links
                .values()
                .all(|l| l.failing || l.synced >= seq)
            {
                return;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return;
            }
        }
    }

    /// `(pubsub_links, pubsub_clients)` for `PROXYSTATS`: the links
    /// connected to a master now, and the client connections holding at
    /// least one subscription.
    pub(crate) fn gauges(&self) -> (usize, usize) {
        let st = self.lock();
        let links = st.links.values().filter(|l| l.connected).count();
        let clients: HashSet<u64> = st
            .clients
            .values()
            .flat_map(|c| c.keys().copied())
            .collect();
        (links, clients.len())
    }

    /// Hand a message from a master to every client holding `name`.
    fn deliver(&self, ns: &[u8], kind: Kind, name: &[u8], m: Message) {
        let st = self.lock();
        let Some(holders) = st.clients.get(&(ns.to_vec(), kind, name.to_vec())) else {
            return;
        };
        let m = Arc::new(m);
        for out in holders.values() {
            out.push(m.clone());
        }
    }

    /// Run `f` on this link's state, unless the link has been replaced or
    /// closed, which `None` answers.
    fn with_link<R>(&self, addr: &str, id: u64, f: impl FnOnce(&mut State) -> R) -> Option<R> {
        let mut st = self.lock();
        if st.links.get(addr).is_none_or(|l| l.id != id) {
            return None;
        }
        Some(f(&mut st))
    }

    /// One master's link, for as long as that address is a master and this
    /// proxy has subscriptions: connected, and dialed again when it is not.
    async fn link(&'static self, addr: String, id: u64, wake: Arc<Notify>) {
        let mut backoff = LINK_BACKOFF_FIRST;
        loop {
            let (connected, err) = match self.run_link(&addr, id, &wake).await {
                Ok(()) => return,
                Err(e) => e,
            };
            if connected {
                backoff = LINK_BACKOFF_FIRST;
            }
            let still = self.with_link(&addr, id, |st| {
                if let Some(l) = st.links.get_mut(&addr) {
                    l.failing = true;
                    l.connected = false;
                }
            });
            self.progress.notify_waiters();
            if still.is_none() {
                return;
            }
            LINK_REDIALS_TOTAL.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[{}] pubsub link to {addr}: {err}; dialing again in {} ms",
                crate::log_ms(),
                backoff.as_millis()
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(LINK_BACKOFF_MAX);
        }
    }

    /// One connection of a link. `Ok` when the link is closed on purpose;
    /// otherwise the error, and whether it had connected.
    async fn run_link(
        &'static self,
        addr: &str,
        id: u64,
        wake: &Notify,
    ) -> Result<(), (bool, std::io::Error)> {
        let fail = |e: std::io::Error| (false, e);
        let topo = self
            .topo
            .get()
            .ok_or_else(|| fail(std::io::Error::other("not started")))?;
        let mut buf: Vec<u8> = Vec::new();
        let stream = tokio::time::timeout(LINK_DIAL_BUDGET, async {
            let mut s = flint_tls::aio::connect_reloadable(addr, &topo.backend_tls).await?;
            s.write_all(&command(&[b"FLINTSUBSCRIBER"])).await?;
            s.flush().await?;
            match read_one(&mut s, &mut buf).await? {
                Value::Simple(ok) if ok == "OK" => Ok(s),
                other => Err(std::io::Error::other(format!(
                    "seat refused FLINTSUBSCRIBER: {other:?}"
                ))),
            }
        })
        .await
        .map_err(|_| fail(std::io::Error::other("dial timed out")))?
        .map_err(fail)?;

        // A new connection holds nothing at the seat: register everything.
        let started = self.with_link(addr, id, |st| {
            let keys: HashSet<SubKey> = st.clients.keys().cloned().collect();
            if let Some(l) = st.links.get_mut(addr) {
                l.dirty = keys;
                l.failing = false;
                l.connected = true;
            }
        });
        if started.is_none() {
            return Ok(());
        }
        let lost = |e: std::io::Error| (true, e);

        enum Expect {
            Count(SubKey),
            Pong,
            Synced(u64),
        }
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut applied: HashMap<SubKey, u64> = HashMap::new();
        let mut expect: VecDeque<Expect> = VecDeque::new();
        let mut pending: Vec<u8> = Vec::new();
        let mut unflushed = false;
        let mut need_sync = true;
        let mut synced: Option<u64> = None;
        let mut chunk = vec![0u8; 64 * 1024];
        let mut heard = Instant::now();
        let mut ping = tokio::time::interval(LINK_PING);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if need_sync {
                need_sync = false;
                let batch = self.with_link(addr, id, |st| {
                    let seq = st.seq;
                    let dirty = st
                        .links
                        .get_mut(addr)
                        .map(|l| std::mem::take(&mut l.dirty))
                        .unwrap_or_default();
                    let wants: Vec<(SubKey, u64)> = dirty
                        .into_iter()
                        .map(|k| {
                            let n = st.clients.get(&k).map_or(0, |h| h.len() as u64);
                            (k, n)
                        })
                        .collect();
                    (seq, wants)
                });
                let Some((seq, wants)) = batch else {
                    return Ok(());
                };
                for (key, want) in wants {
                    let have = applied.get(&key).copied().unwrap_or(0);
                    if want == have {
                        continue;
                    }
                    let delta = (want as i64 - have as i64).to_string();
                    pending.extend_from_slice(&command(&[
                        key.1.register(),
                        &key.0,
                        &key.2,
                        delta.as_bytes(),
                    ]));
                    expect.push_back(Expect::Count(key));
                }
                expect.push_back(Expect::Synced(seq));
            }
            // Changes with nothing left to answer are registered.
            while let Some(Expect::Synced(seq)) = expect.front() {
                synced = Some(*seq);
                expect.pop_front();
            }
            if let Some(seq) = synced.take() {
                let current = self.with_link(addr, id, |st| {
                    if let Some(l) = st.links.get_mut(addr) {
                        l.synced = l.synced.max(seq);
                    }
                });
                self.progress.notify_waiters();
                if current.is_none() {
                    return Ok(());
                }
            }
            tokio::select! {
                _ = wake.notified() => {
                    need_sync = true;
                }
                _ = ping.tick() => {
                    if heard.elapsed() > LINK_SILENCE {
                        return Err(lost(std::io::Error::other(format!(
                            "nothing heard for {} s",
                            LINK_SILENCE.as_secs()
                        ))));
                    }
                    pending.extend_from_slice(&command(&[b"PING"]));
                    expect.push_back(Expect::Pong);
                }
                // Written as the seat reads, never all at once: a link that
                // blocked on a full socket while the seat blocked writing it
                // messages would wait on itself.
                r = async {
                    if pending.is_empty() {
                        wr.flush().await.map(|()| None)
                    } else {
                        wr.write(&pending).await.map(Some)
                    }
                }, if !pending.is_empty() || unflushed => {
                    match r.map_err(lost)? {
                        Some(n) => {
                            pending.drain(..n);
                            unflushed = true;
                        }
                        None => unflushed = false,
                    }
                }
                r = rd.read(&mut chunk) => {
                    let n = r.map_err(lost)?;
                    if n == 0 {
                        return Err(lost(std::io::Error::other("seat closed the link")));
                    }
                    heard = Instant::now();
                    buf.extend_from_slice(&chunk[..n]);
                    let mut used = 0;
                    loop {
                        let v = match decode(&buf[used..]) {
                            Ok(Decoded::Complete(v, n)) => {
                                used += n;
                                v
                            }
                            Ok(Decoded::NeedMore) => break,
                            Err(e) => {
                                return Err(lost(std::io::Error::other(format!(
                                    "undecodable frame from the seat: {e:?}"
                                ))));
                            }
                        };
                        if let Value::Array(Some(items)) = v {
                            self.message(items);
                            continue;
                        }
                        match (v, expect.pop_front()) {
                            (Value::Integer(n), Some(Expect::Count(key))) => {
                                // The seat's count is the truth: the next
                                // change is sent against it.
                                if n > 0 {
                                    applied.insert(key, n as u64);
                                } else {
                                    applied.remove(&key);
                                }
                            }
                            (Value::Simple(p), Some(Expect::Pong)) if p == "PONG" => {}
                            (other, _) => {
                                return Err(lost(std::io::Error::other(format!(
                                    "unexpected reply on the link: {other:?}"
                                ))));
                            }
                        }
                        while let Some(Expect::Synced(seq)) = expect.front() {
                            synced = Some(*seq);
                            expect.pop_front();
                        }
                    }
                    buf.drain(..used);
                }
            }
        }
    }

    /// `flintmessage <ns> <channel> <payload>` or `flintpmessage <ns>
    /// <pattern> <channel> <payload>`.
    fn message(&self, items: Vec<Value>) {
        let mut parts = items.into_iter().map(|v| match v {
            Value::Bulk(Some(b)) => b,
            _ => Vec::new(),
        });
        let (Some(kind), Some(ns)) = (parts.next(), parts.next()) else {
            return;
        };
        let rest: Vec<Vec<u8>> = parts.collect();
        match (kind.as_slice(), rest.as_slice()) {
            (b"flintmessage", [channel, payload]) => self.deliver(
                &ns,
                Kind::Channel,
                channel,
                Message {
                    pattern: None,
                    channel: channel.clone(),
                    payload: payload.clone(),
                },
            ),
            (b"flintpmessage", [pattern, channel, payload]) => self.deliver(
                &ns,
                Kind::Pattern,
                pattern,
                Message {
                    pattern: Some(pattern.clone()),
                    channel: channel.clone(),
                    payload: payload.clone(),
                },
            ),
            _ => {}
        }
    }
}

fn command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            parts
                .iter()
                .map(|p| Value::Bulk(Some(p.to_vec())))
                .collect(),
        )),
        &mut out,
    );
    out
}

async fn read_one<S: tokio::io::AsyncRead + Unpin>(
    s: &mut S,
    buf: &mut Vec<u8>,
) -> std::io::Result<Value> {
    let mut chunk = [0u8; 4096];
    loop {
        match decode(buf) {
            Ok(Decoded::Complete(v, used)) => {
                buf.drain(..used);
                return Ok(v);
            }
            Ok(Decoded::NeedMore) => {}
            Err(e) => return Err(std::io::Error::other(format!("decode: {e:?}"))),
        }
        match s.read(&mut chunk).await? {
            0 => return Err(std::io::Error::other("seat closed the connection")),
            n => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// Names in the order they were subscribed, which is the order an
/// `UNSUBSCRIBE` with no arguments answers in, as Valkey's does.
#[derive(Default)]
struct Names {
    next: u64,
    by_name: HashMap<Vec<u8>, u64>,
    in_order: BTreeMap<u64, Vec<u8>>,
}

impl Names {
    fn insert(&mut self, name: &[u8]) -> bool {
        if self.by_name.contains_key(name) {
            return false;
        }
        self.next += 1;
        self.by_name.insert(name.to_vec(), self.next);
        self.in_order.insert(self.next, name.to_vec());
        true
    }

    fn remove(&mut self, name: &[u8]) -> bool {
        let Some(at) = self.by_name.remove(name) else {
            return false;
        };
        self.in_order.remove(&at);
        true
    }

    fn all(&self) -> Vec<Vec<u8>> {
        self.in_order.values().cloned().collect()
    }

    fn len(&self) -> usize {
        self.by_name.len()
    }
}

/// One client connection's subscriptions. Dropping it, as the connection
/// ends, drops them all.
pub(crate) struct Subscriber {
    id: u64,
    ns: Vec<u8>,
    pub(crate) outbox: Arc<Outbox>,
    channels: Names,
    patterns: Names,
}

impl Subscriber {
    pub(crate) fn new(ns: &[u8]) -> Self {
        Self {
            id: HUB.next_id.fetch_add(1, Ordering::Relaxed),
            ns: ns.to_vec(),
            outbox: Arc::default(),
            channels: Names::default(),
            patterns: Names::default(),
        }
    }

    /// How many channels and patterns this client holds, which is what each
    /// confirmation counts.
    pub(crate) fn count(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }

    fn names(&mut self, kind: Kind) -> &mut Names {
        match kind {
            Kind::Channel => &mut self.channels,
            Kind::Pattern => &mut self.patterns,
        }
    }

    fn confirm(&self, word: &[u8], name: Option<&[u8]>) -> Value {
        Value::Push(vec![
            Value::Bulk(Some(word.to_vec())),
            name.map_or(Value::Null, |n| Value::Bulk(Some(n.to_vec()))),
            Value::Integer(self.count() as i64),
        ])
    }

    /// `SUBSCRIBE` or `PSUBSCRIBE`: one confirmation per name, sent once the
    /// masters hold the new ones.
    pub(crate) async fn subscribe(&mut self, kind: Kind, names: &[Vec<u8>]) -> Vec<Value> {
        let mut frames = Vec::with_capacity(names.len());
        let mut added = Vec::new();
        for name in names {
            if self.names(kind).insert(name) {
                added.push(name.clone());
            }
            frames.push(self.confirm(kind.subscribed(), Some(name)));
        }
        if !added.is_empty() {
            let seq = HUB.add(&self.ns, kind, &added, self.id, &self.outbox);
            HUB.registered(seq).await;
        }
        frames
    }

    /// `UNSUBSCRIBE` or `PUNSUBSCRIBE`: of `names`, or of every one held
    /// when none is named.
    pub(crate) fn unsubscribe(&mut self, kind: Kind, names: &[Vec<u8>]) -> Vec<Value> {
        let names = if names.is_empty() {
            self.names(kind).all()
        } else {
            names.to_vec()
        };
        if names.is_empty() {
            return vec![self.confirm(kind.unsubscribed(), None)];
        }
        let mut frames = Vec::with_capacity(names.len());
        let mut removed = Vec::new();
        for name in &names {
            if self.names(kind).remove(name) {
                removed.push(name.clone());
            }
            frames.push(self.confirm(kind.unsubscribed(), Some(name)));
        }
        if !removed.is_empty() {
            HUB.remove(&self.ns, kind, &removed, self.id);
        }
        frames
    }
}

impl Subscriber {
    /// Say that this client was cut off for falling [`CLIENT_LIMIT_BYTES`]
    /// behind: the one trace a tenant's lost messages leave. `id` and `name`
    /// are the connection's, as `CLIENT ID` and `CLIENT SETNAME` know it.
    pub(crate) fn log_cut_off(&self, id: u64, name: Option<&[u8]>) {
        let name = name.map_or_else(String::new, |n| {
            format!(" ({})", String::from_utf8_lossy(n))
        });
        eprintln!(
            "[{}] pubsub: client {id}{name} of namespace {} cut off, {} MiB behind (ADR-0052): \
             it was disconnected, and the messages queued for it were dropped",
            crate::log_ms(),
            String::from_utf8_lossy(&self.ns),
            CLIENT_LIMIT_BYTES >> 20
        );
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let channels = self.channels.all();
        let patterns = self.patterns.all();
        if !channels.is_empty() {
            HUB.remove(&self.ns, Kind::Channel, &channels, self.id);
        }
        if !patterns.is_empty() {
            HUB.remove(&self.ns, Kind::Pattern, &patterns, self.id);
        }
    }
}

/// The commands that change a connection's subscriptions, which the proxy
/// answers itself.
pub(crate) fn subscription_kind(name: &[u8]) -> Option<(Kind, bool)> {
    let upper = name.to_ascii_uppercase();
    Some(match upper.as_slice() {
        b"SUBSCRIBE" => (Kind::Channel, true),
        b"PSUBSCRIBE" => (Kind::Pattern, true),
        b"UNSUBSCRIBE" => (Kind::Channel, false),
        b"PUNSUBSCRIBE" => (Kind::Pattern, false),
        _ => return None,
    })
}

/// What a RESP2 client may send while it holds a subscription; Redis
/// refuses the rest.
pub(crate) fn allowed_while_subscribed(name: &[u8]) -> bool {
    let upper = name.to_ascii_uppercase();
    subscription_kind(&upper).is_some()
        || matches!(
            upper.as_slice(),
            b"PING" | b"QUIT" | b"RESET" | b"SSUBSCRIBE" | b"SUNSUBSCRIBE"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(b: &[u8]) -> Value {
        Value::Bulk(Some(b.to_vec()))
    }

    /// Valkey's confirmations, in both protocols: one per name, counting
    /// channels and patterns together, and a null name when there was
    /// nothing to unsubscribe.
    #[tokio::test]
    async fn confirmations_count_what_the_client_holds() {
        let mut s = Subscriber::new(b"confirm-test");
        let f = s
            .subscribe(Kind::Channel, &[b"a".to_vec(), b"b".to_vec()])
            .await;
        assert_eq!(
            f,
            vec![
                Value::Push(vec![bulk(b"subscribe"), bulk(b"a"), Value::Integer(1)]),
                Value::Push(vec![bulk(b"subscribe"), bulk(b"b"), Value::Integer(2)]),
            ]
        );
        let f = s.subscribe(Kind::Channel, &[b"a".to_vec()]).await;
        assert_eq!(
            f[0],
            Value::Push(vec![bulk(b"subscribe"), bulk(b"a"), Value::Integer(2)])
        );
        let f = s.subscribe(Kind::Pattern, &[b"a*".to_vec()]).await;
        assert_eq!(
            f[0],
            Value::Push(vec![bulk(b"psubscribe"), bulk(b"a*"), Value::Integer(3)])
        );
        // Not held: answered with the count unchanged.
        let f = s.unsubscribe(Kind::Channel, &[b"zz".to_vec()]);
        assert_eq!(
            f[0],
            Value::Push(vec![bulk(b"unsubscribe"), bulk(b"zz"), Value::Integer(3)])
        );
        // All, in the order subscribed.
        let f = s.unsubscribe(Kind::Channel, &[]);
        assert_eq!(
            f,
            vec![
                Value::Push(vec![bulk(b"unsubscribe"), bulk(b"a"), Value::Integer(2)]),
                Value::Push(vec![bulk(b"unsubscribe"), bulk(b"b"), Value::Integer(1)]),
            ]
        );
        let f = s.unsubscribe(Kind::Channel, &[]);
        assert_eq!(
            f,
            vec![Value::Push(vec![
                bulk(b"unsubscribe"),
                Value::Null,
                Value::Integer(1)
            ])]
        );
        assert_eq!(s.count(), 1);
        drop(s);
        assert!(
            !HUB.lock().clients.keys().any(|k| k.0 == b"confirm-test"),
            "a dropped connection holds nothing"
        );
    }

    #[tokio::test]
    async fn a_message_reaches_each_holder_once_and_a_slow_one_is_cut_off() {
        let mut a = Subscriber::new(b"deliver-test");
        let mut b = Subscriber::new(b"deliver-test");
        a.subscribe(Kind::Channel, &[b"c".to_vec()]).await;
        b.subscribe(Kind::Pattern, &[b"c*".to_vec()]).await;
        HUB.message(vec![
            bulk(b"flintmessage"),
            bulk(b"deliver-test"),
            bulk(b"c"),
            bulk(b"hi"),
        ]);
        HUB.message(vec![
            bulk(b"flintpmessage"),
            bulk(b"deliver-test"),
            bulk(b"c*"),
            bulk(b"cx"),
            bulk(b"ho"),
        ]);
        // Another namespace's channel of the same name reaches neither.
        HUB.message(vec![
            bulk(b"flintmessage"),
            bulk(b"other"),
            bulk(b"c"),
            bulk(b"no"),
        ]);
        let handed = MESSAGES_TOTAL.load(Ordering::Relaxed);
        let got_a: Vec<Value> = a.outbox.drain().iter().map(|m| m.frame()).collect();
        let got_b: Vec<Value> = b.outbox.drain().iter().map(|m| m.frame()).collect();
        // Other tests drain too, so at least these two.
        assert!(MESSAGES_TOTAL.load(Ordering::Relaxed) >= handed + 2);
        assert_eq!(
            got_a,
            vec![Value::Push(vec![bulk(b"message"), bulk(b"c"), bulk(b"hi")])]
        );
        assert_eq!(
            got_b,
            vec![Value::Push(vec![
                bulk(b"pmessage"),
                bulk(b"c*"),
                bulk(b"cx"),
                bulk(b"ho")
            ])]
        );
        let cut = CLIENTS_CUT_TOTAL.load(Ordering::Relaxed);
        let big = vec![b'x'; CLIENT_LIMIT_BYTES / 4];
        for _ in 0..6 {
            HUB.message(vec![
                bulk(b"flintmessage"),
                bulk(b"deliver-test"),
                bulk(b"c"),
                bulk(&big),
            ]);
        }
        assert!(a.outbox.overflowed());
        assert!(a.outbox.drain().is_empty(), "what it held is released");
        assert!(!b.outbox.overflowed());
        // Counted once, though a message reached it after the cut too. No
        // other test cuts a client off, so the counter moves by one.
        assert_eq!(CLIENTS_CUT_TOTAL.load(Ordering::Relaxed), cut + 1);
    }

    #[test]
    fn a_resp2_subscriber_may_send_only_what_redis_allows() {
        for ok in [
            &b"subscribe"[..],
            b"PUNSUBSCRIBE",
            b"ping",
            b"QUIT",
            b"RESET",
        ] {
            assert!(allowed_while_subscribed(ok), "{ok:?}");
        }
        for no in [&b"GET"[..], b"PUBLISH", b"MULTI", b"HELLO", b"AUTH"] {
            assert!(!allowed_while_subscribed(no), "{no:?}");
        }
    }
}
