// SPDX-License-Identifier: Elastic-2.0
//! Pub/sub at the seat (ADR-0052 D5): the broker for the namespaces this
//! seat serves.
//!
//! Tenants never connect here to subscribe. A proxy opens one subscriber
//! connection to each master (`FLINTSUBSCRIBER`) and registers, per
//! namespace, how many of its own clients want each channel or pattern
//! (`FLINTSUB`, `FLINTPSUB`, with a signed delta). A `PUBLISH` on any
//! connection of the namespace is answered with the number of clients that
//! receive it, and one frame per interested subscriber connection is queued
//! for that connection's writer thread, which writes it at once; the proxy
//! fans it out to its clients.
//!
//! Every proxy registers every subscription at every master of the
//! namespace's pairs, so a message may be published on whichever pair a
//! command reaches, a transaction's included, and each subscriber still
//! receives it exactly once. That is also why a slot moving between pairs
//! needs nothing here.
//!
//! Delivery is at most once, as in Redis. A subscriber connection that
//! falls [`OUTBOX_LIMIT_BYTES`] behind is dropped, and its proxy
//! reconnects and registers again, as Redis drops a pub/sub client past
//! its output-buffer limit.
//!
//! A `PUBLISH` inside a transaction or a script is counted when it runs but
//! written only once the writes around it commit ([`Deferral`]), and not at
//! all if they do not: a subscriber must not hear of a write that did not
//! happen. Celery writes a task's result and publishes it in one `MULTI`.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};

use flint_resp::{Decoded, Value, decode, encode};

/// A subscriber connection, as the broker knows it.
pub type SubId = u64;

/// How far a subscriber connection may fall behind before it is dropped:
/// Redis's default hard limit for a pub/sub client.
pub const OUTBOX_LIMIT_BYTES: usize = 32 * 1024 * 1024;

/// Channel subscriptions and pattern subscriptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Channel,
    Pattern,
}

/// Frames queued for one subscriber connection: its messages and its
/// replies, in order, written by the connection's writer thread.
#[derive(Default)]
pub struct Outbox {
    queue: Mutex<VecDeque<Vec<u8>>>,
    ready: Condvar,
    bytes: AtomicUsize,
    overflowed: AtomicBool,
    closed: AtomicBool,
    /// The connection's socket, to cut it off on overflow without waiting
    /// for its writer, which may be stuck in a write to a proxy that stopped
    /// reading.
    socket: Mutex<Option<std::net::TcpStream>>,
}

impl Outbox {
    fn push(&self, frame: Vec<u8>) {
        if self.overflowed.load(Ordering::Relaxed) {
            return;
        }
        let len = frame.len();
        if self.bytes.fetch_add(len, Ordering::Relaxed) + len > OUTBOX_LIMIT_BYTES {
            self.overflowed.store(true, Ordering::Relaxed);
            self.lock().clear();
            if let Some(sock) = self
                .socket
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                let _ = sock.shutdown(std::net::Shutdown::Both);
            }
            self.ready.notify_all();
            return;
        }
        self.lock().push_back(frame);
        self.ready.notify_all();
    }

    /// Everything queued so far, oldest first.
    #[cfg(test)]
    pub fn drain(&self) -> Vec<Vec<u8>> {
        let frames: Vec<Vec<u8>> = self.lock().drain(..).collect();
        let n: usize = frames.iter().map(Vec::len).sum();
        self.bytes.fetch_sub(n, Ordering::Relaxed);
        frames
    }

    /// Wait for frames, and take them; `None` once the connection is over.
    fn wait_drain(&self) -> Option<Vec<Vec<u8>>> {
        let mut q = self.lock();
        loop {
            if self.closed.load(Ordering::Relaxed) || self.overflowed() {
                return None;
            }
            if !q.is_empty() {
                let frames: Vec<Vec<u8>> = q.drain(..).collect();
                let n: usize = frames.iter().map(Vec::len).sum();
                self.bytes.fetch_sub(n, Ordering::Relaxed);
                return Some(frames);
            }
            q = self.ready.wait(q).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        let _q = self.lock();
        self.ready.notify_all();
    }

    /// The connection fell too far behind; it is being closed.
    pub fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Relaxed)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Vec<u8>>> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// One namespace's subscriptions: each channel or pattern, and how many
/// clients of each subscriber connection want it.
#[derive(Default)]
struct Subs {
    channels: HashMap<Vec<u8>, HashMap<SubId, u64>>,
    patterns: HashMap<Vec<u8>, HashMap<SubId, u64>>,
}

impl Subs {
    fn of(&mut self, kind: Kind) -> &mut HashMap<Vec<u8>, HashMap<SubId, u64>> {
        match kind {
            Kind::Channel => &mut self.channels,
            Kind::Pattern => &mut self.patterns,
        }
    }

    fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty()
    }
}

#[derive(Default)]
struct State {
    by_ns: HashMap<Vec<u8>, Subs>,
    outboxes: HashMap<SubId, Arc<Outbox>>,
}

/// The seat's broker. One per process ([`BROKER`]).
#[derive(Default)]
pub struct Broker {
    state: Mutex<State>,
    next: AtomicU64,
}

/// This seat's broker.
pub static BROKER: LazyLock<Broker> = LazyLock::new(Broker::default);

impl Broker {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A new subscriber connection, and the outbox its thread writes from.
    pub fn register(&self) -> (SubId, Arc<Outbox>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let outbox = Arc::new(Outbox::default());
        self.lock().outboxes.insert(id, outbox.clone());
        (id, outbox)
    }

    /// The connection is gone, and every subscription it held with it.
    pub fn unregister(&self, id: SubId) {
        let mut st = self.lock();
        st.outboxes.remove(&id);
        st.by_ns.retain(|_, subs| {
            for kind in [Kind::Channel, Kind::Pattern] {
                subs.of(kind).retain(|_, by_conn| {
                    by_conn.remove(&id);
                    !by_conn.is_empty()
                });
            }
            !subs.is_empty()
        });
    }

    /// Add `delta` clients of connection `id` to `name` in `ns`, removing
    /// the entry at zero; a count never goes below zero. Answers the
    /// connection's count after the change.
    pub fn adjust(&self, id: SubId, ns: &[u8], kind: Kind, name: &[u8], delta: i64) -> u64 {
        let mut st = self.lock();
        let subs = st.by_ns.entry(ns.to_vec()).or_default();
        let map = subs.of(kind);
        let by_conn = map.entry(name.to_vec()).or_default();
        let now = by_conn
            .get(&id)
            .copied()
            .unwrap_or(0)
            .saturating_add_signed(delta);
        if now == 0 {
            by_conn.remove(&id);
            if by_conn.is_empty() {
                map.remove(name);
            }
        } else {
            by_conn.insert(id, now);
        }
        if subs.is_empty() {
            st.by_ns.remove(ns);
        }
        now
    }

    /// How many clients a message on `channel` in `ns` reaches: each
    /// subscriber of the channel once, and once more for each pattern it
    /// matches, as Redis counts.
    pub fn receivers(&self, ns: &[u8], channel: &[u8]) -> i64 {
        let st = self.lock();
        let Some(subs) = st.by_ns.get(ns) else {
            return 0;
        };
        let direct: u64 = subs.channels.get(channel).map_or(0, |m| m.values().sum());
        let patterned: u64 = subs
            .patterns
            .iter()
            .filter(|(p, _)| crate::glob::glob_match(p, channel))
            .map(|(_, m)| m.values().sum::<u64>())
            .sum();
        (direct + patterned) as i64
    }

    /// Deliver `message` on `channel` in `ns` now, and answer how many
    /// clients it reaches.
    pub fn publish(&self, ns: &[u8], channel: &[u8], message: &[u8]) -> i64 {
        let st = self.lock();
        let Some(subs) = st.by_ns.get(ns) else {
            return 0;
        };
        let mut reached = 0u64;
        if let Some(by_conn) = subs.channels.get(channel) {
            let frame = frame(&[b"flintmessage", ns, channel, message]);
            for (id, n) in by_conn {
                reached += n;
                if let Some(out) = st.outboxes.get(id) {
                    out.push(frame.clone());
                }
            }
        }
        for (pattern, by_conn) in &subs.patterns {
            if !crate::glob::glob_match(pattern, channel) {
                continue;
            }
            let frame = frame(&[b"flintpmessage", ns, pattern, channel, message]);
            for (id, n) in by_conn {
                reached += n;
                if let Some(out) = st.outboxes.get(id) {
                    out.push(frame.clone());
                }
            }
        }
        reached as i64
    }

    /// `PUBSUB CHANNELS [pattern]`: the channels of `ns` with a subscriber.
    pub fn channels(&self, ns: &[u8], pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let st = self.lock();
        let mut out: Vec<Vec<u8>> = st.by_ns.get(ns).map_or_else(Vec::new, |subs| {
            subs.channels
                .keys()
                .filter(|c| pattern.is_none_or(|p| crate::glob::glob_match(p, c)))
                .cloned()
                .collect()
        });
        out.sort();
        out
    }

    /// `PUBSUB NUMSUB`: the clients subscribed to `channel` itself.
    pub fn numsub(&self, ns: &[u8], channel: &[u8]) -> i64 {
        let st = self.lock();
        st.by_ns
            .get(ns)
            .and_then(|s| s.channels.get(channel))
            .map_or(0, |m| m.values().sum::<u64>() as i64)
    }

    /// `PUBSUB NUMPAT`: the distinct patterns of `ns` with a subscriber.
    pub fn numpat(&self, ns: &[u8]) -> i64 {
        let st = self.lock();
        st.by_ns.get(ns).map_or(0, |s| s.patterns.len() as i64)
    }
}

/// A proxy's subscriber connection, for as long as it lasts
/// (`FLINTSUBSCRIBER`, ADR-0052 D5). `buf` holds what arrived after the
/// `FLINTSUBSCRIBER` itself.
///
/// It answers the proxy's registrations, `FLINTSUB <ns> <channel> <delta>`
/// and `FLINTPSUB <ns> <pattern> <delta>`, with the connection's count
/// after the change, and between them writes every message queued for it,
/// as `flintmessage <ns> <channel> <payload>` or
/// `flintpmessage <ns> <pattern> <channel> <payload>`. Replies come in the
/// order the commands did; a message may come between any two of them.
///
/// Two threads: this one reads the proxy's commands, and a writer writes
/// the outbox, woken as soon as a frame is queued. So a message leaves the
/// seat as it is published, while this thread waits on the proxy.
///
/// Closing the connection drops its subscriptions. So does falling
/// [`OUTBOX_LIMIT_BYTES`] behind, and the proxy registers again on a new
/// connection.
pub fn serve_subscriber(stream: flint_tls::Stream, mut buf: Vec<u8>) -> std::io::Result<()> {
    struct Registered(SubId, Arc<Outbox>);
    impl Drop for Registered {
        fn drop(&mut self) {
            BROKER.unregister(self.0);
            self.1.close();
        }
    }
    let (mut rd, wr) = stream.into_duplex()?;
    let (id, outbox) = BROKER.register();
    *outbox.socket.lock().unwrap_or_else(|e| e.into_inner()) = Some(rd.socket_handle()?);
    let registered = Registered(id, outbox.clone());
    let mut ok = Vec::new();
    encode(&Value::Simple("OK".into()), &mut ok);
    outbox.push(ok);
    let writer = {
        let outbox = outbox.clone();
        std::thread::Builder::new()
            .name("subscriber-writer".into())
            .spawn(move || {
                while let Some(frames) = outbox.wait_drain() {
                    let sent = frames
                        .iter()
                        .try_for_each(|f| wr.append(f))
                        .and_then(|()| wr.flush().map(|_| ()));
                    if sent.is_err() {
                        break;
                    }
                }
                // The reader learns the connection is over from its socket.
                if let Some(sock) = outbox
                    .socket
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                {
                    let _ = sock.shutdown(std::net::Shutdown::Both);
                }
            })?
    };
    let mut chunk = [0u8; 16 * 1024];
    let result = loop {
        let mut used = 0;
        loop {
            match decode(&buf[used..]) {
                Ok(Decoded::Complete(v, n)) => {
                    used += n;
                    let mut reply = Vec::new();
                    encode(&subscriber_command(id, v), &mut reply);
                    outbox.push(reply);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    let mut reply = Vec::new();
                    encode(&Value::Error("ERR Protocol error".into()), &mut reply);
                    outbox.push(reply);
                    used = buf.len();
                    break;
                }
            }
        }
        buf.drain(..used);
        match rd.read(&mut chunk) {
            Ok(0) => break Ok(()),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            // A socket shut by an overflow or by the writer ends here too.
            Err(e) => break if outbox.overflowed() { Ok(()) } else { Err(e) },
        }
    };
    drop(registered);
    let _ = writer.join();
    result
}

fn subscriber_command(id: SubId, v: Value) -> Value {
    let Value::Array(Some(items)) = v else {
        return Value::Error("ERR Protocol error: expected an array".into());
    };
    let args: Vec<Vec<u8>> = items
        .into_iter()
        .filter_map(|i| match i {
            Value::Bulk(Some(b)) => Some(b),
            _ => None,
        })
        .collect();
    let name = args
        .first()
        .map(|n| n.to_ascii_uppercase())
        .unwrap_or_default();
    let kind = match name.as_slice() {
        b"PING" => return Value::Simple("PONG".into()),
        b"FLINTSUB" => Kind::Channel,
        b"FLINTPSUB" => Kind::Pattern,
        _ => {
            return Value::Error(
                "ERR a subscriber connection takes FLINTSUB, FLINTPSUB and PING".into(),
            );
        }
    };
    let delta = args
        .get(3)
        .and_then(|d| std::str::from_utf8(d).ok()?.parse::<i64>().ok());
    match (args.get(1), args.get(2), delta) {
        (Some(ns), Some(name), Some(delta)) if args.len() == 4 => {
            Value::Integer(BROKER.adjust(id, ns, kind, name, delta) as i64)
        }
        _ => Value::Error("ERR usage: FLINTSUB|FLINTPSUB <ns> <name> <delta>".into()),
    }
}

fn frame(parts: &[&[u8]]) -> Vec<u8> {
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

/// A publish held for the commit of its scope: namespace, channel, message.
type HeldPublish = (Vec<u8>, Vec<u8>, Vec<u8>);

thread_local! {
    /// Publishes held for the commit of the transaction or script running
    /// on this connection's thread, and how many scopes are open.
    static HELD: RefCell<(Vec<HeldPublish>, usize)> =
        const { RefCell::new((Vec::new(), 0)) };
}

/// Holds this thread's `PUBLISH`es until the writes around them commit.
///
/// Scopes nest: a script inside a transaction opens one inside the
/// transaction's. [`Deferral::commit`] on the outermost delivers everything
/// held; on an inner one it keeps its publishes for the outer scope to
/// deliver. Dropping a scope without committing it discards what was
/// published inside it, as a failed script keeps none of its writes.
pub struct Deferral {
    mark: usize,
    done: bool,
}

impl Deferral {
    pub fn begin() -> Self {
        HELD.with(|h| {
            let mut h = h.borrow_mut();
            h.1 += 1;
            Self {
                mark: h.0.len(),
                done: false,
            }
        })
    }

    /// The writes committed: deliver, or leave the publishes to the scope
    /// this one is inside.
    pub fn commit(mut self) {
        self.done = true;
        let ready = HELD.with(|h| {
            let mut h = h.borrow_mut();
            h.1 -= 1;
            if h.1 == 0 {
                std::mem::take(&mut h.0)
            } else {
                Vec::new()
            }
        });
        for (ns, channel, message) in ready {
            BROKER.publish(&ns, &channel, &message);
        }
    }
}

impl Drop for Deferral {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        HELD.with(|h| {
            let mut h = h.borrow_mut();
            h.0.truncate(self.mark);
            h.1 -= 1;
        });
    }
}

/// `PUBLISH` from a tenant's command: delivered now, or held when a
/// transaction or script is running on this thread. Either way the reply
/// counts the clients subscribed now.
pub fn publish(ns: &[u8], channel: &[u8], message: &[u8]) -> i64 {
    let held = HELD.with(|h| {
        let mut h = h.borrow_mut();
        if h.1 == 0 {
            return false;
        }
        h.0.push((ns.to_vec(), channel.to_vec(), message.to_vec()));
        true
    });
    if held {
        BROKER.receivers(ns, channel)
    } else {
        BROKER.publish(ns, channel, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_are_per_connection_and_publish_reaches_each_once() {
        let b = Broker::default();
        let (p1, o1) = b.register();
        let (p2, o2) = b.register();
        assert_eq!(b.adjust(p1, b"t", Kind::Channel, b"jobs", 2), 2);
        assert_eq!(b.adjust(p2, b"t", Kind::Channel, b"jobs", 1), 1);
        assert_eq!(b.adjust(p2, b"t", Kind::Pattern, b"j*", 1), 1);
        assert_eq!(b.adjust(p2, b"other", Kind::Channel, b"jobs", 1), 1);
        // Two clients of p1, one of p2, one pattern client of p2.
        assert_eq!(b.publish(b"t", b"jobs", b"hi"), 4);
        assert_eq!(o1.drain().len(), 1, "one frame per connection");
        assert_eq!(o2.drain().len(), 2, "its channel and its pattern");
        assert_eq!(b.receivers(b"t", b"jobs"), 4);
        assert_eq!(b.numsub(b"t", b"jobs"), 3);
        assert_eq!(b.numpat(b"t"), 1);
        assert_eq!(b.channels(b"t", None), vec![b"jobs".to_vec()]);
        assert_eq!(b.channels(b"t", Some(b"x*")), Vec::<Vec<u8>>::new());
        // Namespaces are apart.
        assert_eq!(b.publish(b"u", b"jobs", b"hi"), 0);
        assert_eq!(
            b.adjust(p1, b"t", Kind::Channel, b"jobs", -5),
            0,
            "never below zero"
        );
        b.unregister(p2);
        assert_eq!(b.publish(b"t", b"jobs", b"hi"), 0);
        assert_eq!(b.publish(b"other", b"jobs", b"hi"), 0);
        assert!(b.lock().by_ns.is_empty(), "nothing is left behind");
    }

    #[test]
    fn a_connection_that_falls_behind_is_cut_off_not_buffered() {
        let b = Broker::default();
        let (id, out) = b.register();
        b.adjust(id, b"t", Kind::Channel, b"c", 1);
        let big = vec![b'x'; OUTBOX_LIMIT_BYTES / 4];
        for _ in 0..5 {
            b.publish(b"t", b"c", &big);
        }
        assert!(out.overflowed());
        assert!(out.drain().is_empty(), "what it held is released");
    }

    #[test]
    fn a_publish_waits_for_the_commit_and_dies_with_a_rollback() {
        let (id, out) = BROKER.register();
        BROKER.adjust(id, b"deferral-test", Kind::Channel, b"c", 1);
        {
            let txn = Deferral::begin();
            assert_eq!(publish(b"deferral-test", b"c", b"1"), 1, "counted now");
            {
                let _failed_script = Deferral::begin();
                publish(b"deferral-test", b"c", b"dropped");
            }
            let script = Deferral::begin();
            publish(b"deferral-test", b"c", b"2");
            script.commit();
            assert!(out.drain().is_empty(), "nothing before the commit");
            txn.commit();
        }
        let frames = out.drain();
        assert_eq!(frames.len(), 2);
        assert!(frames[0].ends_with(b"$1\r\n1\r\n") && frames[1].ends_with(b"$1\r\n2\r\n"));
        {
            let _aborted = Deferral::begin();
            publish(b"deferral-test", b"c", b"never");
        }
        assert!(out.drain().is_empty());
        assert_eq!(publish(b"deferral-test", b"c", b"now"), 1);
        assert_eq!(
            out.drain().len(),
            1,
            "outside a scope it is delivered at once"
        );
        BROKER.unregister(id);
    }
}
