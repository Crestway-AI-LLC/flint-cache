# BUG-0269: every co-processor write dialled its own channel and left its socket in TIME_WAIT, so a bulk load ran the host out of ephemeral ports (FIXED 2026-10-20)

**Status:** **FIXED 2026-10-20.** The co-processor closes a finished channel
with a reset, which leaves no TIME_WAIT on its side. Found by running
langchain-redis 0.2.6's integration suite against a local fleet (ADR-0060's
client verification). Held by `coproc_vec_drill`'s BUG-0269 section and by
`flint-tls`'s
`an_abortive_close_resets_where_an_ordinary_one_ends_the_stream`.
**Severity:** medium. Writes failed with an error and were not acknowledged,
so nothing was lost. It applies to a fleet that declares a co-processor
(`coproc VEC.`) and writes vectors faster than the co-processor's host
recycles ports.
**Area:** `crates/flint-vec/src/main.rs`, `perform_persist` and
`rebuild_chunk_inner`; `crates/flint-tls/src/lib.rs`, `Stream`.

## What happened

Each write, `VEC.CREATE`, `VEC.SET`, `VEC.DEL` and the TTL commands, makes
its durable write over a channel: a new connection from the co-processor back to the
proxy's edge, opened with `PROXYCHAN <token>`, on a single-use grant
(ADR-0010 D2). When the write landed, the co-processor dropped the
connection. The side that closes a TCP connection first keeps it in
TIME_WAIT, 30 seconds on macOS and 60 on Linux, and the port stays taken
toward that address until then.

Measured on the laptop, release binaries of public main (c2c4de3b), one pair,
one co-processor and a proxy, 2,000 `VEC.SET` over one client connection:
- 3,994 writes a second;
- TIME_WAIT sockets toward the proxy's edge port went from 4 to 2,004;
- every one was on the co-processor's side of its connection.

macOS has 16,384 ephemeral ports, so at that rate the co-processor runs out
after about 4 seconds. From then on each dial fails, and so does each write:

    COPROCUNAVAIL channel dial failed: Can't assign requested address (os error 49)

langchain-redis's suite met it in 20 seconds. Its fixtures load documents
in pipelines, through ADR-0060's `FT.` family (on its branch), which writes
over the same kind of channel.

On Linux the default range is 32,768 to 60,999 and TIME_WAIT lasts 60
seconds. That allows about 470 channels a second, sustained, from one
co-processor host to one proxy address. Linux reuses a port in TIME_WAIT
for a new outgoing connection only toward a loopback address
(`tcp_tw_reuse` = 2). So a co-processor on its own host, as `coproc-host`
places one, has that ceiling. A gate box runs everything over loopback,
which is why no drill met it.

A rebuild opens a channel per chunk of up to 256 commands, so it leaves far
fewer of them, but the same way.

## Fix

- **`flint_tls::Stream::set_abortive_close`** sets `SO_LINGER` to 0. Closing
  the connection then sends a reset rather than a FIN, and neither side keeps
  it in TIME_WAIT.
- **Both of flint-vec's channel dials set it** as soon as the connection is
  made, so every way out closes it so:
  - the write channel, in `perform_persist`;
  - the rebuild's, in `rebuild_chunk_inner`.
- **Every reply is read before the channel is closed**, so a reset loses
  nothing. The proxy treats the reset as the end of the connection, as it
  treats any client's: `serve_client`'s result is not counted or logged.
- **No protocol change, and no version order.** A proxy of any release sees
  the channel end as before.

**Not changed:** a channel per write still costs a dial, and with
`--client-tls` a TLS handshake, on every write. Reusing a channel for the
next command needs a change to ADR-0010 D2's single-use grant. That is a
design for Jeff, not part of this fix.

**Found beside it:** the proxy answers `QUIT` with `+OK` but keeps the
connection open, where Redis closes it. No Flint caller depends on either.

## Evidence

- `an_abortive_close_resets_where_an_ordinary_one_ends_the_stream`: over a
  loopback connection, the peer of an abortive close reads a reset. The
  control is the same connection closed the ordinary way, whose peer reads
  the end of the stream.
- **The same 2,000 writes with the fix:** 3,837 a second, and TIME_WAIT
  sockets toward the edge went from 2 to 2. The control was the same tree
  without the fix's two calls, its binary's md5 different.
- `coproc_vec_drill`, BUG-0269 section: 300 `VEC.SET` over one client
  connection must leave fewer than 30 new TIME_WAIT sockets toward the
  proxy's edge. On the control build every earlier section passed and this
  one failed, with 301, one per write and the client's own. With the fix the
  whole drill passes, and the 300 writes leave 1, the client's.
