# BUG-0203: `flint-vec` ignored `--bind` and listened on every interface, so a seat asked for loopback served any tenant's vectors to its network (FIXED 2026-10-03)

**Status:** **FIXED 2026-10-03.** Found building ADR-0050 (ops), which puts
the co-processor on every fleet: `flintctl` hands the seat `--bind <host>`,
and the binary had no such flag. Held by `coproc_vec_drill`'s BUG-0203
section, which probes the seat from the host's own non-loopback address.
**Severity:** high on a fleet with mesh TLS off, low with it on. With TLS off,
a co-processor seat placed on loopback answered `FLINTFAM` on every
interface. Any host that could reach the port read a tenant's vectors by
naming its namespace, and could start a rebuild that dials an address of its
choosing. Fleets rendered for the marketplace always run `tls on`, so their
`FLINTFAM` listener still demanded a mesh client certificate. They lost
defence in depth, not data. Vector search is not offered to tenants yet.

## What was measured

flint-server (`--engine mem`), flint-proxy and flint-vec, the last built
from ADR-0049's item-2 branch, on a laptop with a LAN address `10.0.0.206`.

Two runs, each started with `--bind 127.0.0.1`:

- On port 6799, `flint-vec` printed `co-processor on 0.0.0.0:6799`, and
  `lsof` showed `TCP *:6799 (LISTEN)`.
- On port 6792, it printed `co-processor on 0.0.0.0:6792`, and the rest of
  this list was measured.
- Through the proxy, tenant `ns` created set `docs` and stored id `secret`.
- A client connected to `10.0.0.206:6792`, not loopback, and sent
  `FLINTFAM not-a-token 127.0.0.1:1 ns VEC.SEARCH docs 1,0,0 2`. The reply was
  `*1 *2 secret 0`: the tenant's nearest vector.
- The same command, sent before the namespace was warm, answered `-LOADING`
  and started a rebuild that dials the callback the caller named.

## Why

`main.rs` built its listen address as `format!("0.0.0.0:{port}")`, whatever
it was given. `flintctl`'s `coproc_args` passes `--bind` with the host of the
inventory's `coproc` address. BUG-0140's fix relies on that: it keeps the
literal in `coproc_args` "because it binds". Nothing read it.

The boundary matters because of what `FLINTFAM` trusts. A read on a warm
namespace is answered from the co-processor's memory on the caller's word for
the namespace. The channel token is checked only when the co-processor dials
the proxy back, which a read never does (ADR-0017's "search opens no
channel"). Mesh mTLS (ADR-0010 D5/D6) authenticates the caller. Without it,
the listen address is the only boundary, and it was every interface.

## The fix

`flint-vec` reads `--bind`, defaulting to `127.0.0.1` as flint-server and
the control plane do, and listens on `(host, port)`. A seat placed on
loopback now answers on loopback only. A seat given its host's address, as
the chaos harness's multi-host inventory gives it, listens there.

**The drill** starts a second co-processor on a spare port three times and
connects to it from the host's own non-loopback address:
- `--bind 0.0.0.0` must connect, the control that the probe can reach a
  listener;
- `--bind 127.0.0.1` must be refused;
- no `--bind` at all must be refused.

The unfixed binary fails the second.

## Not changed

With mesh TLS off, a process on the same host can still send `FLINTFAM` over
loopback. That is the plaintext trust model every fleet seat shares. Mesh
TLS is the authentication, and marketplace fleets always run it.
