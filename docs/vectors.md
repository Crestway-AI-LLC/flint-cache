# Vector search (preview)

**Preview.** The `VEC.*` commands below may change, with notice in the
release notes. There is no latency or availability commitment beyond the
cache's own (`slo.md`). Durability is the cache's: `VEC.SET` is acknowledged
after the vector is a durable row in your namespace, like any other write.

Flint answers nearest-neighbour searches on the same endpoint, with the same
token and the same namespace as your keys. Vectors are not a separate store
kept in sync with the cache: they are rows in it.

## What is in RAM, and what is on NVMe

Flint keeps data on NVMe, not in RAM, and vectors are no exception: every
vector is a durable row on the data nodes. The search index is the one part
that is in RAM. A vector co-processor holds it, beside the data nodes:

- With `QUANT bin` (recommended), the index keeps a **1-bit compressed copy of
  each vector, about 500 bytes for a 1536-dimension vector**. The full
  vectors are in a file on the co-processor's local NVMe, and each search
  re-ranks its best candidates against them.
- With `QUANT sq8`, the copy is 8 bits a dimension, about 2 KB at 1536.
- With no `QUANT`, the index holds every full vector in RAM: about 6.6 KB at
  1536.

Measured on 999,000 OpenAI `text-embedding-3-large` vectors (1536
dimensions) on an i4i.2xlarge:

| set | RAM per vector | recall@10, defaults | `VEC.SEARCH` p50 |
|---|---:|---:|---:|
| `INDEX hnsw` | 6,601 B | 0.970 | 1.87 ms |
| `INDEX hnsw QUANT bin` | 500 B | 0.974 | 1.96 ms |

Recall is against an exact search, and latency is through the proxy. On a
smaller model, 384-dimension `all-MiniLM-L6-v2`, `bin`'s best recall was
0.964 against 0.984 for full vectors. Choose `QUANT sq8` there when you need
the top of that range: it matched full vectors on both models.

A search is answered from the co-processor's memory and its local file. It
sends nothing to the data nodes, so it adds no reads to them.

## Commands

| command | does |
|---|---|
| `VEC.CREATE set DIM d METRIC cosine\|l2\|ip [INDEX flat\|hnsw] [QUANT sq8\|bin]` | makes a set; `INDEX` defaults to `flat` (exact), and `QUANT` needs `INDEX hnsw` |
| `VEC.SET set id vector [META bytes] [EX secs \| PX ms]` | stores or replaces a vector; `vector` is comma-separated floats, `[...]` optional |
| `VEC.GET set id` | the exact vector you stored, and its `META` |
| `VEC.DEL set id` | `1` if it existed, else `0` |
| `VEC.SEARCH set query k [EF n] [RERANK n]` | the `k` nearest ids, each with a score; higher is nearer |
| `VEC.INFO set` | dimension, metric, index, count, how many expire, the code, and your namespace's index memory and cap |
| `VEC.TTL set id` | milliseconds left; `-1` permanent, `-2` absent |
| `VEC.EXPIRE set id secs`, `VEC.PEXPIRE set id ms` | sets a deadline without resending the vector |
| `VEC.PERSIST set id` | removes the deadline |

```
VEC.CREATE docs DIM 1536 METRIC cosine INDEX hnsw QUANT bin
VEC.SET docs doc:42 0.013,-0.271,... META {"title":"..."}
VEC.SEARCH docs 0.011,-0.265,... 10
```

`EF` trades latency for recall (default 64): on the `bin` set above, `EF 256`
reached 0.984 at 2.99 ms p50. `RERANK` is how many candidates are re-ranked
on the full vectors: by default 10 × `k` for `bin` and 4 × `k` for `sq8`.
Doubling it added under 0.01 recall in the measurements. A vector with `EX` or
`PX` stops appearing in `GET` and `SEARCH` the moment it expires.

## Errors to expect

- **`-LOADING`** after the co-processor restarts (an upgrade, a crash). It
  rebuilds your sets from their durable rows on first touch; retry until
  the command answers. Nothing is lost.
- **`-VECFULL ... for this namespace`**: your namespace's index has reached
  its memory cap. The write is refused before anything is stored. Delete
  vectors, let them expire, or ask the operator for a higher cap.
- **`-VECFULL vector index memory is full on this server`**: every tenant's
  indexes together have reached the server's share of memory, which keeps
  vector search from taking RAM the cache needs. Reads still work; retry
  later.
- **`-WRONGDIM`**: the vector's length is not the set's `DIM`.

## Running it

On Crestway's hosted service there is nothing to set: the fleet your
endpoint is on runs the co-processor. A self-hosted fleet declares one in its
inventory:

```
coproc VEC. 127.0.0.1:7420
coproc-index-bytes 1073741824          # per namespace (default 1 GiB)
coproc-index-total-bytes 8589934592    # every namespace together (no default)
```

Set `coproc-index-total-bytes` below what the host's data nodes need.
`flintctl upgrade` starts a co-processor the inventory gained and rolls a
running one. Its index memory, both caps and refusals are reported as
`flint_coproc_*`: by the fleet agent, which `flintctl` tells about every
co-processor, and by `flint-exporter --coproc 127.0.0.1:7420`.
