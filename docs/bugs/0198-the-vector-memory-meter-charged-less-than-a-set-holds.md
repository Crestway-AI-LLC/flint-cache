# BUG-0198: the vector co-processor's memory meter charged less than its sets hold, though it said it over-estimated (FIXED 2026-10-01)

**Status:** **FIXED 2026-10-01.** Found measuring the starting point for
ADR-0049 step 3, the flattened node layout, whose case rests on what a node
really costs. Held by `crates/flint-vec/tests/meter.rs`, which fails on each
of three mutants below. Released in the next cut after v0.1.0-rc.78.
**Severity:** medium, latent. The D4 meter (`entry_bytes`) is what caps one
tenant's index RAM (`--index-mem-bytes`, 1 GiB per namespace by default from
`flintctl`), so a co-processor shared by many tenants is not exhausted by one.
It charged up to a third less than a set held, so a cap admitted that much
more. Its comment called it "deliberately a slight OVER-estimate". `flint-vec`
runs only in test harnesses today.

## What was measured

A counting allocator, so heap bytes as requested rather than what the OS
reports (this laptop compresses and swaps memory, and its resident size came
out lower for a set that holds strictly more). Bytes held per vector against
the meter's charge, with the containers just past a doubling (the worst case):

| set | dim 128 | dim 1000 |
|---|---:|---:|
| flat | 684 held / 585 charged | 4,172 / 4,073 |
| HNSW | 978 / 777 | 4,466 / 4,265 |
| HNSW `QUANT sq8`, full vectors in RAM | 1,154 / 913 | 5,514 / 5,273 |
| HNSW `QUANT sq8`, full vectors in `--vec-dir` | 594 / 401 | 1,466 / 1,273 |

And before the first fix, at dim 1536: flat 8,330 held / 6,213 charged; sq8 in
RAM 10,172 / 7,949.

## Mechanism

1. **A parsed vector kept the spare capacity it was collected with.**
   `parse_vector` pushed floats into a `Vec` that grew by doubling, so a
   1,536-float vector sat in room for 2,048: a third more than the vector, for
   as long as the set held it. Flat sets and quantized sets with full vectors
   in RAM keep the parsed `Vec` itself. A plain HNSW set copies it, which is
   why it did not show there. At a power-of-two dimension there is no slack,
   which is why 128 hid it.
2. **The fixed per-entry charge was a guess, and low.** 64 B for a flat entry
   and 256 B for an HNSW node. Measured, beyond the vector, id and meta: 163 B
   and 457 B, with the map and the node array each just past a doubling, so
   about half empty and charged to the entries already in them. A quantized
   set's in-RAM full vector also has its own `Vec` header, which was not
   charged.

## Fix

- `parse_vector` shrinks the vector to its length (the rebuild path parses
  durable rows with it too).
- The fixed charges are 192 B (flat) and 512 B (HNSW), with 24 B for the
  in-RAM full vector's `Vec`. Each now sits 24 to 55 B a vector above what the
  test measures.
- `tests/meter.rs` builds every kind of set through the real commands at
  dimensions 128 and 200 (not a power of two), counts the heap with its own
  allocator, and fails if any holds more than it is charged. A test binary of
  its own, because the count is process-wide.
- `bench --memory` prints the same comparison at any size and dimension.

## Verification

`tests/meter.rs` fails on each mutant: no `shrink_to_fit` (flat and sq8-in-RAM
at dim 200), the flat charge back at 64, and the HNSW charge back at 256 (all
three HNSW kinds). `index_memory_cap_bounds_a_namespace` and
`a_vec_dir_moves_quantized_vectors_out_of_ram` were updated for the new
charges.

## What this does not do

- It measures requested heap, not the allocator's rounding or the OS's view.
  On the fleet's hosts, ADR-0049's verification 1 (RSS growth at 1M vectors)
  is still the number to publish.
- The meter is still an estimate per entry, now one a test keeps above the
  truth. ADR-0049 step 3, the flattened layout, will move the truth, and the
  test will say whether the charges still hold.
