#!/usr/bin/env python3
# SPDX-License-Identifier: Elastic-2.0
"""Independent transcription of MurmurHash3 x64_128, to cross-check the Rust.

Why this exists: the hash in crates/flint-storage/src/bloom.rs is on-disk
format for every Bloom filter (ADR-0016 D4), and no murmur3 reference is
installed on this machine to check it against. Self-generated test vectors
would only prove the Rust has not changed, not that it implements the
algorithm — so this is written from the published algorithm rather than
from the Rust, and the two are compared on a corpus that covers every tail
length 0..16.

It cannot catch a shared misunderstanding of the spec. It does catch
transcription errors — a wrong rotate, a swapped constant, a mishandled
tail byte — which is what actually goes wrong here.

Usage:
    python3 tools/murmur3_crosscheck.py            # print vectors
    python3 tools/murmur3_crosscheck.py --check    # compare against Rust
"""

import subprocess
import sys

M64 = (1 << 64) - 1
C1 = 0x87C37B91114253D5
C2 = 0x4CF5AD432745937F


def rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & M64


def fmix64(k):
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & M64
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & M64
    k ^= k >> 33
    return k


def murmur3_x64_128(data, seed=0):
    h1 = seed & M64
    h2 = seed & M64
    nblocks = len(data) // 16

    for i in range(nblocks):
        o = i * 16
        k1 = int.from_bytes(data[o:o + 8], "little")
        k2 = int.from_bytes(data[o + 8:o + 16], "little")

        k1 = (k1 * C1) & M64
        k1 = rotl(k1, 31)
        k1 = (k1 * C2) & M64
        h1 ^= k1
        h1 = rotl(h1, 27)
        h1 = (h1 + h2) & M64
        h1 = (h1 * 5 + 0x52DCE729) & M64

        k2 = (k2 * C2) & M64
        k2 = rotl(k2, 33)
        k2 = (k2 * C1) & M64
        h2 ^= k2
        h2 = rotl(h2, 31)
        h2 = (h2 + h1) & M64
        h2 = (h2 * 5 + 0x38495AB5) & M64

    tail = data[nblocks * 16:]
    k1 = 0
    k2 = 0
    n = len(tail)

    # The reference is a fallthrough switch on (len & 15); written out as
    # the explicit cases it compiles to, so a reader can diff it by eye.
    if n >= 15:
        k2 ^= tail[14] << 48
    if n >= 14:
        k2 ^= tail[13] << 40
    if n >= 13:
        k2 ^= tail[12] << 32
    if n >= 12:
        k2 ^= tail[11] << 24
    if n >= 11:
        k2 ^= tail[10] << 16
    if n >= 10:
        k2 ^= tail[9] << 8
    if n >= 9:
        k2 ^= tail[8]
        k2 = (k2 * C2) & M64
        k2 = rotl(k2, 33)
        k2 = (k2 * C1) & M64
        h2 ^= k2
    if n >= 8:
        k1 ^= tail[7] << 56
    if n >= 7:
        k1 ^= tail[6] << 48
    if n >= 6:
        k1 ^= tail[5] << 40
    if n >= 5:
        k1 ^= tail[4] << 32
    if n >= 4:
        k1 ^= tail[3] << 24
    if n >= 3:
        k1 ^= tail[2] << 16
    if n >= 2:
        k1 ^= tail[1] << 8
    if n >= 1:
        k1 ^= tail[0]
        k1 = (k1 * C1) & M64
        k1 = rotl(k1, 31)
        k1 = (k1 * C2) & M64
        h1 ^= k1

    h1 ^= len(data)
    h2 ^= len(data)
    h1 = (h1 + h2) & M64
    h2 = (h2 + h1) & M64
    h1 = fmix64(h1)
    h2 = fmix64(h2)
    h1 = (h1 + h2) & M64
    h2 = (h2 + h1) & M64
    return h1, h2


# Every tail length 0..16 is covered, because the tail switch is where a
# transcription error hides: lengths 8 and 9 straddle the k2 branch, and 16
# is the first input with a full body block and no tail at all.
CORPUS = [
    b"",
    b"a",
    b"ab",
    b"abc",
    b"abcd",
    b"abcde",
    b"abcdef",
    b"abcdefg",
    b"abcdefgh",
    b"abcdefghi",
    b"abcdefghij",
    b"abcdefghijk",
    b"abcdefghijkl",
    b"abcdefghijklm",
    b"abcdefghijklmn",
    b"abcdefghijklmno",
    b"abcdefghijklmnop",
    b"The quick brown fox jumps over the lazy dog",
    bytes(range(256)),
]


def main():
    check = "--check" in sys.argv
    mine = [(c, murmur3_x64_128(c, 0)) for c in CORPUS]

    if not check:
        for c, (h1, h2) in mine:
            print(f"{c!r:12.12} -> (0x{h1:016X}, 0x{h2:016X})")
        return 0

    out = subprocess.run(
        ["cargo", "run", "--quiet", "--package", "flint-storage",
         "--example", "murmur3_vectors"],
        capture_output=True, text=True, check=True,
    ).stdout.strip().splitlines()

    if len(out) != len(mine):
        print(f"FAIL: rust emitted {len(out)} lines, expected {len(mine)}")
        return 1

    bad = 0
    for (c, (h1, h2)), line in zip(mine, out):
        want = f"{h1:016x}{h2:016x}"
        if line.strip() != want:
            print(f"FAIL len={len(c):3d}  rust={line.strip()}  python={want}")
            bad += 1
    if bad:
        print(f"{bad}/{len(mine)} vectors disagree")
        return 1
    print(f"OK: {len(mine)} vectors agree, tail lengths 0..16 covered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
