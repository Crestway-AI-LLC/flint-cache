// SPDX-License-Identifier: Elastic-2.0
//! Emits this build's MurmurHash3 x64_128 digests for the cross-check
//! corpus, one hex line each, for `tools/murmur3_crosscheck.py --check`.
//!
//! The corpus is duplicated between here and the Python on purpose: if it
//! were shared, a change to one side's input list would silently change
//! what the other side compares against.

use flint_storage::bloom::murmur3_x64_128;

fn main() {
    let mut corpus: Vec<Vec<u8>> = Vec::new();
    let alphabet = b"abcdefghijklmnop";
    for n in 0..=alphabet.len() {
        corpus.push(alphabet[..n].to_vec());
    }
    corpus.push(b"The quick brown fox jumps over the lazy dog".to_vec());
    corpus.push((0..=255u8).collect());

    for item in &corpus {
        let (h1, h2) = murmur3_x64_128(item, 0);
        println!("{h1:016x}{h2:016x}");
    }
}
