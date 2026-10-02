// SPDX-License-Identifier: Elastic-2.0
//! The 1-bit code a quantized HNSW graph can hold besides `sq8` (ADR-0049
//! item 2): a short stand-in for a vector that answers distances as
//! ESTIMATES, good enough to steer the graph walk and pick the candidates,
//! which the re-rank then scores on the full vectors.
//!
//! A vector is rotated by a fixed random rotation and keeps the sign
//! of each dimension, `dim / 8` bytes, plus one float. The rotation spreads
//! each input dimension over all of them, so no single sign carries most of a
//! vector. Against a query (in full), the estimate is RaBitQ's: with `u` the
//! rotated unit vector and `x̄ = sign(u) / √dim`, `⟨u, q⟩ ≈ ⟨x̄, q⟩ / ⟨x̄, u⟩`,
//! and `⟨x̄, u⟩` is the float a code keeps. Between two codes it is SimHash's:
//! two unit vectors at angle θ differ in about `dim × θ / π` signs.
//!
//! Product quantization was built here too, and dropped (ADR-0049, 2026-10-02):
//! measured on two embedding models it trailed the 1-bit code's recall at
//! about the same RAM, and it needed a codebook trained on a thread.

use crate::kernel;

/// Rounds of the rotation. Each mixes every dimension into every other at
/// least once; a few rounds make the result close to a random rotation's.
const ROUNDS: usize = 4;

/// The rotation's seed. Fixed, so a vector's code is the same in every set and
/// after every rebuild.
const SEED: u64 = 0x0049_B17C_0DE5_EED5;

struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Rotate `v` by the fixed rotation: rounds of a random sign flip and a
/// normalized Walsh-Hadamard transform. A transform needs a power of two
/// values, so for any other `dim` each round transforms the first and then
/// the last `h` values, `h` the largest power of two in `dim`; the two
/// overlap and cover every value. Every step is orthogonal, so lengths and
/// angles are kept exactly.
pub(crate) fn rotate(v: &[f32]) -> Vec<f32> {
    let d = v.len();
    let mut x = v.to_vec();
    if d == 0 {
        return x;
    }
    let h = 1usize << (usize::BITS - 1 - d.leading_zeros());
    let mut rng = Xorshift(SEED);
    for _ in 0..ROUNDS {
        flip(&mut x, &mut rng);
        hadamard(&mut x[..h]);
        flip(&mut x, &mut rng);
        hadamard(&mut x[d - h..]);
    }
    x
}

fn flip(x: &mut [f32], rng: &mut Xorshift) {
    for chunk in x.chunks_mut(64) {
        let bits = rng.next();
        for (i, v) in chunk.iter_mut().enumerate() {
            if bits >> i & 1 == 1 {
                *v = -*v;
            }
        }
    }
}

/// In place, scaled by `1 / √len` so it is orthogonal. `x.len()` is a power of
/// two.
fn hadamard(x: &mut [f32]) {
    let n = x.len();
    let mut len = 1;
    while len < n {
        for block in x.chunks_mut(2 * len) {
            let (a, b) = block.split_at_mut(len);
            for (p, q) in a.iter_mut().zip(b) {
                let (s, t) = (*p, *q);
                *p = s + t;
                *q = s - t;
            }
        }
        len *= 2;
    }
    let scale = 1.0 / (n as f32).sqrt();
    for v in x {
        *v *= scale;
    }
}

/// Bytes a 1-bit code of `dim` values takes.
pub(crate) fn bin_bytes(dim: usize) -> usize {
    dim.div_ceil(8)
}

/// The 1-bit code of `v`, whose norm is `norm`, into `bits`: one bit a
/// dimension, set when the rotated value is positive, low bit first. Returns
/// `⟨x̄, u⟩`, which the estimate divides by; 1 for a zero vector, whose every
/// estimate is 0 anyway.
pub(crate) fn bin_encode(v: &[f32], norm: f32, bits: &mut [u8]) -> f32 {
    let r = rotate(v);
    bits.fill(0);
    let mut abs = 0f32;
    for (i, &x) in r.iter().enumerate() {
        if x > 0.0 {
            bits[i / 8] |= 1 << (i % 8);
        }
        abs += x.abs();
    }
    let f = abs / ((r.len() as f32).sqrt() * norm);
    if f.is_finite() && f > 0.0 { f } else { 1.0 }
}

/// A vector prepared once to be compared with many 1-bit codes: rotated, and
/// a table of every nibble's sum of it.
pub(crate) struct BinQuery {
    lut: Vec<[f32; 32]>,
    sum: f32,
    root_dim: f32,
}

impl BinQuery {
    pub(crate) fn new(q: &[f32]) -> BinQuery {
        let r = rotate(q);
        let mut lut = vec![[0f32; 32]; bin_bytes(r.len())];
        for (i, row) in lut.iter_mut().enumerate() {
            for half in 0..2 {
                let at = i * 8 + half * 4;
                let vals: [f32; 4] = std::array::from_fn(|b| r.get(at + b).copied().unwrap_or(0.0));
                for m in 0..16usize {
                    row[half * 16 + m] = (0..4).filter(|b| m >> b & 1 == 1).map(|b| vals[b]).sum();
                }
            }
        }
        BinQuery {
            lut,
            sum: r.iter().sum(),
            root_dim: (r.len() as f32).sqrt(),
        }
    }

    /// The estimate of `⟨v, q⟩` for the vector `v` with this code, norm and
    /// factor: `‖v‖ ⟨x̄, Rq⟩ / ⟨x̄, u⟩`.
    pub(crate) fn dot(&self, bits: &[u8], norm: f32, factor: f32) -> f32 {
        // ⟨x̄, Rq⟩ = (Σ at set bits − Σ at clear bits) / √dim.
        let set = kernel::nibble_sum(&self.lut, bits);
        norm * (2.0 * set - self.sum) / self.root_dim / factor
    }
}

/// The estimate of `⟨a, b⟩` from two 1-bit codes of `dim` values, the vectors'
/// norms `an` and `bn`: `‖a‖ ‖b‖ cos θ`, θ from the signs they differ in.
pub(crate) fn bin_dot(a: &[u8], b: &[u8], dim: usize, an: f32, bn: f32) -> f32 {
    let differ = kernel::hamming(a, b) as f32;
    an * bn * (std::f32::consts::PI * differ / dim as f32).cos()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vecs(n: usize, d: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Xorshift(seed);
        (0..n)
            .map(|_| {
                (0..d)
                    .map(|_| (rng.next() >> 40) as f32 / 16_777_216.0 * 2.0 - 1.0)
                    .collect()
            })
            .collect()
    }

    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    /// The rotation is orthogonal at every kind of length (a power of two, one
    /// past, one short), so it keeps norms and dot products, and it is the
    /// same rotation every call.
    #[test]
    fn the_rotation_keeps_lengths_and_angles() {
        for d in [1usize, 2, 7, 8, 9, 128, 200, 1536] {
            let v = vecs(2, d, d as u64 + 1);
            let (a, b) = (rotate(&v[0]), rotate(&v[1]));
            let tol = 1e-4 * d as f32;
            assert!((dot(&a, &a) - dot(&v[0], &v[0])).abs() < tol, "norm, d={d}");
            assert!((dot(&a, &b) - dot(&v[0], &v[1])).abs() < tol, "dot, d={d}");
            assert_eq!(a, rotate(&v[0]), "fixed, d={d}");
        }
    }

    /// The 1-bit estimates track the true dot products: across random pairs
    /// at dim 1536, the estimate's error is a small fraction of the norms'
    /// product, and a vector is nearest to itself.
    #[test]
    fn one_bit_estimates_track_the_dot_product() {
        let d = 1536;
        let v = vecs(64, d, 7);
        let norms: Vec<f32> = v.iter().map(|x| dot(x, x).sqrt()).collect();
        let mut codes = vec![vec![0u8; bin_bytes(d)]; v.len()];
        let factors: Vec<f32> = v
            .iter()
            .zip(&norms)
            .zip(&mut codes)
            .map(|((x, &n), c)| bin_encode(x, n, c))
            .collect();
        let q = BinQuery::new(&v[0]);
        for i in 1..v.len() {
            let truth = dot(&v[0], &v[i]) / (norms[0] * norms[i]);
            let asym = q.dot(&codes[i], norms[i], factors[i]) / (norms[0] * norms[i]);
            let sym = bin_dot(&codes[0], &codes[i], d, norms[0], norms[i]) / (norms[0] * norms[i]);
            assert!(
                (asym - truth).abs() < 0.1,
                "asymmetric: {asym} against {truth}"
            );
            assert!(
                (sym - truth).abs() < 0.15,
                "symmetric: {sym} against {truth}"
            );
        }
        let own = q.dot(&codes[0], norms[0], factors[0]) / (norms[0] * norms[0]);
        assert!(
            (own - 1.0).abs() < 0.1,
            "a vector against its own code: {own}"
        );
    }
}
