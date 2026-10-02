// SPDX-License-Identifier: Elastic-2.0
//! The distance arithmetic every search and insert runs, written so the
//! compiler vectorizes it.
//!
//! A sum kept in one variable is a chain of dependent adds, and without leave
//! to reassociate floats, which Rust does not give, the compiler adds in
//! order, one element at a time. Eight independent sums, combined at the end,
//! let it use vector registers. A result can differ from the in-order sum in
//! its last bits; nothing compares the two.
//!
//! On x86-64 each kernel also has an AVX2 copy, the same code compiled eight
//! floats wide instead of the baseline's four, chosen at run time when the CPU
//! has it (the fleet's i4i hosts do). Nothing else differs between the two.
//!
//! An 8-bit code's value `i` is `lo + step * byte[i]` (ADR-0049). The kernels
//! that take one decode it as they go, with the same arithmetic the graph used
//! when it decoded through an iterator, so a code's distances are the same up
//! to summation order.
//!
//! The 1-bit and PQ codes (ADR-0049 item 2, `quant.rs`) need three more: a
//! sub-vector against every centroid of a PQ subspace at once, the squared
//! distance between two PQ codes, and the bits two 1-bit codes differ in. Their table
//! lookups are plain functions: a lookup per element gains nothing from wider
//! registers.

const LANES: usize = 8;

/// The eight sums, paired so the combination is itself balanced.
#[inline]
fn fold(acc: [f32; LANES]) -> f32 {
    ((acc[0] + acc[4]) + (acc[1] + acc[5])) + ((acc[2] + acc[6]) + (acc[3] + acc[7]))
}

/// `Σ f(a[i], b[i])`, eight elements at a time. Always inlined, so each
/// kernel, and each AVX2 copy, is compiled with its own instruction set.
#[inline(always)]
fn sum2<A: Copy, B: Copy>(a: &[A], b: &[B], f: impl Fn(A, B) -> f32) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let (ac, ar) = a.as_chunks::<LANES>();
    let (bc, br) = b.as_chunks::<LANES>();
    let mut acc = [0f32; LANES];
    for (x, y) in ac.iter().zip(bc) {
        for l in 0..LANES {
            acc[l] += f(x[l], y[l]);
        }
    }
    let mut s = fold(acc);
    for (&x, &y) in ar.iter().zip(br) {
        s += f(x, y);
    }
    s
}

/// An 8-bit code, decoded: `(bytes, lo, step)`.
pub(crate) type Sq8<'a> = (&'a [u8], f32, f32);

/// Values in a PQ subspace, and so in each of its centroids.
pub(crate) const SUB: usize = 16;

/// Centroids a PQ subspace has: one byte of code picks one.
pub(crate) const CENTROIDS: usize = 256;

/// A PQ centroid.
pub(crate) type Centroid = [f32; SUB];

/// Sum sixteen lanes, paired as [`fold`] pairs eight.
#[inline]
fn fold16(acc: [f32; SUB]) -> f32 {
    let (lo, hi) = acc.split_at(LANES);
    let mut half = [0f32; LANES];
    for l in 0..LANES {
        half[l] = lo[l] + hi[l];
    }
    fold(half)
}

/// The kernels' bodies, once: `kernels!` defines each in a module, and the
/// module is compiled twice, once portable and once with AVX2 enabled.
macro_rules! kernels {
    ($(#[$attr:meta])*) => {
        use super::{fold16, sum2, Centroid, Sq8, CENTROIDS, SUB};

        $(#[$attr])*
        pub(super) fn dot(a: &[f32], b: &[f32]) -> f32 {
            sum2(a, b, |x, y| x * y)
        }

        $(#[$attr])*
        pub(super) fn l2sq(a: &[f32], b: &[f32]) -> f32 {
            sum2(a, b, |x, y| (x - y) * (x - y))
        }

        $(#[$attr])*
        pub(super) fn sq8_dot(c: Sq8<'_>, q: &[f32]) -> f32 {
            let (bytes, lo, step) = c;
            sum2(bytes, q, |b, y| (lo + step * b as f32) * y)
        }

        $(#[$attr])*
        pub(super) fn sq8_l2sq(c: Sq8<'_>, q: &[f32]) -> f32 {
            let (bytes, lo, step) = c;
            sum2(bytes, q, |b, y| {
                let d = (lo + step * b as f32) - y;
                d * d
            })
        }

        $(#[$attr])*
        pub(super) fn sq8_dot_sq8(a: Sq8<'_>, b: Sq8<'_>) -> f32 {
            let ((x, xl, xs), (y, yl, ys)) = (a, b);
            sum2(x, y, |p, q| (xl + xs * p as f32) * (yl + ys * q as f32))
        }

        $(#[$attr])*
        pub(super) fn sq8_l2sq_sq8(a: Sq8<'_>, b: Sq8<'_>) -> f32 {
            let ((x, xl, xs), (y, yl, ys)) = (a, b);
            sum2(x, y, |p, q| {
                let d = (xl + xs * p as f32) - (yl + ys * q as f32);
                d * d
            })
        }

        /// `out[j] = Σ_t x[t] * cols[t * out.len() + j]`: a sub-vector's dot
        /// product with every centroid of its subspace at once, the centroids
        /// stored a dimension at a time so the inner loop runs along them.
        $(#[$attr])*
        pub(super) fn dots_t(cols: &[f32], x: &[f32], out: &mut [f32]) {
            let k = out.len();
            out.fill(0.0);
            for (t, &xt) in x.iter().enumerate() {
                for (o, &c) in out.iter_mut().zip(&cols[t * k..(t + 1) * k]) {
                    *o += xt * c;
                }
            }
        }

        /// The squared distance between two PQ codes: subspace `s`'s centroid
        /// `j` is `rows[s * CENTROIDS + j]`.
        $(#[$attr])*
        pub(super) fn pq_l2sq(rows: &[Centroid], a: &[u8], b: &[u8]) -> f32 {
            let mut acc = [0f32; SUB];
            for (s, (&x, &y)) in a.iter().zip(b).enumerate() {
                let (p, q) = (&rows[s * CENTROIDS + x as usize], &rows[s * CENTROIDS + y as usize]);
                for l in 0..SUB {
                    let d = p[l] - q[l];
                    acc[l] += d * d;
                }
            }
            fold16(acc)
        }

        /// How many bits `a` and `b` differ in.
        $(#[$attr])*
        pub(super) fn hamming(a: &[u8], b: &[u8]) -> u32 {
            let (ac, ar) = a.as_chunks::<8>();
            let (bc, br) = b.as_chunks::<8>();
            let mut n = 0;
            for (x, y) in ac.iter().zip(bc) {
                n += (u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y)).count_ones();
            }
            for (x, y) in ar.iter().zip(br) {
                n += (x ^ y).count_ones();
            }
            n
        }
    };
}

mod portable {
    kernels!();
}

#[cfg(target_arch = "x86_64")]
mod avx2 {
    kernels!(#[target_feature(enable = "avx2,popcnt")]);
}

/// Whether this CPU runs the AVX2 copies, which also count bits with POPCNT
/// (every CPU with AVX2 has it). `is_x86_feature_detected!` caches its
/// answer, so asking per call costs a load.
#[cfg(target_arch = "x86_64")]
fn avx2() -> bool {
    std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("popcnt")
}

/// Each public kernel: the AVX2 copy where the CPU has it, else the portable.
macro_rules! dispatch {
    ($($name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {$(
        pub(crate) fn $name($($arg: $ty),*) -> $ret {
            #[cfg(target_arch = "x86_64")]
            if avx2() {
                // SAFETY: only reached when the CPU reports AVX2 and POPCNT.
                return unsafe { avx2::$name($($arg),*) };
            }
            portable::$name($($arg),*)
        }
    )*};
}

dispatch! {
    dot(a: &[f32], b: &[f32]) -> f32;
    l2sq(a: &[f32], b: &[f32]) -> f32;
    sq8_dot(c: Sq8<'_>, q: &[f32]) -> f32;
    sq8_l2sq(c: Sq8<'_>, q: &[f32]) -> f32;
    sq8_dot_sq8(a: Sq8<'_>, b: Sq8<'_>) -> f32;
    sq8_l2sq_sq8(a: Sq8<'_>, b: Sq8<'_>) -> f32;
    dots_t(cols: &[f32], x: &[f32], out: &mut [f32]) -> ();
    pq_l2sq(rows: &[Centroid], a: &[u8], b: &[u8]) -> f32;
    hamming(a: &[u8], b: &[u8]) -> u32;
}

/// `Σ lut[s][codes[s]]`: a PQ code against a query's table, one row of
/// [`CENTROIDS`] a subspace.
pub(crate) fn lut_sum(lut: &[[f32; CENTROIDS]], codes: &[u8]) -> f32 {
    let mut acc = [0f32; 4];
    for (s, (row, &c)) in lut.iter().zip(codes).enumerate() {
        acc[s & 3] += row[c as usize];
    }
    (acc[0] + acc[1]) + (acc[2] + acc[3])
}

/// `Σ` of the query values at a 1-bit code's set bits, from a table of every
/// nibble's sum: byte `i`'s low nibble indexes `lut[i][..16]`, its high one
/// `lut[i][16..]`.
pub(crate) fn nibble_sum(lut: &[[f32; 32]], bits: &[u8]) -> f32 {
    let (mut a, mut b) = (0f32, 0f32);
    for (row, &byte) in lut.iter().zip(bits) {
        a += row[(byte & 15) as usize];
        b += row[16 + (byte >> 4) as usize];
    }
    a + b
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kernel against the plain in-order sum, at lengths that leave
    /// every possible remainder after the eight-wide chunks.
    #[test]
    fn kernels_agree_with_the_in_order_sums() {
        let mut x = 0x5EEDu64;
        let mut f = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 40) as f32 / 16_777_216.0 * 2.0 - 1.0
        };
        for n in [1usize, 7, 8, 9, 15, 16, 17, 128, 1536] {
            let a: Vec<f32> = (0..n).map(|_| f()).collect();
            let b: Vec<f32> = (0..n).map(|_| f()).collect();
            let ca: Vec<u8> = (0..n).map(|i| (i * 37 % 256) as u8).collect();
            let cb: Vec<u8> = (0..n).map(|i| (i * 91 % 256) as u8).collect();
            let (al, as_, bl, bs) = (-0.7f32, 0.0051f32, -0.4f32, 0.0037f32);
            let da: Vec<f32> = ca.iter().map(|&c| al + as_ * c as f32).collect();
            let db: Vec<f32> = cb.iter().map(|&c| bl + bs * c as f32).collect();
            let inorder_dot =
                |u: &[f32], v: &[f32]| u.iter().zip(v).map(|(p, q)| p * q).sum::<f32>();
            let inorder_l2 =
                |u: &[f32], v: &[f32]| u.iter().zip(v).map(|(p, q)| (p - q) * (p - q)).sum::<f32>();
            let close = |got: f32, want: f32, what: &str| {
                assert!(
                    (got - want).abs() <= 1e-4 * want.abs().max(1.0),
                    "{what}, n={n}: {got} against {want}"
                );
            };
            close(dot(&a, &b), inorder_dot(&a, &b), "dot");
            close(l2sq(&a, &b), inorder_l2(&a, &b), "l2sq");
            close(sq8_dot((&ca, al, as_), &b), inorder_dot(&da, &b), "sq8_dot");
            close(
                sq8_l2sq((&ca, al, as_), &b),
                inorder_l2(&da, &b),
                "sq8_l2sq",
            );
            close(
                sq8_dot_sq8((&ca, al, as_), (&cb, bl, bs)),
                inorder_dot(&da, &db),
                "sq8_dot_sq8",
            );
            close(
                sq8_l2sq_sq8((&ca, al, as_), (&cb, bl, bs)),
                inorder_l2(&da, &db),
                "sq8_l2sq_sq8",
            );
        }
    }

    /// The 1-bit and PQ kernels against plain loops: bit counts at lengths
    /// with every remainder after the eight-byte words, and the tables.
    #[test]
    fn code_kernels_agree_with_plain_loops() {
        let mut x = 0xB17u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for n in [1usize, 7, 8, 9, 15, 16, 17, 192] {
            let a: Vec<u8> = (0..n).map(|_| next() as u8).collect();
            let b: Vec<u8> = (0..n).map(|_| next() as u8).collect();
            let want: u32 = a.iter().zip(&b).map(|(p, q)| (p ^ q).count_ones()).sum();
            assert_eq!(hamming(&a, &b), want, "hamming, n={n}");
        }
        let f = |v: u64| (v >> 40) as f32 / 16_777_216.0 * 2.0 - 1.0;
        let x16: Vec<f32> = (0..SUB).map(|_| f(next())).collect();
        let cols: Vec<f32> = (0..SUB * CENTROIDS).map(|_| f(next())).collect();
        let mut out = vec![0f32; CENTROIDS];
        dots_t(&cols, &x16, &mut out);
        for (j, &o) in out.iter().enumerate() {
            let want: f32 = (0..SUB).map(|t| x16[t] * cols[t * CENTROIDS + j]).sum();
            assert!(
                (o - want).abs() < 1e-4,
                "dots_t, centroid {j}: {o} against {want}"
            );
        }
        let lut: Vec<[f32; CENTROIDS]> =
            (0..5).map(|_| std::array::from_fn(|_| f(next()))).collect();
        let codes: Vec<u8> = (0..5).map(|_| next() as u8).collect();
        let want: f32 = lut.iter().zip(&codes).map(|(r, &c)| r[c as usize]).sum();
        assert!((lut_sum(&lut, &codes) - want).abs() < 1e-5, "lut_sum");
        let nib: Vec<[f32; 32]> = (0..3).map(|_| std::array::from_fn(|_| f(next()))).collect();
        let bits: Vec<u8> = (0..3).map(|_| next() as u8).collect();
        let want: f32 = nib
            .iter()
            .zip(&bits)
            .map(|(r, &b)| r[(b & 15) as usize] + r[16 + (b >> 4) as usize])
            .sum();
        assert!((nibble_sum(&nib, &bits) - want).abs() < 1e-5, "nibble_sum");
    }
}
