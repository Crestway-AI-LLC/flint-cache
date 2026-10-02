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

/// The kernels' bodies, once: `kernels!` defines each in a module, and the
/// module is compiled twice, once portable and once with AVX2 enabled.
macro_rules! kernels {
    ($(#[$attr:meta])*) => {
        use super::{sum2, Sq8};

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
    };
}

mod portable {
    kernels!();
}

#[cfg(target_arch = "x86_64")]
mod avx2 {
    kernels!(#[target_feature(enable = "avx2")]);
}

/// Whether this CPU runs the AVX2 copies. `is_x86_feature_detected!` caches
/// its answer, so asking per call costs a load.
#[cfg(target_arch = "x86_64")]
fn avx2() -> bool {
    std::is_x86_feature_detected!("avx2")
}

/// Each public kernel: the AVX2 copy where the CPU has it, else the portable.
macro_rules! dispatch {
    ($($name:ident($($arg:ident: $ty:ty),*);)*) => {$(
        pub(crate) fn $name($($arg: $ty),*) -> f32 {
            #[cfg(target_arch = "x86_64")]
            if avx2() {
                // SAFETY: only reached when the CPU reports AVX2.
                return unsafe { avx2::$name($($arg),*) };
            }
            portable::$name($($arg),*)
        }
    )*};
}

dispatch! {
    dot(a: &[f32], b: &[f32]);
    l2sq(a: &[f32], b: &[f32]);
    sq8_dot(c: Sq8<'_>, q: &[f32]);
    sq8_l2sq(c: Sq8<'_>, q: &[f32]);
    sq8_dot_sq8(a: Sq8<'_>, b: Sq8<'_>);
    sq8_l2sq_sq8(a: Sq8<'_>, b: Sq8<'_>);
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
}
