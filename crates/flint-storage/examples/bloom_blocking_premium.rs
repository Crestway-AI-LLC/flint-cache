// SPDX-License-Identifier: Elastic-2.0
//! Measures the BLOCKING PREMIUM — ADR-0016's Verification 4, the one that
//! gates the 4 KiB block-size constant.
//!
//! Confining an item's `k` probes to one block (D2) trades space for I/O:
//! per-block occupancy varies, a fuller block has a worse error rate than
//! the average, so a blocked filter needs more bits per item than a flat
//! one to hit the same MEASURED false-positive rate. ADR-0016 claims that
//! premium is under a percent at 4 KiB blocks, and says so on reasoning
//! rather than measurement. This is the measurement.
//!
//! It matters now rather than later because the block size is ON-DISK
//! FORMAT: every stored filter's bits are placed by it, so changing it
//! after a release is a migration, not a tuning change.
//!
//! Method: sweep bits-per-item for each layout and keep the whole FPR
//! curve. The premium is read as the FPR difference at one reference
//! bits/item, converted into bits through the flat curve's own exchange
//! rate (the least-squares slope of ln(fpr) against bits/item).
//!
//! Going through the slope rather than just comparing crossing points is
//! not sophistication for its own sake: the sweep step is 0.25 bits/item,
//! about 2.6% of the figure, so a crossing-point comparison cannot see a
//! premium of the size being claimed at all — every small-block layout
//! lands on the same grid point as flat and reports a flat 0%, which reads
//! as a result and is really just the grid.
//!
//! `k` is held at whatever `plan` picks for the target rate, because that
//! is what the shipped code uses. The blocked side runs the REAL
//! `place()`; the flat side is a reference filter written here for
//! comparison, using the same hash and the same double-hashing derivation
//! so that confinement to a block is the only difference between them.
//!
//! The 64 B row is the positive control. Blocking is supposed to cost real
//! space at cache-line block sizes, so a harness that reported ~0%
//! everywhere would be one that cannot detect a premium — and would
//! "confirm" the ADR by being blind. The run FAILS if 64 B stops
//! resolving above 5%.
//!
//! Run: cargo run --release -p flint-storage --example bloom_blocking_premium

use flint_storage::bloom::{bit_is_set, murmur3_x64_128, place, plan, set_bit};
use flint_storage::encoding::BloomLink;

const N: usize = 1_000_000;
/// 4 M absent lookups puts ~40,000 false positives in each measurement at
/// the 1% target, so the counting error is ~0.5% relative. That matters:
/// the premium being tested for is itself around a percent, and a sample
/// that cannot separate 1% from 0% would "confirm" the ADR by being blunt.
const TRIALS: usize = 4_000_000;

/// Bits/item the premium is reported AT: the closest grid point to the
/// classic 9.585 optimum for a 1% rate, which is where `plan` sizes a real
/// filter.
const REFERENCE_BPI: f64 = 9.75;

fn item(i: usize) -> Vec<u8> {
    format!("item:{i}").into_bytes()
}

fn absent(i: usize) -> Vec<u8> {
    format!("absent:{i}").into_bytes()
}

/// The same mix the placement uses to derive a probe stride. Duplicated
/// here rather than exported: this file is a measurement, and the library
/// surface should not grow a hook that exists only for it.
fn mix(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// Flat reference filter: `k` probes anywhere in `m_bits`.
///
/// Probe collisions are possible here (unlike the blocked layout, where an
/// odd stride against a power-of-two block makes them impossible), but at
/// k=7 over millions of bits the chance is ~1e-6 per item — three orders
/// of magnitude below this measurement's resolution, and it would bias
/// AGAINST the blocked layout, which is the safe direction.
fn flat_probes(data: &[u8], m_bits: u64, k: usize) -> Vec<u64> {
    let (h1, h2) = murmur3_x64_128(data, 0);
    let stride = mix(h1 ^ h2) | 1;
    (0..k)
        .map(|i| h2.wrapping_add((i as u64).wrapping_mul(stride)) % m_bits)
        .collect()
}

fn measure_flat(m_bits: u64, k: usize) -> f64 {
    let mut bits = vec![0u8; (m_bits as usize).div_ceil(8)];
    for i in 0..N {
        for p in flat_probes(&item(i), m_bits, k) {
            bits[(p / 8) as usize] |= 1 << (p % 8);
        }
    }
    let mut fp = 0usize;
    for i in 0..TRIALS {
        if flat_probes(&absent(i), m_bits, k)
            .into_iter()
            .all(|p| bits[(p / 8) as usize] & (1 << (p % 8)) != 0)
        {
            fp += 1;
        }
    }
    fp as f64 / TRIALS as f64
}

fn measure_blocked(m_bits: u64, k: u8, block_bits_log2: u8) -> f64 {
    let block_bits = 1u64 << block_bits_log2;
    let blocks = m_bits.div_ceil(block_bits);
    let link = BloomLink {
        k,
        block_bits_log2,
        blocks,
        items: 0,
        capacity: N as u64,
    };
    let block_bytes = (block_bits / 8) as usize;
    let mut store: Vec<Vec<u8>> = vec![Vec::new(); blocks as usize];

    for i in 0..N {
        let p = place(&item(i), &link);
        let b = &mut store[p.block as usize];
        for &bit in p.bits() {
            set_bit(b, bit, block_bytes);
        }
    }
    let mut fp = 0usize;
    for i in 0..TRIALS {
        let p = place(&absent(i), &link);
        let b = &store[p.block as usize];
        if p.bits().iter().all(|&bit| bit_is_set(b, bit)) {
            fp += 1;
        }
    }
    fp as f64 / TRIALS as f64
}

/// The whole FPR curve for one layout: (bits/item, measured fpr).
fn sweep(mut measure: impl FnMut(u64) -> f64) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    let mut bpi = 8.0;
    while bpi <= 11.0 + 1e-9 {
        let m_bits = (N as f64 * bpi).ceil() as u64;
        let fpr = measure(m_bits);
        println!("      {bpi:>5.2} bits/item -> fpr {fpr:.5}");
        out.push((bpi, fpr));
        bpi += 0.25;
    }
    out
}

/// Least-squares slope of ln(fpr) against bits/item — how much one extra
/// bit per item buys. Negative. This is what converts an FPR difference
/// into the bits it is worth, which is the only way to resolve a premium
/// smaller than the sweep's step.
fn ln_fpr_slope(curve: &[(f64, f64)]) -> f64 {
    let n = curve.len() as f64;
    let mx = curve.iter().map(|(x, _)| x).sum::<f64>() / n;
    let my = curve.iter().map(|(_, y)| y.ln()).sum::<f64>() / n;
    let num: f64 = curve.iter().map(|(x, y)| (x - mx) * (y.ln() - my)).sum();
    let den: f64 = curve.iter().map(|(x, _)| (x - mx) * (x - mx)).sum();
    num / den
}

fn at(curve: &[(f64, f64)], bpi: f64) -> f64 {
    curve
        .iter()
        .find(|(x, _)| (x - bpi).abs() < 1e-9)
        .map(|(_, y)| *y)
        .expect("reference bits/item is on the sweep grid")
}

/// Smallest bits/item on the curve whose measured FPR meets `target`.
fn crossing(curve: &[(f64, f64)], target: f64) -> Option<f64> {
    curve.iter().find(|(_, f)| *f <= target).map(|(x, _)| *x)
}

fn main() {
    let target = 0.01;
    let k = plan(N as u64, target).expect("plan").k;
    println!("blocking premium: n={N} items, {TRIALS} absent trials, target fpr {target}, k={k}\n");

    println!("  flat (probes anywhere):");
    let flat = sweep(|m| measure_flat(m, k as usize));
    let slope = ln_fpr_slope(&flat);
    let flat_fpr = at(&flat, REFERENCE_BPI);

    let mut rows = vec![("flat", flat_fpr, 0.0, crossing(&flat, target))];
    for log2 in [9u8, 12, 15] {
        let bytes = (1usize << log2) / 8;
        println!("\n  blocked, {bytes} B blocks:");
        let curve = sweep(|m| measure_blocked(m, k, log2));
        let fpr = at(&curve, REFERENCE_BPI);
        // Extra bits/item this layout needs to bring its rate down to the
        // flat one's, read off the flat curve's own exchange rate.
        let extra_bits = (fpr / flat_fpr).ln() / -slope;
        rows.push((
            match log2 {
                9 => "blocked 64 B",
                12 => "blocked 512 B",
                _ => "blocked 4 KiB",
            },
            fpr,
            extra_bits / REFERENCE_BPI * 100.0,
            crossing(&curve, target),
        ));
    }

    println!(
        "\n  one extra bit/item multiplies the rate by {:.3}\n",
        slope.exp()
    );
    println!("  layout           fpr @ {REFERENCE_BPI}   premium   crossing (fpr<={target})");
    for (name, fpr, premium, cross) in &rows {
        let c = cross.map(|c| format!("{c:.2}")).unwrap_or("none".into());
        println!("  {name:<15} {fpr:>11.5}   {premium:>6.2}%   {c:>18}");
    }

    // How small a premium this run can tell apart from zero. Each cell
    // holds ~TRIALS*fpr false positives, so a single rate carries
    // 1/sqrt(count) relative counting error and a RATIO of two carries
    // sqrt(2/count); converting that through the exchange rate gives the
    // floor in bits. Stated rather than implied, so neither the verdict
    // nor a reader can take the premium for more precision than it has.
    let four_kib = rows.last().expect("4 KiB row").2;
    let count = TRIALS as f64 * flat_fpr;
    let floor = (1.0 + (2.0 / count).sqrt()).ln() / -slope / REFERENCE_BPI * 100.0;
    println!("\n  ~{count:.0} false positives per cell");
    println!("  resolution floor: {floor:.2}% of bits/item");
    println!("  4 KiB premium: {four_kib:.2}%");

    // The 64 B row is this measurement's positive control. Blocking IS
    // supposed to cost real space at cache-line block sizes, so a harness
    // that reported ~0% everywhere would be one that cannot detect a
    // premium at all — and would "confirm" the ADR by being blind.
    let sixty_four = rows[1].2;
    if sixty_four < 5.0 {
        println!(
            "  FAIL: 64 B blocks measured only {sixty_four:.2}% — a premium is \
             expected there, so this harness is not detecting one. The 4 KiB \
             result cannot be trusted."
        );
        std::process::exit(1);
    }
    println!(
        "  positive control: 64 B blocks resolve at {sixty_four:.2}%, so the harness does detect a premium"
    );

    // Signed, not absolute: confining probes to a block cannot IMPROVE the
    // rate in expectation, so a negative reading is scatter and belongs in
    // the same bucket as a small positive one. Reporting it as a resolved
    // improvement would be reading noise as a result.
    if four_kib <= floor {
        println!(
            "  VERDICT: at or below the resolution floor — indistinguishable from \
             zero at this sample size, NOT measured to be zero. ADR-0016 D2's \
             \"under a percent\" holds and the 4 KiB constant stands."
        );
    } else if four_kib < 1.0 {
        println!(
            "  VERDICT: resolved at {four_kib:.2}%, under a percent. ADR-0016 D2's \
             claim holds and the 4 KiB constant stands."
        );
    } else {
        println!(
            "  VERDICT: {four_kib:.2}% — ADR-0016 D2 claims under a percent. \
             Revisit the block size BEFORE a release makes it a migration."
        );
        std::process::exit(1);
    }
}
