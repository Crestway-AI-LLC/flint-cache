// SPDX-License-Identifier: Elastic-2.0
//! HyperLogLog: `PFADD`, `PFCOUNT` and `PFMERGE`, in Redis's own format.
//!
//! An HLL is a string, as in Redis, and holds the bytes Redis and Valkey
//! store, so `GET`, `SET`, `DUMP`-style copies and a migration move it
//! between them unchanged, and `PFCOUNT` answers what they answer for the
//! same elements:
//!
//! - a 16-byte header: `HYLL`, the encoding (0 dense, 1 sparse), three
//!   unused bytes, and the last count, little-endian, whose top bit marks
//!   it stale;
//! - 16,384 six-bit registers, packed (dense, 12,304 bytes in all), or
//!   run-length opcodes (sparse) while the string stays within 3,000 bytes
//!   and every register within 32;
//! - an element's register and value from MurmurHash64A, seed `0xadc83b19`;
//! - the count from Ertl's estimator over the registers' histogram.
//!
//! Each step below follows Redis's `hyperloglog.c` operation for
//! operation, because the sparse form is not canonical: the bytes depend on
//! the order registers were set in, and on when adjacent runs are merged.

use crate::strings::{StoreError, StringStore};

/// Registers in every HLL (2^14).
pub const REGISTERS: usize = 1 << P;
const P: u32 = 14;
/// Hash bits left once the register's are taken.
const Q: u32 = 64 - P;
const HDR: usize = 16;
const REGISTER_MAX: u32 = 63;
/// A dense HLL's exact length.
pub const DENSE_SIZE: usize = HDR + (REGISTERS * 6).div_ceil(8);
const DENSE: u8 = 0;
const SPARSE: u8 = 1;
/// Past this a sparse HLL becomes dense: Redis's `hll-sparse-max-bytes`.
const SPARSE_MAX_BYTES: usize = 3000;
const VAL_MAX_VALUE: u8 = 32;
const VAL_MAX_LEN: usize = 4;
const ZERO_MAX_LEN: usize = 64;
const XZERO_MAX_LEN: usize = 16384;
/// 0.5 / ln 2.
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;

/// The HLL's bytes do not describe 16,384 registers.
struct Corrupt;

/// MurmurHash64A, as Redis computes it: the input read as little-endian
/// words whatever the machine.
fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let (words, tail) = key.as_chunks::<8>();
    for w in words {
        let mut k = u64::from_le_bytes(*w).wrapping_mul(M);
        k ^= k >> R;
        h ^= k.wrapping_mul(M);
        h = h.wrapping_mul(M);
    }
    if !tail.is_empty() {
        for (i, &b) in tail.iter().enumerate() {
            h ^= u64::from(b) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^ (h >> R)
}

/// An element's register, and the value it offers that register: one more
/// than the trailing zeros of the hash's other 50 bits.
fn pattern(element: &[u8]) -> (usize, u8) {
    let hash = murmur64a(element, 0xadc8_3b19);
    let index = (hash & (REGISTERS as u64 - 1)) as usize;
    let rest = (hash >> P) | (1u64 << Q);
    (index, rest.trailing_zeros() as u8 + 1)
}

/// A new, empty HLL: sparse, every register zero, as `PFADD` and
/// `PFMERGE` create one.
fn empty() -> Vec<u8> {
    let mut v = b"HYLL".to_vec();
    v.extend_from_slice(&[SPARSE, 0, 0, 0]);
    v.extend_from_slice(&[0; 8]);
    let mut left = REGISTERS;
    while left > 0 {
        let run = left.min(XZERO_MAX_LEN);
        v.extend_from_slice(&xzero(run));
        left -= run;
    }
    v
}

/// What Redis accepts as an HLL before reading its registers.
fn valid(v: &[u8]) -> bool {
    v.len() >= HDR
        && v.starts_with(b"HYLL")
        && v[4] <= SPARSE
        && (v[4] != DENSE || v.len() == DENSE_SIZE)
}

fn invalidate(v: &mut [u8]) {
    v[15] |= 0x80;
}

fn cached(v: &[u8]) -> Option<u64> {
    if v[15] & 0x80 != 0 {
        return None;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&v[8..HDR]);
    Some(u64::from_le_bytes(b))
}

fn set_cached(v: &mut [u8], count: u64) {
    v[8..HDR].copy_from_slice(&count.to_le_bytes());
}

// Dense registers: register `i` starts at bit `6i`, low bits first. The
// last one's second byte would be past the end, where Redis reads the
// string's terminating zero; both of its bits are below that byte.

fn dense_get(regs: &[u8], i: usize) -> u8 {
    let (byte, fb) = (i * 6 / 8, (i * 6) & 7);
    let b0 = u32::from(regs[byte]);
    let b1 = u32::from(regs.get(byte + 1).copied().unwrap_or(0));
    (((b0 >> fb) | (b1 << (8 - fb))) & REGISTER_MAX) as u8
}

fn dense_put(regs: &mut [u8], i: usize, value: u8) {
    let (byte, fb) = (i * 6 / 8, (i * 6) & 7);
    let v = u32::from(value);
    regs[byte] &= !((REGISTER_MAX << fb) as u8);
    regs[byte] |= (v << fb) as u8;
    if let Some(b) = regs.get_mut(byte + 1) {
        *b &= !((REGISTER_MAX >> (8 - fb)) as u8);
        *b |= (v >> (8 - fb)) as u8;
    }
}

/// Raise register `i` to `count`; whether it rose.
fn dense_set(regs: &mut [u8], i: usize, count: u8) -> bool {
    if count > dense_get(regs, i) {
        dense_put(regs, i, count);
        true
    } else {
        false
    }
}

// Sparse opcodes: ZERO `00xxxxxx` (1-64 zero registers), XZERO `01xxxxxx
// yyyyyyyy` (1-16,384 zero registers), VAL `1vvvvvxx` (1-4 registers of
// value 1-32).

fn is_zero(b: u8) -> bool {
    b & 0xc0 == 0
}

fn is_xzero(b: u8) -> bool {
    b & 0xc0 == 0x40
}

fn is_val(b: u8) -> bool {
    b & 0x80 != 0
}

fn zero_len(b: u8) -> usize {
    usize::from(b & 0x3f) + 1
}

/// An XZERO's run. One cut off after its first byte reads its second from
/// `past_end`: Redis reads the byte after the string, its terminator,
/// which is zero unless this command has written past the end (see
/// [`sparse_set`]).
fn xzero_len(v: &[u8], p: usize, past_end: u8) -> usize {
    (usize::from(v[p] & 0x3f) << 8 | usize::from(v.get(p + 1).copied().unwrap_or(past_end))) + 1
}

fn val_value(b: u8) -> u8 {
    ((b >> 2) & 0x1f) + 1
}

fn val_len(b: u8) -> usize {
    usize::from(b & 3) + 1
}

fn val(value: u8, len: usize) -> u8 {
    ((value - 1) << 2 | (len as u8 - 1)) | 0x80
}

fn zero(len: usize) -> u8 {
    (len - 1) as u8
}

fn xzero(len: usize) -> [u8; 2] {
    let l = len - 1;
    [(l >> 8) as u8 | 0x40, (l & 0xff) as u8]
}

/// A zero run of `len` registers, as ZERO or XZERO.
fn zeros(seq: &mut Vec<u8>, len: usize) {
    if len > ZERO_MAX_LEN {
        seq.extend_from_slice(&xzero(len));
    } else {
        seq.push(zero(len));
    }
}

/// Rewrite a sparse HLL as dense, keeping its header but the encoding.
fn to_dense(v: &mut Vec<u8>, past_end: u8) -> Result<(), Corrupt> {
    if v[4] == DENSE {
        return Ok(());
    }
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..HDR].copy_from_slice(&v[..HDR]);
    dense[4] = DENSE;
    let (mut idx, mut p) = (0, HDR);
    while p < v.len() {
        let b = v[p];
        if is_zero(b) {
            idx += zero_len(b);
            p += 1;
        } else if is_xzero(b) {
            idx += xzero_len(v, p, past_end);
            p += 2;
        } else {
            let (run, value) = (val_len(b), val_value(b));
            // Past the last register: corrupt, even if the count was
            // exact before it. Valkey 9.1 and Redis 8.2 refuse it so.
            if run + idx > REGISTERS {
                return Err(Corrupt);
            }
            for _ in 0..run {
                dense_put(&mut dense[HDR..], idx, value);
                idx += 1;
            }
            p += 1;
        }
    }
    if idx != REGISTERS {
        return Err(Corrupt);
    }
    *v = dense;
    Ok(())
}

/// Raise register `index` of a sparse HLL to `count`, as Redis's
/// `hllSparseSet`: split the opcode that covers it, then merge equal
/// neighbouring values, or turn the HLL dense when the value or the
/// length outgrows the sparse form. Whether the register rose.
///
/// `past_end` is the byte after the string as Redis's memory holds it for
/// the rest of the command: zero, until a split of a cut-off XZERO writes
/// its last byte there. Redis keeps that byte only in memory, so this
/// matches Redis for one command on stored bytes, which is all a reload or
/// a Redis replica sees either.
fn sparse_set(
    v: &mut Vec<u8>,
    index: usize,
    count: u8,
    past_end: &mut u8,
) -> Result<bool, Corrupt> {
    if count > VAL_MAX_VALUE {
        return promote(v, index, count, *past_end);
    }
    // Find the opcode covering the register.
    let (mut p, mut first, mut span) = (HDR, 0, 0);
    let mut prev = None;
    while p < v.len() {
        let b = v[p];
        let oplen;
        (span, oplen) = if is_zero(b) {
            (zero_len(b), 1)
        } else if is_val(b) {
            (val_len(b), 1)
        } else {
            (xzero_len(v, p, *past_end), 2)
        };
        if index < first + span {
            break;
        }
        prev = Some(p);
        p += oplen;
        first += span;
    }
    if span == 0 || p >= v.len() {
        return Err(Corrupt);
    }
    let b = v[p];
    let (op_zero, op_xzero) = (is_zero(b), is_xzero(b));
    let runlen = span;
    if !op_zero && !op_xzero {
        if val_value(b) >= count {
            return Ok(false);
        }
        if runlen == 1 {
            v[p] = val(count, 1);
            merge_values(v, prev.unwrap_or(HDR), past_end);
            invalidate(v);
            return Ok(true);
        }
    }
    if op_zero && runlen == 1 {
        v[p] = val(count, 1);
        merge_values(v, prev.unwrap_or(HDR), past_end);
        invalidate(v);
        return Ok(true);
    }
    // Split the run around the register: at most XZERO, VAL, XZERO.
    let last = first + span - 1;
    let mut seq = Vec::with_capacity(5);
    if op_zero || op_xzero {
        if index != first {
            zeros(&mut seq, index - first);
        }
        seq.push(val(count, 1));
        if index != last {
            zeros(&mut seq, last - index);
        }
    } else {
        let current = val_value(b);
        if index != first {
            seq.push(val(current, index - first));
        }
        seq.push(val(count, 1));
        if index != last {
            seq.push(val(current, last - index));
        }
    }
    let oldlen = if op_xzero { 2 } else { 1 };
    if seq.len() > oldlen && v.len() + (seq.len() - oldlen) > SPARSE_MAX_BYTES {
        return promote(v, index, count, *past_end);
    }
    if p + oldlen <= v.len() {
        v.splice(p..p + oldlen, seq);
        *past_end = 0;
    } else {
        // An XZERO cut off after its first byte. Redis read the missing
        // byte as the string's terminating zero, and now writes the new
        // sequence over both but grows the string by the difference only,
        // so its last byte falls outside: Valkey 9.1 and Redis 8.2 agree.
        let len = v.len() + seq.len() - oldlen;
        *past_end = seq.last().copied().unwrap_or(0);
        v.truncate(p);
        v.extend_from_slice(&seq);
        v.truncate(len);
    }
    merge_values(v, prev.unwrap_or(HDR), past_end);
    invalidate(v);
    Ok(true)
}

/// Merge adjacent VAL opcodes of one value whose runs fit one opcode,
/// scanning up to five opcodes from `from`, as Redis does after each set.
fn merge_values(v: &mut Vec<u8>, from: usize, past_end: &mut u8) {
    let (mut p, mut scan) = (from, 5);
    while p < v.len() && scan > 0 {
        scan -= 1;
        let b = v[p];
        if is_xzero(b) {
            p += 2;
            continue;
        }
        if is_zero(b) {
            p += 1;
            continue;
        }
        if p + 1 < v.len() && is_val(v[p + 1]) {
            let value = val_value(b);
            let len = val_len(b) + val_len(v[p + 1]);
            if value == val_value(v[p + 1]) && len <= VAL_MAX_LEN {
                v[p + 1] = val(value, len);
                v.remove(p);
                // The string shrank; its new terminator is a zero.
                *past_end = 0;
                continue;
            }
        }
        p += 1;
    }
}

fn promote(v: &mut Vec<u8>, index: usize, count: u8, past_end: u8) -> Result<bool, Corrupt> {
    to_dense(v, past_end)?;
    Ok(dense_set(&mut v[HDR..], index, count))
}

/// Add one element; whether a register rose.
fn add(v: &mut Vec<u8>, element: &[u8], past_end: &mut u8) -> Result<bool, Corrupt> {
    let (index, count) = pattern(element);
    match v[4] {
        DENSE => Ok(dense_set(&mut v[HDR..], index, count)),
        SPARSE => sparse_set(v, index, count, past_end),
        _ => Err(Corrupt),
    }
}

/// Raise each of `max`'s registers to the HLL's.
fn merge_into(max: &mut [u8; REGISTERS], v: &[u8]) -> Result<(), Corrupt> {
    if v[4] == DENSE {
        for (i, m) in max.iter_mut().enumerate() {
            *m = (*m).max(dense_get(&v[HDR..], i));
        }
        return Ok(());
    }
    let (mut i, mut p) = (0, HDR);
    while p < v.len() {
        let b = v[p];
        if is_zero(b) {
            i += zero_len(b);
            p += 1;
        } else if is_xzero(b) {
            i += xzero_len(v, p, 0);
            p += 2;
        } else {
            let (run, value) = (val_len(b), val_value(b));
            if run + i > REGISTERS {
                return Err(Corrupt);
            }
            for m in &mut max[i..i + run] {
                *m = (*m).max(value);
            }
            i += run;
            p += 1;
        }
    }
    if i != REGISTERS {
        return Err(Corrupt);
    }
    Ok(())
}

/// How many registers hold each value.
fn histogram(v: &[u8]) -> Result<[u32; 64], Corrupt> {
    let mut h = [0u32; 64];
    if v[4] == DENSE {
        for i in 0..REGISTERS {
            h[usize::from(dense_get(&v[HDR..], i))] += 1;
        }
        return Ok(h);
    }
    let (mut idx, mut p) = (0, HDR);
    while p < v.len() {
        let b = v[p];
        if is_zero(b) {
            idx += zero_len(b);
            h[0] += zero_len(b) as u32;
            p += 1;
        } else if is_xzero(b) {
            let run = xzero_len(v, p, 0);
            idx += run;
            h[0] += run as u32;
            p += 2;
        } else {
            idx += val_len(b);
            h[usize::from(val_value(b))] += val_len(b) as u32;
            p += 1;
        }
    }
    if idx != REGISTERS {
        return Err(Corrupt);
    }
    Ok(h)
}

fn raw_histogram(max: &[u8; REGISTERS]) -> [u32; 64] {
    let mut h = [0u32; 64];
    for &r in max {
        h[usize::from(r & 63)] += 1;
    }
    h
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut y, mut z) = (1.0, x);
    loop {
        x *= x;
        let before = z;
        z += x * y;
        y += y;
        if before == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut y, mut z) = (1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let before = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if before == z {
            return z / 3.0;
        }
    }
}

/// Ertl's estimate ("New cardinality estimation algorithms for HyperLogLog
/// sketches", 2017), Redis's since 5.0.
fn estimate(h: &[u32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - f64::from(h[Q as usize + 1])) / m);
    for &n in h[1..=Q as usize].iter().rev() {
        z += f64::from(n);
        z *= 0.5;
    }
    z += m * sigma(f64::from(h[0]) / m);
    let e = (ALPHA_INF * m * m / z).round();
    // Past 2^63, which only registers no hash can produce reach, Redis's
    // `llroundl` overflows to its minimum, and Redis answers that as a
    // negative count: so do we.
    if e.is_finite() && e < 9_223_372_036_854_775_808.0 {
        e as u64
    } else {
        1 << 63
    }
}

fn corrupt(_: Corrupt) -> StoreError {
    StoreError::CorruptHll
}

impl StringStore<'_> {
    /// A live key's HLL, its expiry, and whether it exists: `NotHll` for a
    /// string that is not one, `WrongType` for another type.
    fn hll_at(&self, slot: u16, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>, StoreError> {
        match self.value_and_expiry(slot, key)? {
            Some((v, _)) if !valid(&v) => Err(StoreError::NotHll),
            found => Ok(found),
        }
    }

    fn put_hll(&self, slot: u16, key: &[u8], v: Vec<u8>, expire_ms: u64) -> Result<(), StoreError> {
        if v.len() as u64 > self.max_value_bytes() {
            return Err(StoreError::ValueTooLarge);
        }
        self.store_value(slot, key, v, expire_ms);
        Ok(())
    }

    /// `PFADD key [element ...]`: whether the HLL changed, or was created.
    /// Nothing is written when it did not, and nothing at all on an error.
    pub fn pfadd(&self, slot: u16, key: &[u8], elements: &[Vec<u8>]) -> Result<bool, StoreError> {
        let (mut v, expire_ms, mut updated) = match self.hll_at(slot, key)? {
            Some((v, expire_ms)) => (v, expire_ms, false),
            None => (empty(), 0, true),
        };
        let mut past_end = 0;
        for e in elements {
            updated |= add(&mut v, e, &mut past_end).map_err(corrupt)?;
        }
        if updated {
            invalidate(&mut v);
            self.put_hll(slot, key, v, expire_ms)?;
        }
        Ok(updated)
    }

    /// `PFCOUNT key [key ...]`. One key's count is kept in its header, as
    /// Redis keeps it, so the HLL is rewritten when that count was stale;
    /// several keys' union is counted and nothing is written. A missing key
    /// counts as empty. The keys share `slot`.
    pub fn pfcount(&self, slot: u16, keys: &[Vec<u8>]) -> Result<u64, StoreError> {
        if let [key] = keys {
            let Some((mut v, expire_ms)) = self.hll_at(slot, key)? else {
                return Ok(0);
            };
            if let Some(n) = cached(&v) {
                return Ok(n);
            }
            let n = estimate(&histogram(&v).map_err(corrupt)?);
            set_cached(&mut v, n);
            self.put_hll(slot, key, v, expire_ms)?;
            return Ok(n);
        }
        let mut max = [0u8; REGISTERS];
        for key in keys {
            if let Some((v, _)) = self.hll_at(slot, key)? {
                merge_into(&mut max, &v).map_err(corrupt)?;
            }
        }
        Ok(estimate(&raw_histogram(&max)))
    }

    /// `PFMERGE dst [src ...]`: `dst` becomes the union of itself and the
    /// sources. It is written dense when any of them is dense, and
    /// otherwise register by register into its sparse form, as Redis does,
    /// so the bytes are Redis's. The keys share `slot`.
    pub fn pfmerge(&self, slot: u16, dst: &[u8], sources: &[Vec<u8>]) -> Result<(), StoreError> {
        let mut max = [0u8; REGISTERS];
        let mut dense = false;
        let target = self.hll_at(slot, dst)?;
        if let Some((v, _)) = &target {
            dense |= v[4] == DENSE;
            merge_into(&mut max, v).map_err(corrupt)?;
        }
        for key in sources {
            if let Some((v, _)) = self.hll_at(slot, key)? {
                dense |= v[4] == DENSE;
                merge_into(&mut max, &v).map_err(corrupt)?;
            }
        }
        let (mut v, expire_ms) = target.unwrap_or_else(|| (empty(), 0));
        if dense {
            to_dense(&mut v, 0).map_err(corrupt)?;
        }
        let mut past_end = 0;
        for (i, &m) in max.iter().enumerate() {
            if m == 0 {
                continue;
            }
            if v[4] == DENSE {
                dense_set(&mut v[HDR..], i, m);
            } else {
                // Its own registers were checked by the merge above.
                let _ = sparse_set(&mut v, i, m, &mut past_end);
            }
        }
        invalidate(&mut v);
        self.put_hll(slot, dst, v, expire_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    fn now() -> u64 {
        1_000
    }

    #[test]
    fn a_new_hll_is_redis_bytes() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        assert_eq!(s.pfadd(1, b"k", &[]), Ok(true));
        // Valkey 9.1 and Redis 8.2: sparse, one XZERO of 16,384, the
        // count marked stale.
        let mut want = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        want.extend_from_slice(&[0x7f, 0xff]);
        assert_eq!(s.get(1, b"k"), Ok(Some(want)));
        assert_eq!(s.pfadd(1, b"k", &[]), Ok(false));
    }

    #[test]
    fn three_elements_are_redis_bytes_and_count() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let abc: Vec<Vec<u8>> = ["a", "b", "c"]
            .iter()
            .map(|e| e.as_bytes().to_vec())
            .collect();
        assert_eq!(s.pfadd(1, b"k", &abc), Ok(true));
        // Valkey 9.1 and Redis 8.2 store exactly these bytes.
        let mut want = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        want.extend_from_slice(&[
            0x60, 0xf3, 0x80, 0x50, 0xb1, 0x84, 0x4b, 0xfb, 0x80, 0x42, 0x5a,
        ]);
        assert_eq!(s.get(1, b"k"), Ok(Some(want.clone())));
        assert_eq!(s.pfcount(1, &[b"k".to_vec()]), Ok(3));
        // The count is kept, as Redis keeps it.
        want[8] = 3;
        want[15] = 0;
        assert_eq!(s.get(1, b"k"), Ok(Some(want)));
        assert_eq!(s.pfadd(1, b"k", &abc), Ok(false));
    }

    fn elements(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("e{i}").into_bytes()).collect()
    }

    /// Lengths and counts Valkey 9.1 and Redis 8.2 give for the same
    /// elements: 200 stay sparse in 513 bytes, 2,000 turn dense; merged, a
    /// dense source makes the result dense; the union of {a, b, c} and
    /// {c, d} is these bytes.
    #[test]
    fn sizes_counts_and_a_merge_are_redis_s() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let len = |k: &[u8]| s.get(1, k).ok().flatten().map(|v| v.len());
        s.pfadd(1, b"s", &elements(200)).expect("pfadd");
        s.pfadd(1, b"d", &elements(2000)).expect("pfadd");
        assert_eq!((len(b"s"), len(b"d")), (Some(513), Some(DENSE_SIZE)));
        assert_eq!(s.pfcount(1, &[b"s".to_vec()]), Ok(200));
        assert_eq!(s.pfcount(1, &[b"d".to_vec()]), Ok(2000));
        s.pfmerge(1, b"x", &[b"s".to_vec(), b"d".to_vec()])
            .expect("pfmerge");
        assert_eq!(
            (len(b"x"), s.pfcount(1, &[b"x".to_vec()])),
            (Some(DENSE_SIZE), Ok(2000))
        );
        let abc: Vec<Vec<u8>> = ["a", "b", "c"]
            .iter()
            .map(|e| e.as_bytes().to_vec())
            .collect();
        s.pfadd(1, b"a", &abc).expect("pfadd");
        s.pfadd(1, b"b", &[b"c".to_vec(), b"d".to_vec()])
            .expect("pfadd");
        assert_eq!(
            s.pfcount(1, &[b"a".to_vec(), b"b".to_vec(), b"none".to_vec()]),
            Ok(4)
        );
        s.pfmerge(1, b"m", &[b"a".to_vec(), b"b".to_vec()])
            .expect("pfmerge");
        let mut want = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        want.extend_from_slice(&[
            0x5c, 0x7b, 0x80, 0x44, 0x76, 0x80, 0x50, 0xb1, 0x84, 0x4b, 0xfb,
        ]);
        want.extend_from_slice(&[0x80, 0x42, 0x5a]);
        assert_eq!(s.get(1, b"m"), Ok(Some(want)));
        // One register set in an otherwise empty dense HLL: the merge is
        // still dense, as Redis makes it.
        let mut one = vec![0u8; DENSE_SIZE];
        one[..HDR].copy_from_slice(b"HYLL\0\0\0\0\0\0\0\0\0\0\0\x80");
        dense_put(&mut one[HDR..], 7, 3);
        s.set(1, b"one", &one, Default::default()).expect("set");
        s.pfmerge(1, b"y", &[b"one".to_vec()]).expect("pfmerge");
        assert_eq!(len(b"y"), Some(DENSE_SIZE));
    }

    #[test]
    fn a_value_past_32_turns_a_sparse_hll_dense() {
        let mut v = empty();
        assert!(matches!(sparse_set(&mut v, 100, 33, &mut 0), Ok(true)));
        assert_eq!(v.len(), DENSE_SIZE);
        assert_eq!(v[4], DENSE);
        assert_eq!(dense_get(&v[HDR..], 100), 33);
        assert_eq!(histogram(&v).ok().map(|h| (h[0], h[33])), Some((16_383, 1)));
    }

    /// A sparse HLL ending in half an XZERO: what Valkey 9.1 and Redis 8.2
    /// leave after `PFADD t a` (it panicked here before).
    #[test]
    fn an_add_into_a_cut_off_xzero_writes_what_redis_writes() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let hdr = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        for (tail, want) in [
            (&[0x7f][..], &[0x71, 0xa6, 0x84, 0x4d][..]),
            (&[0x80, 0x7f], &[0x80, 0x71, 0xa5, 0x84, 0x4d]),
        ] {
            s.set(
                1,
                b"t",
                &[hdr.clone(), tail.to_vec()].concat(),
                Default::default(),
            )
            .expect("set");
            assert_eq!(s.pfadd(1, b"t", &[b"a".to_vec()]), Ok(true));
            assert_eq!(
                s.get(1, b"t"),
                Ok(Some([hdr.clone(), want.to_vec()].concat()))
            );
        }
    }

    /// Three elements into a cut-off XZERO in one command: each split
    /// leaves its last byte past the end, and the next element, landing in
    /// the run that byte measures, reads it there, as Valkey 9.1 and Redis
    /// 8.2 do. Read as zero, the third element's register would lie past
    /// the run, and the HLL would read as corrupt.
    #[test]
    fn a_second_split_reads_the_byte_the_first_left_past_the_end() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let hdr = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        s.set(
            1,
            b"t",
            &[hdr.clone(), vec![0x7f]].concat(),
            Default::default(),
        )
        .expect("set");
        // Registers 12,711, 15,949 and 16,023.
        let three = [b"a".to_vec(), b"e1".to_vec(), b"e13".to_vec()];
        assert_eq!(s.pfadd(1, b"t", &three), Ok(true));
        let tail = [0x71, 0xa6, 0x84, 0x4c, 0xa4, 0x84, 0x40, 0x48, 0x80, 0x40];
        let want = [hdr, tail.to_vec()].concat();
        assert_eq!(s.get(1, b"t"), Ok(Some(want)));
    }

    /// Registers at 51, which only a 2^-50 hash reaches, are where Ertl's
    /// tau term counts. Valkey 9.1 and Redis 8.2 count this HLL as 402,777.
    #[test]
    fn the_estimate_matches_redis_where_registers_saturate() {
        let mut v = vec![0u8; DENSE_SIZE];
        v[..HDR].copy_from_slice(b"HYLL\0\0\0\0\0\0\0\0\0\0\0\x80");
        for i in 0..REGISTERS {
            dense_put(&mut v[HDR..], i, if i < 1000 { 51 } else { 5 });
        }
        assert_eq!(histogram(&v).ok().map(|h| estimate(&h)), Some(402_777));
    }

    /// Mid-range counts, where the zero registers' term (sigma) still
    /// weighs: Valkey 9.1 and Redis 8.2 count these as 9,008 and 29,797.
    #[test]
    fn mid_range_counts_match_redis() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let u: Vec<Vec<u8>> = (0..9000).map(|i| format!("u{i}").into_bytes()).collect();
        s.pfadd(1, b"u", &u).expect("pfadd");
        s.pfadd(1, b"e", &elements(30_000)).expect("pfadd");
        assert_eq!(s.pfcount(1, &[b"u".to_vec()]), Ok(9008));
        assert_eq!(s.pfcount(1, &[b"e".to_vec()]), Ok(29_797));
    }

    /// Registers above 51, which no hash produces, as Valkey 9.1 and Redis
    /// 8.2 count them: ignored, and an estimate past 2^63 answered as the
    /// most negative count.
    #[test]
    fn impossible_registers_count_as_redis_counts_them() {
        let dense = |f: &dyn Fn(usize) -> u8| {
            let mut v = vec![0u8; DENSE_SIZE];
            v[..HDR].copy_from_slice(b"HYLL\0\0\0\0\0\0\0\0\0\0\0\x80");
            for i in 0..REGISTERS {
                dense_put(&mut v[HDR..], i, f(i));
            }
            histogram(&v).ok().map(|h| estimate(&h) as i64)
        };
        assert_eq!(dense(&|_| 63), Some(i64::MIN));
        assert_eq!(dense(&|i| if i < 16_000 { 51 } else { 60 }), Some(i64::MIN));
        assert_eq!(dense(&|i| if i < 8192 { 52 } else { 3 }), Some(189_097));
    }

    #[test]
    fn dense_registers_round_trip_at_every_offset() {
        let mut regs = vec![0u8; DENSE_SIZE - HDR];
        for i in 0..REGISTERS {
            dense_put(&mut regs, i, (i % 64) as u8);
        }
        for i in 0..REGISTERS {
            assert_eq!(dense_get(&regs, i), (i % 64) as u8, "register {i}");
        }
    }

    #[test]
    fn not_an_hll_and_a_corrupt_one_are_refused_untouched() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        s.set(1, b"s", b"hello", Default::default()).expect("set");
        assert_eq!(s.pfadd(1, b"s", &[b"x".to_vec()]), Err(StoreError::NotHll));
        // A sparse HLL whose opcodes cover too few registers.
        let bad = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80\x00".to_vec();
        s.set(1, b"bad", &bad, Default::default()).expect("set");
        assert_eq!(
            s.pfcount(1, &[b"bad".to_vec()]),
            Err(StoreError::CorruptHll)
        );
        assert_eq!(s.get(1, b"bad"), Ok(Some(bad)));
        // Every register covered, then one VAL more: corrupt to a merge too.
        let mut past = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        past.extend_from_slice(&[0x7f, 0xfe, 0x80, 0x80]);
        s.set(1, b"past", &past, Default::default()).expect("set");
        let pair = [b"past".to_vec(), b"none".to_vec()];
        assert_eq!(s.pfcount(1, &pair), Err(StoreError::CorruptHll));
        assert_eq!(
            s.pfmerge(1, b"d", &[b"past".to_vec()]),
            Err(StoreError::CorruptHll)
        );
        // A dense one of the wrong length is not an HLL at all.
        let short = b"HYLL\x00\0\0\0\0\0\0\0\0\0\0\x80".to_vec();
        s.set(1, b"short", &short, Default::default()).expect("set");
        assert_eq!(s.pfcount(1, &[b"short".to_vec()]), Err(StoreError::NotHll));
    }
}
