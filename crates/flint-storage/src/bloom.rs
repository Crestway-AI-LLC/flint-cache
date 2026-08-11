// SPDX-License-Identifier: Elastic-2.0
//! Bloom filter placement: the hash, the sizing, and where an item's bits
//! live (ADR-0016).
//!
//! Everything in this file is ON-DISK FORMAT. A change to the hash, to the
//! probe derivation, or to the bit order inside a block does not corrupt a
//! stored filter in any way a check would notice — it silently relocates
//! every bit, so old filters start answering wrongly in BOTH directions,
//! including the false negatives a Bloom filter is not allowed to have.
//! That is why `BloomMeta` records a hash id and a layout id: a future
//! change is a new id and a read-path branch, never a reinterpretation of
//! rows already written.
//!
//! The layout is the blocked (split-block) filter of ADR-0016 D2: an item
//! hashes to exactly ONE block, and all `k` of its probes land inside that
//! block. One block is one subkey row, so `BF.EXISTS` is one point get and
//! `BF.ADD` is one get plus one put.

use crate::Kv;
use crate::encoding::{
    BloomLink, BloomMeta, Cf, MetaHeader, ValueType, VersionGen, envelope, subkey_envelope,
};
use crate::strings::{Clock, StoreError};

/// Bits per block at full size: 4 KiB. The block is the unit an item is
/// confined to, so this is the knob the blocking premium trades against
/// (ADR-0016 D2) — bigger blocks mean less per-block occupancy variance and
/// therefore less wasted space, smaller blocks mean less write
/// amplification per set bit. 4 KiB holds ~3,400 items at a 1% error rate,
/// where occupancy varies by under 2%.
pub const BLOCK_BITS_LOG2_MAX: u8 = 15;

/// Smallest block, for filters whose whole bitmap is tiny: 64 bits. A
/// filter below one block is a single row, which is just a plain Bloom
/// filter stored inline — the scheme degenerates instead of needing a
/// promotion path.
pub const BLOCK_BITS_LOG2_MIN: u8 = 6;

/// Ceiling on probes per item. `k` is `log2(1/error)`, so this bounds the
/// error rate a filter may ask for at about 2^-32.
pub const MAX_K: usize = 32;

/// MurmurHash3 x64_128 — the finalized 128-bit variant, little-endian body
/// reads, as published by Austin Appleby.
///
/// In-tree with no dependency, matching how `flint-slot` carries CRC16: the
/// requirement is byte-stability forever, which a pinned local
/// implementation gives and a version-ranged dependency does not.
pub fn murmur3_x64_128(data: &[u8], seed: u64) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1 = seed;
    let mut h2 = seed;

    let nblocks = data.len() / 16;
    for i in 0..nblocks {
        let o = i * 16;
        let mut k1 = le_u64(&data[o..o + 8]);
        let mut k2 = le_u64(&data[o + 8..o + 16]);

        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27).wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31).wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;
    // The reference is a fallthrough switch; here each byte present is
    // simply folded in, which is the same thing written forwards.
    for (i, &b) in tail.iter().enumerate() {
        if i < 8 {
            k1 ^= (b as u64) << (8 * i);
        } else {
            k2 ^= (b as u64) << (8 * (i - 8));
        }
    }
    if tail.len() > 8 {
        k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 ^= k2;
    }
    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

/// Little-endian u64 from the first 8 bytes. Total by construction — it
/// folds whatever is there — rather than a fallible array conversion, so
/// the hash has no panic path at all.
fn le_u64(b: &[u8]) -> u64 {
    let mut v = 0u64;
    for (i, &x) in b.iter().enumerate().take(8) {
        v |= (x as u64) << (8 * i);
    }
    v
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// Where one item's bits live: a block index, and `k` bit offsets INSIDE
/// that block.
#[derive(Debug, Clone, Copy)]
pub struct Placement {
    pub block: u64,
    bits: [u32; MAX_K],
    k: usize,
}

impl Placement {
    /// The probe offsets, in bits from the start of the block.
    pub fn bits(&self) -> &[u32] {
        &self.bits[..self.k]
    }
}

/// Place `item` in `link`.
///
/// Block choice uses the high-multiply reduction rather than `%`: it
/// consumes all 64 bits of the hash and needs no power-of-two block count.
///
/// The probes are `h2 + i*stride (mod block_bits)` with **stride forced
/// odd** and `block_bits` a power of two. That combination is not
/// decoration: an odd stride is invertible modulo a power of two, so the
/// `k` probes are guaranteed DISTINCT. With an even stride they could
/// collapse — in the worst case every probe onto one bit, which would gut
/// the error rate of a filter that still looked and tested fine.
pub fn place(item: &[u8], link: &BloomLink) -> Placement {
    let (h1, h2) = murmur3_x64_128(item, 0);
    let block = ((h1 as u128 * link.blocks.max(1) as u128) >> 64) as u64;
    let stride = fmix64(h1 ^ h2) | 1;
    let mask = (1u64 << link.block_bits_log2) - 1;

    let k = (link.k as usize).clamp(1, MAX_K);
    let mut bits = [0u32; MAX_K];
    for (i, slot) in bits.iter_mut().enumerate().take(k) {
        // Wrapping is exactly right: 2^block_bits_log2 divides 2^64, so
        // arithmetic mod 2^64 then masked equals arithmetic mod block_bits.
        *slot = (h2.wrapping_add((i as u64).wrapping_mul(stride)) & mask) as u32;
    }
    Placement { block, bits, k }
}

/// Bit `p` of a block lives in byte `p / 8`, at bit `p % 8` counted from
/// the least significant end. Stated once, here, because it is format.
#[inline]
pub fn bit_is_set(block: &[u8], bit: u32) -> bool {
    let byte = (bit / 8) as usize;
    // A block row shorter than the nominal block — or absent entirely, read
    // as an empty slice — means those bits were never set.
    byte < block.len() && block[byte] & (1 << (bit % 8)) != 0
}

/// Set bit `p`, growing `block` if the row does not reach that far yet.
/// Returns whether the bit was previously clear.
#[inline]
pub fn set_bit(block: &mut Vec<u8>, bit: u32, block_bytes: usize) -> bool {
    let byte = (bit / 8) as usize;
    if block.len() < block_bytes {
        block.resize(block_bytes, 0);
    }
    let was = block[byte] & (1 << (bit % 8)) != 0;
    block[byte] |= 1 << (bit % 8);
    !was
}

/// Size a filter for `capacity` items at `error` false-positive rate.
///
/// The classic optimum: `m/n = -ln(p) / (ln 2)^2` bits per item and
/// `k = log2(1/p)` probes. Returns `None` for an error rate outside
/// `(0, 1)`, or a capacity of zero.
///
/// The result is STORED in the metadata row, never recomputed on read
/// (ADR-0016 D4) — this function may change, filters already on disk may
/// not.
pub fn plan(capacity: u64, error: f64) -> Option<BloomLink> {
    if capacity == 0 || !(error > 0.0 && error < 1.0) {
        return None;
    }
    let bits_per_item = -error.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2);
    let m = (capacity as f64 * bits_per_item).ceil();
    if !m.is_finite() || m > u64::MAX as f64 {
        return None;
    }
    let m = (m as u64).max(1 << BLOCK_BITS_LOG2_MIN);

    let k = (-error.log2()).round().clamp(1.0, MAX_K as f64) as u8;

    let (block_bits_log2, blocks) = if m <= (1u64 << BLOCK_BITS_LOG2_MAX) {
        // One block, rounded up to a power of two so the probe stride
        // argument holds and the mask is exact.
        let log2 = (64 - (m - 1).leading_zeros()) as u8;
        (log2.clamp(BLOCK_BITS_LOG2_MIN, BLOCK_BITS_LOG2_MAX), 1)
    } else {
        let per = 1u64 << BLOCK_BITS_LOG2_MAX;
        (BLOCK_BITS_LOG2_MAX, m.div_ceil(per))
    };

    Some(BloomLink {
        k,
        block_bits_log2,
        blocks,
        items: 0,
        capacity,
    })
}

/// Bytes a fully-materialized filter would occupy. What it occupies TODAY
/// is the sum of the rows that exist, since blocks materialize lazily
/// (ADR-0016 D3) — this is the ceiling, used to refuse a reservation past
/// the max-value cap before any row is written.
pub fn nominal_bytes(link: &BloomLink) -> u64 {
    link.blocks.saturating_mul(1u64 << link.block_bits_log2) / 8
}

/// Capacity an auto-created filter (a `BF.ADD` with no prior `BF.RESERVE`)
/// is sized for.
///
/// RedisBloom's default is 100. Raised here deliberately (ADR-0016 D5):
/// there, over-provisioning costs bytes of RAM and a chain link is a
/// pointer chase; here a link is a DISK READ on every lookup, and a filter
/// left at 100 that grows to a million items becomes ~14 links — a p99 set
/// by a default the user never saw. Blocks materialize lazily (D3), so the
/// larger default costs nothing until it is used.
pub const DEFAULT_CAPACITY: u64 = 100_000;

/// Default false-positive rate, matching RedisBloom.
pub const DEFAULT_ERROR: f64 = 0.01;

/// Default growth factor for the next link, matching RedisBloom.
pub const DEFAULT_EXPANSION: u8 = 2;

/// Ratio each new link's error rate is tightened by, so the compounded
/// error of the whole chain stays bounded (at r = 1/2 the chain's rate is
/// under 2x the requested one). Matches RedisBloom's scalable filter.
const TIGHTENING: f64 = 0.5;

/// Cap on chain length (ADR-0016 D5). Reaching it is an ERROR, not a
/// silent degradation: every link is another point get on every lookup, so
/// a filter that needs a 33rd is a filter that needed `BF.RESERVE`, and
/// the user should be told rather than served a slow one.
pub const MAX_LINKS: usize = 32;

/// Bloom filters on versioned subkey rows: one block per row, keyed by
/// block index (ADR-0016).
pub struct BloomStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

/// What `BF.INFO` reports.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomInfo {
    pub capacity: u64,
    pub size_bytes: u64,
    pub filters: usize,
    pub items: u64,
    pub expansion: u8,
    pub error: f64,
}

impl<'a> BloomStore<'a> {
    pub fn new(kv: &'a dyn Kv, ns: &[u8], clock: Clock) -> Self {
        Self::with_max_value_bytes(kv, ns, clock, crate::DEFAULT_MAX_VALUE_BYTES)
    }

    /// `max_value_bytes` = 0 disables the cap.
    pub fn with_max_value_bytes(kv: &'a dyn Kv, ns: &[u8], clock: Clock, max: u64) -> Self {
        Self {
            kv,
            ns: ns.to_vec(),
            clock,
            max_value_bytes: if max == 0 { u64::MAX } else { max },
        }
    }

    fn meta_key(&self, slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, &self.ns, slot, key)
    }

    /// A block's row key. The field is `link | block` as fixed-width
    /// big-endian, so rows sort in block order within a link and two links
    /// can never address the same row.
    fn block_key(&self, slot: u16, key: &[u8], version: u64, link: usize, block: u64) -> Vec<u8> {
        let mut field = Vec::with_capacity(10);
        field.extend_from_slice(&(link as u16).to_be_bytes());
        field.extend_from_slice(&block.to_be_bytes());
        subkey_envelope(&self.ns, slot, key, version, &field)
    }

    /// Live Bloom metadata; `Ok(None)` if missing/expired, WRONGTYPE if the
    /// key holds another type.
    fn read_meta(&self, slot: u16, key: &[u8]) -> Result<Option<BloomMeta>, StoreError> {
        let mk = self.meta_key(slot, key);
        let Some(row) = self.kv.get(&mk) else {
            return Ok(None);
        };
        let Some(header) = MetaHeader::decode(&row) else {
            return Ok(None);
        };
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return Ok(None);
        }
        if header.value_type() != Some(ValueType::Bloom) {
            return Err(StoreError::WrongType);
        }
        BloomMeta::decode(&row)
            .ok_or(StoreError::WrongType)
            .map(Some)
    }

    /// BF.RESERVE. Errors if the key already exists — the filter's
    /// parameters are fixed at creation, so re-reserving would either
    /// silently ignore the new ones or silently discard the data.
    pub fn reserve(
        &self,
        slot: u16,
        key: &[u8],
        capacity: u64,
        error: f64,
        expansion: u8,
    ) -> Result<(), StoreError> {
        if self.read_meta(slot, key)?.is_some() {
            return Err(StoreError::KeyExists);
        }
        let link = plan(capacity, error).ok_or(StoreError::BadParameter)?;
        // Refuse at reserve time rather than discovering the ceiling on
        // some later write: the nominal size is knowable now.
        if nominal_bytes(&link) > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        let now = (self.clock)();
        let mut meta = BloomMeta::new(VersionGen::next(now), capacity, error, expansion, link);
        meta.base.touch(now);
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(())
    }

    /// BF.ADD. `true` when the item was newly added, `false` when the
    /// filter already reported it present.
    pub fn add(&self, slot: u16, key: &[u8], item: &[u8]) -> Result<bool, StoreError> {
        let now = (self.clock)();
        let (mut meta, fresh) = match self.read_meta(slot, key)? {
            Some(m) => (m, false),
            None => {
                let link = plan(DEFAULT_CAPACITY, DEFAULT_ERROR).ok_or(StoreError::BadParameter)?;
                (
                    BloomMeta::new(
                        VersionGen::next(now),
                        DEFAULT_CAPACITY,
                        DEFAULT_ERROR,
                        DEFAULT_EXPANSION,
                        link,
                    ),
                    true,
                )
            }
        };

        // Present anywhere in the chain means present. Checking every link
        // is not an optimization to skip: an item added before a scale-out
        // lives in an older link, and reporting it absent would be a false
        // negative.
        if !fresh && self.lookup(slot, key, item, &meta)? {
            return Ok(false);
        }

        if self.grow_if_full(&mut meta)? {
            // A new link changes nothing about the old ones, so no rewrite.
        }

        let idx = meta.links.len() - 1;
        let link = meta.links[idx];
        let p = place(item, &link);
        let block_bytes = (1usize << link.block_bits_log2) / 8;
        let bk = self.block_key(slot, key, meta.base.version, idx, p.block);
        let mut block = self.kv.get(&bk).unwrap_or_default();
        let existed = !block.is_empty();

        for &b in p.bits() {
            set_bit(&mut block, b, block_bytes);
        }

        // Materializing a block is what grows the key. Check before
        // writing, so a refusal leaves the store untouched.
        if !existed {
            let after = meta.base.bytes.saturating_add(block_bytes as u64);
            if after > self.max_value_bytes {
                return Err(StoreError::ValueTooLarge);
            }
            meta.base.bytes = after;
        }

        // THE BLOCK ROW GOES FIRST (ADR-0016 D6). A crash between these
        // two puts must leave a set bit that was never counted — BF.CARD
        // reads low and the filter is still correct. The other order
        // leaves a counted item whose bits were never written, which is a
        // false negative.
        self.kv.put(&bk, &block);
        meta.links[idx].items += 1;
        meta.base.size = meta.base.size.saturating_add(1);
        meta.base.touch(now);
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(true)
    }

    /// BF.MADD: one result per item, in order.
    pub fn madd(&self, slot: u16, key: &[u8], items: &[Vec<u8>]) -> Result<Vec<bool>, StoreError> {
        items.iter().map(|i| self.add(slot, key, i)).collect()
    }

    /// BF.EXISTS.
    pub fn exists(&self, slot: u16, key: &[u8], item: &[u8]) -> Result<bool, StoreError> {
        match self.read_meta(slot, key)? {
            Some(meta) => self.lookup(slot, key, item, &meta),
            None => Ok(false),
        }
    }

    /// BF.MEXISTS: one result per item, in order.
    pub fn mexists(
        &self,
        slot: u16,
        key: &[u8],
        items: &[Vec<u8>],
    ) -> Result<Vec<bool>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(vec![false; items.len()]);
        };
        items
            .iter()
            .map(|i| self.lookup(slot, key, i, &meta))
            .collect()
    }

    /// BF.CARD. Zero for a missing key, matching RedisBloom.
    pub fn card(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self.read_meta(slot, key)?.map(|m| m.card()).unwrap_or(0))
    }

    /// BF.INFO. `Ok(None)` when the key does not exist.
    pub fn info(&self, slot: u16, key: &[u8]) -> Result<Option<BloomInfo>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        Ok(Some(BloomInfo {
            capacity: meta.links.iter().map(|l| l.capacity).sum(),
            // What the key occupies NOW: materialized blocks, not the
            // nominal size of the chain (D3).
            size_bytes: meta.base.bytes,
            filters: meta.links.len(),
            items: meta.card(),
            expansion: meta.expansion,
            error: meta.error,
        }))
    }

    /// Present in ANY link. Newest first, because a recently added item is
    /// the likeliest query and each miss costs a point get.
    fn lookup(
        &self,
        slot: u16,
        key: &[u8],
        item: &[u8],
        meta: &BloomMeta,
    ) -> Result<bool, StoreError> {
        for (idx, link) in meta.links.iter().enumerate().rev() {
            let p = place(item, link);
            let bk = self.block_key(slot, key, meta.base.version, idx, p.block);
            let block = self.kv.get(&bk).unwrap_or_default();
            if p.bits().iter().all(|&b| bit_is_set(&block, b)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Append a link when the newest one is full. Returns whether it did.
    fn grow_if_full(&self, meta: &mut BloomMeta) -> Result<bool, StoreError> {
        let last = *meta.links.last().ok_or(StoreError::WrongType)?;
        if last.items < last.capacity {
            return Ok(false);
        }
        if meta.expansion == 0 {
            // BF.RESERVE ... NONSCALING. RedisBloom errors here rather
            // than degrading past the promised rate, and so do we.
            return Err(StoreError::FilterFull);
        }
        if meta.links.len() >= MAX_LINKS {
            return Err(StoreError::FilterFull);
        }
        let capacity = last.capacity.saturating_mul(meta.expansion as u64);
        let error = meta.error * TIGHTENING.powi(meta.links.len() as i32);
        let link = plan(capacity, error).ok_or(StoreError::BadParameter)?;
        if nominal_bytes(&link).saturating_add(meta.base.bytes) > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        meta.links.push(link);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one property of MurmurHash3 x64_128 checkable without a
    /// reference implementation: with seed 0 and no input, every step is a
    /// no-op and `fmix64(0)` is 0, so the digest is (0, 0). It fails for
    /// most transcription errors in the finalizer.
    #[test]
    fn murmur3_empty_seed_zero_is_zero() {
        assert_eq!(murmur3_x64_128(b"", 0), (0, 0));
    }

    /// Pinned digests. These are REGRESSION PINS, not conformance vectors
    /// from an external authority: no murmur3 reference is installed here,
    /// so they were produced by this implementation and cross-checked
    /// against an independent transcription of the published algorithm
    /// (`tools/murmur3_crosscheck.py --check`, 19 inputs covering every
    /// tail length 0..16, all agreeing). That catches a wrong rotate or a
    /// mishandled tail byte; it cannot catch a misreading of the spec
    /// shared by both.
    ///
    /// Their job here is narrower and absolute: fail loudly if this
    /// function ever changes, because every filter on disk depends on it.
    /// The three lengths are chosen around the tail switch — 1 byte, the
    /// last k1-only length, and the first that reaches the k2 branch.
    #[test]
    fn murmur3_pinned_vectors() {
        assert_eq!(
            murmur3_x64_128(b"a", 0),
            (0x8555_5565_F659_7889, 0xE6B5_3A48_510E_895A)
        );
        assert_eq!(
            murmur3_x64_128(b"abcdefgh", 0),
            (0xCC8A_0AB0_37EF_8C02, 0x4889_0D60_EB69_40A1)
        );
        assert_eq!(
            murmur3_x64_128(b"abcdefghi", 0),
            (0x0547_C0CF_F13C_7964, 0x79B5_3DF5_B741_E033)
        );
    }

    /// An odd stride modulo a power of two is invertible, so the probes of
    /// any one item are distinct. If this ever fails, every filter's error
    /// rate is worse than requested and nothing else would say so.
    #[test]
    fn probes_are_distinct() {
        let link = plan(100_000, 0.01).expect("plan");
        for i in 0..2000u32 {
            let p = place(&i.to_be_bytes(), &link);
            let mut seen = p.bits().to_vec();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), p.bits().len(), "item {i} probed a bit twice");
        }
    }

    #[test]
    fn probes_stay_inside_the_block() {
        let link = plan(10_000_000, 0.01).expect("plan");
        let limit = 1u32 << link.block_bits_log2;
        for i in 0..5000u32 {
            let p = place(&i.to_be_bytes(), &link);
            assert!(p.block < link.blocks, "block {} out of range", p.block);
            for &b in p.bits() {
                assert!(b < limit, "bit {b} outside block of {limit}");
            }
        }
    }

    /// Blocks must be hit near-uniformly, because a hot block is a block
    /// whose error rate is worse than the filter promises. Chi-square over
    /// the block histogram, with a bound loose enough never to flake and
    /// tight enough to catch a hash that has collapsed.
    #[test]
    fn blocks_are_uniformly_hit() {
        let link = plan(1_000_000, 0.01).expect("plan");
        let blocks = link.blocks as usize;
        let n = blocks * 100;
        let mut hist = vec![0u64; blocks];
        for i in 0..n {
            hist[place(format!("key:{i}").as_bytes(), &link).block as usize] += 1;
        }
        let expected = (n / blocks) as f64;
        let chi2: f64 = hist
            .iter()
            .map(|&o| {
                let d = o as f64 - expected;
                d * d / expected
            })
            .sum();
        // For df = blocks-1 the expectation is df itself; 2x is far outside
        // sampling noise at this df and far inside a broken hash.
        assert!(
            chi2 < 2.0 * blocks as f64,
            "chi2 {chi2} over {blocks} blocks suggests a non-uniform hash"
        );
    }

    #[test]
    fn plan_matches_the_classic_optimum() {
        let link = plan(1_000_000, 0.01).expect("plan");
        assert_eq!(link.k, 7, "k = log2(1/0.01) rounds to 7");
        let bits = nominal_bytes(&link) * 8;
        let per_item = bits as f64 / 1_000_000.0;
        // 9.585 bits/item, plus whatever the last partial block rounds up.
        assert!(
            (9.5..9.7).contains(&per_item),
            "{per_item} bits/item is not the 1% optimum"
        );
    }

    #[test]
    fn a_small_filter_is_one_block() {
        let link = plan(100, 0.01).expect("plan");
        assert_eq!(link.blocks, 1);
        assert!(link.block_bits_log2 <= BLOCK_BITS_LOG2_MAX);
        // 100 items x 9.585 bits = 959 bits, rounded up to 1024.
        assert_eq!(link.block_bits_log2, 10);
    }

    #[test]
    fn plan_rejects_impossible_requests() {
        assert!(plan(0, 0.01).is_none());
        assert!(plan(100, 0.0).is_none());
        assert!(plan(100, 1.0).is_none());
        assert!(plan(100, -0.5).is_none());
    }

    #[test]
    fn bit_helpers_round_trip() {
        let link = plan(1000, 0.01).expect("plan");
        let block_bytes = (1usize << link.block_bits_log2) / 8;
        let mut block = Vec::new();
        let p = place(b"hello", &link);
        for &b in p.bits() {
            assert!(!bit_is_set(&block, b));
            assert!(set_bit(&mut block, b, block_bytes));
            assert!(
                !set_bit(&mut block, b, block_bytes),
                "second set is a no-op"
            );
            assert!(bit_is_set(&block, b));
        }
        assert_eq!(block.len(), block_bytes);
    }

    /// An absent block row is an empty slice, and must read as all-zero
    /// rather than panicking — that is what makes lazy materialization
    /// (D3) work.
    #[test]
    fn an_absent_block_reads_as_empty() {
        let link = plan(1_000_000, 0.01).expect("plan");
        let p = place(b"never-added", &link);
        for &b in p.bits() {
            assert!(!bit_is_set(&[], b));
        }
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;
    use crate::MemKv;
    use std::sync::atomic::{AtomicU64, Ordering};

    macro_rules! test_clock {
        ($static_name:ident, $fn_name:ident, $initial:expr) => {
            static $static_name: AtomicU64 = AtomicU64::new($initial);
            fn $fn_name() -> u64 {
                $static_name.load(Ordering::Relaxed)
            }
        };
    }

    fn item(i: usize) -> Vec<u8> {
        format!("item:{i}").into_bytes()
    }

    #[test]
    fn add_exists_card_lifecycle() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);

        assert_eq!(
            b.exists(1, b"f", b"a"),
            Ok(false),
            "missing key is not an error"
        );
        assert_eq!(b.card(1, b"f"), Ok(0));
        assert_eq!(b.add(1, b"f", b"a"), Ok(true));
        assert_eq!(b.add(1, b"f", b"a"), Ok(false), "re-adding reports present");
        assert_eq!(b.exists(1, b"f", b"a"), Ok(true));
        assert_eq!(b.exists(1, b"f", b"b"), Ok(false));
        assert_eq!(b.card(1, b"f"), Ok(1));
        assert_eq!(
            b.madd(1, b"f", &[b"a".to_vec(), b"c".to_vec()]),
            Ok(vec![false, true])
        );
        assert_eq!(
            b.mexists(1, b"f", &[b"a".to_vec(), b"c".to_vec(), b"zz".to_vec()]),
            Ok(vec![true, true, false])
        );
        assert_eq!(b.card(1, b"f"), Ok(2));
    }

    /// THE test (ADR-0016 Verification 2). A Bloom filter may say yes
    /// wrongly; it may never say no wrongly. Every item ever added stays
    /// present — here across the scale-outs that put earlier items in
    /// older links, which is the case a chain can get wrong.
    #[test]
    fn no_false_negatives_across_scale_outs() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);
        b.reserve(1, b"f", 500, 0.01, 2).expect("reserve");

        // 4000 items into a filter sized for 500 forces three scale-outs.
        for i in 0..4000 {
            b.add(1, b"f", &item(i)).expect("add");
        }
        let info = b.info(1, b"f").expect("info").expect("present");
        assert!(
            info.filters > 1,
            "expected a chain, got {} link(s)",
            info.filters
        );

        for i in 0..4000 {
            assert_eq!(
                b.exists(1, b"f", &item(i)),
                Ok(true),
                "item {i} went missing — false negative"
            );
        }

        // BF.CARD counts items the filter ACCEPTED, and an item that
        // false-positives on insert is reported already-present and never
        // counted — so the card is at most the number added, and short by
        // however many collided. That is inherent to the structure, not an
        // accounting bug: the filter cannot tell a collision from a repeat.
        // RedisBloom under-counts the same way.
        let card = b.card(1, b"f").expect("card");
        assert!(
            (3900..=4000).contains(&card),
            "card {card} is not a plausible insert-time false-positive shortfall"
        );
    }

    /// The positive control for the test above, and the executable form of
    /// D6's write-ordering argument.
    ///
    /// `add` writes the BLOCK row and then the metadata row. Each half of
    /// a torn write is simulated here by reverting the other, and the two
    /// outcomes are not symmetric: the shipped order loses a count, the
    /// reverse order loses the ITEM. If this ever passes with the halves
    /// swapped, the ordering has been broken and the false-negative test
    /// above would still be green.
    #[test]
    fn only_one_write_order_makes_a_tear_benign() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);
        b.reserve(1, b"f", 1000, 0.01, 2).expect("reserve");

        let meta_key = b.meta_key(1, b"f");
        let before = kv.get(&meta_key).expect("reserved row");
        b.add(1, b"f", b"x").expect("add");

        // Tear the shipped order: block landed, metadata did not.
        kv.put(&meta_key, &before);
        assert_eq!(
            b.exists(1, b"f", b"x"),
            Ok(true),
            "the filter must survive losing the count"
        );
        assert_eq!(b.card(1, b"f"), Ok(0), "the count is what is lost");

        // Tear the reverse order: metadata landed, block did not. This is
        // the false negative the ordering exists to prevent.
        b.add(1, b"f", b"y").expect("add");
        let meta = b.read_meta(1, b"f").expect("read").expect("present");
        let link = *meta.links.last().expect("link");
        let p = place(b"y", &link);
        let bk = b.block_key(1, b"f", meta.base.version, meta.links.len() - 1, p.block);
        kv.delete(&bk);
        assert_eq!(
            b.exists(1, b"f", b"y"),
            Ok(false),
            "losing the block loses the item — which is why it is written first"
        );
        assert!(
            b.card(1, b"f").expect("card") > 0,
            "while the count survives"
        );
    }

    /// The rate is a contract, so it gets measured — at two error rates,
    /// because a filter that ignores the request passes a single-point
    /// test. Deterministic inputs, so this cannot flake.
    #[test]
    fn false_positive_rate_tracks_the_request() {
        for (error, key) in [(0.01, &b"f1"[..]), (0.001, &b"f2"[..])] {
            test_clock!(NOW, now, 1_000_000);
            let kv = MemKv::new();
            let b = BloomStore::new(&kv, b"t", now);
            b.reserve(1, key, 10_000, error, 0).expect("reserve");
            for i in 0..9_000 {
                b.add(1, key, &item(i)).expect("add");
            }

            let trials = 100_000;
            let mut fp = 0;
            for i in 0..trials {
                if b.exists(1, key, format!("absent:{i}").as_bytes()) == Ok(true) {
                    fp += 1;
                }
            }
            let rate = fp as f64 / trials as f64;
            // Loose enough to absorb the blocking premium and the 90% fill,
            // tight enough that a filter answering always-no (rate 0) or
            // always-yes fails. Both bounds matter: the lower one is what
            // catches a filter that has stopped storing anything.
            assert!(
                rate > error * 0.05 && rate < error * 2.5,
                "error {error}: measured {rate}, outside [{}, {}]",
                error * 0.05,
                error * 2.5
            );
        }
    }

    #[test]
    fn reserve_is_once_only_and_validates() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);
        assert_eq!(b.reserve(1, b"f", 100, 0.01, 2), Ok(()));
        assert_eq!(b.reserve(1, b"f", 100, 0.01, 2), Err(StoreError::KeyExists));
        assert_eq!(
            b.reserve(1, b"g", 0, 0.01, 2),
            Err(StoreError::BadParameter)
        );
        assert_eq!(
            b.reserve(1, b"g", 100, 1.5, 2),
            Err(StoreError::BadParameter)
        );
    }

    /// NONSCALING (expansion 0) refuses past capacity rather than quietly
    /// serving a worse error rate than it promised.
    #[test]
    fn a_nonscaling_filter_refuses_when_full() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);
        b.reserve(1, b"f", 50, 0.01, 0).expect("reserve");
        let mut added = 0;
        for i in 0..200 {
            match b.add(1, b"f", &item(i)) {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(StoreError::FilterFull) => break,
                Err(e) => panic!("unexpected {e:?}"),
            }
        }
        assert_eq!(added, 50, "it filled to capacity and then refused");
        assert_eq!(b.add(1, b"f", b"one-more"), Err(StoreError::FilterFull));
        // Everything accepted before the refusal is still there.
        for i in 0..added {
            assert_eq!(b.exists(1, b"f", &item(i)), Ok(true));
        }
    }

    /// Blocks materialize lazily (D3): a filter reserved for a million
    /// items and holding three occupies three rows, not the nominal size.
    #[test]
    fn blocks_materialize_lazily() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);
        b.reserve(1, b"f", 1_000_000, 0.01, 2).expect("reserve");

        let nominal = {
            let m = b.read_meta(1, b"f").expect("read").expect("present");
            nominal_bytes(m.links.last().expect("link"))
        };
        assert!(nominal > 1_000_000, "sanity: nominal is ~1.2 MB");

        let info = b.info(1, b"f").expect("info").expect("present");
        assert_eq!(info.size_bytes, 0, "nothing on disk before the first add");

        for i in 0..3 {
            b.add(1, b"f", &item(i)).expect("add");
        }
        let info = b.info(1, b"f").expect("info").expect("present");
        assert!(
            info.size_bytes <= 3 * 4096,
            "three items should touch at most three blocks, got {}",
            info.size_bytes
        );
        assert_eq!(info.capacity, 1_000_000);
        assert_eq!(info.items, 3);
        assert_eq!(info.filters, 1);
    }

    #[test]
    fn wrong_type_is_refused_both_ways() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = crate::strings::StringStore::new(&kv, b"t", now);
        s.set(1, b"str", b"v", crate::strings::SetOptions::default())
            .expect("set");

        let b = BloomStore::new(&kv, b"t", now);
        assert_eq!(b.add(1, b"str", b"x"), Err(StoreError::WrongType));
        assert_eq!(b.exists(1, b"str", b"x"), Err(StoreError::WrongType));
        assert_eq!(b.card(1, b"str"), Err(StoreError::WrongType));
        assert_eq!(b.info(1, b"str"), Err(StoreError::WrongType));

        b.add(1, b"bf", b"x").expect("add");
        assert_eq!(s.get(1, b"bf"), Err(StoreError::WrongType));
    }

    /// A filter whose nominal size exceeds the max-value cap is refused at
    /// RESERVE, before any row is written.
    #[test]
    fn an_oversized_reservation_is_refused_up_front() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::with_max_value_bytes(&kv, b"t", now, 64 * 1024);
        assert_eq!(
            b.reserve(1, b"f", 10_000_000, 0.01, 2),
            Err(StoreError::ValueTooLarge)
        );
        assert_eq!(b.exists(1, b"f", b"x"), Ok(false), "nothing was created");
    }

    /// Expiry and type are read from the shared header, so TTL works on a
    /// filter exactly as on any other key — and an expired filter reads as
    /// absent rather than as a stale one.
    #[test]
    fn expiry_applies_to_a_filter() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let b = BloomStore::new(&kv, b"t", now);
        b.add(1, b"f", b"x").expect("add");
        let ks = crate::keyspace::Keyspace::new(&kv, b"t", now);
        assert_eq!(
            ks.value_type(1, b"f").map(|t| t.name()),
            Some("bloom"),
            "TYPE answers `bloom`, not RedisBloom's `MBbloom--` (ADR-0016 D7.1)"
        );
        assert!(ks.expire_at(1, b"f", 1_000_010));
        NOW.store(1_000_011, Ordering::Relaxed);
        assert_eq!(b.exists(1, b"f", b"x"), Ok(false));
        assert_eq!(b.card(1, b"f"), Ok(0));
    }
}
