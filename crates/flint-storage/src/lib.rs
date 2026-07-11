//! Storage engine: Kvrocks-style structure-on-KV encoding over an LSM.
//!
//! M0 scope: RocksDB FFI spike + the p99.9-under-compaction benchmark rig,
//! then the encoding layer (`namespace|slot|user_key` metadata rows,
//! versioned subkeys). See docs/design.md section 2.4.

pub const TODO: &str = "M0: rocksdb spike + key encoding";
