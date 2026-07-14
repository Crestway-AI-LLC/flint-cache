//! Bulk filler for benchmark datasets: pipelined SETs of `--keys` entries,
//! `--val-size` bytes each, key format `key:%012d` — exactly the shape
//! redis-benchmark generates with `-r N` (`key:__rand_int__`), so a fill
//! here can be read back by redis-benchmark GET for client-observed latency
//! measurements against ANY RESP server (flint or valkey).
//!
//! Values are pseudorandom (xorshift) and unique per key — INCOMPRESSIBLE.
//! This is load-bearing for beyond-RAM tests: constant values compress ~15:1
//! in RocksDB and the "100 GB" dataset quietly fits in page cache, faking
//! RAM-speed reads (the 2026-07-11 bench lesson).
//!
//! Usage: fill --port 6380 [--keys 100000000] [--val-size 1024] [--batch 512] [--start 0]

use flint_chaos::cluster::{Client, arg};

fn main() {
    let port: u16 = arg("--port", 6380);
    let keys: u64 = arg("--keys", 100_000_000);
    let val_size: usize = arg("--val-size", 1024);
    let batch: usize = arg("--batch", 512);
    let start: u64 = arg("--start", 0); // resume support
    println!("fill: {keys} keys x {val_size}B to :{port} (batch {batch}, from {start})");

    let mut c = Client::connect(port).expect("connect");
    let started = std::time::Instant::now();
    let mut rng: u64 = 0x9E3779B97F4A7C15;
    let mut cmds: Vec<Vec<Vec<u8>>> = Vec::with_capacity(batch);
    for i in start..keys {
        // Incompressible value: xorshift stream, reseeded per key so content
        // is unique and position-dependent.
        let mut v = Vec::with_capacity(val_size);
        rng ^= i.wrapping_mul(0xD1B54A32D192ED03);
        while v.len() < val_size {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            v.extend_from_slice(&rng.to_le_bytes());
        }
        v.truncate(val_size);
        cmds.push(vec![
            b"SET".to_vec(),
            format!("key:{i:012}").into_bytes(),
            v,
        ]);
        if cmds.len() == batch {
            c.pipeline(&cmds).expect("pipeline");
            cmds.clear();
            if (i + 1) % 5_000_000 == 0 {
                let secs = started.elapsed().as_secs_f64();
                let done = i + 1 - start;
                println!(
                    "  {}M keys, {:.0}s, {:.0}K keys/s",
                    (i + 1) / 1_000_000,
                    secs,
                    done as f64 / secs / 1e3
                );
            }
        }
    }
    if !cmds.is_empty() {
        c.pipeline(&cmds).expect("pipeline tail");
    }
    println!(
        "fill done: {} keys in {:.0}s",
        keys - start,
        started.elapsed().as_secs_f64()
    );
}
