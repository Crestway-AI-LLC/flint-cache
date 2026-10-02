// SPDX-License-Identifier: Elastic-2.0
//! BUG-0198: the D4 meter must charge at least what a set holds. It is what
//! stops one tenant's vectors from exhausting the co-processor's RAM, and it
//! said it over-estimated while a set held up to a third more than it charged.
//!
//! A test binary of its own, because it replaces the global allocator with one
//! that counts, and because the count is process-wide: a second test running
//! beside this one would allocate into it. So there is one test.

use flint_resp::Value;
use flint_vec::{Plan, Store};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

static HEAP: AtomicIsize = AtomicIsize::new(0);

struct Counting;

// SAFETY: every call is forwarded to `System` unchanged; this only counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        HEAP.fetch_add(l.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        HEAP.fetch_sub(l.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        HEAP.fetch_add(new as isize - l.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn exec(st: &mut Store, ns: &[u8], args: &[Vec<u8>]) {
    match st.plan(ns, args, 0) {
        Plan::Write { apply, .. } => st.commit(ns, apply),
        Plan::Reply(Value::Error(e)) => panic!("{e}"),
        Plan::Reply(_) => {}
    }
}

#[test]
fn the_meter_charges_at_least_what_each_kind_of_set_holds() {
    // Each count is just past a power of two: the arrays and maps that hold
    // the entries have just grown, so their spare room is charged to the
    // vectors already in them. The worst case for any count.
    let dir = std::env::temp_dir().join(format!("flint-vec-meter-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let mut x = 0x0198_u64;
    let mut short = Vec::new();
    // 200 is not a power of two: a parsed vector's spare capacity showed
    // there. 1536 is a real embedding's size, where spare room that grows with
    // the vector outweighs everything else a node holds (ADR-0049 step 3).
    for (dim, n) in [(128usize, 257usize), (200, 257), (1536, 33)] {
        let vectors: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                let v: Vec<String> = (0..dim)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        format!("{:.6}", (x >> 40) as f32 / 16_777_216.0)
                    })
                    .collect();
                b(&v.join(","))
            })
            .collect();
        let ids: Vec<Vec<u8>> = (0..n).map(|i| b(&format!("id-{i:06}"))).collect();
        let bin = &["INDEX", "hnsw", "QUANT", "bin"][..];
        for (label, extra, on_disk) in [
            ("flat", &[][..], false),
            ("hnsw", &["INDEX", "hnsw"][..], false),
            ("hnsw sq8", &["INDEX", "hnsw", "QUANT", "sq8"][..], false),
            (
                "hnsw sq8 on disk",
                &["INDEX", "hnsw", "QUANT", "sq8"][..],
                true,
            ),
            ("hnsw bin", bin, false),
            ("hnsw bin on disk", bin, true),
        ] {
            let ns = b("meter");
            let mut st = Store::new();
            if on_disk {
                st.set_vec_dir(dir.clone());
            }
            let dim_s = dim.to_string();
            let mut create = vec![
                b("VEC.CREATE"),
                b("s"),
                b("DIM"),
                b(&dim_s),
                b("METRIC"),
                b("l2"),
            ];
            create.extend(extra.iter().map(|s| b(s)));
            exec(&mut st, &ns, &create);
            // From here: the meter charges each entry, not the set's own
            // struct and the maps VEC.CREATE makes, which a count of 33 would
            // otherwise spread over too few vectors to mean anything.
            let before = HEAP.load(Ordering::Relaxed);
            for (id, v) in ids.iter().zip(&vectors) {
                exec(&mut st, &ns, &[b("VEC.SET"), b("s"), id.clone(), v.clone()]);
            }
            let held = HEAP.load(Ordering::Relaxed) - before;
            let meter = st.ns_mem_bytes(&ns) as isize;
            eprintln!(
                "{label}, dim {dim}: holds {} B a vector, the meter charges {}",
                held / n as isize,
                meter / n as isize
            );
            if held > meter {
                short.push(format!(
                    "{label}, dim {dim}: holds {held} B, charged {meter} B"
                ));
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        short.is_empty(),
        "the meter charges less than:\n{}",
        short.join("\n")
    );
}
