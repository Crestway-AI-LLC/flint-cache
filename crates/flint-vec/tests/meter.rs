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
        // ADR-0059: a TAG and a NUMERIC field. `own` gives every vector a tag
        // value of its own, each a whole new value in the index, the worst
        // case; `shared` gives all of them one value and two of a handful.
        let fields = ["FIELD", "folder", "TAG", "FIELD", "price", "NUMERIC"];
        let flat_f = &fields[..];
        let hnsw_f = &[
            "INDEX", "hnsw", "FIELD", "folder", "TAG", "FIELD", "price", "NUMERIC",
        ][..];
        let bin_f = &[
            "INDEX", "hnsw", "QUANT", "bin", "FIELD", "folder", "TAG", "FIELD", "price", "NUMERIC",
        ][..];
        let own = |i: usize| Some(format!(r#"{{"folder":"folder-{i:06}","price":{i}}}"#));
        let shared = |i: usize| {
            Some(format!(
                r#"{{"folder":["all","group-{}"],"price":{}.5}}"#,
                i % 4,
                i % 3
            ))
        };
        let none = |_: usize| None;
        type Meta<'a> = &'a dyn Fn(usize) -> Option<String>;
        let cases: [(&str, &[&str], bool, Meta); 11] = [
            ("flat", &[][..], false, &none),
            ("hnsw", &["INDEX", "hnsw"][..], false, &none),
            (
                "hnsw sq8",
                &["INDEX", "hnsw", "QUANT", "sq8"][..],
                false,
                &none,
            ),
            (
                "hnsw sq8 on disk",
                &["INDEX", "hnsw", "QUANT", "sq8"][..],
                true,
                &none,
            ),
            ("hnsw bin", bin, false, &none),
            ("hnsw bin on disk", bin, true, &none),
            ("flat, fields, own tags", flat_f, false, &own),
            ("flat, fields, shared tags", flat_f, false, &shared),
            ("hnsw, fields, own tags", hnsw_f, false, &own),
            ("hnsw, fields, shared tags", hnsw_f, false, &shared),
            ("hnsw bin on disk, fields, own tags", bin_f, true, &own),
        ];
        for (label, extra, on_disk, meta) in cases {
            let ns = b("meter");
            let mut st = Store::new();
            st.set_filters(true);
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
            // Each META is made before the count starts, as `vectors` were.
            let metas: Vec<Option<Vec<u8>>> = (0..n).map(|i| meta(i).map(|m| b(&m))).collect();
            let before = HEAP.load(Ordering::Relaxed);
            for ((id, v), m) in ids.iter().zip(&vectors).zip(&metas) {
                let mut set = vec![b("VEC.SET"), b("s"), id.clone(), v.clone()];
                if let Some(m) = m {
                    set.extend([b("META"), m.clone()]);
                }
                exec(&mut st, &ns, &set);
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
