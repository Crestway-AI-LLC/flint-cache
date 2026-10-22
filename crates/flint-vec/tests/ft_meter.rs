// SPDX-License-Identifier: Elastic-2.0
//! ADR-0063: the D4 meter must charge at least what an `FT.*` index holds,
//! its TEXT fields' words, stems and postings included, as `meter.rs` holds
//! it to for vector sets (BUG-0198).
//!
//! A test binary of its own, because it replaces the global allocator with one
//! that counts, and because the count is process-wide: a second test running
//! beside this one would allocate into it. So there is one test.

use flint_vec::ft::parse_create;
use flint_vec::ftindex::{FtIndex, Source};
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

#[test]
fn the_meter_charges_at_least_what_an_ft_index_holds() {
    let mut x = 0x0198_u64;
    let mut next = move |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    let mut short = Vec::new();
    // Each count is just past a power of two, where the maps have just
    // grown and their spare room is charged to the documents already in.
    for n in [257usize, 1025] {
        // `own`: every word new to the index, the most terms; `shared`: a
        // small vocabulary in long documents, the most postings; `tags`: a
        // TAG value of each document's own beside a short text.
        type Doc = Vec<(Vec<u8>, Vec<u8>)>;
        let own: Vec<Doc> = (0..n)
            .map(|i| {
                let words: Vec<String> = (0..12).map(|w| format!("word{i}x{w}ing")).collect();
                vec![(b("t"), b(&words.join(" "))), (b("c"), b("all"))]
            })
            .collect();
        let vocab = [
            "running", "engine", "doctors", "cancer", "lungs", "medical", "redis", "search",
        ];
        let shared: Vec<Doc> = (0..n)
            .map(|_| {
                let words: Vec<&str> = (0..60).map(|_| vocab[next(8) as usize]).collect();
                vec![(b("t"), b(&words.join(" "))), (b("c"), b("all"))]
            })
            .collect();
        let tags: Vec<Doc> = (0..n)
            .map(|i| {
                vec![
                    (b("t"), b("a short text")),
                    (b("c"), b(&format!("tag-{i:06},all"))),
                ]
            })
            .collect();
        for (label, docs) in [
            ("own words", &own),
            ("shared words", &shared),
            ("tags", &tags),
        ] {
            let (_, def) = parse_create(&[
                b("FT.CREATE"),
                b("idx"),
                b("PREFIX"),
                b("1"),
                b("doc:"),
                b("SCHEMA"),
                b("t"),
                b("TEXT"),
                b("c"),
                b("TAG"),
            ])
            .expect("parses");
            let mut ix = FtIndex::new(def);
            let keys: Vec<Vec<u8>> = (0..n).map(|i| b(&format!("doc:{i:06}"))).collect();
            let before = HEAP.load(Ordering::Relaxed);
            for (k, d) in keys.iter().zip(docs.iter()) {
                ix.put(k, Source::Hash(d), None).expect("indexed");
            }
            let held = HEAP.load(Ordering::Relaxed) - before;
            let meter = ix.bytes() as isize;
            eprintln!(
                "{label}, {n} documents: holds {} B a document, the meter charges {}",
                held / n as isize,
                meter / n as isize
            );
            if held > meter {
                short.push(format!("{label}, {n}: holds {held} B, charged {meter} B"));
            }
        }
    }
    assert!(
        short.is_empty(),
        "the meter charges less than:\n{}",
        short.join("\n")
    );
}
