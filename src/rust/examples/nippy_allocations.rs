//! Allocation counts for one decode and drop, separate from timing/profiling.
use datalevin_codec::nippy::{Value, fast_thaw};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
};

struct CountingAllocator;
static TRACKING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static REALLOCS: AtomicUsize = AtomicUsize::new(0);
static REALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static FREES: AtomicUsize = AtomicUsize::new(0);
static FREE_BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: Every request is forwarded unchanged to System, with the caller's
// pointer/layout contract preserved. Counters never allocate or touch payloads.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if TRACKING.load(Relaxed) && !ptr.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if TRACKING.load(Relaxed) && !ptr.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if TRACKING.load(Relaxed) && !result.is_null() {
            REALLOCS.fetch_add(1, Relaxed);
            REALLOC_BYTES.fetch_add(size, Relaxed);
        }
        result
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.load(Relaxed) {
            FREES.fetch_add(1, Relaxed);
            FREE_BYTES.fetch_add(layout.size(), Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn reset() {
    for counter in [
        &ALLOCS,
        &ALLOC_BYTES,
        &REALLOCS,
        &REALLOC_BYTES,
        &FREES,
        &FREE_BYTES,
    ] {
        counter.store(0, Relaxed);
    }
}

fn report(phase: &str) {
    println!(
        "{phase}\t{}\t{}\t{}\t{}\t{}\t{}",
        ALLOCS.load(Relaxed),
        ALLOC_BYTES.load(Relaxed),
        REALLOCS.load(Relaxed),
        REALLOC_BYTES.load(Relaxed),
        FREES.load(Relaxed),
        FREE_BYTES.load(Relaxed),
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 3, "usage: nippy_allocations CORPUS CASE");
    let corpus = std::fs::read_to_string(&args[1]).unwrap();
    let hex = corpus
        .lines()
        .find_map(|line| {
            let (name, hex) = line.split_once('\t')?;
            (name == args[2]).then_some(hex)
        })
        .expect("case in corpus");
    let bytes: Vec<_> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    drop(black_box(fast_thaw(black_box(&bytes)).unwrap()));

    reset();
    TRACKING.store(true, Relaxed);
    let result = black_box(fast_thaw(black_box(&bytes)));
    TRACKING.store(false, Relaxed);
    let value = result.unwrap();
    println!(
        "case\t{}\ninput_bytes\t{}\nvalue_size\t{}",
        args[2],
        bytes.len(),
        std::mem::size_of::<Value>()
    );
    println!(
        "phase\tallocations\tallocated_bytes\treallocations\treallocation_new_bytes\tfrees\tfreed_bytes"
    );
    report("decode");

    reset();
    TRACKING.store(true, Relaxed);
    drop(black_box(value));
    TRACKING.store(false, Relaxed);
    report("drop");
}
