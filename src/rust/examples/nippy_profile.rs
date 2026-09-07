//! Single-operation loop for a sampling profiler; use nippy_bench for timings.
use datalevin_codec::nippy::{fast_freeze, fast_thaw};
use std::hint::black_box;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        5,
        "usage: nippy_profile CORPUS CASE encode|decode ITERATIONS"
    );
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
    let value = fast_thaw(&bytes).unwrap();
    let iterations: usize = args[4].parse().unwrap();
    eprintln!("PID {}: {} {}", std::process::id(), args[2], args[3]);
    match args[3].as_str() {
        "encode" => {
            for _ in 0..iterations {
                drop(black_box(fast_freeze(black_box(&value)).unwrap()));
            }
        }
        "decode" => {
            for _ in 0..iterations {
                drop(black_box(fast_thaw(black_box(&bytes)).unwrap()));
            }
        }
        _ => panic!("expected encode or decode"),
    }
}
