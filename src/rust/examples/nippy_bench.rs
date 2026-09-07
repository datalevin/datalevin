//! Standalone release-mode timing; no bridge, hex parsing, or fixture setup in the loop.
use datalevin_codec::nippy::{Decoder, Limits, Value, fast_freeze, fast_thaw};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};

fn batch<F: FnMut()>(f: &mut F, iterations: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    start.elapsed().as_nanos()
}

fn measure<F: FnMut()>(
    out: &mut impl Write,
    label: (&str, &str, usize, usize),
    config: (usize, u64, u64),
    mut f: F,
) -> std::io::Result<()> {
    let (samples, warm_ms, sample_ms) = config;
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(warm_ms) {
        batch(&mut f, 128);
    }
    let mut n = 1;
    while batch(&mut f, n) < u128::from(sample_ms) * 1_000_000 && n < 10_000_000 {
        n = (n * 2).min(10_000_000);
    }
    let (id, op, input_size, encoded_size) = label;
    for sample in 0..samples {
        let elapsed = batch(&mut f, n);
        writeln!(
            out,
            "{id}\t{op}\t{sample}\t{n}\t{elapsed}\t{input_size}\t{encoded_size}"
        )?;
    }
    out.flush()
}

fn unhex(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if !s.len().is_multiple_of(2) {
        return Err("odd hex length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&s[i..i + 2], 16)?))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").unwrap();
    }
    output
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        return Err("usage: nippy_bench prepare|measure CORPUS OUTPUT [SAMPLES WARM_MS SAMPLE_MS forward|reverse]".into());
    }
    let content = std::fs::read_to_string(&args[1])?;
    let mut corpus: Vec<(String, Vec<u8>, Value, Vec<u8>)> = Vec::new();
    for line in content
        .lines()
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
    {
        let (id, payload) = line.split_once('\t').ok_or("missing tab")?;
        let bytes = unhex(payload)?;
        let value = fast_thaw(&bytes).map_err(|e| format!("{id}: {e}"))?;
        let encoded = fast_freeze(&value)?;
        // The public writer defaults to 3.7's schema-dependent deftype encoding.
        let mut decoder = Decoder::new(&encoded, Limits::default())?
            .with_legacy_deftype("taoensso.nippy.StressType", 2);
        let roundtrip = decoder.read_value()?;
        assert_eq!(decoder.position(), encoded.len(), "{id}: trailing bytes");
        assert_eq!(value, roundtrip, "{id}: Rust roundtrip");
        corpus.push((id.into(), bytes, value, encoded));
    }
    let mut out = BufWriter::new(std::fs::File::create(&args[2])?);
    if args[0] == "prepare" {
        for (id, _, _, encoded) in &corpus {
            writeln!(out, "{id}\t{}", hex(encoded))?;
        }
        println!("Validated Rust roundtrips for {} cases", corpus.len());
        return Ok(());
    }
    if args[0] != "measure" || args.len() != 7 {
        return Err("invalid benchmark arguments".into());
    }
    let config = (args[3].parse()?, args[4].parse()?, args[5].parse()?);
    if args[6] == "reverse" {
        corpus.reverse();
    }
    writeln!(
        out,
        "id\top\tsample\titerations\telapsed_ns\tinput_bytes\tencoded_bytes"
    )?;
    measure(
        &mut out,
        ("harness/identity", "baseline", 0, 0),
        config,
        || {
            black_box(black_box(1_u64));
        },
    )?;
    for (index, (id, bytes, value, encoded)) in corpus.iter().enumerate() {
        measure(
            &mut out,
            (id, "encode", bytes.len(), encoded.len()),
            config,
            || {
                drop(black_box(fast_freeze(black_box(value)).unwrap()));
            },
        )?;
        measure(
            &mut out,
            (id, "decode", bytes.len(), encoded.len()),
            config,
            || {
                drop(black_box(fast_thaw(black_box(bytes)).unwrap()));
            },
        )?;
        if index % 10 == 0 {
            println!("Measured {index} of {}: {id}", corpus.len());
        }
    }
    Ok(())
}
