//! Line-oriented peer used by the JVM/Rust interoperability property tests.

use datalevin_codec::cbor::{
    Error, ExtensionId, Integer, Limits, Mode, Value, decode, decode_storage, decode_with_limits,
    encode, encode_storage,
};
use std::collections::HashSet;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

fn main() -> io::Result<()> {
    let input = io::stdin();
    let output = io::stdout();
    let mut output = BufWriter::new(output.lock());

    writeln!(output, "ready")?;
    output.flush()?;

    for line in BufReader::new(input.lock()).lines() {
        let line = line?;
        if line == "quit" {
            break;
        }

        match process(&line) {
            Ok(bytes) => writeln!(output, "ok\t{}", hex_encode(&bytes))?,
            Err(message) => writeln!(output, "error\t{message}")?,
        }
        output.flush()?;
    }
    Ok(())
}

fn process(line: &str) -> Result<Vec<u8>, String> {
    let (operation, payload) = line
        .split_once('\t')
        .ok_or_else(|| protocol_failure("request must contain an operation and payload"))?;
    if operation == "generate" || operation == "generate-fast" {
        let seed = u64::from_str_radix(payload, 16)
            .map_err(|error| protocol_failure(&format!("invalid generation seed: {error}")))?;
        let mode = if operation == "generate" {
            Mode::Canonical
        } else {
            Mode::Fast
        };
        return encode(&generated_value(seed), mode).map_err(codec_failure);
    }

    let bytes = hex_decode(payload).map_err(|error| protocol_failure(&error))?;
    match operation {
        "canonical" => decode(&bytes, true)
            .and_then(|value| encode(&value, Mode::Canonical))
            .map_err(codec_failure),
        "fast" => decode(&bytes, false)
            .and_then(|value| encode(&value, Mode::Canonical))
            .map_err(codec_failure),
        "storage" => decode_storage(&bytes, true)
            .and_then(|value| encode_storage(&value, Mode::Canonical))
            .map_err(codec_failure),
        "limit-input" | "limit-depth" | "limit-collection" | "limit-string" | "limit-bignum"
        | "limit-extension" => decode_with_limits(&bytes, true, limits_for(operation))
            .and_then(|value| encode(&value, Mode::Canonical))
            .map_err(codec_failure),
        _ => Err(protocol_failure(&format!("unknown operation: {operation}"))),
    }
}

fn codec_failure(error: Error) -> String {
    format!("{}\t{}", error.kind.code(), error.offset)
}

fn protocol_failure(message: &str) -> String {
    format!("PROTOCOL\t0\t{}", message.replace(['\t', '\n'], " "))
}

fn limits_for(operation: &str) -> Limits {
    let mut limits = Limits::default();
    match operation {
        "limit-input" => limits.max_input_bytes = 4,
        "limit-depth" => limits.max_depth = 2,
        "limit-collection" => limits.max_collection_len = 2,
        "limit-string" => limits.max_string_bytes = 2,
        "limit-bignum" => {
            limits.max_string_bytes = 64;
            limits.max_bignum_bytes = 8;
        }
        "limit-extension" => limits.max_extension_bytes = 4,
        _ => unreachable!(),
    }
    limits
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64 gives every input seed a well-mixed, reproducible stream.
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn bounded(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        (self.next_u64() % upper as u64) as usize
    }

    fn bytes(&mut self, maximum_length: usize) -> Vec<u8> {
        let length = self.bounded(maximum_length + 1);
        (0..length).map(|_| self.next_u64() as u8).collect()
    }
}

fn generated_value(seed: u64) -> Value {
    generated_value_at(&mut DeterministicRng::new(seed), 0)
}

fn generated_value_at(random: &mut DeterministicRng, depth: usize) -> Value {
    let alternatives = if depth >= 4 { 24 } else { 30 };
    match random.bounded(alternatives) {
        0 => Value::Null,
        1 => Value::Bool(random.next_u64() & 1 != 0),
        2 => Value::Integer(random_i64(random).into()),
        3 => Value::Integer(random_big_integer(random, false)),
        4 => Value::Integer(random_big_integer(random, true)),
        5 => Value::Float32(f32::from_bits(random.next_u64() as u32)),
        6 => Value::Float64(f64::from_bits(random.next_u64())),
        7 => Value::Bytes(random.bytes(64)),
        8 => Value::Text(random_text(random)),
        9 => random_decimal(random),
        10 => random_ratio(random),
        11 => Value::Uri(random_uri(random)),
        12 => Value::Uuid(random_uuid(random)),
        13 => Value::InstantMillis(random_i64(random)),
        14 => Value::Uint16Array(
            (0..random.bounded(33))
                .map(|_| random.next_u64() as u16)
                .collect(),
        ),
        15 => Value::Int16Array(
            (0..random.bounded(33))
                .map(|_| random.next_u64() as i16)
                .collect(),
        ),
        16 => Value::Int32Array(
            (0..random.bounded(33))
                .map(|_| random.next_u64() as i32)
                .collect(),
        ),
        17 => Value::Int64Array(
            (0..random.bounded(33))
                .map(|_| random.next_u64() as i64)
                .collect(),
        ),
        18 => Value::Float32Array(
            (0..random.bounded(33))
                .map(|_| f32::from_bits(random.next_u64() as u32))
                .collect(),
        ),
        19 => Value::Float64Array(
            (0..random.bounded(33))
                .map(|_| f64::from_bits(random.next_u64()))
                .collect(),
        ),
        20 => Value::Keyword {
            namespace: (random.next_u64() & 1 != 0).then(|| random_text(random)),
            name: random_text(random),
        },
        21 => Value::Symbol {
            namespace: (random.next_u64() & 1 != 0).then(|| random_text(random)),
            name: random_text(random),
        },
        22 => Value::Character(random.next_u64() as u16),
        23 => random_regex(random),
        24 => Value::Array(
            (0..random.bounded(7))
                .map(|_| generated_value_at(random, depth + 1))
                .collect(),
        ),
        25 => random_map(random, depth),
        26 => random_set(random, depth),
        27 => Value::List(
            (0..random.bounded(7))
                .map(|_| generated_value_at(random, depth + 1))
                .collect(),
        ),
        28 => Value::Queue(
            (0..random.bounded(7))
                .map(|_| generated_value_at(random, depth + 1))
                .collect(),
        ),
        29 => Value::Extension {
            type_id: ExtensionId::Name("org.example/generated".into()),
            arguments: (0..random.bounded(5))
                .map(|_| generated_value_at(random, depth + 1))
                .collect(),
        },
        _ => unreachable!(),
    }
}

fn random_i64(random: &mut DeterministicRng) -> i64 {
    const BOUNDARIES: &[i64] = &[
        i64::MIN,
        -4_294_967_297,
        -25,
        -24,
        -1,
        0,
        23,
        24,
        255,
        256,
        65_535,
        65_536,
        4_294_967_295,
        4_294_967_296,
        i64::MAX,
    ];
    if random.bounded(4) == 0 {
        BOUNDARIES[random.bounded(BOUNDARIES.len())]
    } else {
        random.next_u64() as i64
    }
}

fn random_big_integer(random: &mut DeterministicRng, negative: bool) -> Integer {
    let length = 9 + random.bounded(15);
    let mut magnitude: Vec<u8> = (0..length).map(|_| random.next_u64() as u8).collect();
    if magnitude[0] == 0 {
        magnitude[0] = 1;
    }
    if negative {
        Integer::NegativeBig(magnitude)
    } else {
        Integer::PositiveBig(magnitude)
    }
}

fn random_text(random: &mut DeterministicRng) -> String {
    (0..random.bounded(33))
        .map(|_| {
            let (start, width) = match random.bounded(16) {
                0..=9 => (0_u32, 0x80_u32),
                10..=11 => (0x80, 0xd780),
                12..=13 => (0xe000, 0x2000),
                _ => (0x10000, 0x100000),
            };
            let code_point = start + (random.next_u64() % u64::from(width)) as u32;
            char::from_u32(code_point).expect("generated Unicode scalar")
        })
        .collect()
}

fn random_decimal(random: &mut DeterministicRng) -> Value {
    if random.bounded(8) == 0 {
        Value::Decimal {
            exponent: 0,
            mantissa: 0.into(),
        }
    } else {
        Value::Decimal {
            exponent: random.bounded(65) as i32 - 32,
            mantissa: Integer::I64(random.next_u64() as i64 | 1),
        }
    }
}

fn random_ratio(random: &mut DeterministicRng) -> Value {
    let numerator = random.bounded(2_000_001) as i64 - 1_000_000;
    if numerator == 0 {
        return Value::Ratio {
            numerator: 0.into(),
            denominator: 1.into(),
        };
    }
    let denominator = random.bounded(1_000_000) as i64 + 1;
    let divisor = gcd(numerator.unsigned_abs(), denominator as u64) as i64;
    Value::Ratio {
        numerator: (numerator / divisor).into(),
        denominator: (denominator / divisor).into(),
    }
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

fn random_uri(random: &mut DeterministicRng) -> String {
    let suffix: String = (0..random.bounded(33))
        .map(|_| match random.bounded(62) as u8 {
            value @ 0..=9 => char::from(b'0' + value),
            value @ 10..=35 => char::from(b'a' + value - 10),
            value => char::from(b'A' + value - 36),
        })
        .collect();
    format!("https://example.test/{suffix}")
}

fn random_uuid(random: &mut DeterministicRng) -> [u8; 16] {
    let mut bytes = [0; 16];
    for byte in &mut bytes {
        *byte = random.next_u64() as u8;
    }
    bytes
}

fn random_regex(random: &mut DeterministicRng) -> Value {
    const FLAGS: &[u16] = &[0, 1, 2, 4, 8, 16, 32, 64, 320, 383];
    let source: String = (0..random.bounded(33))
        .map(|_| match random.bounded(62) as u8 {
            value @ 0..=9 => char::from(b'0' + value),
            value @ 10..=35 => char::from(b'a' + value - 10),
            value => char::from(b'A' + value - 36),
        })
        .collect();
    Value::Regex {
        source,
        flags: FLAGS[random.bounded(FLAGS.len())],
    }
}

fn random_map_key(random: &mut DeterministicRng) -> Value {
    match random.bounded(12) {
        0 => Value::Null,
        1 => Value::Bool(random.next_u64() & 1 != 0),
        2 => Value::Integer(random_i64(random).into()),
        3 => Value::Integer(random_big_integer(random, false)),
        4 => Value::Integer(random_big_integer(random, true)),
        5 => Value::Text(random_text(random)),
        6 => Value::Bytes(random.bytes(32)),
        7 => Value::Uri(random_uri(random)),
        8 => Value::Uuid(random_uuid(random)),
        9 => Value::Keyword {
            namespace: None,
            name: random_text(random),
        },
        10 => Value::Symbol {
            namespace: None,
            name: random_text(random),
        },
        11 => Value::Character(random.next_u64() as u16),
        _ => unreachable!(),
    }
}

fn random_map(random: &mut DeterministicRng, depth: usize) -> Value {
    let length = random.bounded(7);
    let mut entries = Vec::with_capacity(length);
    let mut seen = HashSet::new();
    while entries.len() < length {
        let key = random_collection_member(random, depth);
        let canonical_key = encode(&key, Mode::Canonical).expect("generated map key");
        if seen.insert(canonical_key) {
            entries.push((key, generated_value_at(random, depth + 1)));
        }
    }
    Value::Map(entries)
}

fn random_set(random: &mut DeterministicRng, depth: usize) -> Value {
    let length = random.bounded(9);
    let mut values = Vec::with_capacity(length);
    let mut seen = HashSet::new();
    while values.len() < length {
        let value = random_collection_member(random, depth);
        let canonical_value = encode(&value, Mode::Canonical).expect("generated set value");
        if seen.insert(canonical_value) {
            values.push(value);
        }
    }
    Value::Set(values)
}

fn random_collection_member(random: &mut DeterministicRng, depth: usize) -> Value {
    match random.bounded(16) {
        // Exercise identities that ordinary Clojure maps and sets merge.
        0 => Value::Array(vec![]),
        1 => Value::List(vec![]),
        2 => Value::Queue(vec![]),
        3 => Value::Float32(0.0),
        4 => Value::Float32(-0.0),
        5 => Value::Float64(0.0),
        6 => Value::Float64(-0.0),
        7 => Value::Float32(1.0),
        8 => Value::Float64(1.0),
        9..=12 => generated_value_at(random, depth + 1),
        _ => random_map_key(random),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    result
}

fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex payload has odd length".to_owned());
    }
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(format!("invalid hex digit: {}", char::from(value))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_operations_round_trip() {
        assert_eq!(vec![0x18, 0x18], process("canonical\t1818").unwrap());
        assert_eq!(
            vec![0xa2, 0x61, b'b', 1, 0x62, b'a', b'a', 2],
            process("fast\ta262616102616201").unwrap()
        );
        assert_eq!(vec![0xff, 0xf6], process("storage\tfff6").unwrap());
        let generated = process("generate\t0123456789abcdef").unwrap();
        assert_eq!(generated, process("generate\t0123456789abcdef").unwrap());
        assert!(decode(&generated, true).is_ok());
        let generated_fast = process("generate-fast\t0123456789abcdef").unwrap();
        let decoded_fast = decode(&generated_fast, false).unwrap();
        assert_eq!(generated, encode(&decoded_fast, Mode::Canonical).unwrap());
    }

    #[test]
    fn malformed_protocol_input_is_rejected() {
        assert!(process("canonical").is_err());
        assert!(process("canonical\t0").is_err());
        assert!(process("canonical\tzz").is_err());
        assert!(process("unknown\t00").is_err());
        assert_eq!("NON_SHORTEST\t0", process("canonical\t1817").unwrap_err());
    }
}
