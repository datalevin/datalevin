//! DL-CBOR v1 Phase 0 codec.
//!
//! This initial implementation deliberately has no third-party dependency. It
//! gives the JVM implementation an independent exact-byte oracle and provides
//! the purpose-built side of the Phase 0 library comparison. It is not yet a
//! durable storage codec.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;

const TAG_POSITIVE_BIGNUM: u64 = 2;
const TAG_NEGATIVE_BIGNUM: u64 = 3;
const TAG_DECIMAL: u64 = 4;
const TAG_RATIO: u64 = 30;
const TAG_URI: u64 = 32;
const TAG_UUID: u64 = 37;
const TAG_SET: u64 = 258;

const CANONICAL_FLOAT32_NAN: u32 = 0x7fc0_0000;
const CANONICAL_FLOAT64_NAN: u64 = 0x7ff8_0000_0000_0000;

/// Encoding policy for collection ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Canonical,
    Fast,
}

/// An integer in the portable DL-CBOR value model.
///
/// Big-integer byte vectors contain the unsigned magnitude used by CBOR tag 2
/// or tag 3. They are big-endian, minimal, and never empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Integer {
    I64(i64),
    PositiveBig(Vec<u8>),
    NegativeBig(Vec<u8>),
}

impl From<i64> for Integer {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

/// Neutral values covered by the initial shared corpus.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(Integer),
    Float32(f32),
    Float64(f64),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Set(Vec<Value>),
    Decimal {
        exponent: i32,
        mantissa: Integer,
    },
    Ratio {
        numerator: Integer,
        denominator: Integer,
    },
    Uri(String),
    Uuid([u8; 16]),
    Tagged {
        tag: u64,
        value: Box<Value>,
    },
}

impl Value {
    pub fn int(value: i64) -> Self {
        Self::Integer(Integer::I64(value))
    }
}

/// Bounds checked before allocation by the decoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub max_input_bytes: usize,
    pub max_depth: usize,
    pub max_collection_len: usize,
    pub max_string_bytes: usize,
    pub max_bignum_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 * 1024 * 1024,
            max_depth: 256,
            max_collection_len: 1_000_000,
            max_string_bytes: 16 * 1024 * 1024,
            max_bignum_bytes: 4 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    InputTooLarge,
    OutputTooSmall,
    Truncated,
    TrailingBytes,
    InvalidAdditionalInfo,
    IndefiniteLength,
    NonShortest,
    UnsupportedSimpleValue,
    IntegerOutOfRange,
    LengthOutOfRange,
    InvalidUtf8,
    InvalidUnicode,
    DepthLimit,
    CollectionLimit,
    StringLimit,
    BignumLimit,
    InvalidBignum,
    InvalidDecimal,
    InvalidRatio,
    InvalidUri,
    InvalidUuid,
    DuplicateKey,
    DuplicateSetMember,
    NonCanonical,
    UnescapedTypedHeader,
    UnnecessaryStorageEscape,
    UnsupportedValue,
}

impl ErrorKind {
    /// Stable cross-language identifier shared with the JVM codec.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InputTooLarge => "INPUT_TOO_LARGE",
            Self::OutputTooSmall => "OUTPUT_TOO_SMALL",
            Self::Truncated => "TRUNCATED",
            Self::TrailingBytes => "TRAILING_BYTES",
            Self::InvalidAdditionalInfo => "INVALID_ADDITIONAL_INFO",
            Self::IndefiniteLength => "INDEFINITE_LENGTH",
            Self::NonShortest => "NON_SHORTEST",
            Self::UnsupportedSimpleValue => "UNSUPPORTED_SIMPLE_VALUE",
            Self::IntegerOutOfRange => "INTEGER_OUT_OF_RANGE",
            Self::LengthOutOfRange => "LENGTH_OUT_OF_RANGE",
            Self::InvalidUtf8 => "INVALID_UTF8",
            Self::InvalidUnicode => "INVALID_UNICODE",
            Self::DepthLimit => "DEPTH_LIMIT",
            Self::CollectionLimit => "COLLECTION_LIMIT",
            Self::StringLimit => "STRING_LIMIT",
            Self::BignumLimit => "BIGNUM_LIMIT",
            Self::InvalidBignum => "INVALID_BIGNUM",
            Self::InvalidDecimal => "INVALID_DECIMAL",
            Self::InvalidRatio => "INVALID_RATIO",
            Self::InvalidUri => "INVALID_URI",
            Self::InvalidUuid => "INVALID_UUID",
            Self::DuplicateKey => "DUPLICATE_KEY",
            Self::DuplicateSetMember => "DUPLICATE_SET_MEMBER",
            Self::NonCanonical => "NON_CANONICAL",
            Self::UnescapedTypedHeader => "UNESCAPED_TYPED_HEADER",
            Self::UnnecessaryStorageEscape => "UNNECESSARY_STORAGE_ESCAPE",
            Self::UnsupportedValue => "UNSUPPORTED_VALUE",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub kind: ErrorKind,
    pub offset: usize,
}

impl Error {
    fn new(kind: ErrorKind, offset: usize) -> Self {
        Self { kind, offset }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DL-CBOR {} at byte {}", self.kind.code(), self.offset)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

trait Writer {
    fn position(&self) -> usize;
    fn put(&mut self, bytes: &[u8]) -> Result<()>;

    fn put_u8(&mut self, value: u8) -> Result<()> {
        self.put(&[value])
    }
}

struct VecWriter {
    bytes: Vec<u8>,
}

impl VecWriter {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }
}

impl Writer for VecWriter {
    fn position(&self) -> usize {
        self.bytes.len()
    }

    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

struct SliceWriter<'a> {
    output: &'a mut [u8],
    position: usize,
}

impl Writer for SliceWriter<'_> {
    fn position(&self) -> usize {
        self.position
    }

    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or_else(|| Error::new(ErrorKind::OutputTooSmall, self.position))?;
        if end > self.output.len() {
            return Err(Error::new(ErrorKind::OutputTooSmall, self.position));
        }
        self.output[self.position..end].copy_from_slice(bytes);
        self.position = end;
        Ok(())
    }
}

struct CountWriter {
    position: usize,
}

impl Writer for CountWriter {
    fn position(&self) -> usize {
        self.position
    }

    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        self.position = self
            .position
            .checked_add(bytes.len())
            .ok_or_else(|| Error::new(ErrorKind::LengthOutOfRange, self.position))?;
        Ok(())
    }
}

/// Return the exact encoded length for `value` in `mode`.
pub fn encoded_len(value: &Value, mode: Mode) -> Result<usize> {
    let mut writer = CountWriter { position: 0 };
    encode_value(&mut writer, value, mode)?;
    Ok(writer.position())
}

/// Encode into caller-owned storage and return the number of bytes written.
pub fn encode_into(value: &Value, mode: Mode, output: &mut [u8]) -> Result<usize> {
    let mut writer = SliceWriter {
        output,
        position: 0,
    };
    encode_value(&mut writer, value, mode)?;
    Ok(writer.position())
}

/// Allocating convenience wrapper around [`encode_into`].
pub fn encode(value: &Value, mode: Mode) -> Result<Vec<u8>> {
    let mut writer = VecWriter::new();
    encode_value(&mut writer, value, mode)?;
    Ok(writer.bytes)
}

/// Encode the storage-only collision wrapper described by DL-CBOR v1.
pub fn encode_storage(value: &Value, mode: Mode) -> Result<Vec<u8>> {
    let bare = encode(value, mode)?;
    if bare.first().copied().is_some_and(is_typed_header) {
        let mut escaped = Vec::with_capacity(bare.len() + 1);
        escaped.push(0xff);
        escaped.extend_from_slice(&bare);
        Ok(escaped)
    } else {
        Ok(bare)
    }
}

/// Decode one complete bare DL-CBOR item.
pub fn decode(input: &[u8], canonical: bool) -> Result<Value> {
    decode_with_limits(input, canonical, Limits::default())
}

pub fn decode_with_limits(input: &[u8], canonical: bool, limits: Limits) -> Result<Value> {
    if input.len() > limits.max_input_bytes {
        return Err(Error::new(ErrorKind::InputTooLarge, 0));
    }
    let mut decoder = Decoder {
        input,
        position: 0,
        canonical,
        limits,
    };
    let value = decoder.value(0)?;
    if decoder.position != input.len() {
        return Err(Error::new(ErrorKind::TrailingBytes, decoder.position));
    }
    Ok(value)
}

/// Decode a complete storage-wrapped untyped item.
pub fn decode_storage(input: &[u8], canonical: bool) -> Result<Value> {
    decode_storage_with_limits(input, canonical, Limits::default())
}

/// Decode a complete storage-wrapped untyped item with explicit allocation
/// and nesting limits.
pub fn decode_storage_with_limits(input: &[u8], canonical: bool, limits: Limits) -> Result<Value> {
    let Some(first) = input.first().copied() else {
        return Err(Error::new(ErrorKind::Truncated, 0));
    };
    if first == 0xff {
        let Some(bare_first) = input.get(1).copied() else {
            return Err(Error::new(ErrorKind::Truncated, 1));
        };
        if !is_typed_header(bare_first) {
            return Err(Error::new(ErrorKind::UnnecessaryStorageEscape, 0));
        }
        decode_with_limits(&input[1..], canonical, limits)
    } else if is_typed_header(first) {
        Err(Error::new(ErrorKind::UnescapedTypedHeader, 0))
    } else {
        decode_with_limits(input, canonical, limits)
    }
}

pub fn is_typed_header(byte: u8) -> bool {
    byte == 0xc0 || byte == 0xc1 || (0xf1..=0xfe).contains(&byte)
}

fn encode_value<W: Writer>(writer: &mut W, value: &Value, mode: Mode) -> Result<()> {
    match value {
        Value::Null => writer.put_u8(0xf6),
        Value::Bool(false) => writer.put_u8(0xf4),
        Value::Bool(true) => writer.put_u8(0xf5),
        Value::Integer(integer) => encode_integer(writer, integer),
        Value::Float32(value) => {
            writer.put_u8(0xfa)?;
            let bits = if value.is_nan() {
                CANONICAL_FLOAT32_NAN
            } else {
                value.to_bits()
            };
            writer.put(&bits.to_be_bytes())
        }
        Value::Float64(value) => {
            writer.put_u8(0xfb)?;
            let bits = if value.is_nan() {
                CANONICAL_FLOAT64_NAN
            } else {
                value.to_bits()
            };
            writer.put(&bits.to_be_bytes())
        }
        Value::Bytes(bytes) => {
            encode_head(writer, 2, bytes.len() as u64)?;
            writer.put(bytes)
        }
        Value::Text(text) => {
            validate_unicode(text)?;
            encode_head(writer, 3, text.len() as u64)?;
            writer.put(text.as_bytes())
        }
        Value::Array(values) => {
            encode_head(writer, 4, values.len() as u64)?;
            for value in values {
                encode_value(writer, value, mode)?;
            }
            Ok(())
        }
        Value::Map(entries) => encode_map(writer, entries, mode),
        Value::Set(values) => {
            encode_head(writer, 6, TAG_SET)?;
            encode_set(writer, values, mode)
        }
        Value::Decimal { exponent, mantissa } => {
            validate_decimal(*exponent, mantissa)?;
            encode_head(writer, 6, TAG_DECIMAL)?;
            encode_head(writer, 4, 2)?;
            encode_i64(writer, i64::from(*exponent))?;
            encode_integer(writer, mantissa)
        }
        Value::Ratio {
            numerator,
            denominator,
        } => {
            validate_ratio(numerator, denominator)?;
            encode_head(writer, 6, TAG_RATIO)?;
            encode_head(writer, 4, 2)?;
            encode_integer(writer, numerator)?;
            encode_integer(writer, denominator)
        }
        Value::Uri(uri) => {
            if uri.is_empty() || !uri.is_ascii() {
                return Err(Error::new(ErrorKind::InvalidUri, writer.position()));
            }
            encode_head(writer, 6, TAG_URI)?;
            encode_value(writer, &Value::Text(uri.clone()), mode)
        }
        Value::Uuid(bytes) => {
            encode_head(writer, 6, TAG_UUID)?;
            encode_head(writer, 2, 16)?;
            writer.put(bytes)
        }
        Value::Tagged { tag, value } => {
            encode_head(writer, 6, *tag)?;
            encode_value(writer, value, mode)
        }
    }
}

fn encode_integer<W: Writer>(writer: &mut W, integer: &Integer) -> Result<()> {
    match integer {
        Integer::I64(value) => encode_i64(writer, *value),
        Integer::PositiveBig(magnitude) => {
            validate_big_magnitude(magnitude, writer.position())?;
            encode_head(writer, 6, TAG_POSITIVE_BIGNUM)?;
            encode_head(writer, 2, magnitude.len() as u64)?;
            writer.put(magnitude)
        }
        Integer::NegativeBig(magnitude) => {
            validate_big_magnitude(magnitude, writer.position())?;
            encode_head(writer, 6, TAG_NEGATIVE_BIGNUM)?;
            encode_head(writer, 2, magnitude.len() as u64)?;
            writer.put(magnitude)
        }
    }
}

fn encode_i64<W: Writer>(writer: &mut W, value: i64) -> Result<()> {
    if value >= 0 {
        encode_head(writer, 0, value as u64)
    } else {
        encode_head(writer, 1, (!value) as u64)
    }
}

fn encode_head<W: Writer>(writer: &mut W, major: u8, argument: u64) -> Result<()> {
    let prefix = major << 5;
    match argument {
        0..=23 => writer.put_u8(prefix | argument as u8),
        24..=0xff => writer.put(&[prefix | 24, argument as u8]),
        0x100..=0xffff => {
            writer.put_u8(prefix | 25)?;
            writer.put(&(argument as u16).to_be_bytes())
        }
        0x1_0000..=0xffff_ffff => {
            writer.put_u8(prefix | 26)?;
            writer.put(&(argument as u32).to_be_bytes())
        }
        _ => {
            writer.put_u8(prefix | 27)?;
            writer.put(&argument.to_be_bytes())
        }
    }
}

fn encode_map<W: Writer>(writer: &mut W, entries: &[(Value, Value)], mode: Mode) -> Result<()> {
    let mut prepared = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        prepared.push((encode(key, Mode::Canonical)?, key, value));
    }
    if mode == Mode::Canonical {
        prepared.sort_by(|left, right| canonical_cmp(&left.0, &right.0));
    }
    reject_duplicate_bytes(
        prepared.iter().map(|entry| entry.0.as_slice()),
        ErrorKind::DuplicateKey,
        writer.position(),
    )?;

    encode_head(writer, 5, prepared.len() as u64)?;
    for (canonical_key, key, value) in prepared {
        if mode == Mode::Canonical {
            writer.put(&canonical_key)?;
        } else {
            encode_value(writer, key, mode)?;
        }
        encode_value(writer, value, mode)?;
    }
    Ok(())
}

fn encode_set<W: Writer>(writer: &mut W, values: &[Value], mode: Mode) -> Result<()> {
    let mut prepared = Vec::with_capacity(values.len());
    for value in values {
        prepared.push((encode(value, Mode::Canonical)?, value));
    }
    if mode == Mode::Canonical {
        prepared.sort_by(|left, right| canonical_cmp(&left.0, &right.0));
    }
    reject_duplicate_bytes(
        prepared.iter().map(|entry| entry.0.as_slice()),
        ErrorKind::DuplicateSetMember,
        writer.position(),
    )?;

    encode_head(writer, 4, prepared.len() as u64)?;
    for (canonical_value, value) in prepared {
        if mode == Mode::Canonical {
            writer.put(&canonical_value)?;
        } else {
            encode_value(writer, value, mode)?;
        }
    }
    Ok(())
}

fn reject_duplicate_bytes<'a>(
    values: impl Iterator<Item = &'a [u8]>,
    kind: ErrorKind,
    offset: usize,
) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_vec()) {
            return Err(Error::new(kind, offset));
        }
    }
    Ok(())
}

fn canonical_cmp(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn validate_big_magnitude(magnitude: &[u8], offset: usize) -> Result<()> {
    if magnitude.is_empty() || magnitude[0] == 0 {
        return Err(Error::new(ErrorKind::InvalidBignum, offset));
    }
    let threshold = i64::MAX.to_be_bytes();
    if magnitude.len() < threshold.len()
        || (magnitude.len() == threshold.len() && magnitude <= threshold.as_slice())
    {
        return Err(Error::new(ErrorKind::InvalidBignum, offset));
    }
    Ok(())
}

fn validate_decimal(exponent: i32, mantissa: &Integer) -> Result<()> {
    if integer_is_zero(mantissa) {
        return if exponent == 0 {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::InvalidDecimal, 0))
        };
    }
    if integer_abs_mod_small(mantissa, 10) == 0 {
        Err(Error::new(ErrorKind::InvalidDecimal, 0))
    } else {
        Ok(())
    }
}

fn validate_ratio(numerator: &Integer, denominator: &Integer) -> Result<()> {
    match denominator {
        Integer::I64(denominator) => {
            if *denominator <= 0 {
                return Err(Error::new(ErrorKind::InvalidRatio, 0));
            }
        }
        Integer::PositiveBig(_) => {}
        Integer::NegativeBig(_) => {
            return Err(Error::new(ErrorKind::InvalidRatio, 0));
        }
    }
    if integers_are_coprime(numerator, denominator) {
        Ok(())
    } else {
        Err(Error::new(ErrorKind::InvalidRatio, 0))
    }
}

fn integer_is_zero(value: &Integer) -> bool {
    matches!(value, Integer::I64(0))
}

fn integer_abs_mod_small(value: &Integer, divisor: u16) -> u16 {
    match value {
        Integer::I64(value) => (value.unsigned_abs() % u64::from(divisor)) as u16,
        Integer::PositiveBig(magnitude) => magnitude_mod_small(magnitude, divisor),
        Integer::NegativeBig(magnitude) => (magnitude_mod_small(magnitude, divisor) + 1) % divisor,
    }
}

fn magnitude_mod_small(magnitude: &[u8], divisor: u16) -> u16 {
    magnitude.iter().fold(0, |remainder, byte| {
        ((remainder << 8) + u16::from(*byte)) % divisor
    })
}

fn integers_are_coprime(left: &Integer, right: &Integer) -> bool {
    let mut left = integer_abs_magnitude(left);
    let mut right = integer_abs_magnitude(right);
    while !right.is_empty() {
        let remainder = magnitude_remainder(&left, &right);
        left = right;
        right = remainder;
    }
    left == [1]
}

fn integer_abs_magnitude(value: &Integer) -> Vec<u8> {
    match value {
        Integer::I64(value) => u64_magnitude(value.unsigned_abs()),
        Integer::PositiveBig(magnitude) => magnitude.clone(),
        Integer::NegativeBig(magnitude) => magnitude_plus_one(magnitude),
    }
}

fn u64_magnitude(value: u64) -> Vec<u8> {
    if value == 0 {
        return Vec::new();
    }
    let bytes = value.to_be_bytes();
    bytes[bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len())..]
        .to_vec()
}

fn magnitude_plus_one(magnitude: &[u8]) -> Vec<u8> {
    let mut result = magnitude.to_vec();
    let mut carry = true;
    for byte in result.iter_mut().rev() {
        if !carry {
            break;
        }
        let (sum, overflow) = byte.overflowing_add(1);
        *byte = sum;
        carry = overflow;
    }
    if carry {
        result.insert(0, 1);
    }
    result
}

fn magnitude_remainder(dividend: &[u8], divisor: &[u8]) -> Vec<u8> {
    debug_assert!(!divisor.is_empty());
    let mut remainder = Vec::with_capacity(divisor.len());
    for byte in dividend {
        for shift in (0..8).rev() {
            magnitude_shift_left_add(&mut remainder, (byte >> shift) & 1);
            if magnitude_cmp(&remainder, divisor) != Ordering::Less {
                magnitude_subtract(&mut remainder, divisor);
            }
        }
    }
    remainder
}

fn magnitude_shift_left_add(value: &mut Vec<u8>, bit: u8) {
    let mut carry = bit;
    for byte in value.iter_mut().rev() {
        let shifted = (u16::from(*byte) << 1) | u16::from(carry);
        *byte = shifted as u8;
        carry = (shifted >> 8) as u8;
    }
    if carry != 0 {
        value.insert(0, carry);
    }
}

fn magnitude_cmp(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn magnitude_subtract(left: &mut Vec<u8>, right: &[u8]) {
    debug_assert!(magnitude_cmp(left, right) != Ordering::Less);
    let mut borrow = 0_i16;
    for left_index in (0..left.len()).rev() {
        let right_offset = left.len() - 1 - left_index;
        let right_byte = right
            .len()
            .checked_sub(right_offset + 1)
            .map_or(0, |right_index| i16::from(right[right_index]));
        let difference = i16::from(left[left_index]) - right_byte - borrow;
        if difference < 0 {
            left[left_index] = (difference + 256) as u8;
            borrow = 1;
        } else {
            left[left_index] = difference as u8;
            borrow = 0;
        }
    }
    debug_assert_eq!(0, borrow);
    let first_nonzero = left
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(left.len());
    left.drain(..first_nonzero);
}

fn validate_unicode(value: &str) -> Result<()> {
    // A Rust str is already valid UTF-8 and cannot contain an unpaired UTF-16
    // surrogate. Keep a named check so the cross-language contract is visible.
    if value
        .chars()
        .any(|character| (0xd800..=0xdfff).contains(&(character as u32)))
    {
        Err(Error::new(ErrorKind::InvalidUnicode, 0))
    } else {
        Ok(())
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
    canonical: bool,
    limits: Limits,
}

impl Decoder<'_> {
    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > self.limits.max_depth {
            return Err(Error::new(ErrorKind::DepthLimit, self.position));
        }
        let head_offset = self.position;
        let head = self.read_u8()?;
        let major = head >> 5;
        let additional = head & 0x1f;

        match major {
            0 => {
                let argument = self.argument(additional, head_offset)?;
                if argument > i64::MAX as u64 {
                    return Err(Error::new(ErrorKind::IntegerOutOfRange, head_offset));
                }
                Ok(Value::int(argument as i64))
            }
            1 => {
                let argument = self.argument(additional, head_offset)?;
                if argument > i64::MAX as u64 {
                    return Err(Error::new(ErrorKind::IntegerOutOfRange, head_offset));
                }
                Ok(Value::int(!(argument as i64)))
            }
            2 => {
                let length = self.length(additional, head_offset, self.limits.max_string_bytes)?;
                Ok(Value::Bytes(self.read(length)?.to_vec()))
            }
            3 => {
                let length = self.length(additional, head_offset, self.limits.max_string_bytes)?;
                let offset = self.position;
                let bytes = self.read(length)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| Error::new(ErrorKind::InvalidUtf8, offset))?;
                Ok(Value::Text(text.to_owned()))
            }
            4 => {
                let length =
                    self.length(additional, head_offset, self.limits.max_collection_len)?;
                let mut values = Vec::with_capacity(length);
                for _ in 0..length {
                    values.push(self.value(depth + 1)?);
                }
                Ok(Value::Array(values))
            }
            5 => self.map(additional, head_offset, depth),
            6 => {
                let tag = self.argument(additional, head_offset)?;
                if tag == TAG_SET {
                    return self.set(head_offset, depth + 1);
                }
                let value = self.value(depth + 1)?;
                self.tagged(tag, value, head_offset)
            }
            7 => self.simple(additional, head_offset),
            _ => unreachable!(),
        }
    }

    fn map(&mut self, additional: u8, head_offset: usize, depth: usize) -> Result<Value> {
        let length = self.length(additional, head_offset, self.limits.max_collection_len)?;
        let mut entries = Vec::with_capacity(length);
        let mut seen = HashSet::with_capacity(length.min(1024));
        let mut previous: Option<Vec<u8>> = None;
        for _ in 0..length {
            let key_start = self.position;
            let key = self.value(depth + 1)?;
            let key_end = self.position;
            let canonical_key = if self.canonical {
                self.input[key_start..key_end].to_vec()
            } else {
                encode(&key, Mode::Canonical)?
            };
            if !seen.insert(canonical_key.clone()) {
                return Err(Error::new(ErrorKind::DuplicateKey, key_start));
            }
            if self.canonical {
                if let Some(previous) = &previous
                    && canonical_cmp(previous, &canonical_key) != Ordering::Less
                {
                    return Err(Error::new(ErrorKind::NonCanonical, key_start));
                }
                previous = Some(canonical_key);
            }
            let value = self.value(depth + 1)?;
            entries.push((key, value));
        }
        Ok(Value::Map(entries))
    }

    fn set(&mut self, tag_offset: usize, depth: usize) -> Result<Value> {
        if depth > self.limits.max_depth {
            return Err(Error::new(ErrorKind::DepthLimit, self.position));
        }
        let array_offset = self.position;
        let array_head = self.read_u8()?;
        if array_head >> 5 != 4 {
            return Err(Error::new(ErrorKind::UnsupportedValue, tag_offset));
        }
        let length = self.length(
            array_head & 0x1f,
            array_offset,
            self.limits.max_collection_len,
        )?;
        let mut values = Vec::with_capacity(length);
        let mut seen = HashSet::with_capacity(length.min(1024));
        let mut previous: Option<Vec<u8>> = None;
        for _ in 0..length {
            let value_offset = self.position;
            let value = self.value(depth + 1)?;
            let canonical = encode(&value, Mode::Canonical)?;
            if !seen.insert(canonical.clone()) {
                return Err(Error::new(ErrorKind::DuplicateSetMember, value_offset));
            }
            if self.canonical {
                if let Some(previous) = &previous
                    && canonical_cmp(previous, &canonical) != Ordering::Less
                {
                    return Err(Error::new(ErrorKind::NonCanonical, value_offset));
                }
                previous = Some(canonical);
            }
            values.push(value);
        }
        Ok(Value::Set(values))
    }

    fn tagged(&mut self, tag: u64, value: Value, offset: usize) -> Result<Value> {
        match tag {
            TAG_POSITIVE_BIGNUM | TAG_NEGATIVE_BIGNUM => {
                let Value::Bytes(magnitude) = value else {
                    return Err(Error::new(ErrorKind::InvalidBignum, offset));
                };
                if magnitude.len() > self.limits.max_bignum_bytes {
                    return Err(Error::new(ErrorKind::BignumLimit, offset));
                }
                validate_big_magnitude(&magnitude, offset)?;
                if tag == TAG_POSITIVE_BIGNUM {
                    Ok(Value::Integer(Integer::PositiveBig(magnitude)))
                } else {
                    Ok(Value::Integer(Integer::NegativeBig(magnitude)))
                }
            }
            TAG_DECIMAL => {
                let [exponent, mantissa] = expect_pair(value, ErrorKind::InvalidDecimal, offset)?;
                let Integer::I64(exponent) =
                    expect_integer(exponent, ErrorKind::InvalidDecimal, offset)?
                else {
                    return Err(Error::new(ErrorKind::InvalidDecimal, offset));
                };
                let exponent = i32::try_from(exponent)
                    .map_err(|_| Error::new(ErrorKind::InvalidDecimal, offset))?;
                let mantissa = expect_integer(mantissa, ErrorKind::InvalidDecimal, offset)?;
                validate_decimal(exponent, &mantissa)?;
                Ok(Value::Decimal { exponent, mantissa })
            }
            TAG_RATIO => {
                let [numerator, denominator] = expect_pair(value, ErrorKind::InvalidRatio, offset)?;
                let numerator = expect_integer(numerator, ErrorKind::InvalidRatio, offset)?;
                let denominator = expect_integer(denominator, ErrorKind::InvalidRatio, offset)?;
                validate_ratio(&numerator, &denominator)?;
                Ok(Value::Ratio {
                    numerator,
                    denominator,
                })
            }
            TAG_URI => {
                let Value::Text(uri) = value else {
                    return Err(Error::new(ErrorKind::InvalidUri, offset));
                };
                if uri.is_empty() || !uri.is_ascii() {
                    return Err(Error::new(ErrorKind::InvalidUri, offset));
                }
                Ok(Value::Uri(uri))
            }
            TAG_UUID => {
                let Value::Bytes(bytes) = value else {
                    return Err(Error::new(ErrorKind::InvalidUuid, offset));
                };
                let bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| Error::new(ErrorKind::InvalidUuid, offset))?;
                Ok(Value::Uuid(bytes))
            }
            _ => Ok(Value::Tagged {
                tag,
                value: Box::new(value),
            }),
        }
    }

    fn simple(&mut self, additional: u8, offset: usize) -> Result<Value> {
        match additional {
            20 => Ok(Value::Bool(false)),
            21 => Ok(Value::Bool(true)),
            22 => Ok(Value::Null),
            25 => Err(Error::new(ErrorKind::UnsupportedSimpleValue, offset)),
            26 => {
                let bits = u32::from_be_bytes(self.read_array()?);
                if self.canonical && f32::from_bits(bits).is_nan() && bits != CANONICAL_FLOAT32_NAN
                {
                    return Err(Error::new(ErrorKind::NonCanonical, offset));
                }
                Ok(Value::Float32(f32::from_bits(bits)))
            }
            27 => {
                let bits = u64::from_be_bytes(self.read_array()?);
                if self.canonical && f64::from_bits(bits).is_nan() && bits != CANONICAL_FLOAT64_NAN
                {
                    return Err(Error::new(ErrorKind::NonCanonical, offset));
                }
                Ok(Value::Float64(f64::from_bits(bits)))
            }
            31 => Err(Error::new(ErrorKind::IndefiniteLength, offset)),
            _ => Err(Error::new(ErrorKind::UnsupportedSimpleValue, offset)),
        }
    }

    fn argument(&mut self, additional: u8, offset: usize) -> Result<u64> {
        let value = match additional {
            0..=23 => return Ok(u64::from(additional)),
            24 => u64::from(self.read_u8()?),
            25 => u64::from(u16::from_be_bytes(self.read_array()?)),
            26 => u64::from(u32::from_be_bytes(self.read_array()?)),
            27 => u64::from_be_bytes(self.read_array()?),
            31 => return Err(Error::new(ErrorKind::IndefiniteLength, offset)),
            _ => return Err(Error::new(ErrorKind::InvalidAdditionalInfo, offset)),
        };
        if self.canonical {
            let non_shortest = match additional {
                24 => value < 24,
                25 => value <= 0xff,
                26 => value <= 0xffff,
                27 => value <= 0xffff_ffff,
                _ => false,
            };
            if non_shortest {
                return Err(Error::new(ErrorKind::NonShortest, offset));
            }
        }
        Ok(value)
    }

    fn length(&mut self, additional: u8, offset: usize, limit: usize) -> Result<usize> {
        let length = self.argument(additional, offset)?;
        let length =
            usize::try_from(length).map_err(|_| Error::new(ErrorKind::LengthOutOfRange, offset))?;
        if length > limit {
            let kind = if limit == self.limits.max_collection_len {
                ErrorKind::CollectionLimit
            } else {
                ErrorKind::StringLimit
            };
            return Err(Error::new(kind, offset));
        }
        Ok(length)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let Some(value) = self.input.get(self.position).copied() else {
            return Err(Error::new(ErrorKind::Truncated, self.position));
        };
        self.position += 1;
        Ok(value)
    }

    fn read(&mut self, length: usize) -> Result<&[u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| Error::new(ErrorKind::LengthOutOfRange, self.position))?;
        let Some(bytes) = self.input.get(self.position..end) else {
            return Err(Error::new(ErrorKind::Truncated, self.position));
        };
        self.position = end;
        Ok(bytes)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.read(N)?
            .try_into()
            .map_err(|_| Error::new(ErrorKind::Truncated, self.position))
    }
}

fn expect_pair(value: Value, kind: ErrorKind, offset: usize) -> Result<[Value; 2]> {
    let Value::Array(values) = value else {
        return Err(Error::new(kind, offset));
    };
    values.try_into().map_err(|_| Error::new(kind, offset))
}

fn expect_integer(value: Value, kind: ErrorKind, offset: usize) -> Result<Integer> {
    let Value::Integer(integer) = value else {
        return Err(Error::new(kind, offset));
    };
    Ok(integer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::Config as ProptestConfig;

    const GOLDEN: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/datalevin/cbor/v1/golden-vectors.tsv"
    ));
    const MALFORMED: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/datalevin/cbor/v1/malformed-vectors.tsv"
    ));

    fn arbitrary_big_magnitude() -> BoxedStrategy<Vec<u8>> {
        (1_u8..=u8::MAX, prop::collection::vec(any::<u8>(), 8..32))
            .prop_map(|(first, mut rest)| {
                let mut magnitude = Vec::with_capacity(rest.len() + 1);
                magnitude.push(first);
                magnitude.append(&mut rest);
                magnitude
            })
            .boxed()
    }

    fn arbitrary_integer() -> BoxedStrategy<Integer> {
        let boundaries = prop::sample::select(vec![
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
        ]);
        prop_oneof![
            8 => any::<i64>().prop_map(Integer::I64),
            2 => boundaries.prop_map(Integer::I64),
            1 => arbitrary_big_magnitude().prop_map(Integer::PositiveBig),
            1 => arbitrary_big_magnitude().prop_map(Integer::NegativeBig),
        ]
        .boxed()
    }

    fn arbitrary_text() -> BoxedStrategy<String> {
        prop::collection::vec(any::<char>(), 0..32)
            .prop_map(|characters| characters.into_iter().collect())
            .boxed()
    }

    fn arbitrary_decimal() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(Value::Decimal {
                exponent: 0,
                mantissa: Integer::I64(0),
            }),
            (any::<i64>(), -32_i32..=32).prop_map(|(mantissa, exponent)| Value::Decimal {
                exponent,
                // Odd mantissas are already normalized because they cannot be
                // divisible by ten.
                mantissa: Integer::I64(mantissa | 1),
            }),
        ]
        .boxed()
    }

    fn gcd(mut left: u64, mut right: u64) -> u64 {
        while right != 0 {
            (left, right) = (right, left % right);
        }
        left
    }

    fn arbitrary_ratio() -> BoxedStrategy<Value> {
        (-1_000_000_i64..=1_000_000, 1_i64..=1_000_000)
            .prop_map(|(numerator, denominator)| {
                if numerator == 0 {
                    Value::Ratio {
                        numerator: Integer::I64(0),
                        denominator: Integer::I64(1),
                    }
                } else {
                    let divisor = gcd(numerator.unsigned_abs(), denominator as u64) as i64;
                    Value::Ratio {
                        numerator: Integer::I64(numerator / divisor),
                        denominator: Integer::I64(denominator / divisor),
                    }
                }
            })
            .boxed()
    }

    fn arbitrary_uri() -> BoxedStrategy<String> {
        prop::collection::vec(0_u8..62, 0..32)
            .prop_map(|suffix| {
                let suffix: String = suffix
                    .into_iter()
                    .map(|value| match value {
                        0..=9 => char::from(b'0' + value),
                        10..=35 => char::from(b'a' + value - 10),
                        _ => char::from(b'A' + value - 36),
                    })
                    .collect();
                format!("https://example.test/{suffix}")
            })
            .boxed()
    }

    fn arbitrary_map_key() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            arbitrary_integer().prop_map(Value::Integer),
            arbitrary_text().prop_map(Value::Text),
            prop::collection::vec(any::<u8>(), 0..32).prop_map(Value::Bytes),
            any::<[u8; 16]>().prop_map(Value::Uuid),
            arbitrary_uri().prop_map(Value::Uri),
        ]
        .boxed()
    }

    fn deduplicate_values(values: Vec<Value>) -> Vec<Value> {
        let mut seen = HashSet::new();
        values
            .into_iter()
            .filter(|value| seen.insert(encode(value, Mode::Canonical).unwrap()))
            .collect()
    }

    fn deduplicate_entries(entries: Vec<(Value, Value)>) -> Vec<(Value, Value)> {
        let mut seen = HashSet::new();
        entries
            .into_iter()
            .filter(|(key, _)| seen.insert(encode(key, Mode::Canonical).unwrap()))
            .collect()
    }

    fn arbitrary_value() -> BoxedStrategy<Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            arbitrary_integer().prop_map(Value::Integer),
            any::<u32>().prop_map(|bits| Value::Float32(f32::from_bits(bits))),
            any::<u64>().prop_map(|bits| Value::Float64(f64::from_bits(bits))),
            prop::collection::vec(any::<u8>(), 0..64).prop_map(Value::Bytes),
            arbitrary_text().prop_map(Value::Text),
            arbitrary_decimal(),
            arbitrary_ratio(),
            arbitrary_uri().prop_map(Value::Uri),
            any::<[u8; 16]>().prop_map(Value::Uuid),
        ];

        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::vec((arbitrary_map_key(), inner.clone()), 0..6)
                    .prop_map(deduplicate_entries)
                    .prop_map(Value::Map),
                prop::collection::vec(inner, 0..8)
                    .prop_map(deduplicate_values)
                    .prop_map(Value::Set),
            ]
        })
        .boxed()
    }

    fn arbitrary_map_entries() -> BoxedStrategy<Vec<(Value, Value)>> {
        prop::collection::vec((arbitrary_map_key(), arbitrary_value()), 0..12)
            .prop_map(deduplicate_entries)
            .boxed()
    }

    fn arbitrary_set_values() -> BoxedStrategy<Vec<Value>> {
        prop::collection::vec(arbitrary_value(), 0..16)
            .prop_map(deduplicate_values)
            .boxed()
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 500,
            max_shrink_iters: 10_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn canonical_roundtrip_and_output_equivalence(value in arbitrary_value()) {
            let encoded = encode(&value, Mode::Canonical).unwrap();
            prop_assert_eq!(encoded.len(), encoded_len(&value, Mode::Canonical).unwrap());

            let mut output = vec![0; encoded.len()];
            let written = encode_into(&value, Mode::Canonical, &mut output).unwrap();
            prop_assert_eq!(encoded.len(), written);
            prop_assert_eq!(&encoded, &output);

            let mut short_output = vec![0; encoded.len() - 1];
            prop_assert_eq!(
                ErrorKind::OutputTooSmall,
                encode_into(&value, Mode::Canonical, &mut short_output)
                    .unwrap_err()
                    .kind
            );

            let decoded = decode(&encoded, true).unwrap();
            prop_assert_eq!(&encoded, &encode(&decoded, Mode::Canonical).unwrap());

            let storage = encode_storage(&value, Mode::Canonical).unwrap();
            let decoded_storage = decode_storage(&storage, true).unwrap();
            prop_assert_eq!(
                &encoded,
                &encode(&decoded_storage, Mode::Canonical).unwrap()
            );
        }

        #[test]
        fn fast_mode_is_portable(value in arbitrary_value()) {
            let canonical = encode(&value, Mode::Canonical).unwrap();
            let fast = encode(&value, Mode::Fast).unwrap();
            let decoded = decode(&fast, false).unwrap();
            prop_assert_eq!(canonical, encode(&decoded, Mode::Canonical).unwrap());
        }

        #[test]
        fn canonical_collection_encoding_ignores_input_order(
            entries in arbitrary_map_entries(),
            values in arbitrary_set_values(),
        ) {
            let mut reversed_entries = entries.clone();
            reversed_entries.reverse();
            prop_assert_eq!(
                encode(&Value::Map(entries), Mode::Canonical).unwrap(),
                encode(&Value::Map(reversed_entries), Mode::Canonical).unwrap()
            );

            let mut reversed_values = values.clone();
            reversed_values.reverse();
            prop_assert_eq!(
                encode(&Value::Set(values), Mode::Canonical).unwrap(),
                encode(&Value::Set(reversed_values), Mode::Canonical).unwrap()
            );
        }

        #[test]
        fn every_proper_prefix_is_truncated_and_suffix_is_trailing(
            value in arbitrary_value(),
            cut_selector in any::<usize>(),
            trailing_byte in any::<u8>(),
        ) {
            let encoded = encode(&value, Mode::Canonical).unwrap();
            let cut = cut_selector % encoded.len();
            prop_assert_eq!(
                ErrorKind::Truncated,
                decode(&encoded[..cut], true).unwrap_err().kind
            );

            let mut with_trailing = encoded;
            with_trailing.push(trailing_byte);
            prop_assert_eq!(
                ErrorKind::TrailingBytes,
                decode(&with_trailing, true).unwrap_err().kind
            );
        }

        #[test]
        fn arbitrary_input_never_panics(input in prop::collection::vec(any::<u8>(), 0..128)) {
            let limits = Limits {
                max_input_bytes: 128,
                max_depth: 16,
                max_collection_len: 64,
                max_string_bytes: 128,
                max_bignum_bytes: 128,
            };
            let result = std::panic::catch_unwind(|| decode_with_limits(&input, true, limits));
            prop_assert!(result.is_ok(), "decoder panicked for input {input:02x?}");
        }
    }

    #[test]
    fn shared_golden_corpus_round_trips() {
        let mut count = 0;
        for line in GOLDEN
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(8, columns.len(), "invalid fixture row: {line}");
            let id = columns[0];
            let expected = hex_decode(columns[4]);
            let expected_storage = hex_decode(columns[5]);
            let value = fixture_value(id);

            assert_eq!(expected, encode(&value, Mode::Canonical).unwrap(), "{id}");
            assert_eq!(
                expected.len(),
                encoded_len(&value, Mode::Canonical).unwrap(),
                "{id}"
            );

            let mut output = vec![0; expected.len()];
            assert_eq!(
                expected.len(),
                encode_into(&value, Mode::Canonical, &mut output).unwrap(),
                "{id}"
            );
            assert_eq!(expected, output, "{id}");

            let decoded = decode(&expected, true).unwrap();
            assert_value_eq(&value, &decoded, id);
            assert_eq!(expected, encode(&decoded, Mode::Canonical).unwrap(), "{id}");

            assert_eq!(
                expected_storage,
                encode_storage(&value, Mode::Canonical).unwrap(),
                "{id}"
            );
            let decoded_storage = decode_storage(&expected_storage, true).unwrap();
            assert_value_eq(&value, &decoded_storage, id);
            count += 1;
        }
        assert_eq!(45, count);
    }

    #[test]
    fn shared_malformed_corpus_has_exact_error_agreement() {
        let mut count = 0;
        for line in MALFORMED
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(5, columns.len(), "invalid malformed fixture row: {line}");
            let [id, operation, hex, expected, _note]: [&str; 5] = columns.try_into().unwrap();
            let input = hex_decode(hex);
            let error = match operation {
                "canonical" => decode(&input, true),
                "fast" => decode(&input, false),
                "storage" => decode_storage(&input, true),
                "limit-input" | "limit-depth" | "limit-collection" | "limit-string"
                | "limit-bignum" => decode_with_limits(&input, true, malformed_limits(operation)),
                _ => panic!("unknown malformed fixture operation {operation}: {id}"),
            }
            .expect_err(id);
            assert_eq!(expected, error.kind.code(), "{id}");
            count += 1;
        }
        assert_eq!(53, count);
    }

    fn malformed_limits(operation: &str) -> Limits {
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
            _ => unreachable!(),
        }
        limits
    }

    #[test]
    fn rejects_noncanonical_and_malformed_input() {
        assert_eq!(
            ErrorKind::NonShortest,
            decode(&[0x18, 0x17], true).unwrap_err().kind
        );
        assert_eq!(
            ErrorKind::TrailingBytes,
            decode(&[0x00, 0x00], true).unwrap_err().kind
        );
        assert_eq!(
            ErrorKind::IndefiniteLength,
            decode(&[0x9f, 0xff], true).unwrap_err().kind
        );
        assert_eq!(
            ErrorKind::InvalidUtf8,
            decode(&[0x61, 0xff], true).unwrap_err().kind
        );
        assert_eq!(
            ErrorKind::NonCanonical,
            decode(&hex_decode("a262616101616202"), true)
                .unwrap_err()
                .kind
        );
        assert_eq!(
            ErrorKind::DuplicateKey,
            decode(&hex_decode("a201000101"), true).unwrap_err().kind
        );
        assert_eq!(
            ErrorKind::UnescapedTypedHeader,
            decode_storage(&[0xf6], true).unwrap_err().kind
        );
        assert_eq!(
            ErrorKind::UnnecessaryStorageEscape,
            decode_storage(&[0xff, 0x00], true).unwrap_err().kind
        );
        let mut limits = Limits::default();
        limits.max_string_bytes = 2;
        assert_eq!(
            ErrorKind::StringLimit,
            decode_storage_with_limits(&[0x63, b'a', b'b', b'c'], true, limits)
                .unwrap_err()
                .kind
        );
    }

    #[test]
    fn validates_arbitrary_precision_decimal_and_ratio_normalization() {
        let ten_quintillion = vec![0x8a, 0xc7, 0x23, 0x04, 0x89, 0xe8, 0x00, 0x00];
        let ten_quintillion_minus_one = vec![0x8a, 0xc7, 0x23, 0x04, 0x89, 0xe7, 0xff, 0xff];

        for mantissa in [
            Integer::PositiveBig(ten_quintillion),
            Integer::NegativeBig(ten_quintillion_minus_one),
        ] {
            let decimal = Value::Decimal {
                exponent: 0,
                mantissa,
            };
            assert_eq!(
                ErrorKind::InvalidDecimal,
                encode(&decimal, Mode::Canonical).unwrap_err().kind
            );
        }

        let two_to_64 =
            Integer::PositiveBig(vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let negative_two_to_64 = Integer::NegativeBig(vec![0xff; 8]);
        for numerator in [two_to_64.clone(), negative_two_to_64] {
            let ratio = Value::Ratio {
                numerator,
                denominator: Integer::I64(2),
            };
            assert_eq!(
                ErrorKind::InvalidRatio,
                encode(&ratio, Mode::Canonical).unwrap_err().kind
            );
        }

        let coprime = Value::Ratio {
            numerator: Integer::PositiveBig(vec![
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            ]),
            denominator: Integer::I64(2),
        };
        assert!(encode(&coprime, Mode::Canonical).is_ok());

        let zero_over_big = Value::Ratio {
            numerator: Integer::I64(0),
            denominator: two_to_64,
        };
        assert_eq!(
            ErrorKind::InvalidRatio,
            encode(&zero_over_big, Mode::Canonical).unwrap_err().kind
        );
    }

    #[test]
    fn fast_mode_retains_collection_order_but_decodes_portably() {
        let map = Value::Map(vec![
            (Value::Text("aa".into()), Value::int(2)),
            (Value::Text("b".into()), Value::int(1)),
        ]);
        let fast = encode(&map, Mode::Fast).unwrap();
        assert_eq!(hex_decode("a262616102616201"), fast);
        assert_value_eq(&map, &decode(&fast, false).unwrap(), "fast-map");
        assert_eq!(
            hex_decode("a261620162616102"),
            encode(&decode(&fast, false).unwrap(), Mode::Canonical).unwrap()
        );
    }

    fn fixture_value(id: &str) -> Value {
        match id {
            "null" => Value::Null,
            "false" => Value::Bool(false),
            "true" => Value::Bool(true),
            "uint-0" => Value::int(0),
            "uint-23" => Value::int(23),
            "uint-24" => Value::int(24),
            "uint-255" => Value::int(255),
            "uint-256" => Value::int(256),
            "uint-65535" => Value::int(65_535),
            "uint-65536" => Value::int(65_536),
            "uint-u32-max" => Value::int(4_294_967_295),
            "uint-u32-plus-1" => Value::int(4_294_967_296),
            "int64-max" => Value::int(i64::MAX),
            "nint-1" => Value::int(-1),
            "nint-24" => Value::int(-24),
            "nint-25" => Value::int(-25),
            "int64-min" => Value::int(i64::MIN),
            "bigint-positive" => {
                Value::Integer(Integer::PositiveBig(vec![0x80, 0, 0, 0, 0, 0, 0, 0]))
            }
            "bigint-negative" => {
                Value::Integer(Integer::NegativeBig(vec![0x80, 0, 0, 0, 0, 0, 0, 0]))
            }
            "float32-one-half" => Value::Float32(1.5),
            "float32-negative-zero" => Value::Float32(-0.0),
            "float32-positive-infinity" => Value::Float32(f32::INFINITY),
            "float32-nan" => Value::Float32(f32::NAN),
            "float64-one-half" => Value::Float64(1.5),
            "float64-negative-zero" => Value::Float64(-0.0),
            "float64-negative-infinity" => Value::Float64(f64::NEG_INFINITY),
            "float64-nan" => Value::Float64(f64::NAN),
            "bytes-empty" => Value::Bytes(vec![]),
            "bytes-two" => Value::Bytes(vec![0x00, 0xff]),
            "text-empty" => Value::Text(String::new()),
            "text-ascii" => Value::Text("hello".into()),
            "text-lambda" => Value::Text("λ".into()),
            "text-emoji" => Value::Text("😀".into()),
            "vector-empty" => Value::Array(vec![]),
            "vector-mixed" => {
                Value::Array(vec![Value::int(1), Value::int(-1), Value::Text("a".into())])
            }
            "map-empty" => Value::Map(vec![]),
            "map-length-first" => Value::Map(vec![
                (Value::Text("aa".into()), Value::int(2)),
                (Value::Text("b".into()), Value::int(1)),
            ]),
            "map-arbitrary-keys" => Value::Map(vec![
                (Value::Text("a".into()), Value::Text("A".into())),
                (Value::int(10), Value::Text("ten".into())),
            ]),
            "set-mixed" => Value::Set(vec![
                Value::Text("a".into()),
                Value::int(-1),
                Value::int(10),
            ]),
            "decimal-zero" => Value::Decimal {
                exponent: 0,
                mantissa: 0.into(),
            },
            "decimal-one-half" => Value::Decimal {
                exponent: -1,
                mantissa: 15.into(),
            },
            "decimal-thousand" => Value::Decimal {
                exponent: 3,
                mantissa: 1.into(),
            },
            "ratio-one-third" => Value::Ratio {
                numerator: 1.into(),
                denominator: 3.into(),
            },
            "uri-https" => Value::Uri("https://example.com".into()),
            "uuid-sequence" => Value::Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
            _ => panic!("fixture has no Rust value: {id}"),
        }
    }

    fn assert_value_eq(expected: &Value, actual: &Value, id: &str) {
        match (expected, actual) {
            (Value::Float32(left), Value::Float32(right)) if left.is_nan() && right.is_nan() => {}
            (Value::Float64(left), Value::Float64(right)) if left.is_nan() && right.is_nan() => {}
            (Value::Map(_), Value::Map(_)) | (Value::Set(_), Value::Set(_)) => assert_eq!(
                encode(expected, Mode::Canonical).unwrap(),
                encode(actual, Mode::Canonical).unwrap(),
                "{id}"
            ),
            _ => assert_eq!(expected, actual, "{id}"),
        }
    }

    fn hex_decode(value: &str) -> Vec<u8> {
        assert_eq!(0, value.len() % 2, "odd hex length: {value}");
        let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        pairs
            .iter()
            .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
            .collect()
    }

    fn hex_nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            b'A'..=b'F' => value - b'A' + 10,
            _ => panic!("invalid hex digit: {}", value as char),
        }
    }
}
