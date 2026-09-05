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
const TAG_UINT16_LE_ARRAY: u64 = 69;
const TAG_SINT16_LE_ARRAY: u64 = 77;
const TAG_SINT32_LE_ARRAY: u64 = 78;
const TAG_SINT64_LE_ARRAY: u64 = 79;
const TAG_FLOAT32_LE_ARRAY: u64 = 85;
const TAG_FLOAT64_LE_ARRAY: u64 = 86;
const TAG_SET: u64 = 258;
const TAG_EXTENDED_TIME: u64 = 1001;

/// Non-durable Phase 0 stand-in for the not-yet-assigned Datalevin extension
/// tag. Draft fixtures use the same mnemonic `0x444c` value as the JVM codec.
pub const DRAFT_EXTENSION_TAG: u64 = 0x444c;

const EXT_KEYWORD: u64 = 1;
const EXT_SYMBOL: u64 = 2;
const EXT_CHARACTER: u64 = 3;
const EXT_LIST: u64 = 4;
const EXT_QUEUE: u64 = 5;
const EXT_REGEX: u64 = 6;

// Byte-string subtypes. FB/FC/FE reuse Datalevin's typed-data headers.
const SUBTYPE_QUALIFIED_KEYWORD: u8 = 0xe0;
const SUBTYPE_QUALIFIED_SYMBOL: u8 = 0xe1;
const SUBTYPE_CHARACTER: u8 = 0xe2;
const SUBTYPE_JAVA_REGEX: u8 = 0xe3;
const SUBTYPE_KEYWORD: u8 = 0xfb;
const SUBTYPE_SYMBOL: u8 = 0xfc;
const SUBTYPE_BYTES: u8 = 0xfe;

const REGEX_UNICODE_CASE: u16 = 0x0040;
const REGEX_UNICODE_CHARACTER_CLASS: u16 = 0x0100;
const REGEX_FLAGS: u16 = 0x017f;

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

/// Identifier for a neutral, non-executable Datalevin extension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtensionId {
    Integer(u64),
    Name(String),
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
    InstantMillis(i64),
    Uint16Array(Vec<u16>),
    Int16Array(Vec<i16>),
    Int32Array(Vec<i32>),
    Int64Array(Vec<i64>),
    Float32Array(Vec<f32>),
    Float64Array(Vec<f64>),
    Keyword {
        namespace: Option<String>,
        name: String,
    },
    Symbol {
        namespace: Option<String>,
        name: String,
    },
    Character(u16),
    List(Vec<Value>),
    Queue(Vec<Value>),
    Regex {
        source: String,
        flags: u16,
    },
    Extension {
        type_id: ExtensionId,
        arguments: Vec<Value>,
    },
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
    pub max_extension_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 * 1024 * 1024,
            max_depth: 256,
            max_collection_len: 1_000_000,
            max_string_bytes: 16 * 1024 * 1024,
            max_bignum_bytes: 4 * 1024,
            max_extension_bytes: 16 * 1024 * 1024,
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
    InvalidRegex,
    InvalidUuid,
    InvalidInstant,
    InvalidTypedArray,
    InvalidExtension,
    ExtensionLimit,
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
            Self::InvalidRegex => "INVALID_REGEX",
            Self::InvalidUuid => "INVALID_UUID",
            Self::InvalidInstant => "INVALID_INSTANT",
            Self::InvalidTypedArray => "INVALID_TYPED_ARRAY",
            Self::InvalidExtension => "INVALID_EXTENSION",
            Self::ExtensionLimit => "EXTENSION_LIMIT",
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
        active_limit: None,
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
            encode_head(writer, 2, bytes.len() as u64 + 1)?;
            writer.put_u8(SUBTYPE_BYTES)?;
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
        Value::InstantMillis(milliseconds) => encode_instant(writer, *milliseconds),
        Value::Uint16Array(values) => {
            encode_typed_array_head(writer, TAG_UINT16_LE_ARRAY, values.len(), 2)?;
            for value in values {
                writer.put(&value.to_le_bytes())?;
            }
            Ok(())
        }
        Value::Int16Array(values) => {
            encode_typed_array_head(writer, TAG_SINT16_LE_ARRAY, values.len(), 2)?;
            for value in values {
                writer.put(&value.to_le_bytes())?;
            }
            Ok(())
        }
        Value::Int32Array(values) => {
            encode_typed_array_head(writer, TAG_SINT32_LE_ARRAY, values.len(), 4)?;
            for value in values {
                writer.put(&value.to_le_bytes())?;
            }
            Ok(())
        }
        Value::Int64Array(values) => {
            encode_typed_array_head(writer, TAG_SINT64_LE_ARRAY, values.len(), 8)?;
            for value in values {
                writer.put(&value.to_le_bytes())?;
            }
            Ok(())
        }
        Value::Float32Array(values) => {
            encode_typed_array_head(writer, TAG_FLOAT32_LE_ARRAY, values.len(), 4)?;
            for value in values {
                let bits = if value.is_nan() {
                    CANONICAL_FLOAT32_NAN
                } else {
                    value.to_bits()
                };
                writer.put(&bits.to_le_bytes())?;
            }
            Ok(())
        }
        Value::Float64Array(values) => {
            encode_typed_array_head(writer, TAG_FLOAT64_LE_ARRAY, values.len(), 8)?;
            for value in values {
                let bits = if value.is_nan() {
                    CANONICAL_FLOAT64_NAN
                } else {
                    value.to_bits()
                };
                writer.put(&bits.to_le_bytes())?;
            }
            Ok(())
        }
        Value::Keyword { namespace, name } => encode_named_value(
            writer,
            SUBTYPE_KEYWORD,
            SUBTYPE_QUALIFIED_KEYWORD,
            namespace.as_deref(),
            name,
        ),
        Value::Symbol { namespace, name } => encode_named_value(
            writer,
            SUBTYPE_SYMBOL,
            SUBTYPE_QUALIFIED_SYMBOL,
            namespace.as_deref(),
            name,
        ),
        Value::Character(value) => {
            encode_head(writer, 2, if *value <= 0xff { 2 } else { 3 })?;
            writer.put_u8(SUBTYPE_CHARACTER)?;
            if *value > 0xff {
                writer.put_u8((value >> 8) as u8)?;
            }
            writer.put_u8(*value as u8)
        }
        Value::List(values) => encode_sequence_extension(writer, EXT_LIST, values, mode),
        Value::Queue(values) => encode_sequence_extension(writer, EXT_QUEUE, values, mode),
        Value::Regex { source, flags } => {
            validate_regex(source, *flags, writer.position())?;
            let length = 1 + unsigned_varint_size(usize::from(*flags)) + source.len() as u64;
            encode_head(writer, 2, length)?;
            writer.put_u8(SUBTYPE_JAVA_REGEX)?;
            encode_unsigned_varint(writer, usize::from(*flags))?;
            writer.put(source.as_bytes())
        }
        Value::Extension { type_id, arguments } => {
            let item_count = arguments
                .len()
                .checked_add(1)
                .ok_or_else(|| Error::new(ErrorKind::LengthOutOfRange, writer.position()))?;
            encode_extension_head(writer, item_count)?;
            encode_extension_id(writer, type_id)?;
            for argument in arguments {
                encode_value(writer, argument, mode)?;
            }
            Ok(())
        }
        Value::Tagged { tag, value } => {
            if *tag == DRAFT_EXTENSION_TAG {
                return Err(Error::new(ErrorKind::InvalidExtension, writer.position()));
            }
            encode_head(writer, 6, *tag)?;
            if (matches!(*tag, TAG_POSITIVE_BIGNUM | TAG_NEGATIVE_BIGNUM | TAG_UUID)
                || typed_array_width(*tag).is_some())
                && let Value::Bytes(bytes) = value.as_ref()
            {
                encode_head(writer, 2, bytes.len() as u64)?;
                return writer.put(bytes);
            }
            encode_value(writer, value, mode)
        }
    }
}

fn encode_instant<W: Writer>(writer: &mut W, milliseconds: i64) -> Result<()> {
    let seconds = milliseconds.div_euclid(1000);
    let remainder = milliseconds.rem_euclid(1000);
    encode_head(writer, 6, TAG_EXTENDED_TIME)?;
    encode_head(writer, 5, if remainder == 0 { 1 } else { 2 })?;
    encode_i64(writer, 1)?;
    encode_i64(writer, seconds)?;
    if remainder != 0 {
        encode_i64(writer, -3)?;
        encode_i64(writer, remainder)?;
    }
    Ok(())
}

fn encode_extension_head<W: Writer>(writer: &mut W, item_count: usize) -> Result<()> {
    encode_head(writer, 6, DRAFT_EXTENSION_TAG)?;
    encode_head(writer, 4, item_count as u64)
}

fn encode_extension_id<W: Writer>(writer: &mut W, type_id: &ExtensionId) -> Result<()> {
    match type_id {
        ExtensionId::Integer(type_id) => {
            if *type_id > i64::MAX as u64
                || matches!(
                    *type_id,
                    EXT_KEYWORD | EXT_SYMBOL | EXT_CHARACTER | EXT_REGEX
                )
            {
                return Err(Error::new(ErrorKind::InvalidExtension, writer.position()));
            }
            encode_head(writer, 0, *type_id)
        }
        ExtensionId::Name(type_id) => {
            if type_id.is_empty() {
                return Err(Error::new(ErrorKind::InvalidExtension, writer.position()));
            }
            validate_unicode(type_id)?;
            encode_head(writer, 3, type_id.len() as u64)?;
            writer.put(type_id.as_bytes())
        }
    }
}

fn encode_named_value<W: Writer>(
    writer: &mut W,
    subtype: u8,
    qualified_subtype: u8,
    namespace: Option<&str>,
    name: &str,
) -> Result<()> {
    validate_unicode(name)?;
    let mut length = 1 + name.len() as u64;
    if let Some(namespace) = namespace {
        validate_unicode(namespace)?;
        if namespace.len() > i32::MAX as usize {
            return Err(Error::new(ErrorKind::LengthOutOfRange, writer.position()));
        }
        length += namespace.len() as u64 + unsigned_varint_size(namespace.len());
    }
    encode_head(writer, 2, length)?;
    writer.put_u8(if namespace.is_some() {
        qualified_subtype
    } else {
        subtype
    })?;
    if let Some(namespace) = namespace {
        encode_unsigned_varint(writer, namespace.len())?;
        writer.put(namespace.as_bytes())?;
    }
    writer.put(name.as_bytes())
}

fn unsigned_varint_size(mut value: usize) -> u64 {
    let mut length = 1;
    while value >= 128 {
        length += 1;
        value >>= 7;
    }
    length
}

fn encode_unsigned_varint<W: Writer>(writer: &mut W, mut value: usize) -> Result<()> {
    while value >= 128 {
        writer.put_u8((value as u8 & 0x7f) | 0x80)?;
        value >>= 7;
    }
    writer.put_u8(value as u8)
}

fn decode_unsigned_varint(
    input: &[u8],
    offset: usize,
    missing_kind: ErrorKind,
    overflow_kind: ErrorKind,
) -> Result<(usize, usize)> {
    let mut value = 0;
    for (index, shift) in (0..=28).step_by(7).enumerate() {
        let Some(&next) = input.get(index) else {
            return Err(Error::new(missing_kind, offset));
        };
        if shift == 28 && next > 7 {
            return Err(Error::new(overflow_kind, offset));
        }
        value |= usize::from(next & 0x7f) << shift;
        if next & 0x80 == 0 {
            if shift != 0 && next == 0 {
                return Err(Error::new(ErrorKind::NonShortest, offset));
            }
            return Ok((value, index + 1));
        }
    }
    unreachable!("bounded unsigned varint")
}

fn encode_sequence_extension<W: Writer>(
    writer: &mut W,
    type_id: u64,
    values: &[Value],
    mode: Mode,
) -> Result<()> {
    let item_count = values
        .len()
        .checked_add(1)
        .ok_or_else(|| Error::new(ErrorKind::LengthOutOfRange, writer.position()))?;
    encode_extension_head(writer, item_count)?;
    encode_head(writer, 0, type_id)?;
    for value in values {
        encode_value(writer, value, mode)?;
    }
    Ok(())
}

fn validate_regex(source: &str, flags: u16, offset: usize) -> Result<()> {
    validate_unicode(source)?;
    validate_regex_flags(flags, offset)
}

fn validate_regex_flags(flags: u16, offset: usize) -> Result<()> {
    if flags & !REGEX_FLAGS != 0
        || flags & REGEX_UNICODE_CHARACTER_CLASS != 0 && flags & REGEX_UNICODE_CASE == 0
    {
        return Err(Error::new(ErrorKind::InvalidRegex, offset));
    }
    Ok(())
}

fn encode_typed_array_head<W: Writer>(
    writer: &mut W,
    tag: u64,
    element_count: usize,
    width: usize,
) -> Result<()> {
    let byte_length = element_count
        .checked_mul(width)
        .ok_or_else(|| Error::new(ErrorKind::LengthOutOfRange, writer.position()))?;
    encode_head(writer, 6, tag)?;
    encode_head(writer, 2, byte_length as u64)
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
    active_limit: Option<(usize, ErrorKind)>,
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
            2 => self.byte_string_value(additional, head_offset),
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
                if matches!(tag, TAG_POSITIVE_BIGNUM | TAG_NEGATIVE_BIGNUM | TAG_UUID) {
                    return self.raw_byte_tag(tag, head_offset, depth + 1);
                }
                if tag == TAG_SET {
                    return self.set(head_offset, depth + 1);
                }
                if typed_array_width(tag).is_some() {
                    return self.typed_array(tag, head_offset);
                }
                if tag == DRAFT_EXTENSION_TAG {
                    return self.extension_with_limit(head_offset, depth + 1);
                }
                let value = self.value(depth + 1)?;
                self.tagged(tag, value, head_offset)
            }
            7 => self.simple(additional, head_offset),
            _ => unreachable!(),
        }
    }

    fn byte_string_value(&mut self, additional: u8, head_offset: usize) -> Result<Value> {
        let length = self.length(additional, head_offset, self.limits.max_string_bytes)?;
        let payload_offset = self.position;
        let extension_limit = self.limits.max_extension_bytes;
        let payload = self.read(length)?;
        let Some((&subtype, body)) = payload.split_first() else {
            return Err(Error::new(ErrorKind::InvalidExtension, head_offset));
        };
        if subtype == SUBTYPE_BYTES {
            return Ok(Value::Bytes(body.to_vec()));
        }
        if !matches!(
            subtype,
            SUBTYPE_KEYWORD
                | SUBTYPE_SYMBOL
                | SUBTYPE_QUALIFIED_KEYWORD
                | SUBTYPE_QUALIFIED_SYMBOL
                | SUBTYPE_CHARACTER
                | SUBTYPE_JAVA_REGEX
        ) {
            return Err(Error::new(ErrorKind::InvalidExtension, head_offset));
        }
        if length > extension_limit {
            return Err(Error::new(ErrorKind::ExtensionLimit, head_offset));
        }
        if subtype == SUBTYPE_CHARACTER {
            let code_unit = match *body {
                [value] => u16::from(value),
                [0, _] => return Err(Error::new(ErrorKind::NonShortest, head_offset)),
                [high, low] => u16::from_be_bytes([high, low]),
                _ => return Err(Error::new(ErrorKind::InvalidExtension, head_offset)),
            };
            return Ok(Value::Character(code_unit));
        }
        if subtype == SUBTYPE_JAVA_REGEX {
            let (flags, width) = decode_unsigned_varint(
                body,
                head_offset,
                ErrorKind::InvalidRegex,
                ErrorKind::InvalidRegex,
            )?;
            let flags = u16::try_from(flags)
                .map_err(|_| Error::new(ErrorKind::InvalidRegex, head_offset))?;
            validate_regex_flags(flags, head_offset)?;
            let source = std::str::from_utf8(&body[width..])
                .map_err(|_| Error::new(ErrorKind::InvalidUtf8, payload_offset + 1 + width))?;
            return Ok(Value::Regex {
                source: source.to_owned(),
                flags,
            });
        }
        let mut position = 1;
        let namespace = if matches!(
            subtype,
            SUBTYPE_QUALIFIED_KEYWORD | SUBTYPE_QUALIFIED_SYMBOL
        ) {
            let (namespace_length, width) = decode_unsigned_varint(
                body,
                head_offset,
                ErrorKind::InvalidExtension,
                ErrorKind::LengthOutOfRange,
            )?;
            position += width;
            if namespace_length > payload.len() - position {
                return Err(Error::new(ErrorKind::InvalidExtension, head_offset));
            }
            let namespace = std::str::from_utf8(&payload[position..position + namespace_length])
                .map_err(|_| Error::new(ErrorKind::InvalidUtf8, payload_offset + position))?;
            position += namespace_length;
            Some(namespace.to_owned())
        } else {
            None
        };
        let name = std::str::from_utf8(&payload[position..])
            .map_err(|_| Error::new(ErrorKind::InvalidUtf8, payload_offset + position))?
            .to_owned();
        if matches!(subtype, SUBTYPE_KEYWORD | SUBTYPE_QUALIFIED_KEYWORD) {
            Ok(Value::Keyword { namespace, name })
        } else {
            Ok(Value::Symbol { namespace, name })
        }
    }

    // Standard tags own their byte-string contents: they carry no DL subtype.
    fn raw_byte_tag(&mut self, tag: u64, tag_offset: usize, depth: usize) -> Result<Value> {
        if depth > self.limits.max_depth {
            return Err(Error::new(ErrorKind::DepthLimit, self.position));
        }
        let payload_offset = self.position;
        let head = self.read_u8()?;
        let kind = if tag == TAG_UUID {
            ErrorKind::InvalidUuid
        } else {
            ErrorKind::InvalidBignum
        };
        if head >> 5 != 2 {
            return Err(Error::new(kind, tag_offset));
        }
        let length = if tag == TAG_UUID {
            self.length(head & 0x1f, payload_offset, self.limits.max_string_bytes)?
        } else {
            let length = self.argument(head & 0x1f, tag_offset)?;
            if length > self.limits.max_bignum_bytes as u64 {
                return Err(Error::new(ErrorKind::BignumLimit, tag_offset));
            }
            length as usize
        };
        let bytes = self.read(length)?;
        if tag == TAG_UUID {
            let bytes = bytes.try_into().map_err(|_| Error::new(kind, tag_offset))?;
            return Ok(Value::Uuid(bytes));
        }
        validate_big_magnitude(bytes, tag_offset)?;
        let magnitude = bytes.to_vec();
        Ok(Value::Integer(if tag == TAG_POSITIVE_BIGNUM {
            Integer::PositiveBig(magnitude)
        } else {
            Integer::NegativeBig(magnitude)
        }))
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

    fn extension_with_limit(&mut self, tag_offset: usize, depth: usize) -> Result<Value> {
        let previous_limit = self.active_limit;
        let candidate = self
            .position
            .saturating_add(self.limits.max_extension_bytes);
        if previous_limit.is_none_or(|(limit, _)| candidate < limit) {
            self.active_limit = Some((candidate, ErrorKind::ExtensionLimit));
        }
        let result = self.extension(tag_offset, depth);
        self.active_limit = previous_limit;
        result
    }

    fn extension(&mut self, tag_offset: usize, depth: usize) -> Result<Value> {
        if depth > self.limits.max_depth {
            return Err(Error::new(ErrorKind::DepthLimit, self.position));
        }
        let array_offset = self.position;
        let array_head = self.read_u8()?;
        if array_head >> 5 != 4 {
            return Err(Error::new(ErrorKind::InvalidExtension, tag_offset));
        }
        let length = self.length(
            array_head & 0x1f,
            array_offset,
            self.limits.max_collection_len,
        )?;
        if length == 0 {
            return Err(Error::new(ErrorKind::InvalidExtension, tag_offset));
        }
        self.ensure_extension_slots(length)?;
        let raw_type_id = self.value(depth + 1)?;
        let type_id = match raw_type_id {
            Value::Integer(Integer::I64(value)) if value >= 0 => ExtensionId::Integer(value as u64),
            Value::Text(value) if !value.is_empty() => ExtensionId::Name(value),
            _ => return Err(Error::new(ErrorKind::InvalidExtension, tag_offset)),
        };
        let argument_count = length - 1;
        match type_id {
            ExtensionId::Integer(EXT_KEYWORD | EXT_SYMBOL | EXT_CHARACTER | EXT_REGEX) => {
                Err(Error::new(ErrorKind::InvalidExtension, tag_offset))
            }
            ExtensionId::Integer(EXT_LIST) => Ok(Value::List(
                self.extension_arguments(argument_count, depth)?,
            )),
            ExtensionId::Integer(EXT_QUEUE) => Ok(Value::Queue(
                self.extension_arguments(argument_count, depth)?,
            )),
            type_id => Ok(Value::Extension {
                type_id,
                arguments: self.extension_arguments(argument_count, depth)?,
            }),
        }
    }

    fn extension_arguments(&mut self, count: usize, depth: usize) -> Result<Vec<Value>> {
        self.ensure_extension_slots(count)?;
        let mut arguments = Vec::with_capacity(count);
        for _ in 0..count {
            arguments.push(self.value(depth + 1)?);
        }
        Ok(arguments)
    }

    fn ensure_extension_slots(&self, count: usize) -> Result<()> {
        if let Some((limit, kind)) = self.active_limit
            && count > limit.saturating_sub(self.position)
        {
            return Err(Error::new(kind, self.position));
        }
        Ok(())
    }

    fn tagged(&mut self, tag: u64, value: Value, offset: usize) -> Result<Value> {
        match tag {
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
            TAG_EXTENDED_TIME => decode_instant(value, offset),
            _ => Ok(Value::Tagged {
                tag,
                value: Box::new(value),
            }),
        }
    }

    fn typed_array(&mut self, tag: u64, tag_offset: usize) -> Result<Value> {
        let payload_offset = self.position;
        let head = self.read_u8()?;
        if head >> 5 != 2 {
            return Err(Error::new(ErrorKind::InvalidTypedArray, tag_offset));
        }
        let byte_length = self.length(head & 0x1f, payload_offset, self.limits.max_string_bytes)?;
        let width = typed_array_width(tag).expect("typed-array tag checked by caller");
        if byte_length % width != 0 {
            return Err(Error::new(ErrorKind::InvalidTypedArray, tag_offset));
        }
        let element_count = byte_length / width;
        if element_count > self.limits.max_collection_len {
            return Err(Error::new(ErrorKind::CollectionLimit, tag_offset));
        }
        let value_offset = self.position;
        let canonical = self.canonical;
        let bytes = self.read(byte_length)?;
        match tag {
            TAG_UINT16_LE_ARRAY => {
                let (chunks, remainder) = bytes.as_chunks::<2>();
                debug_assert!(remainder.is_empty());
                Ok(Value::Uint16Array(
                    chunks.iter().copied().map(u16::from_le_bytes).collect(),
                ))
            }
            TAG_SINT16_LE_ARRAY => {
                let (chunks, remainder) = bytes.as_chunks::<2>();
                debug_assert!(remainder.is_empty());
                Ok(Value::Int16Array(
                    chunks.iter().copied().map(i16::from_le_bytes).collect(),
                ))
            }
            TAG_SINT32_LE_ARRAY => {
                let (chunks, remainder) = bytes.as_chunks::<4>();
                debug_assert!(remainder.is_empty());
                Ok(Value::Int32Array(
                    chunks.iter().copied().map(i32::from_le_bytes).collect(),
                ))
            }
            TAG_SINT64_LE_ARRAY => {
                let (chunks, remainder) = bytes.as_chunks::<8>();
                debug_assert!(remainder.is_empty());
                Ok(Value::Int64Array(
                    chunks.iter().copied().map(i64::from_le_bytes).collect(),
                ))
            }
            TAG_FLOAT32_LE_ARRAY => {
                let mut values = Vec::with_capacity(element_count);
                let (chunks, remainder) = bytes.as_chunks::<4>();
                debug_assert!(remainder.is_empty());
                for (index, chunk) in chunks.iter().copied().enumerate() {
                    let bits = u32::from_le_bytes(chunk);
                    if canonical && f32::from_bits(bits).is_nan() && bits != CANONICAL_FLOAT32_NAN {
                        return Err(Error::new(
                            ErrorKind::NonCanonical,
                            value_offset + index * 4,
                        ));
                    }
                    values.push(f32::from_bits(bits));
                }
                Ok(Value::Float32Array(values))
            }
            TAG_FLOAT64_LE_ARRAY => {
                let mut values = Vec::with_capacity(element_count);
                let (chunks, remainder) = bytes.as_chunks::<8>();
                debug_assert!(remainder.is_empty());
                for (index, chunk) in chunks.iter().copied().enumerate() {
                    let bits = u64::from_le_bytes(chunk);
                    if canonical && f64::from_bits(bits).is_nan() && bits != CANONICAL_FLOAT64_NAN {
                        return Err(Error::new(
                            ErrorKind::NonCanonical,
                            value_offset + index * 8,
                        ));
                    }
                    values.push(f64::from_bits(bits));
                }
                Ok(Value::Float64Array(values))
            }
            _ => unreachable!(),
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
        Ok(self.read(1)?[0])
    }

    fn read(&mut self, length: usize) -> Result<&[u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| Error::new(ErrorKind::LengthOutOfRange, self.position))?;
        if let Some((limit, kind)) = self.active_limit
            && end > limit
        {
            return Err(Error::new(kind, self.position));
        }
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

fn typed_array_width(tag: u64) -> Option<usize> {
    match tag {
        TAG_UINT16_LE_ARRAY | TAG_SINT16_LE_ARRAY => Some(2),
        TAG_SINT32_LE_ARRAY | TAG_FLOAT32_LE_ARRAY => Some(4),
        TAG_SINT64_LE_ARRAY | TAG_FLOAT64_LE_ARRAY => Some(8),
        _ => None,
    }
}

fn decode_instant(value: Value, offset: usize) -> Result<Value> {
    let Value::Map(entries) = value else {
        return Err(Error::new(ErrorKind::InvalidInstant, offset));
    };
    let entry_count = entries.len();
    if !(1..=2).contains(&entry_count) {
        return Err(Error::new(ErrorKind::InvalidInstant, offset));
    }
    let mut seconds = None;
    let mut remainder = None;
    for (key, value) in entries {
        match key {
            Value::Integer(Integer::I64(1)) => {
                let Value::Integer(Integer::I64(value)) = value else {
                    return Err(Error::new(ErrorKind::InvalidInstant, offset));
                };
                seconds = Some(value);
            }
            Value::Integer(Integer::I64(-3)) => {
                let Value::Integer(Integer::I64(value @ 1..=999)) = value else {
                    return Err(Error::new(ErrorKind::InvalidInstant, offset));
                };
                remainder = Some(value);
            }
            _ => return Err(Error::new(ErrorKind::InvalidInstant, offset)),
        }
    }
    let Some(seconds) = seconds else {
        return Err(Error::new(ErrorKind::InvalidInstant, offset));
    };
    if entry_count != 1 + usize::from(remainder.is_some()) {
        return Err(Error::new(ErrorKind::InvalidInstant, offset));
    }
    let milliseconds = i128::from(seconds) * 1000 + i128::from(remainder.unwrap_or(0));
    let milliseconds =
        i64::try_from(milliseconds).map_err(|_| Error::new(ErrorKind::InvalidInstant, offset))?;
    Ok(Value::InstantMillis(milliseconds))
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
    const DRAFT_EXTENSIONS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/datalevin/cbor/v1/draft-extension-vectors.tsv"
    ));
    const DRAFT_EXTENSION_MALFORMED: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/datalevin/cbor/v1/draft-extension-malformed-vectors.tsv"
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

    fn arbitrary_keyword() -> BoxedStrategy<Value> {
        (any::<bool>(), arbitrary_text(), arbitrary_text())
            .prop_map(|(qualified, namespace, name)| Value::Keyword {
                namespace: qualified.then_some(namespace),
                name,
            })
            .boxed()
    }

    fn arbitrary_symbol() -> BoxedStrategy<Value> {
        (any::<bool>(), arbitrary_text(), arbitrary_text())
            .prop_map(|(qualified, namespace, name)| Value::Symbol {
                namespace: qualified.then_some(namespace),
                name,
            })
            .boxed()
    }

    fn arbitrary_regex() -> BoxedStrategy<Value> {
        let source = prop::collection::vec(0_u8..62, 0..32).prop_map(|values| {
            values
                .into_iter()
                .map(|value| match value {
                    0..=9 => char::from(b'0' + value),
                    10..=35 => char::from(b'a' + value - 10),
                    _ => char::from(b'A' + value - 36),
                })
                .collect()
        });
        (
            source,
            prop::sample::select(vec![0_u16, 1, 2, 4, 8, 16, 32, 64, 320, 383]),
        )
            .prop_map(|(source, flags)| Value::Regex { source, flags })
            .boxed()
    }

    fn arbitrary_map_key() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            arbitrary_integer().prop_map(Value::Integer),
            arbitrary_text().prop_map(Value::Text),
            arbitrary_keyword(),
            arbitrary_symbol(),
            any::<u16>().prop_map(Value::Character),
            prop::collection::vec(any::<u8>(), 0..32).prop_map(Value::Bytes),
            any::<[u8; 16]>().prop_map(Value::Uuid),
            arbitrary_uri().prop_map(Value::Uri),
        ]
        .boxed()
    }

    fn arbitrary_extension_id() -> BoxedStrategy<ExtensionId> {
        prop_oneof![
            Just(ExtensionId::Integer(0)),
            (7_u64..=i64::MAX as u64).prop_map(ExtensionId::Integer),
            Just(ExtensionId::Name("org.example/generated".into())),
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
            arbitrary_keyword(),
            arbitrary_symbol(),
            any::<u16>().prop_map(Value::Character),
            arbitrary_regex(),
            arbitrary_decimal(),
            arbitrary_ratio(),
            arbitrary_uri().prop_map(Value::Uri),
            any::<[u8; 16]>().prop_map(Value::Uuid),
            any::<i64>().prop_map(Value::InstantMillis),
            prop::collection::vec(any::<u16>(), 0..32).prop_map(Value::Uint16Array),
            prop::collection::vec(any::<i16>(), 0..32).prop_map(Value::Int16Array),
            prop::collection::vec(any::<i32>(), 0..32).prop_map(Value::Int32Array),
            prop::collection::vec(any::<i64>(), 0..32).prop_map(Value::Int64Array),
            prop::collection::vec(any::<u32>(), 0..32).prop_map(|bits| Value::Float32Array(
                bits.into_iter().map(f32::from_bits).collect()
            )),
            prop::collection::vec(any::<u64>(), 0..32).prop_map(|bits| Value::Float64Array(
                bits.into_iter().map(f64::from_bits).collect()
            )),
        ];

        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::List),
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Queue),
                prop::collection::vec((arbitrary_map_key(), inner.clone()), 0..6)
                    .prop_map(deduplicate_entries)
                    .prop_map(Value::Map),
                prop::collection::vec(inner.clone(), 0..8)
                    .prop_map(deduplicate_values)
                    .prop_map(Value::Set),
                (arbitrary_extension_id(), prop::collection::vec(inner, 0..4))
                    .prop_map(|(type_id, arguments)| Value::Extension { type_id, arguments }),
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
                max_extension_bytes: 128,
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
        assert_eq!(59, count);
    }

    #[test]
    fn draft_extension_corpus_round_trips() {
        let mut count = 0;
        for line in DRAFT_EXTENSIONS
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(8, columns.len(), "invalid draft fixture row: {line}");
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
            assert_value_eq(
                &value,
                &decode_storage(&expected_storage, true).unwrap(),
                id,
            );
            count += 1;
        }
        assert_eq!(50, count);
    }

    #[test]
    fn extension_writer_rejects_unportable_identifiers() {
        for id in [EXT_KEYWORD, EXT_SYMBOL, EXT_CHARACTER, EXT_REGEX] {
            let retired = Value::Extension {
                type_id: ExtensionId::Integer(id),
                arguments: vec![Value::Text("a".into())],
            };
            for mode in [Mode::Canonical, Mode::Fast] {
                assert_eq!(
                    ErrorKind::InvalidExtension,
                    encode(&retired, mode).unwrap_err().kind
                );
            }
        }
        let too_large = Value::Extension {
            type_id: ExtensionId::Integer(i64::MAX as u64 + 1),
            arguments: vec![],
        };
        assert_eq!(
            ErrorKind::InvalidExtension,
            encode(&too_large, Mode::Canonical).unwrap_err().kind
        );

        let empty_name = Value::Extension {
            type_id: ExtensionId::Name(String::new()),
            arguments: vec![],
        };
        assert_eq!(
            ErrorKind::InvalidExtension,
            encode(&empty_name, Mode::Canonical).unwrap_err().kind
        );

        let canon_eq_regex = Value::Regex {
            source: "a".into(),
            flags: 0x80,
        };
        assert_eq!(
            ErrorKind::InvalidRegex,
            encode(&canon_eq_regex, Mode::Canonical).unwrap_err().kind
        );
    }

    #[test]
    fn named_value_varint_boundaries() {
        for length in [127, 128, 16383, 16384] {
            for value in [
                Value::Keyword {
                    namespace: Some("x".repeat(length)),
                    name: "λ\0/😀".into(),
                },
                Value::Symbol {
                    namespace: Some("x".repeat(length)),
                    name: "λ\0/😀".into(),
                },
            ] {
                for mode in [Mode::Canonical, Mode::Fast] {
                    let encoded = encode(&value, mode).unwrap();
                    assert_value_eq(&value, &decode(&encoded, true).unwrap(), "varint boundary");
                    let mut buffer = vec![0; encoded_len(&value, mode).unwrap()];
                    assert_eq!(
                        buffer.len(),
                        encode_into(&value, mode, &mut buffer).unwrap()
                    );
                    assert_eq!(encoded, buffer);
                }
            }
        }
    }

    #[test]
    fn tagged_byte_payload_context() {
        for (tag, payload, expected) in [
            (2, "8000000000000000", "c2488000000000000000"),
            (3, "8000000000000000", "c3488000000000000000"),
            (
                37,
                "fbfcfee0e1000102030405060708090a",
                "d82550fbfcfee0e1000102030405060708090a",
            ),
            (69, "fbfc", "d84542fbfc"),
            (10000, "fb61", "d9271043fefb61"),
        ] {
            let value = Value::Tagged {
                tag,
                value: Box::new(Value::Bytes(hex_decode(payload))),
            };
            for mode in [Mode::Canonical, Mode::Fast] {
                let encoded = encode(&value, mode).unwrap();
                assert_eq!(hex_decode(expected), encoded);
                assert_eq!(encoded.len(), encoded_len(&value, mode).unwrap());
                assert_eq!(
                    encoded,
                    encode(&decode(&encoded, true).unwrap(), mode).unwrap()
                );
            }
        }
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
        assert_eq!(68, count);
    }

    #[test]
    fn draft_extension_malformed_corpus_has_exact_error_agreement() {
        let mut count = 0;
        for line in DRAFT_EXTENSION_MALFORMED
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(5, columns.len(), "invalid draft malformed row: {line}");
            let [id, operation, hex, expected, _note]: [&str; 5] = columns.try_into().unwrap();
            let input = hex_decode(hex);
            let error = match operation {
                "canonical" => decode(&input, true),
                "fast" => decode(&input, false),
                "storage" => decode_storage(&input, true),
                "limit-input" | "limit-depth" | "limit-collection" | "limit-string"
                | "limit-bignum" | "limit-extension" => {
                    decode_with_limits(&input, true, malformed_limits(operation))
                }
                _ => panic!("unknown malformed fixture operation {operation}: {id}"),
            }
            .expect_err(id);
            assert_eq!(expected, error.kind.code(), "{id}");
            count += 1;
        }
        assert_eq!(64, count);
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
            "limit-extension" => limits.max_extension_bytes = 4,
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
        let noncanonical_array_nan = hex_decode("d855440100c07f");
        assert_eq!(
            ErrorKind::NonCanonical,
            decode(&noncanonical_array_nan, true).unwrap_err().kind
        );
        assert_eq!(
            hex_decode("d855440000c07f"),
            encode(
                &decode(&noncanonical_array_nan, false).unwrap(),
                Mode::Canonical
            )
            .unwrap()
        );
        let limits = Limits {
            max_string_bytes: 2,
            ..Limits::default()
        };
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
            "instant-epoch" => Value::InstantMillis(0),
            "instant-positive-millis" => Value::InstantMillis(1234),
            "instant-negative-millis" => Value::InstantMillis(-1),
            "instant-int64-min" => Value::InstantMillis(i64::MIN),
            "instant-int64-max" => Value::InstantMillis(i64::MAX),
            "uint16-array" => Value::Uint16Array(vec![0, 0x03bb, 0xd800, 0xffff]),
            "int16-array" => Value::Int16Array(vec![i16::MIN, -1, 0, i16::MAX]),
            "int32-array" => Value::Int32Array(vec![i32::MIN, -1, 0, i32::MAX]),
            "int64-array" => Value::Int64Array(vec![i64::MIN, -1, 0, i64::MAX]),
            "float32-array" => {
                Value::Float32Array(vec![1.5, -0.0, f32::INFINITY, f32::from_bits(0x7fc0_0001)])
            }
            "float64-array" => Value::Float64Array(vec![
                1.5,
                -0.0,
                f64::NEG_INFINITY,
                f64::from_bits(0x7ff8_0000_0000_0001),
            ]),
            "bytes-subtype-collisions" => Value::Bytes(vec![0xfb, 0xfc, 0xfe, 0xe0, 0xe1, 0]),
            "bytes-length-22" => Value::Bytes(vec![97; 22]),
            "bytes-length-23" => Value::Bytes(vec![97; 23]),
            "keyword-qualified-example" => Value::Keyword {
                namespace: Some("user".into()),
                name: "id".into(),
            },
            "keyword-empty-name" => Value::Keyword {
                namespace: None,
                name: "".into(),
            },
            "keyword-qualified-empty-name" => Value::Keyword {
                namespace: Some("ns".into()),
                name: "".into(),
            },
            "keyword-empty-parts" => Value::Keyword {
                namespace: Some("".into()),
                name: "".into(),
            },
            "keyword-unqualified-slash" => Value::Keyword {
                namespace: None,
                name: "user/id".into(),
            },
            "keyword-embedded-nul" => Value::Keyword {
                namespace: Some("n\0s".into()),
                name: "a\0b".into(),
            },
            "keyword-unqualified-nul" => Value::Keyword {
                namespace: None,
                name: "a\0b".into(),
            },
            "keyword-unicode" => Value::Keyword {
                namespace: Some("λ".into()),
                name: "😀".into(),
            },
            "keyword-namespace-127" => Value::Keyword {
                namespace: Some("x".repeat(127)),
                name: "a".into(),
            },
            "keyword-namespace-128" => Value::Keyword {
                namespace: Some("x".repeat(128)),
                name: "a".into(),
            },
            "symbol-empty-name" => Value::Symbol {
                namespace: None,
                name: "".into(),
            },
            "symbol-empty-namespace" => Value::Symbol {
                namespace: Some("".into()),
                name: "a".into(),
            },
            "symbol-embedded-nul" => Value::Symbol {
                namespace: Some("n\0s".into()),
                name: "a\0b".into(),
            },
            "symbol-unqualified-slash" => Value::Symbol {
                namespace: None,
                name: "user/id".into(),
            },
            "symbol-unicode" => Value::Symbol {
                namespace: Some("λ".into()),
                name: "😀".into(),
            },
            "symbol-namespace-128" => Value::Symbol {
                namespace: Some("x".repeat(128)),
                name: "a".into(),
            },
            "keyword-name-22" => Value::Keyword {
                namespace: None,
                name: "a".repeat(22),
            },
            "keyword-name-23" => Value::Keyword {
                namespace: None,
                name: "a".repeat(23),
            },
            "keyword-unqualified" => Value::Keyword {
                namespace: None,
                name: "a".into(),
            },
            "keyword-qualified" => Value::Keyword {
                namespace: Some("ns".into()),
                name: "a".into(),
            },
            "keyword-empty-namespace" => Value::Keyword {
                namespace: Some(String::new()),
                name: "a".into(),
            },
            "symbol-unqualified" => Value::Symbol {
                namespace: None,
                name: "a".into(),
            },
            "symbol-qualified" => Value::Symbol {
                namespace: Some("ns".into()),
                name: "a".into(),
            },
            "character-zero" => Value::Character(0),
            "character-byte-max" => Value::Character(0x00ff),
            "character-two-byte-min" => Value::Character(0x0100),
            "character-max" => Value::Character(0xffff),
            "character-low-surrogate" => Value::Character(0xdc00),
            "character-ascii" => Value::Character(0x0061),
            "character-surrogate" => Value::Character(0xd800),
            "list-empty" => Value::List(vec![]),
            "list-mixed" => Value::List(vec![Value::int(1), Value::Text("a".into())]),
            "queue-mixed" => Value::Queue(vec![Value::int(1), Value::Text("a".into())]),
            "regex-empty" => Value::Regex {
                source: "".into(),
                flags: 0,
            },
            "regex-unicode" => Value::Regex {
                source: "λ😀".into(),
                flags: 2,
            },
            "regex-embedded-nul" => Value::Regex {
                source: "a\0b".into(),
                flags: 0,
            },
            "regex-source-21" => Value::Regex {
                source: "a".repeat(21),
                flags: 0,
            },
            "regex-source-22" => Value::Regex {
                source: "a".repeat(22),
                flags: 0,
            },
            "regex-no-flags" => Value::Regex {
                source: "a+".into(),
                flags: 0,
            },
            "regex-unix-lines" => Value::Regex {
                source: "a".into(),
                flags: 1,
            },
            "regex-case-insensitive" => Value::Regex {
                source: "a".into(),
                flags: 2,
            },
            "regex-comments" => Value::Regex {
                source: "a".into(),
                flags: 4,
            },
            "regex-multiline" => Value::Regex {
                source: "a".into(),
                flags: 8,
            },
            "regex-literal" => Value::Regex {
                source: "a".into(),
                flags: 16,
            },
            "regex-dotall" => Value::Regex {
                source: "a".into(),
                flags: 32,
            },
            "regex-unicode-case" => Value::Regex {
                source: "a".into(),
                flags: 64,
            },
            "regex-unicode-character-class" => Value::Regex {
                source: "\\w+".into(),
                flags: 320,
            },
            "regex-all-supported-flags" => Value::Regex {
                source: "a".into(),
                flags: 383,
            },
            "extension-unknown-integer" => Value::Extension {
                type_id: ExtensionId::Integer(42),
                arguments: vec![Value::int(1), Value::Null],
            },
            "extension-unknown-name" => Value::Extension {
                type_id: ExtensionId::Name("org.example/x".into()),
                arguments: vec![Value::int(1), Value::Null],
            },
            _ => panic!("fixture has no Rust value: {id}"),
        }
    }

    fn assert_value_eq(expected: &Value, actual: &Value, id: &str) {
        match (expected, actual) {
            (Value::Float32(left), Value::Float32(right)) if left.is_nan() && right.is_nan() => {}
            (Value::Float64(left), Value::Float64(right)) if left.is_nan() && right.is_nan() => {}
            (Value::Float32Array(_), Value::Float32Array(_))
            | (Value::Float64Array(_), Value::Float64Array(_))
            | (Value::Map(_), Value::Map(_))
            | (Value::Set(_), Value::Set(_)) => assert_eq!(
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
