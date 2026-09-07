//! Nippy 3.7/3.9 wire compatibility, including Datalevin's custom freezers.
//!
//! `fast_freeze`/`fast_thaw` operate on the headerless payload used by
//! datalevin.bits. `freeze`/`thaw` use Nippy's existing NPY header. These are
//! value codecs, not sortable index-key encoders. JVM objects remain inert.

mod bitmap;
mod compression;
mod integers;
mod java_array;
mod read;
mod write;

pub use compression::{Compression, freeze, thaw, thaw_with_limits};
pub use read::Decoder;
pub use write::{Encoder, fast_freeze, fast_freeze_into};

/// Default output remains readable by the repository's pinned Nippy release.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WireVersion {
    #[default]
    V3_7,
    V3_9,
}

/// Ordered entries deliberately avoid imposing Rust hashing/equality on JVM keys.
/// Floating point values store IEEE bits, including NaNs and signed zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Bool(bool),
    Char(u16),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    /// Signed, big-endian two's-complement magnitude, as BigInteger.toByteArray.
    BigInt(Vec<u8>),
    BigInteger(Vec<u8>),
    BigDecimal {
        unscaled: Vec<u8>,
        scale: i32,
    },
    Ratio {
        numerator: Vec<u8>,
        denominator: Vec<u8>,
    },
    Text(String),
    /// Nippy stores the printable name without the leading colon.
    Keyword(String),
    Symbol(String),
    Bytes(Vec<u8>),
    Vector(Vec<Value>),
    List(Vec<Value>),
    Seq(Vec<Value>),
    Set(Vec<Value>),
    Map(Vec<(Value, Value)>),
    SortedSet(Vec<Value>),
    SortedMap(Vec<(Value, Value)>),
    Queue(Vec<Value>),
    MapEntry(Box<(Value, Value)>),
    Meta {
        metadata: Box<Value>,
        value: Box<Value>,
    },
    MetaProtocolKey,
    /// Nippy's regex format carries the pattern string only.
    Regex(String),
    Uri(String),
    Uuid([u8; 16]),
    Date(i64),
    SqlDate(i64),
    Instant {
        seconds: i64,
        nanos: i32,
    },
    Duration {
        seconds: i64,
        nanos: i32,
    },
    Period {
        years: i32,
        months: i32,
        days: i32,
    },
    IntArray(Vec<i32>),
    BooleanArray(Vec<bool>),
    ShortArray(Vec<i16>),
    CharArray(Vec<u16>),
    LongArray(Vec<i64>),
    FloatArray(Vec<u32>),
    DoubleArray(Vec<u64>),
    StringArray(Vec<Value>),
    ObjectArray(Vec<Value>),
    Record {
        class: String,
        fields: Box<Value>,
    },
    Deftype {
        class: String,
        fields: Vec<Value>,
    },
    /// Java Object Serialization bytes, transported without executing JVM code.
    Serializable {
        class: String,
        bytes: Vec<u8>,
    },
    /// Nippy's EDN fallback, preserved without invoking registered data readers.
    Reader(String),
    Datom {
        entity: i64,
        attribute: Box<Value>,
        value: Box<Value>,
        tx: Box<Value>,
    },
    Entity(Box<Value>),
    InterpretedFunction(Box<Value>),
    Bitmap(roaring::RoaringBitmap),
    GrowingIntArray(Vec<i32>),
    SparseIntArray {
        items: Vec<i32>,
        indices: roaring::RoaringBitmap,
    },
    SpillableVector(Vec<Value>),
    SpillableMap(Box<Value>),
    SpillableSet(Box<Value>),
}

impl Value {
    pub fn int(value: i64) -> Self {
        Self::Long(value)
    }
    pub fn float(value: f32) -> Self {
        Self::Float(value.to_bits())
    }
    pub fn double(value: f64) -> Self {
        Self::Double(value.to_bits())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_depth: usize,
    pub max_collection_len: usize,
    /// Total materialized nodes, including copies of cached values.
    pub max_values: usize,
    /// Budget for owned value storage and cache copies, separate from input size.
    pub max_allocation_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_depth: 128,
            max_collection_len: 1_000_000,
            max_values: 2_000_000,
            max_allocation_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Truncated,
    TrailingBytes,
    InvalidLength,
    InvalidUtf8,
    InvalidValue,
    LimitExceeded,
    InvalidCache,
    DictionaryMismatch,
    UnsupportedType(i16),
    UnsupportedHeader(u8),
    Compression,
    Io,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub offset: usize,
}
impl Error {
    pub(crate) fn new(kind: ErrorKind, offset: usize) -> Self {
        Self { kind, offset }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Nippy {:?} at byte {}", self.kind, self.offset)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

pub fn fast_thaw(bytes: &[u8]) -> Result<Value> {
    fast_thaw_with_limits(bytes, Limits::default())
}

pub fn fast_thaw_with_limits(bytes: &[u8], limits: Limits) -> Result<Value> {
    let mut decoder = Decoder::new(bytes, limits)?;
    let value = decoder.read_value()?;
    if decoder.position() != bytes.len() {
        return Err(Error::new(ErrorKind::TrailingBytes, decoder.position()));
    }
    Ok(value)
}

/// Nippy 3.9's optional, append-only shared keyword/string dictionary.
#[derive(Clone, Debug)]
pub struct SharedDictionary {
    entries: Vec<Value>,
    hashes: Vec<u64>,
}

impl SharedDictionary {
    pub fn new(entries: Vec<Value>) -> Result<Self> {
        const OFFSET: u64 = 0xcbf29ce484222325;
        const PRIME: u64 = 0x100000001b3;
        if entries.is_empty() || entries.len() > 32767 {
            return Err(Error::new(ErrorKind::InvalidLength, 0));
        }
        let mut seen = std::collections::HashSet::new();
        let mut hashes = vec![OFFSET];
        for entry in &entries {
            let (kind, text) = match entry {
                Value::Keyword(s) => (0u8, format!(":{s}")),
                Value::Text(s) => (1u8, s.clone()),
                _ => return Err(Error::new(ErrorKind::InvalidValue, 0)),
            };
            if !seen.insert((kind, text.clone())) {
                return Err(Error::new(ErrorKind::InvalidValue, 0));
            }
            let mut h = (OFFSET ^ kind as u64).wrapping_mul(PRIME);
            for b in text.bytes() {
                h = (h ^ b as u64).wrapping_mul(PRIME);
            }
            hashes.push(hashes.last().unwrap().wrapping_mul(PRIME).wrapping_add(h));
        }
        Ok(Self { entries, hashes })
    }
}

// Values of taoensso.nippy.impl/coerce-custom-type-id, verified by JVM fixtures.
pub(crate) const DATOM: i16 = -27026;
pub(crate) const ENTITY: i16 = 6354;
pub(crate) const INTER_FN: i16 = -10700;
pub(crate) const BITMAP: i16 = 25371;
pub(crate) const GROWING: i16 = 1423;
pub(crate) const SPARSE: i16 = -2438;
pub(crate) const SPILL_VEC: i16 = -3600;
pub(crate) const SPILL_MAP: i16 = 28032;
pub(crate) const SPILL_SET: i16 = -25293;
