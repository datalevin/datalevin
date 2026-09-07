use super::*;
use std::{collections::HashMap, hash::BuildHasherDefault, io::Write};

// Keyword::hash already supplies a randomized, cached hash. Hashing that hash
// again adds work to every cache hit without improving collision resistance.
#[derive(Default)]
struct KeywordHasher(u64);
impl Hasher for KeywordHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only Keyword keys use this hasher")
    }
}

/// Keep the scratch buffer outside the recursive value writer's stack frame.
/// Fixed-width conversion into contiguous bytes lets LLVM vectorize byte swaps;
/// each block reaches the destination with one write_all, without a heap buffer.
#[inline(never)]
fn write_numeric_blocks<W: Write, T, const N: usize>(
    output: &mut W,
    position: &mut usize,
    values: &[T],
    to_be_bytes: impl Fn(&T) -> [u8; N],
) -> std::io::Result<()> {
    let mut buffer = [0u8; 4096];
    for block in values.chunks(buffer.len() / N) {
        let bytes = &mut buffer[..block.len() * N];
        for (value, dest) in block.iter().zip(bytes.as_chunks_mut::<N>().0) {
            *dest = to_be_bytes(value);
        }
        output.write_all(bytes)?;
        *position += bytes.len();
    }
    Ok(())
}

/// Writes directly to a Vec, a caller-owned slice, or another std::io::Write.
/// An Encoder is a Nippy cache session. Discard it after a write failure.
pub struct Encoder<W> {
    output: W,
    reserve: Option<fn(&mut W, usize)>,
    pos: usize,
    limits: Limits,
    nodes: usize,
    keywords: HashMap<Keyword, Option<usize>, BuildHasherDefault<KeywordHasher>>,
    cached_keywords: usize,
    version: WireVersion,
}

impl<W: Write> Encoder<W> {
    pub fn new(output: W, limits: Limits) -> Self {
        Self {
            output,
            reserve: None,
            pos: 0,
            limits,
            nodes: 0,
            keywords: HashMap::default(),
            cached_keywords: 0,
            version: WireVersion::default(),
        }
    }
    pub fn with_version(mut self, version: WireVersion) -> Self {
        self.version = version;
        self
    }
    pub fn position(&self) -> usize {
        self.pos
    }
    pub fn into_inner(self) -> W {
        self.output
    }
    pub fn write_value(&mut self, value: &Value) -> Result<()> {
        self.value(value, 0)
    }
    fn err(&self, kind: ErrorKind) -> Error {
        Error::new(kind, self.pos)
    }
    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.limits.max_bytes.saturating_sub(self.pos) {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        self.output
            .write_all(bytes)
            .map_err(|_| self.err(ErrorKind::Io))?;
        self.pos += bytes.len();
        Ok(())
    }
    fn numeric_words<T, const N: usize>(
        &mut self,
        values: &[T],
        to_be_bytes: impl Fn(&T) -> [u8; N],
    ) -> Result<()> {
        let size = values
            .len()
            .checked_mul(N)
            .ok_or_else(|| self.err(ErrorKind::LimitExceeded))?;
        if size > self.limits.max_bytes.saturating_sub(self.pos) {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        if let Some(reserve) = self.reserve {
            reserve(&mut self.output, size);
        }
        if values.len() <= 8 {
            // Tiny arrays cost less than setting up a scratch block.
            for value in values {
                self.put(&to_be_bytes(value))?;
            }
        } else {
            write_numeric_blocks(&mut self.output, &mut self.pos, values, to_be_bytes)
                .map_err(|_| self.err(ErrorKind::Io))?;
        }
        Ok(())
    }
    fn tag(&mut self, tag: u8) -> Result<()> {
        self.put(&[tag])
    }
    fn tagged<const N: usize>(&mut self, tag: u8, bytes: [u8; N]) -> Result<()> {
        let mut buffer = [0; 17];
        buffer[0] = tag;
        buffer[1..N + 1].copy_from_slice(&bytes);
        self.put(&buffer[..N + 1])
    }
    fn count(&mut self, n: usize, width: u8) -> Result<()> {
        match width {
            0 if n <= 255 => self.tag(n as u8 ^ 0x80),
            1 if n <= 127 => self.tag(n as u8),
            2 if n <= 32767 => self.put(&(n as i16).to_be_bytes()),
            4 if n <= i32::MAX as usize => self.put(&(n as i32).to_be_bytes()),
            8 if n <= i64::MAX as usize => self.put(&(n as i64).to_be_bytes()),
            _ => Err(self.err(ErrorKind::InvalidLength)),
        }
    }
    fn blob(&mut self, bytes: &[u8], width: u8) -> Result<()> {
        self.count(bytes.len(), width)?;
        self.put(bytes)
    }
    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let n = bytes.len();
        if n == 0 {
            return self.tag(53);
        }
        let (tag, width) = if n <= 127 {
            (7, 1)
        } else if n <= 32767 {
            (15, 2)
        } else {
            (2, 4)
        };
        self.tag(tag)?;
        self.blob(bytes, width)
    }
    fn string(&mut self, text: &str) -> Result<()> {
        let n = text.len();
        if n == 0 {
            return self.tag(34);
        }
        let (tag, width) = if n <= 255 {
            (96, 0)
        } else if n <= 32767 {
            (16, 2)
        } else {
            (13, 4)
        };
        self.tag(tag)?;
        self.blob(text.as_bytes(), width)
    }
    fn named(&mut self, text: &str, small: u8, medium: u8) -> Result<()> {
        let (tag, width) = if text.len() <= 127 {
            (small, 1)
        } else {
            (medium, 2)
        };
        self.tag(tag)?;
        self.blob(text.as_bytes(), width)
    }
    fn keyword(&mut self, text: &Keyword) -> Result<()> {
        if text.len() > 32767 {
            return Err(self.err(ErrorKind::InvalidLength));
        }
        text.cached_hash();
        if let Some(index) = self.keywords.get_mut(text) {
            if let Some(index) = *index {
                return self.cache_ref(index);
            }
            let next = self.cached_keywords;
            *index = Some(next);
            self.cached_keywords += 1;
            self.cache_ref(next)?;
        } else if self.keywords.len() < 32768 {
            // Nippy 3.9 writes the first occurrence plainly and caches the second.
            self.keywords.insert(text.clone(), None);
        }
        self.named(text, 106, 85)
    }
    fn cache_ref(&mut self, index: usize) -> Result<()> {
        const SMALL: [u8; 8] = [59, 63, 64, 65, 66, 72, 73, 74];
        if index < 8 {
            self.tag(SMALL[index])
        } else if index <= 127 {
            self.tag(67)?;
            self.count(index, 1)
        } else {
            self.tag(68)?;
            self.count(index, 2)
        }
    }
    fn long(&mut self, n: i64) -> Result<()> {
        if n == 0 {
            return self.tag(0);
        }
        let abs = n.unsigned_abs();
        let sign = if n < 0 { 6 } else { 0 };
        if abs <= 255 {
            self.tagged(87 + sign, [abs as u8 ^ 0x80])
        } else if abs <= 65535 {
            self.tagged(88 + sign, (abs as u16 ^ 0x8000).to_be_bytes())
        } else if abs <= u32::MAX as u64 {
            self.tagged(89 + sign, (abs as u32 ^ 0x80000000).to_be_bytes())
        } else {
            self.tagged(43, n.to_be_bytes())
        }
    }
    fn collection(&self, n: usize) -> Result<()> {
        if n > self.limits.max_collection_len {
            Err(self.err(ErrorKind::LimitExceeded))
        } else {
            Ok(())
        }
    }
    fn header(&mut self, n: usize, tags: [u8; 4], unsigned: bool) -> Result<()> {
        self.collection(n)?;
        if n == 0 {
            return self.tag(tags[0]);
        }
        let (tag, width) = if n <= if unsigned { 255 } else { 127 } {
            (tags[1], if unsigned { 0 } else { 1 })
        } else if n <= 32767 {
            (tags[2], 2)
        } else {
            (tags[3], 4)
        };
        self.tag(tag)?;
        self.count(n, width)
    }
    fn values(&mut self, values: &[Value], depth: usize) -> Result<()> {
        self.collection(values.len())?;
        for v in values {
            self.value(v, depth)?;
        }
        Ok(())
    }
    fn pairs(&mut self, values: &[(Value, Value)], depth: usize) -> Result<()> {
        self.collection(values.len())?;
        for (k, v) in values {
            self.value(k, depth)?;
            self.value(v, depth)?;
        }
        Ok(())
    }
    fn custom(&mut self, id: i16) -> Result<()> {
        self.tag(82)?;
        self.put(&id.to_be_bytes())
    }
    fn big(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Err(self.err(ErrorKind::InvalidValue));
        }
        self.blob(bytes, 4)
    }
    fn growing(&mut self, items: &[i32]) -> Result<()> {
        self.collection(items.len())?;
        self.custom(GROWING)?;
        if items.len() > 3 {
            let words = integers::compress(items);
            let n = i32::try_from(words.len()).map_err(|_| self.err(ErrorKind::InvalidLength))?;
            self.put(&(-n).to_be_bytes())?;
            self.numeric_words(&words, |n| n.to_be_bytes())?;
        } else {
            self.count(items.len(), 4)?;
            for word in items {
                self.put(&word.to_be_bytes())?;
            }
        }
        Ok(())
    }
    fn bitmap(&mut self, bitmap: &roaring::RoaringBitmap) -> Result<()> {
        self.collection(bitmap.len() as usize)?;
        self.custom(BITMAP)?;
        let size = bitmap.serialized_size();
        if size > self.limits.max_bytes.saturating_sub(self.pos) {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        if let Some(reserve) = self.reserve {
            reserve(&mut self.output, size);
        } else if size >= 1024 {
            // Keep the portable format in the Roaring library. Reserve once
            // instead of growing a generic destination for every word.
            let mut bytes = Vec::with_capacity(size);
            bitmap
                .serialize_into(&mut bytes)
                .map_err(|_| self.err(ErrorKind::Io))?;
            return self.put(&bytes);
        }
        bitmap
            .serialize_into(&mut self.output)
            .map_err(|_| self.err(ErrorKind::Io))?;
        self.pos += size;
        Ok(())
    }
    #[inline]
    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        self.nodes += 1;
        if depth > self.limits.max_depth || self.nodes > self.limits.max_values {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        match value {
            Value::Null => self.tag(3),
            Value::Bool(v) => self.tag(if *v { 8 } else { 9 }),
            Value::Char(v) => self.tagged(10, v.to_be_bytes()),
            Value::Byte(v) => self.tagged(40, [*v as u8]),
            Value::Short(v) => self.tagged(41, v.to_be_bytes()),
            Value::Integer(v) => self.tagged(42, v.to_be_bytes()),
            Value::Long(v) => self.long(*v),
            Value::Float(v) => self.tagged(60, v.to_be_bytes()),
            Value::Double(v) => {
                if *v == 0 {
                    self.tag(55)
                } else {
                    self.tagged(61, v.to_be_bytes())
                }
            }
            _ => self.compound_value(value, depth + 1),
        }
    }
    fn compound_value(&mut self, value: &Value, d: usize) -> Result<()> {
        match value {
            Value::BigInt(v) => {
                self.tag(44)?;
                self.big(v)
            }
            Value::BigInteger(v) => {
                self.tag(45)?;
                self.big(v)
            }
            Value::BigDecimal { unscaled, scale } => {
                self.tag(62)?;
                self.big(unscaled)?;
                self.put(&scale.to_be_bytes())
            }
            Value::Ratio {
                numerator,
                denominator,
            } => {
                self.tag(70)?;
                self.big(numerator)?;
                self.big(denominator)
            }
            Value::Text(v) => self.string(v),
            Value::Keyword(v) => self.keyword(v),
            Value::Symbol(v) => self.named(v, 56, 86),
            Value::Bytes(v) => self.bytes(v),
            Value::Vector(v) => {
                if v.len() == 2 {
                    self.tag(113)?;
                } else if v.len() == 3 {
                    self.tag(114)?;
                } else {
                    self.header(v.len(), [17, 97, 69, 21], true)?;
                }
                self.values(v, d)
            }
            Value::Set(v) => {
                self.header(v.len(), [18, 98, 32, 23], true)?;
                self.values(v, d)
            }
            Value::Map(v) => {
                self.header(v.len(), [19, 99, 33, 30], true)?;
                self.pairs(v, d)
            }
            Value::List(v) => {
                self.header(v.len(), [35, 36, 54, 20], false)?;
                self.values(v, d)
            }
            Value::Seq(v) => {
                self.header(v.len(), [37, 38, 39, 24], false)?;
                self.values(v, d)
            }
            Value::SortedSet(v) | Value::Queue(v) => {
                self.tag(if matches!(value, Value::Queue(_)) {
                    26
                } else {
                    28
                })?;
                self.count(v.len(), 4)?;
                self.values(v, d)
            }
            Value::SortedMap(v) => {
                self.tag(31)?;
                self.count(v.len(), 4)?;
                self.pairs(v, d)
            }
            Value::MapEntry(v) => {
                self.tag(103)?;
                self.value(&v.0, d)?;
                self.value(&v.1, d)
            }
            Value::Meta { metadata, value } => {
                self.tag(25)?;
                self.value(metadata, d)?;
                self.value(value, d)
            }
            Value::MetaProtocolKey => self.tag(104),
            Value::Regex(v) | Value::Uri(v) => {
                self.tag(if matches!(value, Value::Regex(_)) {
                    58
                } else {
                    71
                })?;
                self.string(v)
            }
            Value::Uuid(v) => {
                self.tag(91)?;
                self.put(v)
            }
            Value::Date(v) | Value::SqlDate(v) => {
                self.tag(if matches!(value, Value::Date(_)) {
                    90
                } else {
                    92
                })?;
                self.put(&v.to_be_bytes())
            }
            Value::Instant { seconds, nanos } | Value::Duration { seconds, nanos } => {
                if !(0..1_000_000_000).contains(nanos) {
                    return Err(self.err(ErrorKind::InvalidValue));
                }
                self.tag(if matches!(value, Value::Instant { .. }) {
                    79
                } else {
                    83
                })?;
                self.put(&seconds.to_be_bytes())?;
                self.put(&nanos.to_be_bytes())
            }
            Value::Period {
                years,
                months,
                days,
            } => {
                self.tag(84)?;
                self.put(&years.to_be_bytes())?;
                self.put(&months.to_be_bytes())?;
                self.put(&days.to_be_bytes())
            }
            Value::IntArray(v) => {
                self.collection(v.len())?;
                self.tag(118)?;
                self.count(v.len(), 4)?;
                self.numeric_words(v, |n| n.to_be_bytes())
            }
            Value::BooleanArray(_) | Value::ShortArray(_) | Value::CharArray(_) => {
                let n = match value {
                    Value::BooleanArray(v) => v.len(),
                    Value::ShortArray(v) => v.len(),
                    Value::CharArray(v) => v.len(),
                    _ => unreachable!(),
                };
                self.collection(n)?;
                if n > i32::MAX as usize || n > self.limits.max_allocation_bytes / 2 {
                    return Err(self.err(ErrorKind::LimitExceeded));
                }
                let (class, bytes) = java_array::encode(value).unwrap();
                self.named(class, 75, 76)?;
                self.bytes(&bytes)
            }
            Value::LongArray(v) => {
                self.collection(v.len())?;
                self.tag(119)?;
                self.count(v.len(), 4)?;
                self.numeric_words(v, |n| n.to_be_bytes())
            }
            Value::FloatArray(v) => {
                self.collection(v.len())?;
                self.tag(120)?;
                self.count(v.len(), 4)?;
                self.numeric_words(v, |n| n.to_be_bytes())
            }
            Value::DoubleArray(v) => {
                self.collection(v.len())?;
                self.tag(121)?;
                self.count(v.len(), 4)?;
                self.numeric_words(v, |n| n.to_be_bytes())
            }
            Value::StringArray(v) | Value::ObjectArray(v) => {
                if matches!(value, Value::StringArray(_))
                    && v.iter().any(|x| !matches!(x, Value::Null | Value::Text(_)))
                {
                    return Err(self.err(ErrorKind::InvalidValue));
                }
                self.tag(if matches!(value, Value::StringArray(_)) {
                    107
                } else {
                    115
                })?;
                self.count(v.len(), 4)?;
                self.values(v, d)
            }
            Value::Record { class, fields } => {
                self.named(class, 48, 49)?;
                self.value(fields, d)
            }
            Value::Deftype { class, fields } => {
                self.tag(if self.version == WireVersion::V3_7 {
                    81
                } else {
                    122
                })?;
                self.string(class)?;
                if self.version == WireVersion::V3_9 {
                    self.count(fields.len(), 0)?;
                }
                self.values(fields, d)
            }
            Value::Serializable { class, bytes } => {
                self.named(class, 75, 76)?;
                self.bytes(bytes)
            }
            Value::Reader(v) => {
                let (tag, width) = if v.len() <= 127 {
                    (47, 1)
                } else if v.len() <= 32767 {
                    (51, 2)
                } else {
                    (52, 4)
                };
                self.tag(tag)?;
                self.blob(v.as_bytes(), width)
            }
            Value::Datom {
                entity,
                attribute,
                value,
                tx,
            } => {
                self.custom(DATOM)?;
                self.put(&entity.to_be_bytes())?;
                self.value(attribute, d)?;
                self.value(value, d)?;
                self.value(tx, d)
            }
            Value::Entity(v) => {
                self.custom(ENTITY)?;
                self.value(v, d)
            }
            Value::InterpretedFunction(v) => {
                self.custom(INTER_FN)?;
                self.value(v, d)
            }
            Value::Bitmap(v) => self.bitmap(v),
            Value::GrowingIntArray(v) => self.growing(v),
            Value::SparseIntArray { items, indices } => {
                if items.len() as u64 != indices.len() {
                    return Err(self.err(ErrorKind::InvalidValue));
                }
                self.custom(SPARSE)?;
                self.growing(items)?;
                self.bitmap(indices)
            }
            Value::SpillableVector(v) => {
                self.custom(SPILL_VEC)?;
                self.count(v.len(), 8)?;
                self.values(v, d)
            }
            Value::SpillableMap(v) => {
                self.custom(SPILL_MAP)?;
                self.value(v, d)
            }
            Value::SpillableSet(v) => {
                self.custom(SPILL_SET)?;
                self.value(v, d)
            }
            _ => unreachable!("primitive values handled by value"),
        }
    }
}

pub fn fast_freeze(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    fast_freeze_into(&mut out, value)?;
    Ok(out)
}

/// Appends one independent payload, reusing capacity. Rolls back on failure.
pub fn fast_freeze_into(out: &mut Vec<u8>, value: &Value) -> Result<()> {
    let start = out.len();
    let mut encoder = Encoder::new(&mut *out, Limits::default());
    // Generic streams need only Write; the Vec API can additionally reserve a
    // known payload size once, including for arrays/bitmaps nested in values.
    encoder.reserve = Some(|out, additional| out.reserve(additional));
    let result = encoder.write_value(value);
    if result.is_err() {
        out.truncate(start);
    }
    result
}
