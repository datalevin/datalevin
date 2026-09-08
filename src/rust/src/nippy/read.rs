use super::*;
use std::mem::MaybeUninit;

struct Cached {
    value: Value,
    nodes: usize,
    bytes: usize,
    height: usize,
}

/// A bounded reader over a borrowed slice. Success advances by one value.
/// Reusing it retains Nippy's cache session for freeze-to-out!/with-cache streams.
/// After an error discard the decoder; partially read sessions are not reusable.
pub struct Decoder<'a> {
    input: &'a [u8],
    pos: usize,
    limits: Limits,
    nodes_remaining: usize,
    bytes_remaining: usize,
    cache: Vec<Option<Cached>>,
    dictionary: Option<&'a SharedDictionary>,
    dict_count: usize,
    height: usize,
    legacy_deftypes: std::collections::HashMap<String, usize>,
}

impl<'a> Decoder<'a> {
    pub fn new(input: &'a [u8], limits: Limits) -> Result<Self> {
        if input.len() > limits.max_bytes {
            return Err(Error::new(ErrorKind::LimitExceeded, 0));
        }
        Ok(Self {
            input,
            pos: 0,
            limits,
            nodes_remaining: limits.max_values,
            bytes_remaining: limits.max_allocation_bytes,
            cache: Vec::new(),
            dictionary: None,
            dict_count: 0,
            height: 0,
            legacy_deftypes: std::collections::HashMap::new(),
        })
    }
    pub fn with_dictionary(mut self, dictionary: &'a SharedDictionary) -> Self {
        self.dictionary = Some(dictionary);
        self
    }
    pub fn position(&self) -> usize {
        self.pos
    }
    /// Old Nippy deftypes omit the field count. Register the class schema;
    /// unknown classes fail rather than consuming the next value as a field.
    pub fn with_legacy_deftype(mut self, class: impl Into<String>, fields: usize) -> Self {
        self.legacy_deftypes.insert(class.into(), fields);
        self
    }
    pub fn read_value(&mut self) -> Result<Value> {
        self.value(0)
    }
    fn err(&self, kind: ErrorKind) -> Error {
        Error::new(kind, self.pos)
    }
    fn charge(&mut self, nodes: usize, bytes: usize) -> Result<()> {
        self.nodes_remaining = self
            .nodes_remaining
            .checked_sub(nodes)
            .ok_or_else(|| self.err(ErrorKind::LimitExceeded))?;
        self.bytes_remaining = self
            .bytes_remaining
            .checked_sub(bytes)
            .ok_or_else(|| self.err(ErrorKind::LimitExceeded))?;
        Ok(())
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| self.err(ErrorKind::InvalidLength))?;
        let bytes = self
            .input
            .get(self.pos..end)
            .ok_or_else(|| self.err(ErrorKind::Truncated))?;
        self.pos = end;
        Ok(bytes)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn numeric_words<T, const N: usize>(
        &mut self,
        n: usize,
        from_be_bytes: impl Fn([u8; N]) -> T,
    ) -> Result<Vec<T>> {
        // Validate and charge the whole span before allocating. An exact-size
        // iterator lets the compiler convert contiguous words in bulk.
        self.collection(n, N)?;
        self.charge(0, n * N)?;
        Ok(self
            .take(n * N)?
            .as_chunks::<N>()
            .0
            .iter()
            .copied()
            .map(from_be_bytes)
            .collect())
    }
    #[inline]
    fn count(&mut self, width: u8) -> Result<usize> {
        let n = match width {
            0 => (self.u8()? ^ 0x80) as i64,
            1 => self.u8()? as i8 as i64,
            2 => self.i16()? as i64,
            4 => self.i32()? as i64,
            8 => self.i64()?,
            _ => unreachable!(),
        };
        usize::try_from(n).map_err(|_| self.err(ErrorKind::InvalidLength))
    }
    fn collection(&self, n: usize, width: usize) -> Result<()> {
        if n > self.limits.max_collection_len {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        if n > (self.input.len() - self.pos) / width {
            return Err(self.err(ErrorKind::Truncated));
        }
        Ok(())
    }
    fn blob(&mut self, width: u8) -> Result<Vec<u8>> {
        let n = self.count(width)?;
        self.charge(0, n)?;
        Ok(self.take(n)?.to_vec())
    }
    #[inline]
    fn string(&mut self, width: u8) -> Result<String> {
        let n = self.count(width)?;
        self.charge(0, n)?;
        let bytes = self.take(n)?;
        // The standard validator already has an efficient long-string loop.
        // Avoid its setup only for short ASCII payloads.
        let s = if n < 32 && bytes.is_ascii() {
            // SAFETY: Every ASCII byte is a complete, valid UTF-8 code point.
            unsafe { std::str::from_utf8_unchecked(bytes) }
        } else {
            std::str::from_utf8(bytes).map_err(|_| self.err(ErrorKind::InvalidUtf8))?
        };
        Ok(s.to_owned())
    }
    fn keyword(&mut self, width: u8) -> Result<Keyword> {
        let n = self.count(width)?;
        self.charge(0, n.saturating_add(2 * std::mem::size_of::<usize>()))?;
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| self.err(ErrorKind::InvalidUtf8))?;
        Ok(s.into())
    }
    fn big(&mut self) -> Result<Vec<u8>> {
        let n = self.blob(4)?;
        if n.is_empty() {
            return Err(self.err(ErrorKind::InvalidValue));
        }
        Ok(n)
    }
    fn text_value(&mut self, depth: usize) -> Result<String> {
        match self.value(depth)? {
            Value::Text(s) => Ok(s),
            _ => Err(self.err(ErrorKind::InvalidValue)),
        }
    }
    #[inline]
    fn values(&mut self, n: usize, depth: usize) -> Result<Vec<Value>> {
        self.collection(n, 1)?;
        match n {
            0 => Ok(Vec::new()),
            1 => self.fixed_values::<1>(depth),
            2 => self.fixed_values::<2>(depth),
            3 => self.fixed_values::<3>(depth),
            _ => self.many_values(n, depth),
        }
    }
    // Keep tiny collections in a separate frame from the growing-vector loop.
    #[inline(never)]
    fn fixed_values<const N: usize>(&mut self, depth: usize) -> Result<Vec<Value>> {
        let mut result = Vec::with_capacity(N);
        for _ in 0..N {
            self.append_value(&mut result, depth)?;
        }
        Ok(result)
    }
    #[inline(never)]
    fn many_values(&mut self, n: usize, depth: usize) -> Result<Vec<Value>> {
        // Reserve medium collections once, bounded by both the remaining
        // decoder budget and a 64 KiB initial allocation cap.
        let capacity = n
            .min(self.nodes_remaining)
            .min(self.bytes_remaining.min(64 * 1024) / std::mem::size_of::<Value>());
        let mut result = Vec::with_capacity(capacity);
        for _ in 0..n {
            self.append_value(&mut result, depth)?;
        }
        Ok(result)
    }
    fn pairs(&mut self, n: usize, depth: usize) -> Result<Vec<(Value, Value)>> {
        self.collection(n, 2)?;
        let mut result = Vec::with_capacity(n.min(128));
        for _ in 0..n {
            result.push((self.value(depth)?, self.value(depth)?));
        }
        Ok(result)
    }
    fn cached(&mut self, index: usize, depth: usize) -> Result<Value> {
        if index < self.dict_count {
            let entry = &self.dictionary.unwrap().entries[index];
            let len = match entry {
                Value::Text(s) => s.len(),
                Value::Keyword(s) => s.len(),
                _ => unreachable!(),
            };
            self.charge(1, std::mem::size_of::<Value>() + len)?;
            return Ok(self.dictionary.unwrap().entries[index].clone());
        }
        let index = index - self.dict_count;
        if index < self.cache.len() {
            let c = self.cache[index]
                .as_ref()
                .ok_or_else(|| self.err(ErrorKind::InvalidCache))?;
            let (nodes, bytes, height) = (c.nodes, c.bytes, c.height);
            if depth + height > self.limits.max_depth {
                return Err(self.err(ErrorKind::LimitExceeded));
            }
            self.height = self.height.max(depth + height);
            self.charge(nodes, bytes)?;
            return Ok(self.cache[index].as_ref().unwrap().value.clone());
        }
        if index != self.cache.len() || index > 32767 {
            return Err(self.err(ErrorKind::InvalidCache));
        }
        self.cache.push(None); // Reserve before nested definitions; cycles fail.
        let (nodes, bytes, previous_height) =
            (self.nodes_remaining, self.bytes_remaining, self.height);
        self.height = depth;
        let value = self.value(depth)?;
        let height = self.height - depth;
        self.height = self.height.max(previous_height);
        let (nodes, bytes) = (nodes - self.nodes_remaining, bytes - self.bytes_remaining);
        self.charge(nodes, bytes)?;
        self.cache[index] = Some(Cached {
            value: value.clone(),
            nodes,
            bytes,
            height,
        });
        Ok(value)
    }
    fn custom(&mut self, id: i16, depth: usize) -> Result<Value> {
        Ok(match id {
            DATOM => Value::Datom {
                entity: self.i64()?,
                attribute: Box::new(self.value(depth)?),
                value: Box::new(self.value(depth)?),
                tx: Box::new(self.value(depth)?),
            },
            ENTITY => Value::Entity(Box::new(self.value(depth)?)),
            INTER_FN => Value::InterpretedFunction(Box::new(self.value(depth)?)),
            SPILL_MAP => Value::SpillableMap(Box::new(self.value(depth)?)),
            SPILL_SET => Value::SpillableSet(Box::new(self.value(depth)?)),
            SPILL_VEC => {
                let n = self.count(8)?;
                Value::SpillableVector(self.values(n, depth)?)
            }
            BITMAP => {
                let (bitmap, used, allocation) = bitmap::read(
                    &self.input[self.pos..],
                    self.limits.max_collection_len,
                    self.bytes_remaining,
                )
                .map_err(|kind| self.err(kind))?;
                self.charge(0, allocation)?;
                self.pos += used;
                Value::Bitmap(bitmap)
            }
            GROWING => {
                let size = self.i32()?;
                let n = size
                    .checked_abs()
                    .ok_or_else(|| self.err(ErrorKind::InvalidLength))?
                    as usize;
                let mut words = self.numeric_words(n, i32::from_be_bytes)?;
                if size < 0 {
                    let count = words.first().copied().unwrap_or(-1);
                    if count < 0 {
                        return Err(self.err(ErrorKind::InvalidLength));
                    }
                    if count as usize > self.limits.max_collection_len {
                        return Err(self.err(ErrorKind::LimitExceeded));
                    }
                    self.charge(0, count as usize * 4)?;
                    words = integers::decompress(&words).map_err(|kind| self.err(kind))?;
                }
                Value::GrowingIntArray(words)
            }
            SPARSE => {
                let items = match self.value(depth)? {
                    Value::GrowingIntArray(v) => v,
                    _ => return Err(self.err(ErrorKind::InvalidValue)),
                };
                let indices = match self.value(depth)? {
                    Value::Bitmap(v) => v,
                    _ => return Err(self.err(ErrorKind::InvalidValue)),
                };
                if items.len() as u64 != indices.len() {
                    return Err(self.err(ErrorKind::InvalidValue));
                }
                Value::SparseIntArray { items, indices }
            }
            _ => return Err(self.err(ErrorKind::UnsupportedType(id))),
        })
    }
    // Inline into each collection loop to construct values in their final slots
    // without adding a helper call for every element.
    #[inline(always)]
    fn append_value(&mut self, result: &mut Vec<Value>, depth: usize) -> Result<()> {
        if result.len() == result.capacity() {
            result.reserve(1);
        }
        self.value_into(depth, &mut result.spare_capacity_mut()[0])?;
        // SAFETY: value_into initializes the spare slot only on success. Until
        // then len excludes it, so errors and unwinding drop only the prefix.
        unsafe { result.set_len(result.len() + 1) };
        Ok(())
    }
    #[inline(always)]
    fn value(&mut self, depth: usize) -> Result<Value> {
        let mut result = MaybeUninit::uninit();
        self.value_into(depth, &mut result)?;
        // SAFETY: Successful value_into initializes exactly one Value.
        Ok(unsafe { result.assume_init() })
    }
    // Write exactly once, after all fallible work for the value is complete.
    // On error or unwind the destination remains uninitialized.
    #[inline(always)]
    fn value_into(&mut self, depth: usize, out: &mut MaybeUninit<Value>) -> Result<()> {
        if depth > self.limits.max_depth {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        self.height = self.height.max(depth);
        self.charge(1, std::mem::size_of::<Value>())?;
        let tag = self.u8()?;
        match tag {
            3 => {
                out.write(Value::Null);
            }
            8 => {
                out.write(Value::Bool(true));
            }
            9 => {
                out.write(Value::Bool(false));
            }
            4 => {
                out.write(Value::Bool(self.u8()? != 0));
            }
            104 => {
                out.write(Value::MetaProtocolKey);
            }
            10 => {
                out.write(Value::Char(self.i16()? as u16));
            }
            40 => {
                out.write(Value::Byte(self.u8()? as i8));
            }
            41 => {
                out.write(Value::Short(self.i16()?));
            }
            42 => {
                out.write(Value::Integer(self.i32()?));
            }
            0 => {
                out.write(Value::Long(0));
            }
            43 => {
                out.write(Value::Long(self.i64()?));
            }
            100 => {
                out.write(Value::Long(self.u8()? as i8 as i64));
            }
            101 => {
                out.write(Value::Long(self.i16()? as i64));
            }
            102 => {
                out.write(Value::Long(self.i32()? as i64));
            }
            87 | 93 => {
                let n = (self.u8()? ^ 0x80) as i64;
                out.write(Value::Long(if tag == 93 { -n } else { n }));
            }
            88 | 94 => {
                let n = (self.i16()? as u16 ^ 0x8000) as i64;
                out.write(Value::Long(if tag == 94 { -n } else { n }));
            }
            89 | 95 => {
                let n = (self.i32()? as u32 ^ 0x80000000) as i64;
                out.write(Value::Long(if tag == 95 { -n } else { n }));
            }
            55 => {
                out.write(Value::Double(0));
            }
            60 => {
                out.write(Value::Float(self.i32()? as u32));
            }
            61 => {
                out.write(Value::Double(self.i64()? as u64));
            }
            17 => {
                out.write(Value::Vector(Vec::new()));
            }
            97 => {
                let n = (self.u8()? ^ 0x80) as usize;
                out.write(Value::Vector(self.values(n, depth + 1)?));
            }
            113 => {
                out.write(Value::Vector(self.values(2, depth + 1)?));
            }
            114 => {
                out.write(Value::Vector(self.values(3, depth + 1)?));
            }
            _ => {
                out.write(self.compound_value(tag, depth + 1)?);
            }
        }
        Ok(())
    }
    fn compound_value(&mut self, tag: u8, d: usize) -> Result<Value> {
        Ok(match tag {
            44 => Value::BigInt(self.big()?),
            45 => Value::BigInteger(self.big()?),
            62 => Value::BigDecimal {
                unscaled: self.big()?,
                scale: self.i32()?,
            },
            70 => Value::Ratio {
                numerator: self.big()?,
                denominator: self.big()?,
            },
            34 => Value::Text(String::new()),
            96 => Value::Text(self.string(0)?),
            105 => Value::Text(self.string(1)?),
            16 => Value::Text(self.string(2)?),
            13 => Value::Text(self.string(4)?),
            106 => Value::Keyword(self.keyword(1)?),
            85 => Value::Keyword(self.keyword(2)?),
            77 | 14 => Value::Keyword(self.keyword(4)?),
            56 => Value::Symbol(self.string(1)?),
            86 => Value::Symbol(self.string(2)?),
            78 | 57 => Value::Symbol(self.string(4)?),
            47 => Value::Reader(self.string(1)?),
            51 => Value::Reader(self.string(2)?),
            52 | 5 => Value::Reader(self.string(4)?),
            53 => Value::Bytes(Vec::new()),
            7 => Value::Bytes(self.blob(1)?),
            15 => Value::Bytes(self.blob(2)?),
            2 => Value::Bytes(self.blob(4)?),
            110 | 69 | 21 => {
                let n = self.count(match tag {
                    110 => 1,
                    69 => 2,
                    _ => 4,
                })?;
                Value::Vector(self.values(n, d)?)
            }
            18 => Value::Set(Vec::new()),
            98 | 111 | 32 | 23 | 28 => {
                let n = self.count(match tag {
                    98 => 0,
                    111 => 1,
                    32 => 2,
                    _ => 4,
                })?;
                let v = self.values(n, d)?;
                if tag == 28 {
                    Value::SortedSet(v)
                } else {
                    Value::Set(v)
                }
            }
            19 => Value::Map(Vec::new()),
            99 | 112 | 33 | 30 | 31 | 22 | 27 | 29 | 123 => {
                let mut n = self.count(match tag {
                    99 | 123 => 0,
                    112 => 1,
                    33 => 2,
                    _ => 4,
                })?;
                if tag == 27 || tag == 29 {
                    if n % 2 != 0 {
                        return Err(self.err(ErrorKind::InvalidLength));
                    }
                    n /= 2;
                }
                let v = self.pairs(n, d)?;
                if tag == 31 || tag == 29 {
                    Value::SortedMap(v)
                } else {
                    Value::Map(v)
                }
            }
            35 => Value::List(Vec::new()),
            37 => Value::Seq(Vec::new()),
            36 | 54 | 20 | 38 | 39 | 24 | 26 => {
                let n = self.count(match tag {
                    36 | 38 => 1,
                    54 | 39 => 2,
                    _ => 4,
                })?;
                let v = self.values(n, d)?;
                match tag {
                    36 | 54 | 20 => Value::List(v),
                    26 => Value::Queue(v),
                    _ => Value::Seq(v),
                }
            }
            103 => Value::MapEntry(Box::new((self.value(d)?, self.value(d)?))),
            25 => Value::Meta {
                metadata: Box::new(self.value(d)?),
                value: Box::new(self.value(d)?),
            },
            58 => Value::Regex(self.text_value(d)?),
            71 => Value::Uri(self.text_value(d)?),
            90 => Value::Date(self.i64()?),
            92 => Value::SqlDate(self.i64()?),
            91 => Value::Uuid(self.take(16)?.try_into().unwrap()),
            79 | 83 => {
                let seconds = self.i64()?;
                let nanos = self.i32()?;
                if !(0..1_000_000_000).contains(&nanos) {
                    return Err(self.err(ErrorKind::InvalidValue));
                }
                if tag == 79 {
                    Value::Instant { seconds, nanos }
                } else {
                    Value::Duration { seconds, nanos }
                }
            }
            84 => Value::Period {
                years: self.i32()?,
                months: self.i32()?,
                days: self.i32()?,
            },
            107 | 115 => {
                let n = self.count(4)?;
                let v = self.values(n, d)?;
                if tag == 107 {
                    if v.iter().any(|x| !matches!(x, Value::Null | Value::Text(_))) {
                        return Err(self.err(ErrorKind::InvalidValue));
                    }
                    Value::StringArray(v)
                } else {
                    Value::ObjectArray(v)
                }
            }
            118..=121 => {
                let n = self.count(4)?;
                match tag {
                    118 => Value::IntArray(self.numeric_words(n, i32::from_be_bytes)?),
                    119 => Value::LongArray(self.numeric_words(n, i64::from_be_bytes)?),
                    120 => Value::FloatArray(self.numeric_words(n, u32::from_be_bytes)?),
                    _ => Value::DoubleArray(self.numeric_words(n, u64::from_be_bytes)?),
                }
            }
            108 | 109 | 116 | 117 => {
                let n = self.count(4)?;
                let v = self.values(n, d)?;
                macro_rules! array {
                    ($variant:ident, $pattern:pat => $expr:expr) => {{
                        let mut result = Vec::with_capacity(n);
                        for value in v {
                            result.push(match value {
                                $pattern => $expr,
                                _ => return Err(self.err(ErrorKind::InvalidValue)),
                            });
                        }
                        Value::$variant(result)
                    }};
                }
                match tag {
                    108 => array!(LongArray, Value::Long(n) => n),
                    109 => array!(IntArray, Value::Integer(n) => n),
                    116 => array!(DoubleArray, Value::Double(n) => n),
                    _ => array!(FloatArray, Value::Float(n) => n),
                }
            }
            48 | 49 | 80 => {
                let class = self.string(match tag {
                    48 => 1,
                    49 => 2,
                    _ => 4,
                })?;
                Value::Record {
                    class,
                    fields: Box::new(self.value(d)?),
                }
            }
            122 => {
                let class = self.text_value(d)?;
                let n = self.count(0)?;
                Value::Deftype {
                    class,
                    fields: self.values(n, d)?,
                }
            }
            81 => {
                let class = self.text_value(d)?;
                let n = *self
                    .legacy_deftypes
                    .get(&class)
                    .ok_or_else(|| self.err(ErrorKind::UnsupportedType(81)))?;
                Value::Deftype {
                    class,
                    fields: self.values(n, d)?,
                }
            }
            75 | 76 => {
                let class = self.string(if tag == 75 { 1 } else { 2 })?;
                let bytes = match self.value(d)? {
                    Value::Bytes(v) => v,
                    _ => return Err(self.err(ErrorKind::InvalidValue)),
                };
                if matches!(
                    class.as_str(),
                    "[Z" | "[S" | "[C" | "[B" | "[I" | "[J" | "[F" | "[D"
                ) {
                    self.charge(0, bytes.len())?;
                }
                java_array::decode(&class, &bytes, self.limits.max_collection_len)
                    .map_err(|kind| self.err(kind))?
                    .unwrap_or(Value::Serializable { class, bytes })
            }
            59 | 63 | 64 | 65 | 66 | 72 | 73 | 74 | 67 | 68 => {
                let index = match tag {
                    59 => 0,
                    63 => 1,
                    64 => 2,
                    65 => 3,
                    66 => 4,
                    72 => 5,
                    73 => 6,
                    74 => 7,
                    67 => self.count(1)?,
                    _ => self.count(2)?,
                };
                self.cached(index, d)?
            }
            124 => {
                let n = self.count(2)?;
                let hash = self.i64()? as u64;
                if self.dict_count != 0
                    || n == 0
                    || self.dictionary.and_then(|dict| dict.hashes.get(n)).copied() != Some(hash)
                {
                    return Err(self.err(ErrorKind::DictionaryMismatch));
                }
                self.dict_count = n;
                self.value(d)?
            }
            82 => {
                let id = self.i16()?;
                self.custom(id, d)?
            }
            128..=255 => self.custom(tag as i8 as i16, d)?,
            // Legacy Java readUTF/deftype and pre-2.15 unframed Serializable
            // require JVM class information; never guess where a payload ends.
            _ => return Err(self.err(ErrorKind::UnsupportedType(tag as i16))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_failed_value_releases_references(suffix: &[u8]) {
        // Define a cached keyword, then fail while a later value owns clones.
        let mut wire = vec![59, 106, 1, b'k'];
        wire.extend_from_slice(suffix);
        let mut decoder = Decoder::new(&wire, Limits::default()).unwrap();
        let Value::Keyword(keyword) = decoder.read_value().unwrap() else {
            panic!("expected the cache definition");
        };
        let weak = Arc::downgrade(&keyword.name);
        drop(keyword);
        assert_eq!(weak.strong_count(), 1);
        assert!(decoder.read_value().is_err());
        // Only the decoder's cache still owns the name. Every partially
        // initialized collection and temporary must have released its clones.
        assert_eq!(weak.strong_count(), 1, "wire suffix: {suffix:?}");
        drop(decoder);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn failed_collection_slots_release_the_initialized_prefix() {
        for count in [1usize, 2, 3, 4, 32, 257, 2048] {
            for fault in [
                &[126][..],           // Unsupported tag.
                &[42, 0][..],         // Truncated integer.
                &[96, 131, b'x'][..], // Truncated string.
                &[96, 129, 0xff][..], // Invalid UTF-8.
                &[97, 129][..],       // Truncated singleton vector.
                &[82, 0, 0][..],      // Unknown custom codec.
            ] {
                for object_array in [false, true] {
                    let mut suffix = if object_array {
                        let mut header = vec![115];
                        header.extend_from_slice(&(count as i32).to_be_bytes());
                        header
                    } else if count <= 255 {
                        vec![97, count as u8 ^ 0x80]
                    } else {
                        let mut header = vec![69];
                        header.extend_from_slice(&(count as i16).to_be_bytes());
                        header
                    };
                    suffix.extend(std::iter::repeat_n(59, count - 1));
                    // The failing final element also has an initialized prefix.
                    suffix.extend_from_slice(&[113, 59]);
                    suffix.extend_from_slice(fault);
                    assert_failed_value_releases_references(&suffix);
                }
            }
        }
        // Failure after a map key is decoded, and after a string-array payload
        // is decoded but fails its element-type validation.
        assert_failed_value_releases_references(&[99, 130, 59, 3, 59, 126]);
        assert_failed_value_releases_references(&[107, 0, 0, 0, 2, 59, 59]);
    }
}
