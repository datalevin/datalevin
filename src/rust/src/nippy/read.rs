use super::*;

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
    nodes: usize,
    allocated: usize,
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
            nodes: 0,
            allocated: 0,
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
        self.nodes = self
            .nodes
            .checked_add(nodes)
            .ok_or_else(|| self.err(ErrorKind::LimitExceeded))?;
        self.allocated = self
            .allocated
            .checked_add(bytes)
            .ok_or_else(|| self.err(ErrorKind::LimitExceeded))?;
        if self.nodes > self.limits.max_values || self.allocated > self.limits.max_allocation_bytes
        {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
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
    fn string(&mut self, width: u8) -> Result<String> {
        let n = self.count(width)?;
        self.charge(0, n)?;
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| self.err(ErrorKind::InvalidUtf8))?;
        Ok(s.to_owned())
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
    fn values(&mut self, n: usize, depth: usize) -> Result<Vec<Value>> {
        self.collection(n, 1)?;
        let mut result = Vec::with_capacity(n.min(256));
        for _ in 0..n {
            result.push(self.value(depth)?);
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
                Value::Text(s) | Value::Keyword(s) => s.len(),
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
        let (nodes, bytes, previous_height) = (self.nodes, self.allocated, self.height);
        self.height = depth;
        let value = self.value(depth)?;
        let height = self.height - depth;
        self.height = self.height.max(previous_height);
        let (nodes, bytes) = (self.nodes - nodes, self.allocated - bytes);
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
                    self.limits
                        .max_allocation_bytes
                        .saturating_sub(self.allocated),
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
                self.collection(n, 4)?;
                self.charge(0, n * 4)?;
                let mut words = Vec::with_capacity(n);
                for _ in 0..n {
                    words.push(self.i32()?);
                }
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
    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > self.limits.max_depth {
            return Err(self.err(ErrorKind::LimitExceeded));
        }
        self.height = self.height.max(depth);
        self.charge(1, std::mem::size_of::<Value>())?;
        let tag = self.u8()?;
        let d = depth + 1;
        Ok(match tag {
            3 => Value::Null,
            8 => Value::Bool(true),
            9 => Value::Bool(false),
            4 => Value::Bool(self.u8()? != 0),
            104 => Value::MetaProtocolKey,
            10 => Value::Char(self.i16()? as u16),
            40 => Value::Byte(self.u8()? as i8),
            41 => Value::Short(self.i16()?),
            42 => Value::Integer(self.i32()?),
            0 => Value::Long(0),
            43 => Value::Long(self.i64()?),
            100 => Value::Long(self.u8()? as i8 as i64),
            101 => Value::Long(self.i16()? as i64),
            102 => Value::Long(self.i32()? as i64),
            87 | 93 => {
                let n = (self.u8()? ^ 0x80) as i64;
                Value::Long(if tag == 93 { -n } else { n })
            }
            88 | 94 => {
                let n = (self.i16()? as u16 ^ 0x8000) as i64;
                Value::Long(if tag == 94 { -n } else { n })
            }
            89 | 95 => {
                let n = (self.i32()? as u32 ^ 0x80000000) as i64;
                Value::Long(if tag == 95 { -n } else { n })
            }
            55 => Value::Double(0),
            60 => Value::Float(self.i32()? as u32),
            61 => Value::Double(self.i64()? as u64),
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
            106 => Value::Keyword(self.string(1)?),
            85 => Value::Keyword(self.string(2)?),
            77 | 14 => Value::Keyword(self.string(4)?),
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
            17 => Value::Vector(Vec::new()),
            113 => Value::Vector(self.values(2, d)?),
            114 => Value::Vector(self.values(3, d)?),
            97 | 110 | 69 | 21 => {
                let n = self.count(match tag {
                    97 => 0,
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
                let width = if tag == 118 || tag == 120 { 4 } else { 8 };
                self.collection(n, width)?;
                self.charge(0, n * width)?;
                match tag {
                    118 => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(self.i32()?);
                        }
                        Value::IntArray(v)
                    }
                    119 => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(self.i64()?);
                        }
                        Value::LongArray(v)
                    }
                    120 => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(self.i32()? as u32);
                        }
                        Value::FloatArray(v)
                    }
                    _ => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(self.i64()? as u64);
                        }
                        Value::DoubleArray(v)
                    }
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
