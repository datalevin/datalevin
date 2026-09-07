use datalevin_codec::nippy::Value;

pub fn fixtures() -> Vec<(&'static str, Value)> {
    use Value::*;
    let bitmap = [0, 1, 65536, u32::MAX].into_iter().collect();
    vec![
        ("nil", Null),
        ("true", Bool(true)),
        ("false", Bool(false)),
        ("char", Char(0x0cac)),
        ("surrogate", Char(0xd800)),
        ("byte", Byte(-128)),
        ("short", Short(-32768)),
        ("integer", Integer(i32::MIN)),
        ("long-min", Long(i64::MIN)),
        ("long-max", Long(i64::MAX)),
        (
            "long-boundaries",
            Vector(
                [
                    0, 1, -1, 127, 128, 255, 256, -255, -256, 65535, 65536, -65536, 4294967295,
                    4294967296,
                ]
                .into_iter()
                .map(Long)
                .collect(),
            ),
        ),
        ("float", Value::float(3.5)),
        ("double", Value::double(-3.5)),
        ("bigint", BigInt(vec![0, 0x80])),
        ("biginteger", BigInteger(vec![0xff, 0x7f])),
        (
            "bigdec",
            BigDecimal {
                unscaled: vec![0x30, 0x39],
                scale: 2,
            },
        ),
        (
            "ratio",
            Ratio {
                numerator: vec![22],
                denominator: vec![7],
            },
        ),
        ("text", Text("ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ".into())),
        ("keyword", Keyword("some/name".into())),
        ("symbol", Symbol("some/name".into())),
        ("bytes", Bytes(vec![0, 128, 255])),
        (
            "map",
            Map(vec![
                (Keyword("a".into()), Long(1)),
                (Keyword("b".into()), Long(2)),
            ]),
        ),
        ("set", Set(vec![Long(1), Long(2)])),
        ("sorted-set", SortedSet(vec![Long(1), Long(2)])),
        (
            "sorted-map",
            SortedMap(vec![(Keyword("a".into()), Long(1))]),
        ),
        ("list", List(vec![Long(1), Long(2)])),
        ("seq", Seq(vec![Long(1), Long(2)])),
        ("queue", Queue(vec![Long(1), Long(2)])),
        (
            "entry",
            MapEntry(Box::new((Text("key".into()), Text("val".into())))),
        ),
        (
            "meta",
            Meta {
                metadata: Box::new(Map(vec![(Keyword("m".into()), Bool(true))])),
                value: Box::new(Vector(vec![Long(1)])),
            },
        ),
        ("regex", Regex("(?i)hello".into())),
        ("uri", Uri("https://clojure.org".into())),
        ("uuid", Uuid([0; 16])),
        ("date", Date(1577884455500)),
        ("sql-date", SqlDate(1577884455500)),
        (
            "instant",
            Instant {
                seconds: 100,
                nanos: 12,
            },
        ),
        (
            "duration",
            Duration {
                seconds: -100,
                nanos: 12,
            },
        ),
        (
            "period",
            Period {
                years: 1,
                months: -2,
                days: 3,
            },
        ),
        ("int-array", IntArray(vec![i32::MIN, 0, i32::MAX])),
        ("boolean-array", BooleanArray(vec![true, false, true])),
        ("short-array", ShortArray(vec![i16::MIN, 0, i16::MAX])),
        ("char-array", CharArray(vec![0, 0xd800, 0xffff])),
        ("long-array", LongArray(vec![i64::MIN, 0, i64::MAX])),
        (
            "float-array",
            FloatArray(vec![
                f32::NAN.to_bits(),
                (-0.0f32).to_bits(),
                f32::INFINITY.to_bits(),
            ]),
        ),
        (
            "double-array",
            DoubleArray(vec![
                f64::NAN.to_bits(),
                (-0.0f64).to_bits(),
                f64::NEG_INFINITY.to_bits(),
            ]),
        ),
        (
            "string-array",
            StringArray(vec![Null, Text("hello".into())]),
        ),
        (
            "object-array",
            ObjectArray(vec![Long(1), Keyword("a".into())]),
        ),
        (
            "record",
            Record {
                class: "taoensso.nippy.StressRecord".into(),
                fields: Box::new(Map(vec![(Keyword("x".into()), Text("data".into()))])),
            },
        ),
        (
            "deftype",
            Deftype {
                class: "taoensso.nippy.StressType".into(),
                fields: vec![Text("normal field".into()), Text("private field".into())],
            },
        ),
        (
            "datom",
            Datom {
                entity: 42,
                attribute: Box::new(Keyword("name".into())),
                value: Box::new(Text("Ada".into())),
                tx: Box::new(Long(-536870913)),
            },
        ),
        ("bitmap", Bitmap(bitmap)),
        ("growing-small", GrowingIntArray(vec![1, -1, 7])),
        (
            "growing-packed",
            GrowingIntArray((0..257).map(|n| if n % 31 == 0 { -n } else { n }).collect()),
        ),
        (
            "sparse",
            SparseIntArray {
                items: vec![1, -1, 7],
                indices: [0, 65536, u32::MAX].into_iter().collect(),
            },
        ),
        (
            "spill-vector",
            SpillableVector(vec![Long(1), Keyword("a".into())]),
        ),
        (
            "spill-map",
            SpillableMap(Box::new(Map(vec![(Keyword("a".into()), Long(1))]))),
        ),
        (
            "spill-set",
            SpillableSet(Box::new(Set(vec![Long(1), Long(2)]))),
        ),
    ]
}

pub fn unhex(text: &str) -> Result<Vec<u8>, &'static str> {
    if !text.len().is_multiple_of(2) {
        return Err("odd hex length");
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| {
            let h = (c[0] as char).to_digit(16).ok_or("invalid hex")?;
            let l = (c[1] as char).to_digit(16).ok_or("invalid hex")?;
            Ok((h * 16 + l) as u8)
        })
        .collect()
}
