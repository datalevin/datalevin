use datalevin_codec::nippy::{
    Encoder, ErrorKind, Limits, Value, fast_freeze, fast_freeze_into, fast_thaw,
};
use std::io::{self, Write};

fn arrays(len: usize) -> [(Value, Vec<u8>); 4] {
    let words32: Vec<u32> = (0..len)
        .map(|i| {
            [
                0,
                u32::MAX,
                0x80000000,
                0x7fc01234,
                0x7f800000,
                0xff800000,
                0x01234567,
            ][i % 7]
        })
        .collect();
    let words64: Vec<u64> = (0..len)
        .map(|i| {
            [
                0,
                u64::MAX,
                0x8000000000000000,
                0x7ff8123456789abc,
                0x7ff0000000000000,
                0xfff0000000000000,
                0x0123456789abcdef,
            ][i % 7]
        })
        .collect();
    [
        (
            118,
            Value::IntArray(words32.iter().map(|v| *v as i32).collect()),
            words32
                .iter()
                .flat_map(|n| n.to_be_bytes())
                .collect::<Vec<_>>(),
        ),
        (
            119,
            Value::LongArray(words64.iter().map(|v| *v as i64).collect()),
            words64.iter().flat_map(|n| n.to_be_bytes()).collect(),
        ),
        (
            120,
            Value::FloatArray(words32.clone()),
            words32.iter().flat_map(|n| n.to_be_bytes()).collect(),
        ),
        (
            121,
            Value::DoubleArray(words64.clone()),
            words64.iter().flat_map(|n| n.to_be_bytes()).collect(),
        ),
    ]
    .map(|(tag, value, payload)| {
        let mut wire = vec![tag];
        wire.extend_from_slice(&(len as i32).to_be_bytes());
        wire.extend(payload);
        (value, wire)
    })
}

#[test]
fn bulk_arrays_preserve_wire_bits_and_buffer_boundaries() {
    // Both element widths around the small-array and 4 KiB block boundaries.
    for len in [
        0, 1, 8, 9, 15, 16, 511, 512, 513, 1023, 1024, 1025, 4096, 4097,
    ] {
        for (value, expected) in arrays(len) {
            assert_eq!(fast_freeze(&value).unwrap(), expected);
            assert_eq!(fast_thaw(&expected).unwrap(), value);
            let mut appended = vec![0xcc; 3];
            fast_freeze_into(&mut appended, &value).unwrap();
            assert_eq!(&appended[..3], &[0xcc; 3]);
            assert_eq!(&appended[3..], expected);
            // Non-word-aligned, exactly sized destination with untouched guards.
            let mut storage = vec![0xcc; expected.len() + 6];
            let mut encoder = Encoder::new(&mut storage[3..3 + expected.len()], Limits::default());
            encoder.write_value(&value).unwrap();
            assert_eq!(encoder.position(), expected.len());
            assert_eq!(&storage[3..3 + expected.len()], expected);
            assert_eq!(&storage[..3], &[0xcc; 3]);
            assert_eq!(&storage[3 + expected.len()..], &[0xcc; 3]);
        }
    }
}

struct ShortWrites {
    bytes: Vec<u8>,
    remaining: usize,
    interrupt: bool,
}

impl Write for ShortWrites {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.interrupt {
            self.interrupt = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.remaining == 0 {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let n = bytes.len().min(3).min(self.remaining);
        self.bytes.extend_from_slice(&bytes[..n]);
        self.remaining -= n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn bulk_arrays_respect_limits_and_stream_failures() {
    for (value, expected) in arrays(4097) {
        let limits = Limits {
            max_bytes: expected.len(),
            ..Limits::default()
        };
        let mut out = Vec::new();
        Encoder::new(&mut out, limits).write_value(&value).unwrap();
        assert_eq!(out, expected);
        let mut out = Vec::new();
        let err = Encoder::new(
            &mut out,
            Limits {
                max_bytes: expected.len() - 1,
                ..limits
            },
        )
        .write_value(&value)
        .unwrap_err();
        assert_eq!(err.kind, ErrorKind::LimitExceeded);
        assert!(out.len() < expected.len());
        let mut out = Vec::new();
        assert_eq!(
            Encoder::new(
                &mut out,
                Limits {
                    max_collection_len: 4096,
                    ..limits
                }
            )
            .write_value(&value)
            .unwrap_err()
            .kind,
            ErrorKind::LimitExceeded
        );
        assert!(out.is_empty());

        let mut writer = ShortWrites {
            bytes: Vec::new(),
            remaining: usize::MAX,
            interrupt: true,
        };
        let mut encoder = Encoder::new(&mut writer, Limits::default());
        encoder.write_value(&value).unwrap();
        assert_eq!(encoder.position(), expected.len());
        assert_eq!(writer.bytes, expected);
        let mut writer = ShortWrites {
            bytes: Vec::new(),
            remaining: 5 + 4096 + 3,
            interrupt: false,
        };
        let mut encoder = Encoder::new(&mut writer, Limits::default());
        let error = encoder.write_value(&value).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Io);
        assert_eq!(error.offset, 5 + 4096);
        assert_eq!(encoder.position(), 5 + 4096);
        assert_eq!(writer.bytes, expected[..5 + 4096 + 3]);

        let nested = Value::Vector(vec![value, Value::Keyword("k".repeat(32768))]);
        let mut out = vec![0xcc; 3];
        assert!(fast_freeze_into(&mut out, &nested).is_err());
        assert_eq!(out, [0xcc; 3]);
    }
}
