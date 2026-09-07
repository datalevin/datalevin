//! Bounded, line-oriented compatibility oracle. Stdout is protocol only.
mod support;
use datalevin_codec::nippy::*;
use std::io::{self, BufRead, Read, Write};

fn request(op: &str, payload: &str) -> std::result::Result<String, Box<dyn std::error::Error>> {
    let out = match op {
        "fixture" => {
            let value = support::fixtures()
                .into_iter()
                .find(|(id, _)| *id == payload)
                .ok_or("unknown fixture")?
                .1;
            fast_freeze(&value)?
        }
        "roundtrip" => {
            let bytes = support::unhex(payload)?;
            let mut decoder = Decoder::new(&bytes, Limits::default())?
                .with_legacy_deftype("taoensso.nippy.StressType", 2);
            let value = decoder.read_value()?;
            if decoder.position() != bytes.len() {
                return Err("trailing bytes".into());
            }
            fast_freeze(&value)?
        }
        "thaw" => fast_freeze(&thaw(&support::unhex(payload)?)?)?,
        "dictionary" => {
            let dict = SharedDictionary::new(vec![
                Value::Keyword("name".into()),
                Value::Text("country".into()),
                Value::Text("currency".into()),
            ])?;
            let bytes = support::unhex(payload)?;
            let value = Decoder::new(&bytes, Limits::default())?
                .with_dictionary(&dict)
                .read_value()?;
            fast_freeze(&value)?
        }
        "freeze" => {
            let (kind, payload) = payload.split_once(':').ok_or("missing compressor")?;
            let compression = match kind {
                "none" => Compression::None,
                "lz4" => Compression::Lz4,
                "snappy" => Compression::Snappy,
                "zstd" => Compression::Zstd,
                "lzma2" => Compression::Lzma2,
                _ => return Err("unknown compressor".into()),
            };
            freeze(&fast_thaw(&support::unhex(payload)?)?, compression)?
        }
        _ => return Err("unknown operation".into()),
    };
    Ok(hex(&out))
}

fn main() -> io::Result<()> {
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    writeln!(output, "ready")?;
    output.flush()?;
    loop {
        let mut line = Vec::new();
        let n = (&mut input)
            .take(16 * 1024 * 1024)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request too large or truncated",
            ));
        }
        let line = std::str::from_utf8(&line)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim_end();
        if line == "quit" {
            break;
        }
        let (op, payload) = line.split_once('\t').unwrap_or((line, ""));
        match request(op, payload) {
            Ok(hex) => writeln!(output, "ok\t{hex}")?,
            Err(error) => writeln!(output, "error\t{error}")?,
        }
        output.flush()?;
    }
    Ok(())
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 15) as usize] as char);
    }
    out
}
