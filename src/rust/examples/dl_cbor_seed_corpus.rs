use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let golden = required_path(&mut arguments, "golden vectors")?;
    let malformed = required_path(&mut arguments, "malformed vectors")?;
    let output = required_path(&mut arguments, "output directory")?;
    if arguments.next().is_some() {
        return Err("usage: dl_cbor_seed_corpus GOLDEN MALFORMED OUTPUT".into());
    }

    let bare = output.join("bare");
    let storage = output.join("storage");
    fs::create_dir_all(&bare)?;
    fs::create_dir_all(&storage)?;

    let mut bare_count = 0;
    let mut storage_count = 0;
    for columns in rows(&golden, 8)? {
        let id = safe_id(&columns[0])?;
        write_seed(&bare, &format!("valid-{id}"), &columns[4])?;
        write_seed(&storage, &format!("valid-{id}"), &columns[5])?;
        bare_count += 1;
        storage_count += 1;
    }
    for columns in rows(&malformed, 5)? {
        let id = safe_id(&columns[0])?;
        let operation = &columns[1];
        let directory = if operation == "storage" {
            storage_count += 1;
            &storage
        } else {
            bare_count += 1;
            &bare
        };
        write_seed(directory, &format!("invalid-{id}"), &columns[2])?;
    }

    println!(
        "prepared {bare_count} bare and {storage_count} storage DL-CBOR fuzz seeds in {}",
        output.display()
    );
    Ok(())
}

fn required_path(
    arguments: &mut impl Iterator<Item = std::ffi::OsString>,
    name: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    arguments.next().map(PathBuf::from).ok_or_else(|| {
        format!("missing {name}; usage: dl_cbor_seed_corpus GOLDEN MALFORMED OUTPUT").into()
    })
}

fn rows(path: &Path, width: usize) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    contents
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
        .map(|(index, line)| {
            let columns: Vec<_> = line.split('\t').map(str::to_owned).collect();
            if columns.len() != width {
                Err(format!(
                    "{}:{} has {} columns; expected {width}",
                    path.display(),
                    index + 1,
                    columns.len()
                )
                .into())
            } else {
                Ok(columns)
            }
        })
        .collect()
}

fn safe_id(id: &str) -> Result<&str, Box<dyn Error>> {
    if !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(id)
    } else {
        Err(format!("unsafe corpus id {id:?}").into())
    }
}

fn write_seed(directory: &Path, name: &str, hex: &str) -> Result<(), Box<dyn Error>> {
    fs::write(directory.join(name), decode_hex(hex)?)?;
    Ok(())
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if !hex.len().is_multiple_of(2) {
        return Err(format!("odd hex length: {hex}").into());
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(byte: u8) -> Result<u8, Box<dyn Error>> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit {:?}", char::from(byte)).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_decoder_accepts_empty_and_mixed_case() {
        assert_eq!(Vec::<u8>::new(), decode_hex("").unwrap());
        assert_eq!(vec![0, 0xab, 0xcd, 0xef], decode_hex("00aBcDeF").unwrap());
    }

    #[test]
    fn hex_decoder_rejects_bad_input() {
        assert!(decode_hex("0").is_err());
        assert!(decode_hex("xz").is_err());
    }
}
