mod support;
use datalevin_codec::nippy::*;
use std::{fs, path::PathBuf};

fn main() -> Result<()> {
    let output = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .expect("usage: nippy_seed_corpus OUTPUT"),
    );
    let io_error = |_| Error {
        kind: ErrorKind::Io,
        offset: 0,
    };
    fs::create_dir_all(output.join("bare")).map_err(io_error)?;
    fs::create_dir_all(output.join("header")).map_err(io_error)?;
    for (version, corpus) in [
        include_str!("../../../resources/datalevin/nippy/jvm-3.7.0-beta1.tsv"),
        include_str!("../../../resources/datalevin/nippy/jvm-3.9.0.tsv"),
    ]
    .iter()
    .enumerate()
    {
        for (index, line) in corpus.lines().filter(|l| !l.starts_with('#')).enumerate() {
            let (_, hex) = line.split_once('\t').expect("fixture row");
            let bytes = support::unhex(hex).expect("fixture hex");
            fs::write(output.join(format!("bare/jvm-{version}-{index}")), bytes)
                .map_err(io_error)?;
        }
    }
    for (name, value) in support::fixtures() {
        for (index, compression) in [
            Compression::None,
            Compression::Lz4,
            Compression::Snappy,
            Compression::Zstd,
            Compression::Lzma2,
        ]
        .iter()
        .enumerate()
        {
            fs::write(
                output.join(format!("header/{name}-{index}")),
                freeze(&value, *compression)?,
            )
            .map_err(io_error)?;
        }
    }
    Ok(())
}
