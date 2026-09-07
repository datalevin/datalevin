# Nippy codec fuzzing

The Rust targets exercise headerless Nippy and its existing compressed NPY
headers. Successful reads must re-encode to a stable representation, and
allocating and caller-owned encoders must agree. Structured errors must point
within the input. Input size, depth, collection lengths, expanded nodes, cache
copies, and decompression are bounded.

Install prerequisites once:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

Run a bounded campaign:

```sh
script/nippy-fuzz smoke 1000
script/nippy-fuzz rust 60
```

The script seeds both JVM version corpora and all supported NPY compressors.
Generated corpora and crash artifacts stay under `target/nippy-fuzz`.
`cargo test --manifest-path src/rust/Cargo.toml` also runs deterministic cases
and property tests on arbitrary bytes without requiring nightly/libFuzzer.
