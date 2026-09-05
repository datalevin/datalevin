# DL-CBOR coverage-guided fuzzing

The Rust fuzz targets exercise the bare DL-CBOR decoder and the storage
collision envelope independently under libFuzzer. They use binary seed corpora
regenerated from the checked-in settled-core and provisional extension valid
and malformed protocol vectors. The extension seeds exercise the draft tag,
including every supported Java-regex flag and invalid flag envelopes; they do
not make that tag durable or assigned.

Install the Rust prerequisites once:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

Then run both targets for a bounded smoke campaign:

```sh
script/cbor-fuzz smoke                 # 1,000 inputs per target
script/cbor-fuzz smoke 10000
```

Run a longer time-bounded campaign with:

```sh
script/cbor-fuzz rust 300              # 300 seconds per Rust target
```

Generated corpora, minimized reproducers, and crash artifacts stay under
`target/cbor-fuzz/`. A successful decode is not enough to pass: the harnesses
also require canonical bytes to be a fixed point, allocating and caller-owned
encoders to agree exactly, encoded-size calculations to agree, storage escaping
to round-trip, and every structured error offset to remain within the input.
Inputs are capped at 64 KiB, depth at 64, and collection length at 4,096 so a
malformed length cannot turn a fuzz worker into an allocation stress test.
