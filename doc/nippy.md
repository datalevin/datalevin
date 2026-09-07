# Rust Nippy codec

Datalevin retains Nippy on the JVM. The CBOR experiment was retired on
2026-09-06 after failing the performance gate against Nippy 3.9. This removes
the need for a storage migration and a replacement of existing JVM storage,
client/server, dump, and internal serialization paths. Historical measurement
artifacts remain under `benchmarks/codec-bench/results`.

`src/rust` implements the existing Nippy format. Production JVM dependencies
are pinned to **3.9.0**; fixtures retain **3.7.0-beta1** compatibility coverage.
The format reference is the `schema.clj`, `io.clj`, and custom freezer implementations in
those resolved releases, with checked-in JVM-generated fixtures. This is a
value codec; Datalevin's sortable index-key encodings remain separate.

## API

```rust
use datalevin_codec::nippy::{Value, fast_freeze, fast_thaw};

let value = Value::Map(vec![
    (Value::Keyword("name".into()), Value::Text("Ada".into())),
]);
let bytes = fast_freeze(&value)?;
assert_eq!(fast_thaw(&bytes)?, value);
```

- `fast_freeze` / `fast_thaw`: headerless Nippy, as used by `datalevin.bits`.
- `freeze` / `thaw`: the existing `NPY` header, with no compression, LZ4,
  Snappy, Zstandard, or LZMA2/XZ. These compressor wrappers are separate from
  Datalevin's client/server message framing and negotiated Zstandard layer.
- `fast_freeze_into`: append into a reusable `Vec<u8>`, rolling back on error.
- `Encoder<W: Write>`: write directly into a caller-owned slice, buffer, or
  stream; retains cache state across values in one Nippy session.
- `Decoder`: reads from a borrowed slice with a position cursor and explicit
  limits. A session can retain cached references across multiple values.
  Strict single-value APIs reject trailing bytes. Discard a session after an
  error rather than continuing from partially consumed data.

## Values and existing extensions

The codec supports primitives, numeric widths, big numbers, UTF-8 names and
strings, primitive and object arrays, vectors, maps, sets, lists, sequences,
queues, sorted collections, map entries, metadata, regex pattern strings,
UUIDs, URIs, dates, Java time values, records, and deftypes. Map entries and set
members retain their sequence in Rust without imposing Rust hashing or
deduplication. Floats retain their IEEE bits. Nippy itself normalizes some
values, such as scalar negative zero and regex flags; the Rust codec does not
add information absent from the payload.

Existing custom IDs are verified against
`taoensso.nippy.impl/coerce-custom-type-id`:

| JVM registration | ID | Rust representation |
| --- | ---: | --- |
| `:datalevin/datom` | -27026 | Entity ID, attribute, value, signed transaction |
| `:datalevin/entity` | 6354 | Inert entity descriptor map, including touched cache |
| `:datalevin/inter-fn` | -10700 | Inert source form, including nested closures |
| `:dtlv/bm` | 25371 | Standard portable Roaring bitmap |
| `:dtlv/gia` | 1423 | Integer array; JavaFastPFOR BinaryPacking + VariableByte |
| `:dtlv/sial` | -2438 | Integer values followed by bitmap indices |
| `:spillable-vec` | -3600 | Counted values |
| `:spillable-map` | 28032 | Map payload |
| `:spillable-set` | -25293 | Set payload, also used by UniqueVectorSet |

Roaring's library owns its container format. Integer compression follows the
existing JavaFastPFOR `IntCompressor` default. Neither gets a new codec layout.
Boolean, short, and character arrays use Nippy's Java Object Serialization
layout and decode to native Rust arrays. Other Java-serialized objects, such
as exceptions, retain the class name and opaque bytes. Records and functions
retain data; decoding does not load classes, access databases, or execute code.

## Compatibility boundaries

Nippy 3.7-beta's deftype tag **81** has no field count. A Rust decoder needs an
explicit schema, e.g. `.with_legacy_deftype("example.Pair", 2)`. Unknown legacy
deftypes fail because their byte boundary cannot be determined without that
information. The test harness registers the upstream `StressType` schema.
The default encoder writes the 3.7-compatible form. An encoder configured with
`WireVersion::V3_9` writes tag **122**, whose field count is self-contained.

Repeated keyword encoding and explicit cached values use Nippy's existing
reference tags. The reader reserves nested cache definitions before reading
their values and rejects cycles, skipped indices, and excessive expansion.
Nippy 3.9 shared dictionaries can be supplied explicitly through
`SharedDictionary` and `Decoder::with_dictionary`; their prefix fingerprints
are validated. Default Rust output uses an ordinary per-session cache and
requires no external dictionary.

Unknown custom IDs, unregistered legacy deftypes, pre-2.15 unframed Java
Serializable objects, encryption, and application-specific compressor headers
fail explicitly. EDN fallback payloads remain inert text. Arbitrary custom
comparator execution and application-defined codecs remain later work.

Default read limits bound input and decompressed bytes, nesting, collection
lengths, materialized nodes, and owned-value/cache allocation. Errors include
a kind and byte offset. Errors inside a compressed payload report its starting
offset, since decompressed offsets do not identify positions in the input.

## Validation

```sh
clojure -T:build compile-java
cargo test --manifest-path src/rust/Cargo.toml
cargo test --manifest-path src/rust/test-adapter/Cargo.toml
lein test datalevin.test.codec.nippy-cross-language-test
```

The cross-language suite covers every upstream Nippy stress leaf and the
complete stress map, independent Rust-authored fixtures, nested cached values,
closures, compressed headers in both directions, generated collections, and
the Rust test-adapter process. Cargo discovery checks `CARGO`, `PATH`, and
`CARGO_HOME/bin` (default `~/.cargo/bin`). Missing Cargo produces an actionable
error and does not silently skip compatibility checks.

Run the same suite against the previous JVM version without changing
production dependencies:

```sh
clojure -Sdeps '{:deps {com.taoensso/nippy {:mvn/version "3.7.0-beta1"}
                      org.clojure/test.check {:mvn/version "1.1.3"}}}' \
  -M:test -e '(require (quote datalevin.test.codec.nippy-cross-language-test))
    (let [r (clojure.test/run-tests (quote datalevin.test.codec.nippy-cross-language-test))]
      (shutdown-agents) (System/exit (+ (:fail r) (:error r))))'
```

`resources/datalevin/nippy` holds JVM-produced payloads from both versions;
`nippy-support/write-vectors!` regenerates them with the resolved JVM library.
Rust tests check their decoded meaning against independent Rust values and
round-trip the entire corpus. Generation changes upstream exception stack
traces, so the exact fixture bytes are versioned rather than regenerated
during every Cargo run. See `fuzz/README.md` for bounded fuzzing.

Validation recorded on 2026-09-07: the affected JVM regression suites passed
25 tests / 581 assertions; the Nippy 3.9 compatibility suite passed 9 tests /
481 assertions. Rust passed 9 codec tests (including 512 cases per property)
and 3 adapter tests. Both corpora contain 121 fixtures: 57 independent values
and all 63 upstream stress leaves plus the complete map. Each fuzz target
completed 1,000 inputs. Java compilation and strict Rust Clippy checks passed;
Clojure lint reported no errors and the existing 623 warnings.

## Performance

The [Rust/JVM benchmark](../benchmarks/nippy-bench/README.md) measures public
headerless encode/decode APIs against JVM Nippy 3.9.0. Its
[2026-09-07 baseline](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-a/report.md)
covers all 121 compatibility/stress fixtures and 16 bulk cases, using two fresh
processes per runtime and identical decoder input bytes. All 137 cases passed
local roundtrips and JVM validation of Rust output before timing.

The baseline had substantial wins but trailed on numeric-array encoding,
large keyword workloads, packed integers, and some bitmap cases. For example,
encoding 4,096 ints took 24.12 µs in Rust versus 2.16 µs in JVM Nippy;
encoding 64 KiB of bytes took 0.86 µs versus 3.97 µs.

Numeric arrays now convert elements into contiguous big-endian blocks and write
up to 4 KiB at once. A bounded stack buffer supports Vecs, caller-owned slices
and streams without an extra heap allocation. Arrays of up to eight elements
retain scalar writes. Compressed integer words use the same bulk writer;
their packing format is unchanged. Tests cover block boundaries, unaligned
destinations, float bit patterns, limits, partial writes and append rollback.

The [bulk-write comparison](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-bulk-arrays/report.md)
remeasured old Rust, new Rust and JVM Nippy sequentially, with two processes
each. Encoding 4,096 elements measured:

| Type | Old Rust | Bulk Rust | JVM Nippy 3.9 |
|---|---:|---:|---:|
| int | 24.87 µs | 0.87 µs | 1.94 µs |
| float | 24.86 µs | 0.85 µs | 1.95 µs |
| long | 13.41 µs | 1.80 µs | 2.25 µs |
| double | 12.47 µs | 1.82 µs | 2.23 µs |

All 138 validation cases produced identical bytes before and after the change
and passed JVM decoding. Eleven Rust tests and strict Clippy checks passed.
Array decoding is unchanged and still trails JVM Nippy on the large cases.
Keyword-cache policy remains another target: Rust caches on first occurrence,
while Nippy 3.9 starts caching on the second occurrence.

The report retains all samples, payload sizes, fork variation, source snapshots
and host-load readings. Rust's inert Java values and entry/member vectors do
different work from JVM runtime objects and persistent collections; those cases
are qualified in the report. This is a codec microbenchmark, not a storage or
client/server throughput result.
