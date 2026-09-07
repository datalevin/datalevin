# Rust Nippy codec

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

`Value::Keyword` contains a `Keyword` with shared text storage and a lazily
cached hash. Construct it with `Value::Keyword(name.into())`; use `.as_ref()`
or string dereferencing to read the name. Cloning a keyword shares its text.

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

Keyword encoding follows Nippy 3.9: write the first occurrence plainly,
define a cache entry on the second, then emit references. Cache tracking is
bounded to 32,768 distinct keywords per session. These and explicit cached
values use Nippy's existing reference tags, readable by both JVM versions.
The reader reserves nested cache definitions before reading
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

Optimized paths also have coverage for all 33 integer packing widths (including
exact JVM bytes), typed-array bit patterns and allocation limits, keyword cache
index boundaries, shared keyword lifetimes, and fragmented/failing writers.

## Performance

JVM **Nippy 3.9.0** versus the Rust implementation, measured on **2026-09-07**.
This single table covers all 138 cases from one full run of the current codec:
57 independent values, all 63 upstream Nippy stress leaves and the complete
stress map, plus 17 bulk workloads. Types without timing coverage are listed
as **not measured** at the end.

The [full comparison and before/after measurements](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-gaps/report.md)
retain raw samples, payload sizes, variation between processes, host load,
source snapshots, and both the old and new Rust results. This run replaces
the earlier combined table assembled from separate measurement passes.

The host was an Apple M3 Pro with 36 GiB RAM, OpenJDK 21.0.12.1, Clojure
1.12.5, and Rust 1.98.1. The current Cargo release profile enables ThinLTO and
one codegen unit; the previous Rust build used the default release profile.
Each implementation ran in two fresh processes, sequentially: JVM/old Rust/new
Rust, then new Rust/old Rust/JVM, reversing case order in the second group.
Each operation received 150 ms warmup and seven calibrated samples targeting
50 ms batches. Times below are the mean of the two process medians.

Both implementations use their public headerless encode/decode APIs
(`fast-freeze` / `fast-thaw` and `fast_freeze` / `fast_thaw`), without
compression or buffer reuse. Decoders receive identical bytes; encoder
inputs are prepared by decoding those bytes outside timing. Output
allocation, JVM garbage collection and Rust result destruction are included.
All 138 cases passed roundtrip validation, including JVM decoding of both
Rust versions' output. See the [benchmark runner](../benchmarks/nippy-bench/README.md)
to reproduce the comparison.

The optimized Rust paths use bulk typed-array decoding, fixed-width integer
packing kernels, shared keyword names with lazy cached hashes, and output
capacity reservation for arrays and Roaring's standard serializer. Primitive
values use smaller encode/decode dispatch functions. The keyword writer now
matches Nippy 3.9's second-occurrence caching policy; this changes 23 corpus
payloads while preserving their meaning and compatibility with JVM Nippy.

Timings reflect different runtime representations. Rust collections hold
vectors of values; its maps and sets do not build JVM-style hashed or sorted
collections. Rust retains records, deftypes and general Java serialized
objects as inert payloads, regex/URI values as strings, and big numbers as
integer bytes. JVM Nippy constructs the corresponding runtime objects. The
complete stress map includes these differences. Encoder payload sizes can
still differ because Rust defaults to the 3.7 deftype format and emits counted
sequences. No aggregate speedup across types is implied. Media-analysis
processes were kept paused during measurement; other desktop activity remained
present. Small timing differences should be treated cautiously.

The remaining consistent deficits are sparse-bitmap encoding and decoding,
object-array decoding, and repeated-keyword encoding. In both paired runs,
Rust is at least 10% faster in 135/138 encoding cases and 132/138 decoding
cases. These counts include the representation differences described above.

Times are **µs per operation**. **Ratio = JVM time / Rust time**: values
above **1× favor Rust**, and values below **1× favor JVM Nippy**.

| Data type / case | JVM encode µs | Rust encode µs | Encode ratio | JVM decode µs | Rust decode µs | Decode ratio |
|---|---:|---:|---:|---:|---:|---:|
| `bigdec` | 0.130 | 0.047 | 2.74× | 0.091 | 0.027 | 3.37× |
| `bigint` | 0.122 | 0.023 | 5.25× | 0.093 | 0.026 | 3.59× |
| `biginteger` | 0.127 | 0.023 | 5.47× | 0.089 | 0.026 | 3.39× |
| `bitmap` | 0.158 | 0.068 | 2.33× | 0.143 | 0.101 | 1.41× |
| `boolean-array` | 0.345 | 0.077 | 4.46× | 1.073 | 0.073 | 14.74× |
| `byte` | 0.124 | 0.018 | 6.74× | 0.078 | 0.011 | 7.15× |
| `bytes` | 0.128 | 0.026 | 4.91× | 0.087 | 0.026 | 3.38× |
| `char` | 0.120 | 0.019 | 6.31× | 0.078 | 0.011 | 7.16× |
| `char-array` | 0.340 | 0.076 | 4.44× | 1.079 | 0.072 | 14.90× |
| `date` | 0.122 | 0.039 | 3.17× | 0.077 | 0.013 | 5.97× |
| `datom` | 0.279 | 0.121 | 2.31× | 0.252 | 0.117 | 2.14× |
| `deftype` | 0.186 | 0.138 | 1.34× | 0.477 | 0.126 | 3.79× |
| `double` | 0.120 | 0.018 | 6.52× | 0.077 | 0.011 | 7.09× |
| `double-array` | 0.158 | 0.053 | 3.00× | 0.128 | 0.026 | 4.91× |
| `duration` | 0.120 | 0.043 | 2.81× | 0.079 | 0.013 | 6.18× |
| `entry` | 0.157 | 0.052 | 3.00× | 0.111 | 0.074 | 1.50× |
| `false` | 0.119 | 0.018 | 6.72× | 0.075 | 0.011 | 6.90× |
| `float` | 0.120 | 0.018 | 6.58× | 0.077 | 0.011 | 7.11× |
| `float-array` | 0.162 | 0.052 | 3.13× | 0.125 | 0.024 | 5.27× |
| `growing-packed` | 0.816 | 0.182 | 4.49× | 0.340 | 0.089 | 3.81× |
| `growing-small` | 0.139 | 0.068 | 2.06× | 0.135 | 0.026 | 5.18× |
| `instant` | 0.123 | 0.042 | 2.91× | 0.080 | 0.013 | 6.22× |
| `int-array` | 0.163 | 0.051 | 3.19× | 0.125 | 0.024 | 5.17× |
| `integer` | 0.120 | 0.018 | 6.47× | 0.078 | 0.011 | 7.27× |
| `keyword` | 0.139 | 0.059 | 2.35× | 0.145 | 0.033 | 4.41× |
| `list` | 0.163 | 0.026 | 6.20× | 0.103 | 0.037 | 2.77× |
| `long-array` | 0.165 | 0.051 | 3.26× | 0.126 | 0.025 | 5.02× |
| `long-boundaries` | 0.261 | 0.149 | 1.75× | 0.209 | 0.116 | 1.80× |
| `long-max` | 0.130 | 0.019 | 6.94× | 0.079 | 0.011 | 7.32× |
| `long-min` | 0.129 | 0.019 | 6.85× | 0.080 | 0.011 | 7.38× |
| `map` | 0.198 | 0.086 | 2.30× | 0.184 | 0.096 | 1.92× |
| `meta` | 0.217 | 0.084 | 2.59× | 0.176 | 0.124 | 1.41× |
| `nil` | 0.127 | 0.018 | 7.18× | 0.075 | 0.011 | 7.00× |
| `object-array` | 0.154 | 0.070 | 2.20× | 0.121 | 0.062 | 1.96× |
| `period` | 0.123 | 0.047 | 2.63× | 0.084 | 0.013 | 6.51× |
| `queue` | 0.179 | 0.045 | 3.98× | 0.125 | 0.037 | 3.36× |
| `ratio` | 0.135 | 0.041 | 3.28× | 0.103 | 0.049 | 2.09× |
| `record` | 0.254 | 0.132 | 1.93× | 0.633 | 0.138 | 4.59× |
| `regex` | 0.130 | 0.040 | 3.25× | 0.152 | 0.037 | 4.08× |
| `seq` | 0.168 | 0.026 | 6.39× | 0.138 | 0.037 | 3.72× |
| `set` | 0.179 | 0.026 | 6.78× | 0.159 | 0.037 | 4.32× |
| `short` | 0.122 | 0.019 | 6.61× | 0.081 | 0.011 | 7.32× |
| `short-array` | 0.394 | 0.076 | 5.20× | 1.116 | 0.073 | 15.32× |
| `sorted-map` | 0.172 | 0.069 | 2.51× | 0.139 | 0.064 | 2.19× |
| `sorted-set` | 0.198 | 0.045 | 4.43× | 0.163 | 0.037 | 4.36× |
| `sparse` | 0.289 | 0.122 | 2.37× | 0.259 | 0.130 | 1.99× |
| `spill-map` | 0.351 | 0.075 | 4.69× | 0.252 | 0.084 | 2.99× |
| `spill-set` | 0.420 | 0.048 | 8.76× | 0.314 | 0.061 | 5.17× |
| `spill-vector` | 0.252 | 0.078 | 3.24× | 0.241 | 0.065 | 3.68× |
| `sql-date` | 0.123 | 0.038 | 3.23× | 0.080 | 0.013 | 6.34× |
| `string-array` | 0.153 | 0.054 | 2.86× | 0.114 | 0.062 | 1.83× |
| `surrogate` | 0.127 | 0.019 | 6.72× | 0.081 | 0.011 | 7.36× |
| `symbol` | 0.135 | 0.039 | 3.46× | 0.107 | 0.029 | 3.65× |
| `text` | 0.152 | 0.056 | 2.72× | 0.122 | 0.053 | 2.30× |
| `true` | 0.123 | 0.018 | 6.67× | 0.076 | 0.011 | 7.04× |
| `uri` | 0.124 | 0.055 | 2.25× | 0.259 | 0.046 | 5.67× |
| `uuid` | 0.126 | 0.051 | 2.48× | 0.080 | 0.013 | 6.03× |
| `bulk/bitmap-dense` | 0.776 | 0.474 | 1.64× | 0.931 | 0.350 | 2.66× |
| `bulk/bitmap-sparse` | 1.485 | 1.836 | 0.81× | 2.611 | 3.428 | 0.76× |
| `bulk/boolean-array-4096` | 1.859 | 0.215 | 8.64× | 2.368 | 0.192 | 12.35× |
| `bulk/bytes-64k` | 3.878 | 0.823 | 4.71× | 1.757 | 0.798 | 2.20× |
| `bulk/datoms-1024` | 155.416 | 31.596 | 4.92× | 158.570 | 108.993 | 1.45× |
| `bulk/double-array-4096` | 2.296 | 1.285 | 1.79× | 1.846 | 0.598 | 3.09× |
| `bulk/float-array-4096` | 2.000 | 0.641 | 3.12× | 1.712 | 0.281 | 6.09× |
| `bulk/growing-4096` | 3.433 | 1.526 | 2.25× | 2.443 | 0.721 | 3.39× |
| `bulk/int-array-4096` | 2.033 | 0.626 | 3.25× | 1.731 | 0.305 | 5.68× |
| `bulk/long-array-4096` | 2.296 | 1.288 | 1.78× | 1.857 | 0.676 | 2.75× |
| `bulk/map-1024` | 36.502 | 32.063 | 1.14× | 108.275 | 42.335 | 2.56× |
| `bulk/set-1024` | 25.978 | 4.056 | 6.40× | 51.862 | 8.589 | 6.04× |
| `bulk/sorted-map-1024` | 40.536 | 31.414 | 1.29× | 365.219 | 40.265 | 9.07× |
| `bulk/sorted-set-1024` | 21.316 | 3.792 | 5.62× | 482.797 | 6.599 | 73.16× |
| `bulk/sparse-1024` | 1.822 | 1.028 | 1.77× | 1.362 | 1.098 | 1.24× |
| `bulk/text-16k` | 1.594 | 0.245 | 6.51× | 0.981 | 0.576 | 1.70× |
| `bulk/vector-1024` | 5.908 | 3.761 | 1.57× | 10.286 | 6.586 | 1.56× |
| `stress/arrays/boolean` | 0.402 | 0.083 | 4.86× | 1.105 | 0.074 | 14.91× |
| `stress/arrays/byte` | 0.130 | 0.053 | 2.48× | 0.088 | 0.027 | 3.22× |
| `stress/arrays/char` | 0.380 | 0.083 | 4.56× | 1.150 | 0.074 | 15.64× |
| `stress/arrays/double` | 0.179 | 0.110 | 1.63× | 0.142 | 0.038 | 3.72× |
| `stress/arrays/float` | 0.176 | 0.097 | 1.81× | 0.139 | 0.029 | 4.80× |
| `stress/arrays/int` | 0.174 | 0.097 | 1.79× | 0.144 | 0.027 | 5.24× |
| `stress/arrays/long` | 0.182 | 0.111 | 1.64× | 0.146 | 0.037 | 3.99× |
| `stress/arrays/object` | 1.328 | 0.517 | 2.57× | 0.729 | 0.977 | 0.75× |
| `stress/arrays/short` | 0.376 | 0.086 | 4.35× | 1.135 | 0.071 | 15.97× |
| `stress/arrays/str` | 0.420 | 0.385 | 1.09× | 0.859 | 0.812 | 1.06× |
| `stress/bigdec` | 0.134 | 0.044 | 3.04× | 0.098 | 0.028 | 3.52× |
| `stress/bigint` | 0.142 | 0.053 | 2.66× | 0.098 | 0.027 | 3.55× |
| `stress/byte` | 0.126 | 0.018 | 6.83× | 0.081 | 0.011 | 7.42× |
| `stress/char` | 0.123 | 0.019 | 6.54× | 0.081 | 0.011 | 7.41× |
| `stress/complete` | 47.750 | 20.486 | 2.33× | 108.102 | 41.837 | 2.58× |
| `stress/defrecord` | 0.254 | 0.138 | 1.84× | 0.639 | 0.140 | 4.56× |
| `stress/deftype` | 0.192 | 0.135 | 1.42× | 0.476 | 0.119 | 3.99× |
| `stress/double` | 0.123 | 0.018 | 6.71× | 0.079 | 0.011 | 7.32× |
| `stress/duration` | 0.123 | 0.043 | 2.89× | 0.080 | 0.013 | 6.33× |
| `stress/false` | 0.122 | 0.018 | 6.61× | 0.076 | 0.011 | 6.96× |
| `stress/false-boxed` | 0.123 | 0.018 | 6.74× | 0.077 | 0.011 | 7.18× |
| `stress/float` | 0.126 | 0.018 | 6.94× | 0.080 | 0.011 | 7.37× |
| `stress/instant` | 0.123 | 0.042 | 2.92× | 0.083 | 0.013 | 6.49× |
| `stress/integer` | 0.122 | 0.018 | 6.68× | 0.079 | 0.011 | 7.28× |
| `stress/kw` | 0.140 | 0.063 | 2.20× | 0.114 | 0.034 | 3.36× |
| `stress/kw-long` | 0.177 | 0.087 | 2.04× | 0.331 | 0.059 | 5.63× |
| `stress/kw-ns` | 0.141 | 0.076 | 1.87× | 0.157 | 0.036 | 4.32× |
| `stress/lazy-seq` | 0.571 | 0.319 | 1.79× | 0.573 | 0.392 | 1.46× |
| `stress/lazy-seq-empty` | 0.153 | 0.021 | 7.48× | 0.113 | 0.016 | 6.85× |
| `stress/list` | 0.325 | 0.124 | 2.62× | 0.246 | 0.173 | 1.42× |
| `stress/long` | 0.126 | 0.019 | 6.83× | 0.080 | 0.011 | 7.15× |
| `stress/many-doubles` | 3.426 | 2.214 | 1.55× | 5.124 | 3.312 | 1.55× |
| `stress/many-keywords` | 5.408 | 7.151 | 0.76× | 10.835 | 8.885 | 1.22× |
| `stress/many-longs` | 3.175 | 1.953 | 1.63× | 5.460 | 3.362 | 1.62× |
| `stress/many-strings` | 7.004 | 4.386 | 1.60× | 13.271 | 13.611 | 0.98× |
| `stress/map` | 0.536 | 0.421 | 1.28× | 0.642 | 0.544 | 1.18× |
| `stress/map-entry` | 0.158 | 0.051 | 3.09× | 0.114 | 0.073 | 1.57× |
| `stress/meta` | 0.255 | 0.195 | 1.30× | 0.274 | 0.211 | 1.30× |
| `stress/nested` | 1.986 | 0.764 | 2.60× | 2.484 | 1.592 | 1.56× |
| `stress/nil` | 0.123 | 0.018 | 6.93× | 0.076 | 0.011 | 7.10× |
| `stress/non-comparable/ex-info` | 4.945 | 0.143 | 34.63× | 24.415 | 0.085 | 287.48× |
| `stress/non-comparable/exception` | 3.667 | 0.143 | 25.56× | 13.557 | 0.087 | 156.25× |
| `stress/non-comparable/regex` | 0.130 | 0.053 | 2.45× | 0.316 | 0.040 | 7.80× |
| `stress/non-comparable/throwable` | 3.566 | 0.143 | 25.01× | 12.530 | 0.088 | 142.83× |
| `stress/period` | 0.131 | 0.047 | 2.80× | 0.083 | 0.013 | 6.42× |
| `stress/queue` | 0.323 | 0.223 | 1.45× | 0.351 | 0.241 | 1.46× |
| `stress/queue-empty` | 0.154 | 0.023 | 6.76× | 0.095 | 0.016 | 5.82× |
| `stress/ratio` | 0.141 | 0.042 | 3.36× | 0.102 | 0.050 | 2.06× |
| `stress/set` | 0.436 | 0.127 | 3.42× | 0.822 | 0.173 | 4.75× |
| `stress/short` | 0.137 | 0.019 | 7.38× | 0.081 | 0.011 | 7.53× |
| `stress/sorted-map` | 0.255 | 0.193 | 1.32× | 0.383 | 0.230 | 1.67× |
| `stress/sorted-set` | 0.236 | 0.052 | 4.54× | 0.466 | 0.061 | 7.58× |
| `stress/sql-date` | 0.122 | 0.038 | 3.21× | 0.078 | 0.013 | 6.11× |
| `stress/str-long` | 0.336 | 0.083 | 4.07× | 0.188 | 0.127 | 1.48× |
| `stress/str-short` | 0.153 | 0.055 | 2.79× | 0.124 | 0.053 | 2.35× |
| `stress/subvec` | 0.184 | 0.056 | 3.27× | 0.130 | 0.068 | 1.91× |
| `stress/sym` | 0.133 | 0.025 | 5.35× | 0.093 | 0.029 | 3.20× |
| `stress/sym-long` | 0.169 | 0.066 | 2.56× | 0.142 | 0.057 | 2.47× |
| `stress/sym-ns` | 0.137 | 0.042 | 3.30× | 0.111 | 0.031 | 3.55× |
| `stress/true` | 0.123 | 0.018 | 6.67× | 0.081 | 0.011 | 7.57× |
| `stress/uri` | 0.129 | 0.057 | 2.26× | 0.261 | 0.046 | 5.65× |
| `stress/util-date` | 0.124 | 0.039 | 3.15× | 0.077 | 0.013 | 6.01× |
| `stress/uuid` | 0.124 | 0.052 | 2.38× | 0.079 | 0.013 | 5.98× |
| `stress/vector` | 0.340 | 0.129 | 2.65× | 0.208 | 0.187 | 1.11× |
| Entity reference | not measured | not measured | — | not measured | not measured | — |
| Interpreted function, including closures | not measured | not measured | — | not measured | not measured | — |
| EDN reader fallback | not measured | not measured | — | not measured | not measured | — |
| Metadata protocol-key marker | not measured | not measured | — | not measured | not measured | — |

Entity resolution and interpreted-function construction require separate
runtime workloads; Rust currently retains their descriptors or source forms.
The EDN reader fallback and metadata protocol-key marker have no standalone
fixture in this performance corpus. Compression and storage/network
throughput are outside this codec comparison.
