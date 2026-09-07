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
The [vendored Roaring patch](../src/rust/vendor/README.md) adds bulk writes and
vectorizable array validation while preserving upstream's portable bytes and
malformed-input diagnostics.
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
They compare Roaring output and malformed-array errors with the unmodified
upstream crate, and check exact node, allocation, and depth limits for small
vectors and cached collection expansion.

## Performance

JVM **Nippy 3.9.0** versus the Rust implementation, measured on **2026-09-07**.
This single table covers all 138 cases from one full run of the current codec:
57 independent values, all 63 upstream Nippy stress leaves and the complete
stress map, plus 17 bulk workloads. Types without timing coverage are listed
as **not measured** at the end.

The [full comparison and before/after measurements](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-remaining/report.md)
retain raw samples, payload sizes, variation between processes, host load,
source snapshots, and both the previous and current Rust results.

The host was an Apple M3 Pro with 36 GiB RAM, OpenJDK 21.0.12.1, Clojure
1.12.5, and Rust 1.98.1. Both Rust builds use ThinLTO and one codegen unit.
The baseline is the preceding optimized Rust implementation.
Each implementation ran in two fresh processes, sequentially: JVM/old Rust/new
Rust, then new Rust/old Rust/JVM, reversing case order in the second group.
Each operation received 150 ms warmup and seven calibrated samples targeting
50 ms batches. Times below are the mean of the two process medians.

Both implementations use their public headerless encode/decode APIs
(`fast-freeze` / `fast-thaw` and `fast_freeze` / `fast_thaw`), without
compression or buffer reuse. Decoders receive identical bytes; encoder
inputs are prepared by decoding those bytes outside timing. Output
allocation, JVM garbage collection and Rust result destruction are included.
All 138 cases passed roundtrip validation, including JVM decoding of Rust
output. The previous and current Rust builds emit identical bytes for every
case. See the [benchmark runner](../benchmarks/nippy-bench/README.md)
to reproduce the comparison.

The optimized Rust paths use bulk typed-array decoding, fixed-width integer
packing kernels, shared keyword names with cached hashes, and output capacity
reservation. Keyword cache promotion now retains the allocation shared by
subsequent references and performs one hash-table lookup. Small-vector decoding
and remaining-budget counters reduce per-element overhead. A
[small local Roaring patch](../src/rust/vendor/README.md) writes contiguous
container words in bulk and vectorizes checked array validation, retaining
upstream's format and error diagnostics. Keyword caching still follows
Nippy 3.9's second-occurrence policy.

The remaining mean-time deficits are object-array decoding (Rust 0.771 µs,
JVM 0.728 µs) and many-strings decoding (13.176 vs 12.090 µs). Sparse bitmap
encoding/decoding now favor Rust by 3.28×/1.49×, and many-keywords encoding by
1.44×. Against the previous Rust build, some primitive decodes cost roughly
1–3 ns more, and 64 KiB byte-array encoding rose from 0.807 to 0.974 µs;
these paths still beat JVM in this run. The report retains every before/after
measurement, including these regressions.

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

Times are **µs per operation**. **Ratio = JVM time / Rust time**: values
above **1× favor Rust**, and values below **1× favor JVM Nippy**.

| Data type / case | JVM encode µs | Rust encode µs | Encode ratio | JVM decode µs | Rust decode µs | Decode ratio |
|---|---:|---:|---:|---:|---:|---:|
| `bigdec` | 0.128 | 0.044 | 2.94× | 0.089 | 0.025 | 3.59× |
| `bigint` | 0.127 | 0.023 | 5.41× | 0.090 | 0.025 | 3.66× |
| `biginteger` | 0.123 | 0.023 | 5.42× | 0.088 | 0.025 | 3.56× |
| `bitmap` | 0.158 | 0.079 | 2.01× | 0.144 | 0.101 | 1.42× |
| `boolean-array` | 0.375 | 0.084 | 4.45× | 1.092 | 0.078 | 13.92× |
| `byte` | 0.121 | 0.019 | 6.48× | 0.077 | 0.013 | 5.75× |
| `bytes` | 0.127 | 0.025 | 5.08× | 0.087 | 0.027 | 3.23× |
| `char` | 0.124 | 0.019 | 6.61× | 0.078 | 0.013 | 6.05× |
| `char-array` | 0.371 | 0.082 | 4.51× | 1.103 | 0.079 | 13.93× |
| `date` | 0.125 | 0.038 | 3.26× | 0.076 | 0.014 | 5.58× |
| `datom` | 0.275 | 0.113 | 2.45× | 0.252 | 0.117 | 2.15× |
| `deftype` | 0.192 | 0.145 | 1.33× | 0.461 | 0.126 | 3.67× |
| `double` | 0.125 | 0.019 | 6.55× | 0.078 | 0.013 | 6.01× |
| `double-array` | 0.163 | 0.055 | 2.93× | 0.125 | 0.028 | 4.45× |
| `duration` | 0.125 | 0.045 | 2.79× | 0.082 | 0.014 | 5.73× |
| `entry` | 0.154 | 0.056 | 2.75× | 0.116 | 0.078 | 1.47× |
| `false` | 0.123 | 0.018 | 6.67× | 0.074 | 0.013 | 5.62× |
| `float` | 0.121 | 0.019 | 6.33× | 0.077 | 0.013 | 5.77× |
| `float-array` | 0.162 | 0.055 | 2.95× | 0.122 | 0.025 | 4.83× |
| `growing-packed` | 0.732 | 0.195 | 3.75× | 0.337 | 0.098 | 3.46× |
| `growing-small` | 0.141 | 0.073 | 1.93× | 0.139 | 0.027 | 5.11× |
| `instant` | 0.121 | 0.045 | 2.71× | 0.079 | 0.014 | 5.73× |
| `int-array` | 0.163 | 0.054 | 3.00× | 0.123 | 0.025 | 4.95× |
| `integer` | 0.122 | 0.019 | 6.37× | 0.077 | 0.013 | 5.80× |
| `keyword` | 0.140 | 0.063 | 2.21× | 0.156 | 0.034 | 4.63× |
| `list` | 0.160 | 0.030 | 5.41× | 0.104 | 0.037 | 2.78× |
| `long-array` | 0.162 | 0.055 | 2.96× | 0.122 | 0.027 | 4.51× |
| `long-boundaries` | 0.260 | 0.160 | 1.63× | 0.206 | 0.127 | 1.62× |
| `long-max` | 0.127 | 0.021 | 6.03× | 0.077 | 0.013 | 5.72× |
| `long-min` | 0.126 | 0.020 | 6.24× | 0.077 | 0.013 | 5.78× |
| `map` | 0.197 | 0.090 | 2.19× | 0.180 | 0.096 | 1.87× |
| `meta` | 0.215 | 0.086 | 2.49× | 0.169 | 0.111 | 1.52× |
| `nil` | 0.121 | 0.018 | 6.81× | 0.072 | 0.013 | 5.73× |
| `object-array` | 0.149 | 0.070 | 2.12× | 0.128 | 0.056 | 2.27× |
| `period` | 0.121 | 0.046 | 2.63× | 0.081 | 0.014 | 5.85× |
| `queue` | 0.168 | 0.044 | 3.80× | 0.122 | 0.036 | 3.37× |
| `ratio` | 0.129 | 0.040 | 3.25× | 0.101 | 0.038 | 2.62× |
| `record` | 0.239 | 0.133 | 1.79× | 0.613 | 0.130 | 4.70× |
| `regex` | 0.128 | 0.040 | 3.23× | 0.152 | 0.039 | 3.85× |
| `seq` | 0.154 | 0.029 | 5.38× | 0.135 | 0.037 | 3.67× |
| `set` | 0.175 | 0.027 | 6.46× | 0.157 | 0.035 | 4.53× |
| `short` | 0.121 | 0.018 | 6.68× | 0.077 | 0.012 | 6.33× |
| `short-array` | 0.367 | 0.079 | 4.66× | 1.062 | 0.078 | 13.66× |
| `sorted-map` | 0.244 | 0.068 | 3.57× | 0.132 | 0.059 | 2.24× |
| `sorted-set` | 0.191 | 0.046 | 4.17× | 0.163 | 0.036 | 4.50× |
| `sparse` | 0.280 | 0.126 | 2.22× | 0.252 | 0.125 | 2.02× |
| `spill-map` | 0.338 | 0.071 | 4.77× | 0.246 | 0.080 | 3.06× |
| `spill-set` | 0.402 | 0.048 | 8.39× | 0.296 | 0.056 | 5.26× |
| `spill-vector` | 0.243 | 0.074 | 3.29× | 0.240 | 0.059 | 4.09× |
| `sql-date` | 0.122 | 0.038 | 3.21× | 0.076 | 0.013 | 5.73× |
| `string-array` | 0.146 | 0.052 | 2.78× | 0.122 | 0.057 | 2.16× |
| `surrogate` | 0.121 | 0.018 | 6.83× | 0.077 | 0.012 | 6.29× |
| `symbol` | 0.131 | 0.038 | 3.42× | 0.106 | 0.030 | 3.51× |
| `text` | 0.148 | 0.053 | 2.78× | 0.122 | 0.053 | 2.30× |
| `true` | 0.120 | 0.018 | 6.84× | 0.073 | 0.012 | 6.05× |
| `uri` | 0.125 | 0.055 | 2.28× | 0.252 | 0.045 | 5.53× |
| `uuid` | 0.124 | 0.051 | 2.41× | 0.077 | 0.013 | 5.76× |
| `bulk/bitmap-dense` | 0.666 | 0.174 | 3.83× | 0.960 | 0.371 | 2.59× |
| `bulk/bitmap-sparse` | 1.393 | 0.425 | 3.28× | 2.650 | 1.773 | 1.49× |
| `bulk/boolean-array-4096` | 1.887 | 0.208 | 9.05× | 2.398 | 0.198 | 12.11× |
| `bulk/bytes-64k` | 3.891 | 0.974 | 3.99× | 1.803 | 0.836 | 2.16× |
| `bulk/datoms-1024` | 159.106 | 29.774 | 5.34× | 155.762 | 110.884 | 1.40× |
| `bulk/double-array-4096` | 2.298 | 1.321 | 1.74× | 1.853 | 0.655 | 2.83× |
| `bulk/float-array-4096` | 2.016 | 0.676 | 2.98× | 1.749 | 0.279 | 6.27× |
| `bulk/growing-4096` | 3.525 | 1.625 | 2.17× | 2.491 | 0.777 | 3.20× |
| `bulk/int-array-4096` | 2.025 | 0.648 | 3.13× | 1.760 | 0.284 | 6.19× |
| `bulk/long-array-4096` | 2.347 | 1.321 | 1.78× | 1.868 | 0.626 | 2.98× |
| `bulk/map-1024` | 37.949 | 30.974 | 1.23× | 110.505 | 41.262 | 2.68× |
| `bulk/set-1024` | 26.651 | 4.614 | 5.78× | 52.489 | 9.425 | 5.57× |
| `bulk/sorted-map-1024` | 58.186 | 30.864 | 1.89× | 372.285 | 39.792 | 9.36× |
| `bulk/sorted-set-1024` | 22.177 | 4.003 | 5.54× | 514.786 | 7.161 | 71.89× |
| `bulk/sparse-1024` | 1.305 | 0.709 | 1.84× | 1.396 | 0.746 | 1.87× |
| `bulk/text-16k` | 1.648 | 0.262 | 6.30× | 0.978 | 0.575 | 1.70× |
| `bulk/vector-1024` | 6.716 | 4.005 | 1.68× | 10.397 | 7.238 | 1.44× |
| `stress/arrays/boolean` | 0.368 | 0.082 | 4.47× | 1.103 | 0.078 | 14.12× |
| `stress/arrays/byte` | 0.127 | 0.053 | 2.39× | 0.085 | 0.027 | 3.10× |
| `stress/arrays/char` | 0.389 | 0.085 | 4.60× | 1.122 | 0.074 | 15.07× |
| `stress/arrays/double` | 0.188 | 0.106 | 1.77× | 0.139 | 0.039 | 3.59× |
| `stress/arrays/float` | 0.173 | 0.095 | 1.83× | 0.136 | 0.028 | 4.90× |
| `stress/arrays/int` | 0.173 | 0.096 | 1.81× | 0.136 | 0.027 | 4.98× |
| `stress/arrays/long` | 0.176 | 0.108 | 1.64× | 0.141 | 0.039 | 3.62× |
| `stress/arrays/object` | 1.101 | 0.524 | 2.10× | 0.728 | 0.771 | 0.94× |
| `stress/arrays/short` | 0.386 | 0.086 | 4.51× | 1.121 | 0.073 | 15.43× |
| `stress/arrays/str` | 0.408 | 0.376 | 1.09× | 0.936 | 0.805 | 1.16× |
| `stress/bigdec` | 0.131 | 0.044 | 2.99× | 0.093 | 0.026 | 3.54× |
| `stress/bigint` | 0.138 | 0.053 | 2.60× | 0.094 | 0.025 | 3.75× |
| `stress/byte` | 0.122 | 0.018 | 6.87× | 0.077 | 0.013 | 5.96× |
| `stress/char` | 0.123 | 0.018 | 6.67× | 0.077 | 0.013 | 5.97× |
| `stress/complete` | 47.502 | 16.739 | 2.84× | 107.614 | 39.749 | 2.71× |
| `stress/defrecord` | 0.237 | 0.136 | 1.74× | 0.617 | 0.132 | 4.68× |
| `stress/deftype` | 0.193 | 0.139 | 1.39× | 0.461 | 0.116 | 3.97× |
| `stress/double` | 0.120 | 0.018 | 6.55× | 0.077 | 0.012 | 6.22× |
| `stress/duration` | 0.123 | 0.045 | 2.74× | 0.080 | 0.013 | 5.89× |
| `stress/false` | 0.121 | 0.017 | 6.91× | 0.073 | 0.012 | 5.86× |
| `stress/false-boxed` | 0.123 | 0.018 | 6.85× | 0.074 | 0.012 | 6.17× |
| `stress/float` | 0.121 | 0.018 | 6.89× | 0.079 | 0.012 | 6.49× |
| `stress/instant` | 0.126 | 0.043 | 2.90× | 0.079 | 0.013 | 6.16× |
| `stress/integer` | 0.121 | 0.019 | 6.49× | 0.077 | 0.013 | 6.13× |
| `stress/kw` | 0.140 | 0.061 | 2.28× | 0.112 | 0.041 | 2.74× |
| `stress/kw-long` | 0.182 | 0.087 | 2.10× | 0.335 | 0.059 | 5.68× |
| `stress/kw-ns` | 0.144 | 0.074 | 1.94× | 0.174 | 0.035 | 4.95× |
| `stress/lazy-seq` | 0.528 | 0.331 | 1.59× | 0.514 | 0.394 | 1.31× |
| `stress/lazy-seq-empty` | 0.148 | 0.020 | 7.32× | 0.107 | 0.016 | 6.88× |
| `stress/list` | 0.322 | 0.130 | 2.49× | 0.273 | 0.159 | 1.72× |
| `stress/long` | 0.123 | 0.018 | 6.74× | 0.077 | 0.012 | 6.46× |
| `stress/many-doubles` | 3.268 | 2.186 | 1.49× | 4.738 | 3.423 | 1.38× |
| `stress/many-keywords` | 5.613 | 3.902 | 1.44× | 11.240 | 8.202 | 1.37× |
| `stress/many-longs` | 3.430 | 2.065 | 1.66× | 5.373 | 3.461 | 1.55× |
| `stress/many-strings` | 6.603 | 4.362 | 1.51× | 12.090 | 13.176 | 0.92× |
| `stress/map` | 0.504 | 0.407 | 1.24× | 0.640 | 0.485 | 1.32× |
| `stress/map-entry` | 0.153 | 0.052 | 2.96× | 0.112 | 0.072 | 1.56× |
| `stress/meta` | 0.248 | 0.194 | 1.28× | 0.264 | 0.199 | 1.33× |
| `stress/nested` | 1.924 | 0.734 | 2.62× | 2.502 | 1.509 | 1.66× |
| `stress/nil` | 0.121 | 0.018 | 6.80× | 0.072 | 0.013 | 5.76× |
| `stress/non-comparable/ex-info` | 4.708 | 0.142 | 33.20× | 23.513 | 0.086 | 273.66× |
| `stress/non-comparable/exception` | 3.629 | 0.143 | 25.39× | 13.355 | 0.086 | 155.27× |
| `stress/non-comparable/regex` | 0.130 | 0.053 | 2.47× | 0.321 | 0.040 | 7.92× |
| `stress/non-comparable/throwable` | 3.481 | 0.141 | 24.69× | 12.410 | 0.089 | 139.80× |
| `stress/period` | 0.122 | 0.047 | 2.60× | 0.081 | 0.013 | 6.09× |
| `stress/queue` | 0.311 | 0.205 | 1.52× | 0.343 | 0.220 | 1.56× |
| `stress/queue-empty` | 0.148 | 0.022 | 6.64× | 0.090 | 0.016 | 5.76× |
| `stress/ratio` | 0.125 | 0.040 | 3.13× | 0.099 | 0.056 | 1.79× |
| `stress/set` | 0.413 | 0.128 | 3.23× | 0.778 | 0.170 | 4.58× |
| `stress/short` | 0.122 | 0.018 | 6.79× | 0.078 | 0.012 | 6.45× |
| `stress/sorted-map` | 0.240 | 0.192 | 1.25× | 0.358 | 0.158 | 2.27× |
| `stress/sorted-set` | 0.228 | 0.054 | 4.25× | 0.463 | 0.063 | 7.31× |
| `stress/sql-date` | 0.124 | 0.037 | 3.36× | 0.077 | 0.013 | 6.05× |
| `stress/str-long` | 0.347 | 0.084 | 4.13× | 0.184 | 0.127 | 1.45× |
| `stress/str-short` | 0.147 | 0.053 | 2.77× | 0.122 | 0.053 | 2.31× |
| `stress/subvec` | 0.180 | 0.056 | 3.21× | 0.123 | 0.066 | 1.87× |
| `stress/sym` | 0.129 | 0.024 | 5.38× | 0.091 | 0.028 | 3.24× |
| `stress/sym-long` | 0.165 | 0.064 | 2.56× | 0.139 | 0.060 | 2.31× |
| `stress/sym-ns` | 0.130 | 0.040 | 3.27× | 0.107 | 0.031 | 3.44× |
| `stress/true` | 0.122 | 0.018 | 6.90× | 0.074 | 0.012 | 6.22× |
| `stress/uri` | 0.126 | 0.054 | 2.35× | 0.252 | 0.044 | 5.72× |
| `stress/util-date` | 0.122 | 0.037 | 3.31× | 0.075 | 0.013 | 5.93× |
| `stress/uuid` | 0.122 | 0.050 | 2.45× | 0.077 | 0.013 | 5.81× |
| `stress/vector` | 0.334 | 0.127 | 2.64× | 0.198 | 0.153 | 1.30× |
| Entity reference | not measured | not measured | — | not measured | not measured | — |
| Interpreted function, including closures | not measured | not measured | — | not measured | not measured | — |
| EDN reader fallback | not measured | not measured | — | not measured | not measured | — |
| Metadata protocol-key marker | not measured | not measured | — | not measured | not measured | — |

Entity resolution and interpreted-function construction require separate
runtime workloads; Rust currently retains their descriptors or source forms.
The EDN reader fallback and metadata protocol-key marker have no standalone
fixture in this performance corpus. Compression and storage/network
throughput are outside this codec comparison.
