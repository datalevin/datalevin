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
vectors and cached collection expansion. String tests compare ASCII and mixed
Unicode payloads with Rust's checked UTF-8 validator, reject invalid sequences
across word boundaries, and verify all supported string-length headers and
allocation limits. A partial-decoding regression checks that initialized
collection slots release their shared references on errors, including nested
collections, truncated values, invalid UTF-8, and invalid element types. The
full Rust suite also passes under AddressSanitizer with direct slot writes.

## Performance

JVM **Nippy 3.9.0** versus the Rust implementation, measured on **2026-09-07**.
This single table covers all 138 cases from one full run of the current codec:
57 independent values, all 63 upstream Nippy stress leaves and the complete
stress map, plus 17 bulk workloads. Types without timing coverage are listed
as **not measured** at the end.

The [full comparison and before/after measurements](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-inplace/report.md)
retain raw samples, payload sizes, variation between processes, host load,
source snapshots, and both the previous and current Rust results.

The host was an Apple M3 Pro with 36 GiB RAM, OpenJDK 21.0.12.1, Clojure
1.12.5, and Rust 1.98.1. Both Rust builds use ThinLTO and one codegen unit.
The baseline is the preceding optimized Rust implementation from the
[short-string run](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-strings/report.md).
Each implementation ran in two fresh processes, sequentially: JVM/old Rust/new
Rust, then new Rust/old Rust/JVM, reversing case order in the second group.
Each operation received 100 ms warmup and five calibrated samples targeting
25 ms batches. Times below are the mean of the two process medians.

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
Nippy 3.9's second-occurrence policy. String decoding now uses an ASCII fast
path below 32 bytes, inlines length handling, and reserves medium collection
storage once within the remaining decoder budgets and a 64 KiB initial cap.
Longer strings use Rust's standard checked UTF-8 validator. Collection decoding
constructs hot-path values directly in their final vector slots; one-, two-,
and three-element collections use separate fixed-size decoder loops. These
changes reduce temporary value moves and collection-call overhead without
changing the public value representation or wire format. The
[object-array profile](../benchmarks/nippy-bench/results/2026-09-07-object-array-profile/report.md)
and [validation and assembly evidence](../benchmarks/nippy-bench/results/2026-09-07-macos-arm64-inplace/validation.md)
explain the changes and their checks.

Object-array decoding improved from 0.750 to 0.660 µs; JVM measured 0.717 µs.
The 1,024-element vector improved from 6.226 to 2.973 µs, and the complete
stress-map decode from 35.898 to 32.558 µs. Rust measured faster in all 138
encode and decode cases in both passes. Margins exceeded 10% in both passes
for 137 encode cases and 136 decode cases; the object-array lead is smaller.

There are tradeoffs against the previous Rust build: date/time and UUID
decoding consistently costs roughly 2–3 ns more, and the stress map and
metadata leaves decode about 9% slower. The 16 KiB text mean also rose from
0.561 to 0.613 µs, though only one of its two processes regressed. These cases
remain faster than JVM Nippy; the report retains every before/after result.

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
| `bigdec` | 0.127 | 0.040 | 3.13× | 0.094 | 0.025 | 3.80× |
| `bigint` | 0.126 | 0.023 | 5.59× | 0.094 | 0.025 | 3.79× |
| `biginteger` | 0.122 | 0.021 | 5.73× | 0.091 | 0.025 | 3.70× |
| `bitmap` | 0.155 | 0.072 | 2.17× | 0.145 | 0.096 | 1.51× |
| `boolean-array` | 0.377 | 0.076 | 4.98× | 1.081 | 0.069 | 15.63× |
| `byte` | 0.122 | 0.018 | 6.84× | 0.078 | 0.013 | 6.09× |
| `bytes` | 0.126 | 0.024 | 5.25× | 0.086 | 0.027 | 3.21× |
| `char` | 0.123 | 0.017 | 7.05× | 0.078 | 0.013 | 5.91× |
| `char-array` | 0.368 | 0.076 | 4.85× | 1.081 | 0.071 | 15.29× |
| `date` | 0.121 | 0.037 | 3.30× | 0.076 | 0.015 | 5.04× |
| `datom` | 0.280 | 0.116 | 2.41× | 0.243 | 0.109 | 2.23× |
| `deftype` | 0.192 | 0.138 | 1.39× | 0.501 | 0.107 | 4.67× |
| `double` | 0.123 | 0.018 | 7.02× | 0.078 | 0.013 | 6.15× |
| `double-array` | 0.161 | 0.050 | 3.22× | 0.125 | 0.027 | 4.63× |
| `duration` | 0.121 | 0.041 | 2.93× | 0.081 | 0.015 | 5.31× |
| `entry` | 0.148 | 0.052 | 2.87× | 0.122 | 0.068 | 1.80× |
| `false` | 0.119 | 0.017 | 6.89× | 0.075 | 0.012 | 6.07× |
| `float` | 0.121 | 0.018 | 6.91× | 0.078 | 0.013 | 6.04× |
| `float-array` | 0.167 | 0.050 | 3.32× | 0.123 | 0.025 | 4.83× |
| `growing-packed` | 0.724 | 0.182 | 3.98× | 0.354 | 0.090 | 3.94× |
| `growing-small` | 0.139 | 0.066 | 2.11× | 0.140 | 0.027 | 5.25× |
| `instant` | 0.122 | 0.041 | 2.96× | 0.080 | 0.015 | 5.26× |
| `int-array` | 0.164 | 0.050 | 3.27× | 0.124 | 0.025 | 4.90× |
| `integer` | 0.121 | 0.018 | 6.83× | 0.078 | 0.013 | 6.07× |
| `keyword` | 0.142 | 0.058 | 2.44× | 0.144 | 0.033 | 4.38× |
| `list` | 0.157 | 0.027 | 5.81× | 0.100 | 0.033 | 3.07× |
| `long-array` | 0.161 | 0.051 | 3.16× | 0.126 | 0.027 | 4.67× |
| `long-boundaries` | 0.251 | 0.145 | 1.73× | 0.205 | 0.079 | 2.59× |
| `long-max` | 0.126 | 0.019 | 6.79× | 0.078 | 0.013 | 5.95× |
| `long-min` | 0.127 | 0.019 | 6.79× | 0.081 | 0.012 | 6.49× |
| `map` | 0.191 | 0.083 | 2.31× | 0.190 | 0.092 | 2.07× |
| `meta` | 0.209 | 0.082 | 2.55× | 0.182 | 0.111 | 1.64× |
| `nil` | 0.119 | 0.018 | 6.64× | 0.073 | 0.013 | 5.62× |
| `object-array` | 0.150 | 0.070 | 2.13× | 0.135 | 0.059 | 2.27× |
| `period` | 0.120 | 0.046 | 2.61× | 0.081 | 0.016 | 5.22× |
| `queue` | 0.169 | 0.044 | 3.86× | 0.120 | 0.033 | 3.61× |
| `ratio` | 0.127 | 0.039 | 3.21× | 0.100 | 0.041 | 2.43× |
| `record` | 0.238 | 0.129 | 1.85× | 0.635 | 0.118 | 5.37× |
| `regex` | 0.130 | 0.039 | 3.29× | 0.156 | 0.034 | 4.54× |
| `seq` | 0.162 | 0.027 | 6.04× | 0.133 | 0.033 | 4.00× |
| `set` | 0.178 | 0.028 | 6.39× | 0.156 | 0.034 | 4.57× |
| `short` | 0.124 | 0.019 | 6.64× | 0.081 | 0.013 | 6.19× |
| `short-array` | 0.373 | 0.075 | 4.97× | 1.152 | 0.073 | 15.89× |
| `sorted-map` | 0.171 | 0.068 | 2.53× | 0.136 | 0.059 | 2.32× |
| `sorted-set` | 0.188 | 0.044 | 4.33× | 0.155 | 0.034 | 4.62× |
| `sparse` | 0.281 | 0.122 | 2.30× | 0.254 | 0.124 | 2.06× |
| `spill-map` | 0.335 | 0.072 | 4.67× | 0.242 | 0.081 | 3.00× |
| `spill-set` | 0.400 | 0.046 | 8.62× | 0.298 | 0.053 | 5.64× |
| `spill-vector` | 0.249 | 0.071 | 3.51× | 0.231 | 0.061 | 3.80× |
| `sql-date` | 0.122 | 0.037 | 3.33× | 0.076 | 0.015 | 5.04× |
| `string-array` | 0.147 | 0.052 | 2.81× | 0.136 | 0.057 | 2.37× |
| `surrogate` | 0.122 | 0.017 | 6.96× | 0.078 | 0.013 | 6.09× |
| `symbol` | 0.134 | 0.038 | 3.54× | 0.110 | 0.027 | 4.04× |
| `text` | 0.149 | 0.054 | 2.78× | 0.126 | 0.052 | 2.40× |
| `true` | 0.120 | 0.018 | 6.83× | 0.074 | 0.013 | 5.92× |
| `uri` | 0.131 | 0.054 | 2.42× | 0.255 | 0.037 | 6.96× |
| `uuid` | 0.120 | 0.050 | 2.43× | 0.078 | 0.015 | 5.29× |
| `bulk/bitmap-dense` | 0.760 | 0.165 | 4.61× | 0.978 | 0.344 | 2.84× |
| `bulk/bitmap-sparse` | 1.421 | 0.382 | 3.72× | 2.634 | 1.572 | 1.67× |
| `bulk/boolean-array-4096` | 1.854 | 0.196 | 9.44× | 2.331 | 0.168 | 13.84× |
| `bulk/bytes-64k` | 3.769 | 0.872 | 4.32× | 1.744 | 0.807 | 2.16× |
| `bulk/datoms-1024` | 152.474 | 27.881 | 5.47× | 154.270 | 92.415 | 1.67× |
| `bulk/double-array-4096` | 2.273 | 1.260 | 1.80× | 1.842 | 0.795 | 2.32× |
| `bulk/float-array-4096` | 1.952 | 0.626 | 3.12× | 1.694 | 0.306 | 5.53× |
| `bulk/growing-4096` | 3.442 | 1.486 | 2.32× | 2.453 | 0.715 | 3.43× |
| `bulk/int-array-4096` | 1.980 | 0.610 | 3.25× | 1.714 | 0.287 | 5.98× |
| `bulk/long-array-4096` | 2.288 | 1.269 | 1.80× | 1.839 | 0.700 | 2.63× |
| `bulk/map-1024` | 36.507 | 28.642 | 1.27× | 117.360 | 39.515 | 2.97× |
| `bulk/set-1024` | 24.947 | 4.512 | 5.53× | 51.146 | 3.501 | 14.61× |
| `bulk/sorted-map-1024` | 39.408 | 27.917 | 1.41× | 369.744 | 38.146 | 9.69× |
| `bulk/sorted-set-1024` | 21.659 | 3.762 | 5.76× | 421.210 | 2.976 | 141.56× |
| `bulk/sparse-1024` | 1.287 | 0.674 | 1.91× | 1.370 | 0.677 | 2.02× |
| `bulk/text-16k` | 1.612 | 0.242 | 6.68× | 0.976 | 0.613 | 1.59× |
| `bulk/vector-1024` | 5.800 | 3.754 | 1.55× | 9.979 | 2.973 | 3.36× |
| `stress/arrays/boolean` | 0.385 | 0.081 | 4.76× | 1.098 | 0.069 | 15.85× |
| `stress/arrays/byte` | 0.126 | 0.052 | 2.45× | 0.085 | 0.028 | 3.02× |
| `stress/arrays/char` | 0.354 | 0.083 | 4.25× | 1.129 | 0.066 | 17.11× |
| `stress/arrays/double` | 0.183 | 0.106 | 1.73× | 0.139 | 0.039 | 3.59× |
| `stress/arrays/float` | 0.175 | 0.094 | 1.87× | 0.136 | 0.030 | 4.58× |
| `stress/arrays/int` | 0.172 | 0.094 | 1.83× | 0.136 | 0.030 | 4.55× |
| `stress/arrays/long` | 0.179 | 0.105 | 1.71× | 0.144 | 0.039 | 3.68× |
| `stress/arrays/object` | 1.219 | 0.490 | 2.49× | 0.717 | 0.660 | 1.09× |
| `stress/arrays/short` | 0.364 | 0.083 | 4.39× | 1.126 | 0.066 | 17.13× |
| `stress/arrays/str` | 0.425 | 0.373 | 1.14× | 1.009 | 0.692 | 1.46× |
| `stress/bigdec` | 0.135 | 0.043 | 3.14× | 0.095 | 0.027 | 3.49× |
| `stress/bigint` | 0.164 | 0.052 | 3.12× | 0.113 | 0.027 | 4.24× |
| `stress/byte` | 0.124 | 0.017 | 7.13× | 0.080 | 0.013 | 6.15× |
| `stress/char` | 0.122 | 0.018 | 6.93× | 0.079 | 0.013 | 6.18× |
| `stress/complete` | 49.209 | 16.614 | 2.96× | 115.294 | 32.558 | 3.54× |
| `stress/defrecord` | 0.244 | 0.128 | 1.91× | 0.649 | 0.119 | 5.45× |
| `stress/deftype` | 0.194 | 0.134 | 1.45× | 0.487 | 0.104 | 4.70× |
| `stress/double` | 0.124 | 0.018 | 7.04× | 0.079 | 0.013 | 6.07× |
| `stress/duration` | 0.123 | 0.041 | 2.99× | 0.081 | 0.015 | 5.28× |
| `stress/false` | 0.120 | 0.018 | 6.87× | 0.077 | 0.013 | 5.97× |
| `stress/false-boxed` | 0.122 | 0.017 | 6.98× | 0.076 | 0.013 | 5.87× |
| `stress/float` | 0.122 | 0.018 | 6.91× | 0.077 | 0.013 | 6.01× |
| `stress/instant` | 0.121 | 0.043 | 2.85× | 0.080 | 0.015 | 5.20× |
| `stress/integer` | 0.120 | 0.018 | 6.64× | 0.078 | 0.013 | 6.01× |
| `stress/kw` | 0.143 | 0.060 | 2.38× | 0.116 | 0.035 | 3.29× |
| `stress/kw-long` | 0.174 | 0.086 | 2.03× | 0.330 | 0.060 | 5.54× |
| `stress/kw-ns` | 0.143 | 0.074 | 1.94× | 0.170 | 0.036 | 4.76× |
| `stress/lazy-seq` | 0.531 | 0.327 | 1.62× | 0.525 | 0.207 | 2.53× |
| `stress/lazy-seq-empty` | 0.143 | 0.020 | 7.20× | 0.106 | 0.016 | 6.80× |
| `stress/list` | 0.303 | 0.128 | 2.38× | 0.241 | 0.138 | 1.75× |
| `stress/long` | 0.122 | 0.018 | 6.72× | 0.077 | 0.013 | 5.91× |
| `stress/many-doubles` | 3.284 | 2.185 | 1.50× | 4.908 | 1.770 | 2.77× |
| `stress/many-keywords` | 5.492 | 3.962 | 1.39× | 10.341 | 8.204 | 1.26× |
| `stress/many-longs` | 3.078 | 2.086 | 1.48× | 5.237 | 1.776 | 2.95× |
| `stress/many-strings` | 6.446 | 4.399 | 1.47× | 15.976 | 10.711 | 1.49× |
| `stress/map` | 0.494 | 0.396 | 1.25× | 0.682 | 0.522 | 1.31× |
| `stress/map-entry` | 0.147 | 0.053 | 2.75× | 0.122 | 0.068 | 1.80× |
| `stress/meta` | 0.242 | 0.193 | 1.25× | 0.283 | 0.210 | 1.35× |
| `stress/nested` | 1.899 | 0.732 | 2.59× | 2.555 | 1.397 | 1.83× |
| `stress/nil` | 0.121 | 0.018 | 6.90× | 0.073 | 0.013 | 5.79× |
| `stress/non-comparable/ex-info` | 4.617 | 0.139 | 33.27× | 23.293 | 0.080 | 292.13× |
| `stress/non-comparable/exception` | 3.581 | 0.142 | 25.17× | 13.169 | 0.076 | 173.06× |
| `stress/non-comparable/regex` | 0.129 | 0.054 | 2.41× | 0.327 | 0.036 | 9.03× |
| `stress/non-comparable/throwable` | 3.483 | 0.145 | 23.97× | 12.372 | 0.079 | 157.47× |
| `stress/period` | 0.122 | 0.046 | 2.67× | 0.080 | 0.015 | 5.25× |
| `stress/queue` | 0.311 | 0.208 | 1.50× | 0.381 | 0.217 | 1.76× |
| `stress/queue-empty` | 0.150 | 0.021 | 7.03× | 0.093 | 0.016 | 6.01× |
| `stress/ratio` | 0.126 | 0.039 | 3.21× | 0.097 | 0.041 | 2.37× |
| `stress/set` | 0.410 | 0.126 | 3.25× | 0.772 | 0.150 | 5.15× |
| `stress/short` | 0.121 | 0.018 | 6.56× | 0.078 | 0.013 | 6.09× |
| `stress/sorted-map` | 0.238 | 0.191 | 1.25× | 0.388 | 0.163 | 2.38× |
| `stress/sorted-set` | 0.227 | 0.053 | 4.26× | 0.403 | 0.050 | 8.10× |
| `stress/sql-date` | 0.120 | 0.037 | 3.30× | 0.076 | 0.015 | 5.08× |
| `stress/str-long` | 0.327 | 0.083 | 3.93× | 0.188 | 0.129 | 1.45× |
| `stress/str-short` | 0.149 | 0.054 | 2.78× | 0.129 | 0.052 | 2.46× |
| `stress/subvec` | 0.179 | 0.056 | 3.22× | 0.125 | 0.049 | 2.55× |
| `stress/sym` | 0.130 | 0.023 | 5.59× | 0.100 | 0.027 | 3.71× |
| `stress/sym-long` | 0.168 | 0.065 | 2.59× | 0.150 | 0.061 | 2.45× |
| `stress/sym-ns` | 0.136 | 0.039 | 3.47× | 0.115 | 0.028 | 4.04× |
| `stress/true` | 0.124 | 0.018 | 7.08× | 0.074 | 0.013 | 5.84× |
| `stress/uri` | 0.127 | 0.054 | 2.34× | 0.260 | 0.037 | 7.03× |
| `stress/util-date` | 0.122 | 0.037 | 3.35× | 0.078 | 0.015 | 5.20× |
| `stress/uuid` | 0.122 | 0.049 | 2.47× | 0.078 | 0.015 | 5.24× |
| `stress/vector` | 0.338 | 0.125 | 2.69× | 0.202 | 0.127 | 1.59× |
| Entity reference | not measured | not measured | — | not measured | not measured | — |
| Interpreted function, including closures | not measured | not measured | — | not measured | not measured | — |
| EDN reader fallback | not measured | not measured | — | not measured | not measured | — |
| Metadata protocol-key marker | not measured | not measured | — | not measured | not measured | — |

Entity resolution and interpreted-function construction require separate
runtime workloads; Rust currently retains their descriptors or source forms.
The EDN reader fallback and metadata protocol-key marker have no standalone
fixture in this performance corpus. Compression and storage/network
throughput are outside this codec comparison.
