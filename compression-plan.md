# Compression Evaluation and Lifecycle Plan

Draft design based on the implementation and bounded JOB sampling on
2026-09-10, rerun with the fixed codec on 2026-09-11. Compression remains opt-in.
Implemented: uniform rank sampling, the codec/builder fixes, duplicate-value
validation, raw bootstrap metadata, and persisted environment compression modes
with dictionary checksums. Per-stream dictionaries, distinct-key list sampling,
stratification, prior tuning, and automatic maintenance remain proposed. The
sample-size results below describe this workload; policy thresholds are proposals.

## Current Implementation

- `src/datalevin/hu.clj` creates dictionaries with 65,536 two-byte
  symbols, 256 odd-final-byte terminals, and one end-of-key terminal. Frequencies
  start at one for every symbol so unseen symbols retain a code. Code lengths
  and codes are persisted; decode tables are reconstructed.
- `src/datalevin/compress.clj` samples 65,536 entries across an environment,
  apportioned by DBI entry counts. For list DBIs it samples duplicate rows,
  counts their values, and deduplicates keys encountered in that sample.
  This pools different byte distributions and does not uniformly sample
  distinct keys in list DBIs.
- The binding has environment-wide key/value compressors, with dictionary
  files `keycode.bin` and `valcode.bin`. The implemented options are
  `:key-compress` and `:val-compress`. The implemented dictionary value
  compressor uses Zstd. The raw `datalevin/kv-info` DBI now persists a
  `:compression` manifest with a generation ID, key/value methods (`:none`,
  `:hu`, or `:zstd` as appropriate), and SHA-256 for each required dictionary.
  Existing stores reopen from this manifest without compression options.
  Explicit conflicting options, missing dictionaries, changed dictionary bytes,
  and unsupported manifests fail opening. Modes can only be selected when
  creating a new environment; changing an existing mode requires a rebuild.
  A raw store without a manifest acquires a raw manifest. The undeployed
  compressed prototype is not a supported input format.
- DBI handles retain their codecs. Bootstrap metadata always bypasses both
  compressors, including writes after opening other DBIs, point lookups, ranges,
  and rank reads. Physical `copy` carries both dictionary files. Compression
  currently requires a persistent environment; temporary/in-memory compression
  options are rejected rather than silently ignored.
- Giant payloads already have thresholded Zstd compression, defaulting to
  1,024 bytes and level 3. Generic environment value compression is a different
  mechanism and is not a complete thresholded value-compression facility.
- `lmdb/re-index*` dumps, clears the source, closes, and reloads. It is not the
  proposed safe mechanism for replacing a live compression generation.
  `migrate.clj` already stages destinations, verifies counts, swaps with a backup,
  and rolls back failed swaps; extend that machinery for compression rebuilds.

## Value Compression

For ordinary, unordered payload values, evaluate dictionary-free compression
first so a threshold adjustment does not also require managing dictionaries.
Benchmark candidate size thresholds from 128 bytes through 16 KiB on serialized
payloads, including typical records, text, custom payloads, already-compressed
bytes, and incompressible bytes. Compare suitable Zstd levels and a fast
alternative before choosing defaults.

A size threshold only decides whether to try compression. Store the compressed
form only if its complete framed size saves enough bytes. The current giant
compression decision compares compressed payload length with raw length before
adding its 9-byte envelope; include that envelope in the decision.

Measure encode/decode CPU, allocations, stored pages, point/range read latency,
and write throughput with both resident and I/O-bound data. A compression ratio
alone cannot locate the workload's performance crossover. Do not infer a new
threshold from the current 1 KiB default; that crossover has not been measured
in this evaluation.

Persist an explicit value-format version and unambiguous raw/compressed modes.
Arbitrary legacy KV bytes cannot safely be distinguished from a new frame by
an unchecked magic-prefix guess. Moving an existing unframed DBI to framing
may need a rebuild; changing the write threshold within an established framed
format need not rewrite old values.

DUPSORT values are ordered index data, even though the API calls them values.
They belong to the key-compression design. This was a live binding bug: only
DUPFIXED values bypassed Zstd, so EAV and variable-width user list values were
compressed with a codec that does not preserve order. For example, `aa ab b ba
bb c` could sort physically as `b c aa ab ba bb`, breaking a `b` through `bb`
range. The binding now rejects opening/creating any non-DUPFIXED DUPSORT DBI
under `:val-compress :zstd`, before allocating or registering that DBI. Opening
an existing environment also validates all persisted DBI declarations. Fixed
8-byte AVE entity-ID duplicates and other DUPFIXED values remain raw. Ordinary
unordered DBI values may use Zstd. This guard means environment value compression
cannot currently be enabled on Datalog stores; a separate duplicate codec or
per-DBI value-compression override is future work.

## Key Compression Scope

Use an immutable dictionary per independently encoded ordered stream, rather
than training one environment-wide mixture. Initially, evaluate per-DBI key
streams and separately consider non-DUPFIXED ordered duplicate streams.
Explicitly identify the comparator and supported scan paths for each stream.

For Datalog, measure AVE keys, EAV keys, and EAV duplicate values separately.
Their distributions differ. Keep the uncompressed primary AVG representation
shared at the logical codec boundary; applying different physical compression
to different index streams requires auditing the binding and all native fast
paths that currently inspect raw IDs, attribute IDs, or AVG fields directly.

Dictionary memory and lookup overhead can outweigh small-index savings. Share
an immutable dictionary only when validation demonstrates that the streams
benefit from sharing it. Keep bootstrap/format metadata uncompressed so opening
a database never requires first decoding the dictionary's own location.

An order-preserving dictionary cannot be changed in place while retaining old
encoded keys. Different generations cannot be mixed in the same B-tree under
the existing byte comparator. A per-key dictionary tag or raw fallback would
change ordering; neither is a shortcut around rebuilding.

The concrete guaranteed input bound is **253 serialized raw bytes** with the
standard 511-byte key buffer. Each pair and terminal uses at most 32 bits, so
an `n`-byte input requires at most `4 * (floor(n / 2) + 1)` encoded bytes.
For `n = 253` this is 508 bytes; for `n = 254` the bound reaches 512 bytes.
Include the existing type header and other framing in that raw input length.
A DBI configured with a smaller key buffer needs a correspondingly smaller bound.

Larger raw keys, up to the existing 511-byte raw-buffer limit, are accepted
only when their actual encoding fits. No raw fallback or truncation is used.
A binding regression exercises a deep dictionary with 32-bit codes: a
253-byte raw key fits, a 511-byte raw key expands past the native limit, and
that failed write rolls back its entire transaction. Thus compression cannot
promise support for every otherwise legal 511-byte key. Zero overflows in a
sample does not remove this restriction. Staged rebuild must encode and validate
every record before activation, leaving the source intact on overflow.

## Sampling Results

Sampled 163,840 records from each of two streams in the same read-only native
transaction: distinct AVE keys from a population of 31,615,420 keys, and EAV
values from 277,878,411 datoms. Collection took approximately 0.8 s and 17.0 s
respectively in this run; these are not controlled cold-cache measurements.
The existing JOB file uses native format 1, so sampling used native 0.19.4
without opening it through auto-migration. No database contents were rewritten.

Ranks were selected uniformly without replacement using Floyd sampling and
`java.util.Random.nextLong(bound)`, independently of the repository sampler.
Each corpus was shuffled with three recorded seeds. Each split reserves 32,768
records for validation; training prefixes grow from 4,096 to 131,072 records.
Training and validation are disjoint within a split; the three splits reuse
the same sampled corpus. The rerun uses the fixed terminal alphabet and heap,
measures actual encoded bytes, and round-trips every held-out record using an
exact-sized decode buffer. Mean encoded sizes as a percentage of raw stream bytes:

| Training records | AVE keys | EAV ordered values |
|------------------|----------|--------------------|
| 4,096 | 63.04% | 58.37% |
| 8,192 | 60.06% | 57.53% |
| 16,384 | 59.30% | 54.68% |
| 32,768 | 58.12% | 53.81% |
| 65,536 | 57.63% | 53.44% |
| 131,072 | 57.45% | 53.17% |

Doubling 64K to 128K training reduces encoded bytes by approximately 0.31% for
AVE keys and 0.52% for EAV values. This makes 64K a candidate starting budget
for this workload; it does not establish a default or a population percentage.
All 1,179,648 held-out round trips across the 36 candidates passed. No candidate
holdout exceeded 511 encoded bytes. Under the 64K dictionaries, the full-corpus
maxima were 378 bytes for AVE and 388 for EAV; two EAV records expanded but
still fit. These observations do not guarantee that future keys fit.

These are codec results, not deployed page savings or end-to-end performance.
Actual file savings depend on native prefix compression, page fill, branching,
and dictionary metadata. The old code-length estimates and failed decode checks
are retained only as historical evidence in the JSON's `historical_pre_fix`.

The source [HOPE paper, Appendix A](https://arxiv.org/pdf/2003.02391) reports
10K–100K samples as a useful general range for its workloads, with larger
samples needed to reach maximum compression. That supports testing this scale;
it does not establish a Datalevin default.

Full measurements, corpus summaries, seeds, and sample-file checksums are in
[the evaluation data](doc/compression-evaluation-2026-09-10.json). The two
[gzipped sample corpora](test/data/compression/README.md) are included, about
1.9 MiB combined. Reproduce the current curves and full-corpus verification:

```sh
clojure -T:build compile-java
clojure -J-Xmx512m -M:dev script/compression_eval.clj /tmp/compression-evaluation.json
```

The script checks corpus SHA-256s, records source hashes and seeds, and refuses
to publish new results if decoding or ordering fails. Build timings vary; byte
counts and verification outcomes are reproducible.

### Proposed Sample Selection

The repository's rank-selection helper has been corrected. The environment
sampler still pools DBIs and deduplicates sampled list keys in memory; distinct-key
list sampling, per-stream dictionaries, stratification, and prior tuning below
are not implemented. Previously, for population
31,615,420 and sample size 163,840, it selected 60,529 records from the first
163,840 ranks; a uniform sample would average about 849. Its skip probability
was fixed from the final population size, retaining too much of the initial
reservoir. It now uses uniform Floyd sampling without replacement, preserving
sorted ranks, deterministic seeds, and caching. Focused regressions check
population coverage and subset frequencies. Both the original saved corpora and the fixed-codec rerun use uniform rank
sampling. The table above replaces the old framing estimates on those corpora.
Validate rank uniformity before judging dictionary quality; more samples cannot
substitute for correcting the sampling method.

- Sample the bytes actually encoded by each dictionary. For a key dictionary,
  sample distinct live keys by rank. For an ordered duplicate dictionary,
  sample duplicate entries by rank. Do not use query frequency or first-N
  insertion order as a proxy for storage distribution.
- Use bounded stratification for heterogeneous streams where needed; aggregate
  with appropriate population/byte weights rather than overrepresenting small
  attributes. New bulk-load samples must represent the completed load.
- Record both key count and symbol count. With the current prior,
  there are 65,793 pseudo-observations before any sample is collected. Small
  training sets can be dominated by that prior. Evaluate prior strength along
  with sample size while retaining codes for unseen symbols.
- Start the experiment budget at 64K training records and 32K held-out records
  per substantial candidate stream. Try 16K/32K for smaller streams, and double
  to 128K/256K if validation improvement or variance warrants it. A candidate
  with less than roughly 1% further encoded-byte improvement on doubling is a
  proposed stopping criterion, not a measured universal threshold.
- Use a fixed holdout when comparing candidates, several seeded training
  splits for the evaluation, and a fresh confirmation sample before committing
  to an expensive rebuild. Report variance and long/expanding-key tails.
- Use a fresh seed/epoch on resampling. `reservoir-sampling` caches indices by
  population/sample size and optional seed; repeatedly using the same cache
  key is not a fresh sample.

## Resampling and Rebuild Decisions

Separate cheap analysis from expensive activation. An old complete dictionary
remains usable as distribution changes; drift reduces efficiency and is not
itself a correctness reason to rebuild. The [HOPE paper, section 5](https://db.cs.cmu.edu/papers/2020/zhang-sigmod2020.pdf)
also recommends scheduling dictionary replacement as maintenance.

Proposed events for a new analysis:

- Completion of an initial or substantial bulk load.
- Significant distinct-key growth, or substantial insertion/deletion churn
  since the last analysis. A starting policy could use doubling or roughly
  20–25% turnover, subject to validation and a cooldown.
- An application-requested check after a known distribution change.
- A sustained deterioration in active-code length estimates for recent keys,
  followed by a representative live-data sample to confirm its significance.

Track actual changes to the encoded key stream. Overwriting an ordinary KV
value without changing its key does not change the key distribution. TTL
cleanup and deletion do. Age alone should not force retraining or rebuilding.
Recent writes can signal drift but cannot stand in for the whole live database.

On one validation set, compare the active dictionary, a newly trained candidate,
and the raw representation. A worse compression ratio than at original
training is insufficient: a new distribution may simply be less compressible.
The question is how much better a candidate is than the active dictionary on
current data.

Require both a meaningful relative gain and enough absolute benefit to justify
rewriting the affected data. As an initial policy to benchmark, require roughly
5–10% fewer encoded bytes than the active dictionary, sufficient estimated page
savings, and acceptable CPU/memory cost. These are proposed decision thresholds,
not results from the sample-size experiment. Use uncertainty margins and a
cooldown to avoid repeated rebuilds for noise or tiny gains.

Measure incremental benefit over the default native page-prefix compression.
Only compacted page counts and representative query/write benchmarks can
establish that Hu-Tucker helps the database. If the current format remains the
best choice, leave it active and schedule no rewrite.

## Dictionary Generation Lifecycle

```text
raw -> analyze -> candidate ready -> staged rebuild -> active generation G
                    | reject                              |
                    v                                     v
                   raw                          analyze -> candidate ready
                                                             |
                                                     staged replacement
                                                             |
                                                    active generation G+1
```

1. **Analyze:** collect bounded samples without mutating the active encoding.
   Record population, sample seed/size, histogram, code-length estimates,
   validation results, and dictionary memory cost.
2. **Prepare:** create an immutable candidate manifest with codec version,
   dictionary identity/checksum, stream ownership, and expected encoding flags.
   Mark it ready only after correctness and profitability checks pass.
3. **Schedule:** initially use an explicit maintenance window with writes
   paused. Prefer piggybacking on a requested re-index or required release
   migration, which already pays much of the rewrite cost. Background analysis
   must not automatically start a disruptive rebuild.
4. **Rebuild:** create a separate destination environment. Read logical bytes
   with the source generation and write them with the candidate generation.
   The dump/load pipeline may stream without retaining a second dump file.
   Preserve the source; do not clear it as the current `re-index*` does.
5. **Validate:** verify all records fit, counts/content match, point and range
   access preserve ordering, dictionaries reopen correctly, and primary plus
   dependent stores are consistent. Verify page savings on the rebuilt copy.
6. **Activate:** sync the completed destination and its metadata, close the
   necessary handles, and switch using a recoverable filesystem/manifest
   protocol. Multiple renames are not assumed to be one atomic operation.
   Reopen only with the matching committed generation.
7. **Retain/recover:** keep the old generation as a backup under the existing
   migration retention policy. A failed build leaves the source usable; a
   failed cutover must have enough durable state to finish or roll back.

Use the existing `migrate.clj` implementation as the rebuild template:
`perform-kv-migration` / `perform-datalog-migration` create a separate destination,
load logical data, compare exported and loaded counts, and call
`switch-databases` to retain the source as a backup. That function attempts
rollback on a failed swap. Extract shared staging/validation/swap helpers rather
than inventing another protocol or using destructive `lmdb/re-index*`.
Add compression-specific dictionary/content/order/size validation to these
helpers. The existing exception rollback is not a durable crash-recovery journal:
activation still needs explicit sync ordering, persistent cutover state, and
startup recovery for a crash between renames.

An online rebuild with WAL catch-up is later work. It requires a stable source
snapshot, a replay boundary, logical translation of mutations to destination
encodings, and a coordinated final switch. Simply replaying old compressed
key bytes into the new generation is incorrect.

Keep dictionary bytes or immutable dictionary artifacts inseparable from their
generation manifest. Missing, corrupt, unsupported, or mismatched dictionaries
must prevent database opening; they must never silently fall back to
interpreting encoded bytes as raw. Persisted metadata, not caller-supplied
options alone, determines how existing data is decoded. This is implemented
for the current environment-wide mode; per-stream generations and their staged
activation remain future work. Bootstrap metadata is readable before installing
any stream codecs.

Dumps, copies, snapshots, remote access, WAL/replay, and restore must carry the
required generation state. Logical rebuild must remap TTL companion identities
and custom payload/type dependencies if their encoded keys or IDs change.
The first compression generation can be created during 1.2.0's migration when
explicitly enabled and validated; compression does not become mandatory just
because the native format already requires migration.

## Correctness and Implementation Gates

Implemented fixes and remaining gates:

1. **Prefix ordering — fixed:** with the previous encoding and a uniform
   dictionary, raw `[01]` sorts before `[01 00 00]`. Their encoded forms were
   `[01 00 00 00 01]` and `[01 00 00 00 00 00 03]`, which sort in the reverse
   order. The codec now uses ordered terminal symbols and zero bit padding,
   with no length trailer.
2. **Short-code decoding — fixed:** the four-bit lookup
   tables previously retained only the last decoded symbol per lookup. A lookup
   can finish several short codewords, so
   earlier symbols were lost. With the 128K-trained JOB dictionaries, actual
   encode/decode checks failed on 402–446 of 1,024 held-out AVE keys and
   856–869 of 1,024 EAV values across the three splits. Flat lookup tables now
   retain up to eight output bytes per nibble and handle key termination and
   exact-sized destination buffers.
3. **Builder memory — fixed:** building the complete uniform-frequency alphabet ran
   out of a 1 GiB heap in `LeftistHeap.merge`. Real sampled dictionaries built
   successfully under that cap. Heap melds now release consumed lookup maps and
   their backing tables, merge the smaller index into the larger one, and remove
   stale parent/child links after deletion. The full uniform dictionary
   now builds under a 512 MiB heap. Clearing donor entries alone was insufficient.
4. **Ordered-duplicate Zstd — guarded:** non-DUPFIXED list DBIs are rejected
   under environment value compression. The string-range regression also checks
   that a rejected mode change leaves the raw store's range results intact.
5. **Reopen and dictionary identity — fixed:** compression mode and checksums
   are persisted in raw metadata. Required files are verified before codec
   construction; caller options cannot silently change decoding. Reopen without
   options and physical copies are tested for key-only, value-only, and combined
   compression, including metadata writes and value-buffer growth.
6. **Key expansion — bounded:** the 253-byte raw-input guarantee above and actual
   511-byte encoded limit are exercised through the binding, including transaction
   rollback. Larger serialized keys are conditional on actual encoded size.
7. **Remaining access/lifecycle work:** audit Datalog native prefix/field probes,
   custom encodings, logical dump/restore and WAL replay across generations.
   Ordinary point/range/key-count/rank paths use the DBI's codec, but this does
   not establish that all native paths support all proposed per-stream codecs.
   Do not expose automatic activation until the staged rebuild checks exist.

The fixed codec was trained on 65,536 records from each saved uniform
JOB sample and checked against all 163,840 records in each corpus. All 327,680
round trips and 327,678 adjacent unsigned ordering checks passed, with
exact-sized decode buffers. These full-corpus results are recorded in the
JSON's current `roundtrip_checks`, separately from its historical failures.
This is a correctness check, not an independent compression benchmark or proof that the remaining binding paths work.

The combined existing and new compression, sampling, and heap suites cover
these fixes. The focused tests also passed with a 512 MiB heap; the Java build
passed, and the Hu-Tucker namespace compiles without reflection or boxed-math
warnings.

Focused regressions cover prefix keys, unsigned bytes, short and 32-bit codes,
byte fixtures, dictionary persistence, exact buffers, sampling uniformity,
and heap meld/deletion. The fixed-codec sample curves have been rerun. After
the remaining storage gates, measure a rebuilt copy with native prefix
compression enabled. Only then settle activation thresholds and expose the
maintenance lifecycle as a supported API.
