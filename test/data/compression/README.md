# JOB compression samples

These are bounded samples from the JOB IMDB database used in the compression
evaluation. They are serialized index records, not LMDB database files.
Each gzip file contains 163,840 records in sampling-rank order. Each record is
a big-endian unsigned 32-bit byte length followed by that many raw bytes.

| File | Ordered stream | SHA-256 of uncompressed file |
|------|----------------|-----------------------------|
| `ave-key.bin.gz` | Distinct AVE keys | `f7d40dccbc3c21bda0add82c220981757adb0e03ed5499c75361092b25a74aeb` |
| `eav-value.bin.gz` | EAV duplicate values | `324f7080df198159b0a646128be55f1a8113676f7d93d69644cd9f0a852491d2` |

Collection used one read-only snapshot on 2026-09-10, populations of 31,615,420
distinct AVE keys and 277,878,411 EAV duplicates, and uniform Floyd rank sampling
with `java.util.Random` seed 92026. The source database was not modified.

Run from the repository root after compiling Java with `clojure -T:build compile-java`:

```sh
clojure -J-Xmx512m -M:dev script/compression_eval.clj /tmp/compression-evaluation.json
```

The script verifies these checksums, trains six nested sample sizes for three
seeds per stream with a disjoint 32,768-record holdout, and measures actual
encoded bytes and round trips. It also verifies all 327,680 records and their
adjacent unsigned ordering with 65,536-record training samples. The full-corpus
check includes training data; the curves use only held-out data. Runtime/build
timings vary between runs. Byte counts and correctness results are reproducible.

The checked-in JSON preserves the pre-fix failures under `historical_pre_fix`.
The current `rows` and `roundtrip_checks` describe the fixed codec. These results
do not establish database page savings, throughput, or activation thresholds.
