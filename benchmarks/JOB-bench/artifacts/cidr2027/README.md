# CIDR 2027 artifact

This is the frozen artifact for *Tail-Robust Query Planning from Triple
Storage*. The `cidr2027` Git tag fixes the paper, implementation, accepted
result files, and the inputs used by the exact-cardinality calibration.

The artifact supports two workflows:

1. Recheck the paper from the accepted per-trial data. This takes minutes and
   does not require rebuilding the 17 GB JOB database.
2. Rerun the experiments. This requires the JOB database and, for Exact,
   either the supplied exact-cardinality checkpoints or an offline native
   recount.

## Environment and dataset

The accepted runs used Datalevin 1.0.0 with native library 0.18.3, OpenJDK
21.0.11, and macOS 26.5.1 on an Apple M3 Pro with 12 cores and 36 GB of
memory. Manifests record the complete JVM, hardware, database fingerprint,
health gates, and executed command for each run. Unless noted otherwise,
queries had a 30-second timeout.

Follow the parent [JOB README](../../README.md) to load the May 2013 JOB data.
A complete load contains 277,878,411 triples and occupies approximately 17 GB.
The database itself is generated data and is not part of this artifact.

## Accepted results

Each manifest is append-only: the final `:complete` EDN form is authoritative.
The accepted analysis timestamp is deliberately listed below because some
local development directories contained earlier duplicate analyses.

|Experiment|Paper use|Accepted files|Compressed timing CSV|
|---|---|---|---|
|30-seed sampled policies|Abstract, Tables 2--3, and Figure 3: Full, No counts, Shrinkage, and Raw|[manifest](../../results/cidr-confirmatory-sampled-policies-20261001/optimizer_manifest_1785311821471.edn), [health](../../results/cidr-confirmatory-sampled-policies-20261001/optimizer_health_1785311821471.edn), [validation](../../results/cidr-confirmatory-sampled-policies-20261001/optimizer_validation_1785323922903.edn), [summary](../../results/cidr-confirmatory-sampled-policies-20261001/optimizer_summary_1785324090644.edn), [slowdowns](../../results/cidr-confirmatory-sampled-policies-20261001/optimizer_slowdowns_1785324090644.csv), [plan disagreements](../../results/cidr-confirmatory-sampled-policies-20261001/optimizer_plan_disagreements_1785324090644.csv)|[`cidr2027-sampled-policies-timing.csv.zst`](raw/cidr2027-sampled-policies-timing.csv.zst)|
|10-schedule No sampling|Abstract, Tables 2--3, and Figure 3|[manifest](../../results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_manifest_1785324144397.edn), [health](../../results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_health_1785324144397.edn), [validation](../../results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_validation_1785332539096.edn), [summary](../../results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_summary_1785332556151.edn), [slowdowns](../../results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_slowdowns_1785332556151.csv), [plan disagreements](../../results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_plan_disagreements_1785332556151.csv)|[`cidr2027-no-sampling-timing.csv.zst`](raw/cidr2027-no-sampling-timing.csv.zst)|
|100-seed plan census|Plan-variability and modal-plan claims|[manifest](../../results/cidr-uncertainty-plan-census-deterministic-20260801/optimizer_manifest_1785309301598.edn), [health](../../results/cidr-uncertainty-plan-census-deterministic-20260801/optimizer_health_1785309301598.edn), [validation](../../results/cidr-uncertainty-plan-census-deterministic-20260801/optimizer_validation_1785311454588.edn), [summary](../../results/cidr-uncertainty-plan-census-deterministic-20260801/optimizer_summary_1785311611670.edn), [slowdowns](../../results/cidr-uncertainty-plan-census-deterministic-20260801/optimizer_slowdowns_1785311611670.csv), [plan disagreements](../../results/cidr-uncertainty-plan-census-deterministic-20260801/optimizer_plan_disagreements_1785311611670.csv)|[`cidr2027-plan-census-timing.csv.zst`](raw/cidr2027-plan-census-timing.csv.zst)|
|One-factor sensitivity|Parameter-sensitivity paragraph|[selection and frozen design](../../experiments/cidr-sensitivity-20261201.edn), [manifest](../../results/cidr-sensitivity-runtime-20261201/optimizer_manifest_1785336257190.edn), [health](../../results/cidr-sensitivity-runtime-20261201/optimizer_health_1785336257190.edn), [validation](../../results/cidr-sensitivity-runtime-20261201/optimizer_validation_1785342541128.edn), [summary](../../results/cidr-sensitivity-runtime-20261201/optimizer_summary_1785342574261.edn), [slowdowns](../../results/cidr-sensitivity-runtime-20261201/optimizer_slowdowns_1785342574261.csv), [plan disagreements](../../results/cidr-sensitivity-runtime-20261201/optimizer_plan_disagreements_1785342574261.csv)|[`cidr2027-sensitivity-timing.csv.zst`](raw/cidr2027-sensitivity-timing.csv.zst)|
|30-seed Exact versus Estimated|Abstract and Section 4.2 calibration|[manifest](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/optimizer_manifest_1785768951795.edn), [health](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/optimizer_health_1785768951795.edn), [rejected-health sidecar](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/optimizer_rejected_health_1785768951795.edn), [summary](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/analysis/optimizer_summary_1785773346893.edn), [slowdowns](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/analysis/optimizer_slowdowns_1785773346893.csv), [plan disagreements](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/analysis/optimizer_plan_disagreements_1785773346893.csv), [per-query table](../../results/true-vs-estimated-strict-all-execution-30x-v100-20260803/per_query_true_vs_estimated_execution_30x.csv)|[`cidr2027-exact-vs-estimated-timing.csv.zst`](raw/cidr2027-exact-vs-estimated-timing.csv.zst)|

The uncompressed slowdown files contain the condition, paired sample seed,
planning and execution times, timeout charge, slowdown, catastrophe flag, and
structural plan hash used by the paper. The compressed files are the original
losslessly compressed timing CSVs and allow the checked-in analysis code to
regenerate each summary. Large estimator-observation and counted-execution
diagnostic CSVs are omitted because no reported statistic depends on them;
their accepted validation reports are included.

## Check the artifact

From this directory, verify every tracked data file with either command:

```bash
shasum -a 256 -c SHA256SUMS
# GNU/Linux alternative:
sha256sum -c SHA256SUMS
```

To regenerate an analysis, decompress its timing archive and invoke the
analysis alias from `benchmarks/JOB-bench`. For example:

```bash
zstd -d -f artifacts/cidr2027/raw/cidr2027-sampled-policies-timing.csv.zst \
  -o /tmp/cidr2027-sampled-policies-timing.csv
clj -Xanalyze \
  :timing-file '"/tmp/cidr2027-sampled-policies-timing.csv"' \
  :timeout-ms 30000 \
  :bootstrap-samples 2000 \
  :bootstrap-seed 20260727 \
  :output-dir '"/tmp/cidr2027-sampled-analysis"'
```

The layer and sensitivity analyses use 2,000 bootstrap replicates. The Exact
comparison uses 10,000 because its single headline calibration interval was
inexpensive to stabilize; pass `:bootstrap-samples 10000` when regenerating
that summary.

Regenerate the paper's SVG figures and PDF from the repository root with:

```bash
python3 doc/conference-submissions/cidr2027/scripts/generate_figures.py
make -C doc/conference-submissions/cidr2027
```

The figure generator asserts the expected row counts and timeout counts before
rewriting Figure 3. The submitted PDF is checked in alongside its TeX source.

## Exact-cardinality calibration

[`cidr2027-exact-cardinalities.tar.zst`](raw/cidr2027-exact-cardinalities.tar.zst)
contains exactly 228 Datalevin-native checkpoint files: one logical and one
materialized-link-input file for each of the 113 JOB queries, plus
`shared.edn` and `shared.material.edn`. It contains neither the superseded
PostgreSQL-assisted files nor development calibration files. From the
repository root, restore it with:

```bash
tar --zstd -xf \
  benchmarks/JOB-bench/artifacts/cidr2027/raw/cidr2027-exact-cardinalities.tar.zst
```

The logical and material checkpoints can instead be recomputed entirely in
Datalevin. From `benchmarks/JOB-bench`, run:

```bash
clj -Xoracle-factorized \
  :output-dir '"results/cidr-exact-cardinalities-20260801"' \
  :timeout-ms 600000
clj -Xoracle-factorized \
  :output-dir '"results/cidr-exact-cardinalities-20260801"' \
  :timeout-ms 600000 \
  :material-cardinalities? true
```

The implementation is in
[`cardinality_factorized.clj`](../../src/datalevin_bench/cardinality_factorized.clj),
with tests in
[`cardinality_factorized_test.clj`](../../test/datalevin_bench/cardinality_factorized_test.clj).
The supplied archive avoids repeating the offline computation, which completed
in under one day on the evaluation machine.

The Exact run changes only the cardinalities supplied to the same enumerator,
pruning rules, physical operators, and cost model. Its manifest preserves the
implementation's historical `:cost-model :legacy` identifier; that keyword
selects Datalevin's unchanged default cost model rather than a separate model
used only by Exact. The run recorded planning and execution separately, and
all paper comparisons use only the `Execution Time (ms)` column. Preparation
time is excluded.

## Rerunning the long experiments

The final `:complete` form of each accepted manifest contains the fully
resolved configuration and the exact command under `[:runtime :command]`.
Run that command through the `:eval` alias (`clj -Xeval` with the recorded
key/value arguments), changing only `:output-dir` to avoid mixing a
reproduction with the accepted files. The runner records new manifests,
health snapshots, raw timings, and diagnostics automatically.

These runs intentionally enforce the paper's isolation protocol: Docker must
be stopped, low-memory or swap-contaminated passes are rejected, query and
condition order is seed-controlled, and a killed worker is restarted and
rewarmed before continuing. Runtime depends on the timeout-heavy ablations;
the exact-cardinality generation is the longest single offline step.
