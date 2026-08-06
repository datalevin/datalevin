# Access-path Benchmark

This benchmark measures the query-engine benefit of Datalevin physical access
paths. It runs the same fulltext and vector queries in two modes:

1. **Conventional**: the existing query function materializes its complete
   source-local top-N result before the residual indexed join and root
   top-k are evaluated.
2. **Access**: the optimizer selects `:adaptive-top-k` and incrementally feeds
   source rows through the residual join until the root top-k result is
   proven complete.

Before timing, the runner requires both modes to return exactly the same ordered
result. Query-result caching is disabled in both modes. Measurements alternate
the execution order of the two modes and report median and p95 latency.

The synthetic workload deliberately asks the source function for a wide top-N
window, joins it with a selective boolean attribute and multi-valued metadata,
and asks the root query for a small ordered result. Conventional execution
expands the full ranked window through those joins; access execution only
expands the candidate prefix needed to prove the top-k. This is the workload
shape the access path is designed to improve.

## Vector benchmark scope

The vector result remains relative to the existing approximate
`vec-neighbors` function. Both modes execute the same HNSW search with the same
`:top` value. The access path does **not** claim to reduce HNSW graph traversal;
it avoids materializing and processing the unused approximate result rows in
the Datalog query pipeline.

The reported candidate window is query-level work, not internal HNSW
candidates.

## Reference result

One default-configuration run on 2026-08-05 produced the following result.
The host was an Intel Core i7-6850K (6 cores/12 threads), Linux amd64, and
OpenJDK 21.0.10. Absolute latency and speedup are machine-dependent.

| Workload | Conventional median | Access median | Speedup | Conventional p95 | Access p95 | Source filter calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fulltext | 95.732 ms | 4.154 ms | 23.05x | 102.939 ms | 5.405 ms | 10,000 -> 65 |
| Vector | 103.999 ms | 11.842 ms | 8.78x | 136.028 ms | 13.014 ms | 10,000 -> 10,000 |

Both access plans used `:adaptive-top-k`, had a maximum candidate budget of
100 instead of the conventional 10,000-row source window, and returned the
same ordered 10 rows as conventional execution. Fulltext's lazy source also
reduced document-filter checks. Vector's unchanged source-filter count confirms
that this benchmark leaves the existing eager approximate HNSW search alone;
its speedup comes from avoiding downstream Datalog expansion and
materialization.

The [raw result](results/linux-i7-6850k-2026-08-05.edn) includes all twenty
latency samples for each workload and mode, configuration, load time, and JVM
metadata.

## Run

From this directory:

```console
clojure -M:bench
```

The defaults load 20,000 entities, add 64 metadata values to one entity in ten,
request a source-local top 10,000, and return the root top 10. Each mode gets
eight warmup runs and twenty measured runs.

For a quick smoke run:

```console
clojure -M:bench --records 2000 --source-top 1000 \
  --dimensions 8 --warmup 2 --iterations 5
```

To save the complete result map, including raw latency samples and host
metadata:

```console
clojure -M:bench --output results.edn
```

Run `clojure -M:bench --help` for all options. Database loading and indexing are
reported separately and are excluded from query latency.

## Interpreting the result

The most useful fields are:

- `median speedup`: conventional median latency divided by access median
  latency.
- `candidate window`: the conventional source top-N compared with the
  optimizer's maximum access attempt before its safe conventional fallback.
- `source filter calls`: diagnostic source work. This shows lazy consumption
  for fulltext; it intentionally stays unchanged for the current eager vector
  source.

The runner fails before timing if the access plan is not selected, adaptive
top-k is not used, the two modes disagree, or the root result window is not
full.
