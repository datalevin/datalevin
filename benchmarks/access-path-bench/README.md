# Access-path Benchmark

This benchmark measures the query-engine benefit of Datalevin physical access
paths. It covers AVE, idoc, fulltext, and vector access, plus a conventional
control query. Each query runs in two modes:

1. **Conventional**: physical access methods are disabled and the query uses
   its normal complete plan.
2. **Access**: physical access methods are enabled. Eligible queries use
   `:adaptive-top-k` or `:adaptive-limit`; the control remains conventional.

Before timing, the runner requires both modes to return exactly the same ordered
result for AVE, fulltext, vector, and control queries. Idoc uses an unordered
limit, so both windows are instead checked against the complete conventional
result. Query-result caching is disabled in both modes. Measurements alternate
the execution order of the two modes and report median and p95 latency.

The synthetic workload deliberately asks the source function for a wide top-N
window, joins it with a selective boolean attribute and multi-valued metadata,
and asks the root query for a small ordered result. Conventional execution
expands the full ranked window through those joins; access execution only
expands the candidate prefix needed to prove the top-k. This is the workload
shape the access path is designed to improve. The idoc workload exercises
adaptive unordered limits. The control workload has no eligible access path and
measures the overhead of leaving access discovery enabled for an ordinary
bounded query.

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

| Workload | Conventional median | Access median | Ratio | Conventional p95 | Access p95 | Source checks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| AVE | 191.874 ms | 34.828 ms | 5.51x | 233.027 ms | 40.178 ms | — |
| Control | 31.048 ms | 30.603 ms | 1.01x | 33.825 ms | 33.150 ms | — |
| Fulltext | 94.647 ms | 2.682 ms | 35.30x | 150.425 ms | 3.540 ms | 10,000 -> 65 |
| Idoc | 68.618 ms | 4.199 ms | 16.34x | 75.498 ms | 4.762 ms | — |
| Vector | 102.201 ms | 11.706 ms | 8.73x | 108.840 ms | 12.752 ms | 10,000 -> 10,000 |

AVE, fulltext, and vector used `:adaptive-top-k`; idoc used `:adaptive-limit`.
Their maximum candidate budgets were 2, 100, 100, and 200 respectively, versus
20,000-row AVE/idoc and 10,000-row fulltext/vector source windows. Fulltext's
lazy source also reduced document-filter checks. Vector's unchanged source
check count confirms that this benchmark leaves the existing eager approximate
HNSW search alone; its speedup comes from avoiding downstream Datalog expansion
and materialization. The control selected no access path and stayed within
measurement noise of disabled access discovery.

The [raw result](results/linux-i7-6850k-2026-08-05.edn) includes all twenty
latency samples for each workload and mode, configuration, load time, and JVM
metadata.

## Run

From this directory:

```console
clojure -M:bench
```

The defaults load 20,000 entities, add 64 metadata values to one entity in ten,
request a source-local fulltext/vector top 10,000, and return the root top 10.
All five workloads run by default. Each mode gets eight warmup runs and twenty
measured runs. Use `--workloads` with a comma-separated subset of
`ave,idoc,fulltext,vector,control` to narrow a run.

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
- `source checks`: diagnostic source work where a provider exposes a safe
  filter hook. This shows lazy consumption for fulltext; it intentionally stays
  unchanged for the current eager vector source.

The runner fails before timing if an access workload does not select its
expected adaptive mode, the control unexpectedly selects an access path, an
ordered result differs, an idoc window contains a row outside the complete
result, or a root result window is not full.
