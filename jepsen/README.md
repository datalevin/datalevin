# Datalevin Jepsen

This directory hosts a Datalevin-specific Jepsen subproject.

The goal is to adapt the useful parts of the Datomic Jepsen suite structure to
Datalevin's HA design:

* reuse Jepsen/Elle workloads, generators, and checkers
* keep Datalevin deployment and fault handling specific to the current HA stack
* start with a single-host 3-node HA cluster so workload semantics are wired
  before full remote fault injection lands

## Current scope

The first cut is intentionally narrow:

* local 3-node HA cluster backend
* append, append-cas, and bank workloads
* Datalevin-specific `grant` and `internal` characterization workloads
* local leader failover, quorum-loss, and clock-skew nemeses
* list-append transactions support reads, appends, and mixed read/write
  sequences

That keeps the first version faithful to Datalevin's existing public API. It
does not claim full parity with the Datomic Jepsen suite.

## Layout

* `src/datalevin/jepsen/local.clj`: single-host 3-node HA cluster harness
* `src/datalevin/jepsen/workload/append.clj`: Datalevin append workload using
  Elle list-append histories
* `src/datalevin/jepsen/workload/append_cas.clj`: append workload variant
  using transaction-local CAS guards for write-write conflicts
* `src/datalevin/jepsen/workload/bank.clj`: transfer workload checking
  conservation of total balance and non-negative account state
* `src/datalevin/jepsen/workload/grant.clj`: transaction-function workload for
  single-decision grant approval races
* `src/datalevin/jepsen/workload/internal.clj`: single-threaded internal
  transaction semantics characterization workload
* `src/datalevin/jepsen/core.clj`: test construction
* `src/datalevin/jepsen/cli.clj`: CLI entrypoint

## Running

Compile Datalevin's Java sources first so `../target/classes` is available:

```bash
clojure -T:build compile-java
```

Run the Jepsen subproject smoke test:

```bash
cd jepsen
lein test
```

Run a local append workload:

```bash
cd jepsen
lein run test --workload append --time-limit 30 --rate 10
```

Run the CAS-guarded variant:

```bash
cd jepsen
lein run test --workload append-cas --time-limit 30 --rate 10
```

Run the grant workload:

```bash
cd jepsen
lein run test --workload grant --time-limit 30 --rate 10
```

Run the bank transfer workload:

```bash
cd jepsen
lein run test --workload bank --time-limit 30 --rate 10 --key-count 8 --account-balance 100 --max-transfer 5
```

Run the internal semantics workload:

```bash
cd jepsen
lein run test --workload internal --time-limit 15 --rate 5
```

Run the same workload with the first HA fault mode:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis failover --time-limit 30 --rate 10
```

Run a quorum-loss cycle:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis quorum-loss --time-limit 30 --rate 10
```

Run a clock-skew pause combined with leader failover:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis clock-skew,failover --time-limit 30 --rate 10
```

The HA disruption nemeses currently require `--control-backend sofa-jraft`,
because node restart/rejoin and quorum recovery depend on persisted authority
membership. The default `in-memory` backend remains useful for no-fault local
iteration.

## Next steps

The next meaningful increments are:

* more nemeses for pause, partition, and clock skew
* JRaft-backed runs as part of the normal matrix
* Datalevin-specific checkers for fencing and no-split-brain behavior
* remote multi-host deployment instead of the current single-host harness
