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
* append, append-cas, bank, register, giant-values, tx-fn-register,
  fencing, rejoin-bootstrap, identity-upsert, and index-consistency workloads
* Datalevin-specific `grant` and `internal` characterization workloads
* local leader pause, arbitrary-node pause, multi-node pause, leader failover,
  leader partition, asymmetric multi-way graph cuts, heterogeneous per-link
  degraded links, leader IO-stall, leader disk-full, follower rejoin,
  quorum-loss, and clock-skew nemeses
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
* `src/datalevin/jepsen/workload/giant_values.clj`: linearizable register
  workload that stores oversized payloads and validates exact replay/readback
* `src/datalevin/jepsen/workload/fencing.clj`: HA admission/fencing workload
  that probes every node directly and fails on split-brain write admission
* `src/datalevin/jepsen/workload/register.clj`: linearizable per-key register
  workload using Jepsen's independent register checker
* `src/datalevin/jepsen/workload/tx_fn_register.clj`: linearizable register
  workload whose writes and CAS operations go through installed giant `:db/fn`s
* `src/datalevin/jepsen/workload/identity_upsert.clj`: unique-identity,
  lookup-ref, and tempid/upsert characterization workload
* `src/datalevin/jepsen/workload/index_consistency.clj`: cross-checks the
  same logical state through entity, pull, query, datoms, and index-range
* `src/datalevin/jepsen/workload/rejoin_bootstrap.clj`: follower rejoin
  convergence workload that restarts missing nodes and verifies cluster-wide
  register state after catch-up
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

Run the linearizable register workload:

```bash
cd jepsen
lein run test --workload register --time-limit 30 --rate 10 --key-count 8
```

Run the giant-value register workload:

```bash
cd jepsen
lein run test --workload giant-values --time-limit 30 --rate 10 --key-count 8
```

Run the public transaction-function register workload:

```bash
cd jepsen
lein run test --workload tx-fn-register --time-limit 30 --rate 10 --key-count 8
```

Run the follower rejoin convergence workload:

```bash
cd jepsen
lein run test --workload rejoin-bootstrap --control-backend sofa-jraft --nemesis rejoin --time-limit 30 --rate 10 --key-count 8
```

Run the identity-upsert characterization workload:

```bash
cd jepsen
lein run test --workload identity-upsert --time-limit 15 --rate 5
```

Run the index-consistency characterization workload:

```bash
cd jepsen
lein run test --workload index-consistency --time-limit 15 --rate 5
```

Run the fencing workload:

```bash
cd jepsen
lein run test --workload fencing --control-backend sofa-jraft --nemesis failover --time-limit 30 --rate 5
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

Run a pause-only lease/election stall without killing the process:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis pause --time-limit 30 --rate 10
```

Run an arbitrary single-node pause so follower stalls are exercised too:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis pause-any --time-limit 30 --rate 10
```

Run a mixed partial stall by pausing a random multi-node subset:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis pause-multi --time-limit 30 --rate 10
```

Exercise the leader-pause nemesis across the local workload set:

```bash
script/jepsen/pause-workloads
```

Exercise the arbitrary single-node pause nemesis across the local workload set:

```bash
script/jepsen/pause-any-workloads
```

Exercise the multi-node pause nemesis across the local workload set:

```bash
script/jepsen/pause-multi-workloads
```

Run a targeted pause subset with extra Jepsen CLI overrides:

```bash
script/jepsen/pause-workloads append bank -- --time-limit 15 --rate 10
```

Run a leader-isolating network partition:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis partition --time-limit 30 --rate 10
```

Exercise the leader-partition nemesis across the local workload set:

```bash
script/jepsen/partition-workloads
```

Run a targeted subset with extra Jepsen CLI overrides:

```bash
script/jepsen/partition-workloads append bank -- --time-limit 15 --rate 10
```

Run an asymmetric multi-way network cut:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis asymmetric --time-limit 30 --rate 10
```

Exercise the asymmetric-partition nemesis across the local workload set:

```bash
script/jepsen/asymmetric-workloads
```

Run a targeted asymmetric subset with extra Jepsen CLI overrides:

```bash
script/jepsen/asymmetric-workloads append bank -- --time-limit 15 --rate 10
```

Run a degraded network profile with heterogeneous per-link delay, jitter, and packet loss:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis degraded --time-limit 30 --rate 10
```

Exercise the degraded-network nemesis across the local workload set:

```bash
script/jepsen/degraded-workloads
```

Run a targeted degraded subset with extra Jepsen CLI overrides:

```bash
script/jepsen/degraded-workloads append bank -- --time-limit 15 --rate 10
```

Run a leader IO-stall without killing the process or breaking the network:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis io-stall --time-limit 30 --rate 10
```

Exercise the leader-io-stall nemesis across the local workload set:

```bash
script/jepsen/io-stall-workloads
```

Run a targeted IO-stall subset with extra Jepsen CLI overrides:

```bash
script/jepsen/io-stall-workloads append bank -- --time-limit 15 --rate 10
```

Run a leader disk-full fault without killing the process or breaking the network:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis disk-full --time-limit 30 --rate 10
```

Exercise the leader-disk-full nemesis across the local workload set:

```bash
script/jepsen/disk-full-workloads
```

Run a targeted disk-full subset with extra Jepsen CLI overrides:

```bash
script/jepsen/disk-full-workloads append bank -- --time-limit 15 --rate 10
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

* stronger forced WAL-gap/snapshot-bootstrap scenarios inside the rejoin path
* full workload characterization under quorum-loss and clock-skew faults
* JRaft-backed runs as part of the normal matrix
* Datalevin-specific checkers for fencing and no-split-brain behavior
* remote multi-host deployment instead of the current single-host harness
