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
  fencing, rejoin-bootstrap, degraded-rejoin, snapshot-db-identity-rejoin,
  snapshot-checksum-rejoin, snapshot-manifest-corruption-rejoin,
  snapshot-copy-corruption-rejoin, witness-topology, membership-drift,
  membership-drift-live,
  fencing-retry, udf-readiness,
  identity-upsert, and index-consistency workloads
* Datalevin-specific `grant` and `internal` characterization workloads
* local leader failover, arbitrary-node kill, leader pause, arbitrary-node
  pause, multi-node pause, leader partition, asymmetric multi-way graph cuts,
  heterogeneous per-link degraded links, leader IO-stall, leader disk-full,
  follower rejoin, quorum-loss, and richer clock-skew nemeses covering
  follower-fast, leader-fast, leader-slow, and mixed-sign skew plans
* list-append transactions support reads, appends, and mixed read/write
  sequences

That keeps the first version faithful to Datalevin's existing public API. It
does not claim full parity with the Datomic Jepsen suite.

## Supported Workloads

`--workload` currently accepts:

* `append`, `append-cas`, `bank`, `degraded-rejoin`, `fencing`,
  `fencing-retry`, `giant-values`, `grant`, `identity-upsert`,
  `index-consistency`, `internal`, `membership-drift`,
  `membership-drift-live`, `rejoin-bootstrap`, `register`,
  `snapshot-checksum-rejoin`, `snapshot-copy-corruption-rejoin`,
  `snapshot-db-identity-rejoin`, `snapshot-manifest-corruption-rejoin`,
  `tx-fn-register`, `udf-readiness`, and `witness-topology`

## Supported Nemeses

`--nemesis` accepts a comma-separated list of aliases or raw fault keywords.

Aliases:

* `none`
* `failover` -> `leader-failover`
* `kill` -> `node-kill`
* `pause` -> `leader-pause`
* `pause-any` -> `node-pause`
* `pause-multi` -> `multi-node-pause`
* `partition` -> `leader-partition`
* `asymmetric` -> `asymmetric-partition`
* `degraded` -> `degraded-network`
* `io-stall` -> `leader-io-stall`
* `disk-full` -> `leader-disk-full`
* `rejoin` -> `follower-rejoin`
* `quorum` -> `quorum-loss`
* `clock-skew` -> `clock-skew-pause`
* `clock-leader-fast` -> `clock-skew-leader-fast`
* `clock-leader-slow` -> `clock-skew-leader-slow`
* `clock-mixed` -> `clock-skew-mixed`

Raw fault keywords:

* `leader-failover`, `node-kill`, `leader-pause`, `node-pause`,
  `multi-node-pause`, `leader-partition`, `asymmetric-partition`,
  `degraded-network`, `leader-io-stall`, `leader-disk-full`,
  `follower-rejoin`, `quorum-loss`, `clock-skew-pause`,
  `clock-skew-leader-fast`, `clock-skew-leader-slow`, and
  `clock-skew-mixed`

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
* `src/datalevin/jepsen/workload/fencing_retry.clj`: witness-topology HA
  failover workload that forces fencing-hook retries and failures on the sole
  surviving candidate, verifies promotion stays blocked with a stable
  `DTLV_FENCE_OP_ID`, and then confirms recovery after the hook is restored
  with the full fencing environment contract (`DTLV_DB_NAME`,
  old-leader identity/endpoint, terms, and `DTLV_FENCE_OP_ID`)
* `src/datalevin/jepsen/workload/udf_readiness.clj`: HA write-admission
  workload that installs a tx-UDF descriptor, enables
  `:ha-require-udf-ready?`, verifies leaders reject writes while the runtime
  registry is missing the tx function, confirms recovery once the registry is
  populated, and is smoke-tested under leader failover, leader partition, and
  degraded-network Jepsen runs
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
* `src/datalevin/jepsen/workload/degraded_rejoin.clj`: forced WAL-gap rejoin
  workload that blocks snapshot copy, verifies degraded follower state, and
  then confirms bootstrap recovery once a valid snapshot source is available
* `src/datalevin/jepsen/workload/degraded_rejoin.clj`: also exposes the
  `snapshot-db-identity-rejoin` variant, which corrupts snapshot metadata,
  verifies the follower rejects the copy, and then confirms recovery from a
  subsequent valid bootstrap
* `src/datalevin/jepsen/workload/degraded_rejoin.clj`: also exposes the
  `snapshot-checksum-rejoin` variant, which injects a checksum-style snapshot
  copy failure, verifies the follower stays degraded, and then confirms
  recovery from a subsequent valid bootstrap
* `src/datalevin/jepsen/workload/degraded_rejoin.clj`: also exposes the
  `snapshot-manifest-corruption-rejoin` variant, which strips the copied
  snapshot's applied-LSN metadata, verifies the follower rejects the malformed
  manifest before install, and then confirms recovery from a subsequent valid
  bootstrap
* `src/datalevin/jepsen/workload/degraded_rejoin.clj`: also exposes the
  `snapshot-copy-corruption-rejoin` variant, which corrupts the copied
  `data.mdb`, verifies the follower reports snapshot install failure, and then
  confirms recovery from a subsequent valid bootstrap
* `src/datalevin/jepsen/workload/witness_topology.clj`: two-data-node plus
  one non-promotable witness topology workload that verifies write availability
  across data-leader loss while control quorum remains intact
* `src/datalevin/jepsen/workload/membership_drift.clj`: follower restart
  workload that injects drifted HA membership, expects startup rejection via
  membership-hash mismatch, and then verifies clean recovery after
  configuration reconciliation
* `src/datalevin/jepsen/workload/membership_drift.clj`: also exposes the
  `membership-drift-live` variant, which injects drift directly into the live
  leader, expects the repaired node to rejoin through follower recovery, and
  then verifies quorum-visible writes after the cluster settles
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

Start a local 3-node Jepsen-backed cluster and keep it running for ad hoc HA
testing:

```bash
script/jepsen/start-local-cluster --workload append
```

Use `--keep-work-dir` to preserve the node directories after shutdown, and
`--print-edn` if you want machine-readable endpoint details.

Bring up a real multi-host Jepsen cluster one node at a time with a shared EDN
config:

```bash
script/jepsen/start-remote-node --config jepsen/remote-cluster.example.edn --node n1
```

Run the same command on each host with its logical node name. Use
`script/jepsen/stop-remote-node --config ... --node ...` to stop a launcher.
The config file is shared across every host and should define the workload,
group identity, data nodes, and any control-only witness nodes.

Standard 3 data-node config:

```edn
{:db-name "jepsen-remote"
 :workload :append
 :group-id "jepsen-remote-group"
 :db-identity "jepsen-remote-db"
 :control-backend :sofa-jraft
 :ssh {:username "ubuntu"
       :password nil}
 :repo-root "/srv/datalevin"
 :nodes
 [{:logical-node "n1" :node-id 1 :endpoint "10.0.0.11:8898" :peer-id "10.0.0.11:15001" :root "/var/tmp/dtlv-jepsen/n1"}
  {:logical-node "n2" :node-id 2 :endpoint "10.0.0.12:8898" :peer-id "10.0.0.12:15001" :root "/var/tmp/dtlv-jepsen/n2"}
  {:logical-node "n3" :node-id 3 :endpoint "10.0.0.13:8898" :peer-id "10.0.0.13:15001" :root "/var/tmp/dtlv-jepsen/n3"}]}
```

Witness-topology or `fencing-retry` config:

```edn
{:db-name "jepsen-witness"
 :workload :witness-topology
 :group-id "jepsen-witness-group"
 :db-identity "jepsen-witness-db"
 :control-backend :sofa-jraft
 :ssh {:username "ubuntu"
       :password nil}
 :repo-root "/srv/datalevin"
 :nodes
 [{:logical-node "n1" :node-id 1 :endpoint "10.0.0.11:8898" :peer-id "10.0.0.11:15001" :root "/var/tmp/dtlv-jepsen/n1"}
  {:logical-node "n2" :node-id 2 :endpoint "10.0.0.12:8898" :peer-id "10.0.0.12:15001" :root "/var/tmp/dtlv-jepsen/n2"}]
 :control-nodes
 [{:logical-node "n1" :node-id 1 :endpoint "10.0.0.11:8898" :peer-id "10.0.0.11:15001" :root "/var/tmp/dtlv-jepsen/n1"}
  {:logical-node "n2" :node-id 2 :endpoint "10.0.0.12:8898" :peer-id "10.0.0.12:15001" :root "/var/tmp/dtlv-jepsen/n2"}
  {:logical-node "n3" :node-id 3 :endpoint "10.0.0.13:8898" :peer-id "10.0.0.13:15001" :root "/var/tmp/dtlv-jepsen/n3" :promotable? false}]}
```

In the witness case, start `n3` with the same `start-remote-node` command; the
launcher will detect that it is control-only and run the control authority
without opening a Datalevin data store.

The top-level `:repo-root` is required by the controller-managed remote runner.
It should point to the Datalevin checkout on every remote host. The manual
per-host `start-remote-node` flow above ignores `:repo-root`.

The top-level `:ssh` map is optional and applies only to the controller-managed
remote runner. Use it to set a non-root SSH username or clear the default
password for key-based auth. CLI/test-level `:ssh` opts still override config
values when both are present.

Run a controller-managed remote Jepsen test from the machine that has SSH
access to every configured node:

```bash
script/jepsen/remote-workloads --config jepsen/remote-cluster.example.edn --nemesis failover
script/jepsen/remote-workloads --config jepsen/remote-cluster.example.edn --nemesis degraded append bank -- --time-limit 15 --rate 10
script/jepsen/remote-workloads --config jepsen/remote-cluster.example.edn --nemesis failover witness-topology
```

The wrapper writes per-workload temporary configs under
`tmp/jepsen-remote-workloads/`, rewrites `:workload`, `:db-name`, `:group-id`,
and `:db-identity` for each run so persistent remote roots do not reuse prior
state, and then invokes the underlying Jepsen remote runner. `witness-topology`
must be requested explicitly with a witness-style config; it is not part of
`--all-workloads`.

For one-off direct control, you can still invoke the remote runner yourself:

```bash
cd jepsen
lein run test --remote-config remote-cluster.example.edn --nemesis leader-failover --time-limit 30 --rate 10
```

In controller-managed remote mode, the workload, db name, HA group identity,
node topology, workload opts, and control backend come from the shared EDN
config instead of the usual local-cluster CLI flags. The controller uploads
that config to every node, restarts the configured launchers over SSH, waits
for the cluster to form, runs Jepsen, and then tears the launchers down again.
The current remote runner supports both standard data-node topologies and
control-only witness topologies such as `witness-topology`. It also supports
the full current controller-managed workload set, including `degraded-rejoin`,
the snapshot-rejoin variants, `membership-drift`, `membership-drift-live`,
`rejoin-bootstrap`, and `fencing-retry`, plus the HA fault injectors exposed
through the controller-managed runner such as degraded links, leader IO stall,
leader disk full, quorum loss, and the clock-skew variants.

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

Run the degraded rejoin bootstrap workload:

```bash
cd jepsen
lein run test --workload degraded-rejoin --control-backend sofa-jraft --time-limit 30 --rate 5 --key-count 4
```

Run the snapshot DB-identity rejection workload:

```bash
cd jepsen
lein run test --workload snapshot-db-identity-rejoin --control-backend sofa-jraft --time-limit 30 --rate 5 --key-count 4
```

Run the snapshot checksum rejection workload:

```bash
cd jepsen
lein run test --workload snapshot-checksum-rejoin --control-backend sofa-jraft --time-limit 30 --rate 5 --key-count 4
```

Run the malformed snapshot manifest rejection workload:

```bash
cd jepsen
lein run test --workload snapshot-manifest-corruption-rejoin --control-backend sofa-jraft --time-limit 30 --rate 5 --key-count 4
```

Run the corrupted snapshot copy rejection workload:

```bash
cd jepsen
lein run test --workload snapshot-copy-corruption-rejoin --control-backend sofa-jraft --time-limit 30 --rate 5 --key-count 4
```

Run the witness-topology workload:

```bash
cd jepsen
lein run test --workload witness-topology --control-backend sofa-jraft --time-limit 20 --rate 1 --key-count 4
```

Run the membership-drift rejoin workload:

```bash
cd jepsen
lein run test --workload membership-drift --control-backend sofa-jraft --time-limit 20 --rate 1 --key-count 4
```

Run the live membership-drift recovery workload:

```bash
cd jepsen
lein run test --workload membership-drift-live --control-backend sofa-jraft --time-limit 20 --rate 1 --key-count 4
```

Run the fencing retry/idempotence workload:

```bash
cd jepsen
lein run test --workload fencing-retry --control-backend sofa-jraft --time-limit 20 --rate 1 --key-count 4
```

Run the UDF-readiness HA admission workload:

```bash
cd jepsen
lein run test --workload udf-readiness --control-backend sofa-jraft --time-limit 20 --rate 1 --key-count 4
```

Run the UDF-readiness HA admission workload during leader failover:

```bash
cd jepsen
lein run test --workload udf-readiness --control-backend sofa-jraft --nemesis failover --time-limit 20 --rate 1 --key-count 4
```

Run the UDF-readiness HA admission workload during leader partition:

```bash
cd jepsen
lein run test --workload udf-readiness --control-backend sofa-jraft --nemesis partition --time-limit 20 --rate 1 --key-count 4
```

Run the UDF-readiness HA admission workload during degraded network:

```bash
cd jepsen
lein run test --workload udf-readiness --control-backend sofa-jraft --nemesis degraded --time-limit 20 --rate 1 --key-count 4
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

Exercise the leader-failover nemesis across the local workload set:

```bash
script/jepsen/failover-workloads
```

Run a targeted failover subset with extra Jepsen CLI overrides:

```bash
script/jepsen/failover-workloads append bank -- --time-limit 15 --rate 10
```

Exercise the follower-rejoin nemesis across the local workload set:

```bash
script/jepsen/rejoin-workloads
```

Run a targeted rejoin subset with extra Jepsen CLI overrides:

```bash
script/jepsen/rejoin-workloads append bank -- --time-limit 15 --rate 10
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

Exercise the quorum-loss nemesis across the local workload set:

```bash
script/jepsen/quorum-workloads
```

Run a targeted quorum-loss subset with extra Jepsen CLI overrides:

```bash
script/jepsen/quorum-workloads append bank -- --time-limit 15 --rate 10
```

Run an arbitrary single-node kill cycle:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis kill --time-limit 30 --rate 10
```

Exercise the arbitrary-node kill nemesis across the local workload set:

```bash
script/jepsen/kill-workloads
```

Run a clock-skew pause combined with leader failover:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis clock-skew,failover --time-limit 30 --rate 10
```

Exercise a likely compound fault against the sentinel workload set:

```bash
script/jepsen/combo-workloads clock-skew,failover
script/jepsen/combo-workloads degraded,rejoin
```

Expand a compound fault run to the full local workload set:

```bash
script/jepsen/combo-workloads clock-skew,failover --all-workloads
```

Run an explicit leader-fast or mixed clock skew:

```bash
cd jepsen
lein run test --workload append --control-backend sofa-jraft --nemesis clock-leader-fast --time-limit 30 --rate 10
lein run test --workload append --control-backend sofa-jraft --nemesis clock-leader-slow --time-limit 30 --rate 10
lein run test --workload append --control-backend sofa-jraft --nemesis clock-mixed --time-limit 30 --rate 10
```

Exercise any clock-skew variant across the local workload set:

```bash
script/jepsen/clock-workloads clock-skew
script/jepsen/clock-workloads clock-leader-fast
script/jepsen/clock-workloads clock-leader-slow
script/jepsen/clock-workloads clock-mixed
```

The HA disruption nemeses currently require `--control-backend sofa-jraft`,
because node restart/rejoin and quorum recovery depend on persisted authority
membership. The default `in-memory` backend remains useful for no-fault local
iteration.

## Next steps

The next meaningful increments are:

* leader-side membership-hash mismatch demotion coverage beyond restart-time rejection
* full workload characterization under quorum-loss and clock-skew faults
* remote multi-host deployment instead of the current single-host harness
