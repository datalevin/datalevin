# Consensus-Lease High Availability (HA) Cluster

Datalevin servers can run as a HA cluster, which combines two pieces:

* the normal Datalevin data path for user data, WAL replay, and snapshot copy
* a separate consensus-backed control plane that decides which node is allowed
  to accept writes for one database

The result is a single-writer HA design. Each HA database has exactly one
authoritative write leader at a time. Followers replicate from that leader and
can serve reads, but they do not accept writes unless they are safely promoted.

See also:

* [WAL and durability](wal.md)
* [Server/client mode](server.md)
* [Jepsen test suite](../jepsen/README.md)

## What This Mode Is For

Consensus-lease HA is meant for:

* automatic failover with one write leader per database
* read-capable followers
* static, operator-managed membership
* safety-first behavior when the system is uncertain

Currently, it is not trying to be:

* multi-leader replication
* automatic failback to the old leader
* dynamic peer discovery or automatic membership management
* consensus replication of every user transaction

The data path still uses Datalevin's WAL and snapshot machinery. Consensus is
used to decide who may lead, not to replicate every user write.

## Mental Model

The easiest way to understand the design is to separate three layers.

### 1. Data plane

The data plane is the Datalevin database itself:

* user transactions append to the leader's local WAL
* followers copy and replay WAL records in order
* if a follower falls behind beyond retained WAL, it bootstraps from a
  snapshot and then resumes WAL replay
* replica progress is tracked with replica floors so WAL retention stays safe

This part is described in more detail in [wal.md](wal.md).

### 2. Control plane

The control plane stores HA authority:

* the current lease owner
* the current leader term
* the leader endpoint
* the leader's last applied LSN
* the authoritative membership hash

The current control-plane backend is `:sofa-jraft`, based on [this
library](https://github.com/sofastack/sofa-jraft).

### 3. Local runtime state machine

Each database on each node keeps a local role:

* `:follower`
* `:candidate`
* `:leader`
* `:demoting`

Those roles are local runtime states. The node still needs the control plane to
prove that its local view is current before writes are allowed.

## Configuration

All HA options are per database.

```clojure
{:db-identity "orders-prod"
 :ha-mode :consensus-lease
 :ha-node-id 2
 :ha-members
 [{:node-id 1 :endpoint "10.0.0.11:8898"}
  {:node-id 2 :endpoint "10.0.0.12:8898"}
  {:node-id 3 :endpoint "10.0.0.13:8898"}]

 :ha-lease-renew-ms 5000
 :ha-lease-timeout-ms 15000
 :ha-promotion-base-delay-ms 300
 :ha-promotion-rank-delay-ms 700
 :ha-max-promotion-lag-lsn 0
 :ha-demotion-drain-ms 1000
 :ha-clock-skew-budget-ms 100

 :ha-client-credentials
 {:username "ha-replica"
  :password "secret"}

 :ha-fencing-hook
 {:cmd ["/usr/local/bin/dtlv-fence"]
  :timeout-ms 3000
  :retries 2
  :retry-delay-ms 1000}

 ;; Control-plane authority (exact V2 shape)
 :ha-control-plane
 {:backend :sofa-jraft
  :group-id "ha-prod"
  :local-peer-id "10.0.0.12:7801"
  :voters [{:peer-id "10.0.0.11:7801" :ha-node-id 1 :promotable? true}
           {:peer-id "10.0.0.12:7801" :ha-node-id 2 :promotable? true}
           {:peer-id "10.0.0.13:7801" :ha-node-id 3 :promotable? true}
           {:peer-id "10.0.0.21:7801" :promotable? false}]
  :rpc-timeout-ms 2000
  :election-timeout-ms 3000
  :operation-timeout-ms 5000}}
```

The options that matter most for understanding the design are:

* `:db-identity`
  A stable database identity. Snapshot/bootstrap checks depend on it.
* `:ha-members`
  The static data-node membership list used for replica and promotion ordering.
* `:ha-control-plane`
  The authoritative voter configuration. Promotable voters must map exactly to
  the `:ha-members` node IDs.
* `:ha-fencing-hook`
  Required for safe promotion on promotable nodes.
* `:ha-client-credentials`
  Needed when internal HA traffic cannot use the built-in default account.

Important validation rules:

* `:ha-node-id` must be a positive member of `:ha-members`.
* `:ha-members` must be unique and deterministic by ascending `node-id`.
* `:ha-lease-timeout-ms` must be at least `2 * :ha-lease-renew-ms`.
* `2 * :ha-clock-skew-budget-ms` must fit inside the stale-leader window
  `(:ha-lease-timeout-ms - :ha-lease-renew-ms)`.
* promotable control-plane voters must map exactly to the `:ha-members`
  `node-id`s
* the fencing hook shape must be valid
* if `:ha-client-credentials` is supplied, it must be a non-blank username and
  password pair

Witness topologies are supported by making a control-plane voter
`{:promotable? false}`. A witness contributes to quorum but never becomes a
Datalevin write leader.

## Main Design Idea

Datalevin does not ask the control plane to approve every user write. That
would make the write path much more expensive. Instead, the control plane grants
a time-bounded lease to one node. That node renews the lease periodically and
caches the result locally. The write path is then guarded by the cached lease
state:

* local role must be `:leader`
* cached lease owner must still be the local node
* cached leader term must still match the local leader term
* cached lease window must still be valid
* the node must not be in `:demoting`

If any of those checks fail, writes are rejected.

That gives Datalevin a fast local write path in the healthy case, while still
failing closed when leadership becomes uncertain.

## Normal Operation

### Follower behavior

A follower continuously tries to converge on the leader:

1. Read the current lease and discover the leader endpoint.
2. Pull WAL records starting at the follower's next LSN.
3. Apply those records in order.
4. Publish replica-floor progress so retention stays safe.
5. If WAL is missing, switch to snapshot bootstrap and then resume WAL replay.

If there is no valid WAL or snapshot source, the follower enters an explicit
degraded state instead of guessing.

### Leader behavior

A leader does two jobs at once:

* serve normal reads and writes
* renew its lease and publish fresh `leader-last-applied-lsn`

If lease renew fails, the owner/term changes, membership becomes inconsistent,
or the renew loop stalls too long, the node stops admitting writes and demotes.

### Read behavior

Reads are simpler than writes:

* reads can go to the leader
* reads can also go directly to a follower or replica
* the normal client APIs are sufficient for follower reads

There is no separate "read-only replica client" API. If you want read-only
enforcement for a user, use RBAC and grant only
`:datalevin.server/view` permission.

## Promotion and Failover

When followers decide the lease has expired, they do not all promote
immediately. Promotion is deliberately conservative.

### Candidate flow

A candidate promotion attempt follows this chain:

1. Wait for a deterministic delay based on `:ha-node-id` rank.
2. Confirm local membership matches the authoritative membership hash.
3. Check promotion lag against `:ha-max-promotion-lag-lsn`.
4. Re-check lag right before CAS using the freshest reachable information.
5. Attempt a compare-and-set lease acquisition in the control plane.
6. If CAS wins, run the fencing hook.
7. Only after fencing succeeds does write admission open.

This ordering matters. Datalevin does not treat lease acquisition alone as
enough to start writing.

### Why the deterministic delay exists

The delay does not decide who is leader. The control-plane CAS still decides
that. The delay exists to reduce avoidable contention and make races easier to
reason about and debug. Nodes use the same deterministic member ordering, so a
healthy cluster converges quickly and predictably.

### Why fencing exists

The lease tells the new leader that it has won. Fencing helps make sure the old
leader is no longer able to cause trouble in the real world.

Datalevin requires a fencing hook on consensus-lease HA nodes. The hook runs on
the CAS winner before the node begins accepting writes. If the hook fails,
promotion is not considered safe enough for write admission.

The hook receives environment variables describing the old leader, new leader,
observed term, and stable operation identifiers:

* `DTLV_DB_NAME`
* `DTLV_OLD_LEADER_NODE_ID`
* `DTLV_OLD_LEADER_ENDPOINT`
* `DTLV_NEW_LEADER_NODE_ID`
* `DTLV_TERM_CANDIDATE`
* `DTLV_TERM_OBSERVED`
* `DTLV_FENCE_OP_ID`
* `DTLV_FENCE_SHARED_OP_ID`

The important operational rule is idempotence. Retries must be safe.

## WAL Gaps, Rejoin, and Degraded Followers

Follower recovery is a major part of the design, because failover is not the
only interesting HA event.

### Rejoin

When a stopped node comes back:

* it rejoins as a follower
* it does not auto-fail back to leadership
* it catches up through WAL replay if possible
* if retained WAL is gone, it bootstraps from a snapshot and then resumes WAL
  replay

That behavior applies even when the restarting node used to be the old leader.

### Snapshot bootstrap

Snapshot bootstrap is not "just copy files and hope". The follower verifies:

* database identity matches
* manifest data is well-formed
* copied payload passes integrity checks
* replay resumes at the expected next LSN

If the snapshot is malformed, copied from the wrong database, or the replay
continuity check fails, the follower rejects the bootstrap source.

### Degraded mode

If a follower cannot find a valid source for WAL or snapshot recovery, it moves
into explicit degraded mode. That keeps the healthy cluster available while
making the broken follower visibly non-promotable until recovery is possible.

## Safety Rules

The design is easiest to trust if you keep the core fail-closed rules in mind.

### One database, one lease owner

Leadership is scoped per database, not per server process. Different databases
may have different leaders.

### No write admission without fresh local proof

Datalevin does not extend a lease locally after renew failure. If the renew
loop cannot maintain confidence, writes stop.

### Membership drift is treated as unsafe

Each node derives a membership hash from `:ha-members` plus the promotable
control-plane voter mapping. That derived hash must match the authoritative
hash. If it does not, promotion and write admission fail closed.

### Promotion is blocked for lagging nodes

With the default `:ha-max-promotion-lag-lsn 0`, a node must be fully caught up
to auto-promote.

### Clock skew is part of the safety model

Consensus-lease HA assumes bounded clock error. Datalevin therefore includes a
clock-skew budget and pause path. If skew grows beyond the configured safety
window, auto-failover pauses rather than guessing.

### No auto-failback

Once a new leader is established, the old leader does not automatically take
leadership back when it returns. It rejoins as a follower.

## What Clients Should Expect

Client behavior is intentionally simple:

* normal Datalevin remote APIs still apply
* ordinary one-shot writes may be retried or redirected as leadership changes
* stale-leader write failures are explicit and retryable
* direct follower reads are allowed by connecting to that node

The retry behavior is bounded rather than open-ended:

* the client probes the currently known endpoints
* if none accepts the write yet, it can retry the same endpoint set in later
  rounds
* the rounds stop when a leader accepts the write or the client's
  `:ha-write-retry-timeout-ms` budget is exhausted

This is important because leader failover is not instant. With the default
control-plane settings, election and promotion convergence is normally measured
in hundreds of milliseconds to seconds, not microseconds.

The normal HA cluster client story is therefore:

* connect to the leader if you want normal write traffic
* connect to a follower directly if you want replica reads
* use RBAC if you need a user to be read-only
* tune `:ha-write-retry-timeout-ms` and `:ha-write-retry-delay-ms` if your
  failover environment needs a longer or shorter write retry window
* do not expect explicit remote write transactions to hop to a new leader after
  `open-transact` / `open-transact-kv`; those stay pinned to their original
  server session

## How The Design Is Verified

Datalevin's HA verification is layered. This document describes the design, but
the repo also contains several layers of executable checks.

### Unit and integration tests

Important HA test layers in `test/datalevin/` include:

* `ha_control_test.clj`
  Control-plane lease, voter, and authority behavior.
* `ha_replication_test.clj`
  Replication bookkeeping and follower progress paths.
* `server_ha_e2e_test.clj`
  End-to-end cluster scenarios such as failover, follower rejoin, rejoin
  bootstrap, clock-skew pause, and degraded-mode recovery.
* `server_ha_fuzz_test.clj`
  Repeated randomized HA scenario coverage on the local cluster harness.
* `server_ha_test_support.clj`
  The reusable cluster harness and assertions that the E2E tests build on.

### Lightweight local drills

For quick operator-style rehearsal, Datalevin maintainers also use a separate
localhost drill harness in the outer development workspace under `script/ha/*`
(outside this checkout).

Common drill scenarios include:

* `failover`
* `follower-rejoin`
* `rejoin-bootstrap`
* `membership-hash-drift`
* `fencing-hook-verify`
* `clock-skew-pause`
* `degraded-mode-no-valid-source`
* `wal-gap`
* `fencing-failure`
* `witness-topology`
* `control-quorum-loss`

That harness is useful for fast local debugging and operator rehearsal, but it
is not a replacement for adversarial fault-injection testing.

### Jepsen test suite

The repository also contains a dedicated Jepsen subproject under
[`../jepsen/`](../jepsen/README.md). This is the highest-level HA and
fault-injection test layer in the repo.

The Jepsen suite currently covers:

* consistency workloads such as `append`, `append-cas`, `bank`, `register`,
  `giant-values`, `tx-fn-register`, `identity-upsert`, and
  `index-consistency`
* HA-specific workloads such as `fencing`, `fencing-retry`,
  `rejoin-bootstrap`, `degraded-rejoin`, `membership-drift`,
  `membership-drift-live`, `udf-readiness`, and `witness-topology`
* nemeses such as failover, kill, pause, partition, degraded network,
  follower rejoin, quorum loss, clock skew, leader IO stall, and leader disk
  full

Useful entry points include:

* `../script/jepsen/start-local-cluster`
* `../script/jepsen/failover-workloads`
* `../script/jepsen/rejoin-workloads`
* `../script/jepsen/quorum-workloads`
* `../script/jepsen/clock-workloads`
* `../script/jepsen/combo-workloads`
* `../script/jepsen/remote-workloads`

Examples:

```bash
cd jepsen
lein test
```

```bash
script/jepsen/start-local-cluster --workload append
script/jepsen/failover-workloads append bank -- --time-limit 15 --rate 10
script/jepsen/rejoin-workloads append
script/jepsen/quorum-workloads append
script/jepsen/clock-workloads clock-skew
```

If you are trying to build confidence in an HA change, the rough progression is:

1. unit and integration tests
2. localhost drill-harness scenarios in the development workspace
3. Jepsen local workloads
4. Jepsen remote workloads on real multi-host infrastructure

## Operating Guidance

If you are operating a consensus-lease HA database, the main rules are:

* keep `:ha-members` and the promotable `:ha-control-plane :voters` mapping
  identical on every node; membership drift is a safety failure, not a warning
* require an idempotent fencing hook on every promotable node; if fencing
  fails, keep that node follower-only until the hook is repaired
* treat control-plane quorum loss as a write-safety event; restore quorum
  before allowing promotion again
* expect a restarted former leader to rejoin as a follower and catch up before
  it becomes promotable
* treat `:ha-follower-degraded?` as non-promotable until WAL or snapshot
  recovery succeeds
* keep clock skew inside the configured budget; outside that budget,
  auto-failover should remain paused

Typical operator responses are correspondingly simple:

* for membership drift, freeze writes, reconcile config everywhere, and resume
  only after the derived membership hash matches again and one leader renews
  cleanly
* for fencing failure, do not bypass the hook; repair it, verify idempotence,
  and let the node rejoin as follower first
* for follower rejoin or WAL gaps, let the node catch up through normal WAL
  replay or snapshot bootstrap; do not force promotion during recovery
* for degraded followers, restore a valid WAL or snapshot source before
  treating the node as healthy
