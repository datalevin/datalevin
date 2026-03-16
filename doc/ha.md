# Consensus-Lease High Availability

This document specifies Datalevin high availability behavior built on [WAL](wal.md)
replication for the data path and a consensus-backed lease authority for the
control plane.

## Scope

This mode targets automatic failover with a single write leader per database.
It keeps Datalevin data replication/transport unchanged and moves lease
authority to a quorum control plane.

In scope:

* Per-database leader lease and term in Raft based consensus control plane
* Read-only replica catch-up over existing Datalevin client/server transport
* Automatic promotion based on deterministic membership order
* Mandatory fencing hook before promotion
* Write admission fencing-token checks

Out of scope (V2):

* Auto-join/dynamic membership
* Automatic failback/preemption
* Consensus replication for user transaction log
* Replacing WAL with consensus-replicated user writes

## Locked Decisions

* Leadership scope: per database
* Membership (data plane): static fixed list, manual changes only
* Lease authority: consensus control plane only (Raft)
* Auto failover requires quorum-capable control plane (minimum 3 voters, witness
  allowed)
* Initial control-plane backend: SOFAJRaft adapter
* Canonical control-plane config key: `:ha-control-plane`
* Control-plane direct dependency lock: `com.alipay.sofa/jraft-core "1.4.0"`
* Dependency lock source: Maven Central (Sonatype) metadata checked
  `2026-03-03` for `com.alipay.sofa/jraft-core` and `com.alipay.sofa/bolt`
* Transport: existing Datalevin client/server transport
* Lease renew interval: `ha-lease-renew-ms=5000`
* Lease timeout: `ha-lease-timeout-ms=15000` (3x renew interval)
* Canonical authoritative lease key format:
  `/<group-id>/ha/v2/db/<db-identity>/lease`
* `db-identity` is immutable UUID assigned at DB creation and persisted in DB
  metadata
* Promotion guard: `last-applied-lsn` only
* `ha-max-promotion-lag-lsn=0` (candidate must be fully caught up)
* WAL durability profile: unchanged by HA
* Fencing: required hook, success required for promotion
* Failback: no auto-failback
* Node identity: stable positive integer `node-id`
* Clock skew budget: all HA nodes and control-plane voters require NTP/chrony,
  observed max absolute skew `<100ms`
* Consensus-lease HA assumes bounded clock error; if clocks diverge enough,
  two nodes can briefly believe they are leader until the stale lease window
  closes and the skew hook pauses promotion
* Write admission is fail-closed and never extends lease locally after renew
  failure
* Per-write admission uses cached state from successful renew/read loop (no
  linearizable control-plane read on every write)
* Fencing hook is idempotent; `DTLV_TERM_CANDIDATE` is advisory pre-CAS
* Membership/config drift is fail-closed via deterministic
  `ha-membership-hash` comparison with authoritative control-plane hash

## Configuration

All HA options are per database. Membership is fixed and shared by operators.

```clojure
{:ha-mode :consensus-lease
 :ha-node-id 2
 :ha-members
 [{:node-id 1 :endpoint "10.0.0.11:8898"}
  {:node-id 2 :endpoint "10.0.0.12:8898"}
  {:node-id 3 :endpoint "10.0.0.13:8898"}]

 :ha-lease-renew-ms 5000
 :ha-lease-timeout-ms 15000

 ;; Deterministic promotion stagger:
 ;; delay-ms = base + rank-index * per-rank
 :ha-promotion-base-delay-ms 300
 :ha-promotion-rank-delay-ms 700

 ;; Must be fully caught up before promotion.
 :ha-max-promotion-lag-lsn 0

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
  :operation-timeout-ms 5000
  ;; Optional; if omitted on server, defaults to:
  ;; <server-root>/ha-control/<group-id>/<local-peer-id>/<hex(db-name)>
  :raft-dir "/var/lib/datalevin/ha-control/ha-prod/10.0.0.12_7801/6f7264657273"}}
```

Validation rules:

* `ha-node-id` must be a positive integer.
* `ha-members` must be non-empty, unique positive `node-id`, and include
  `ha-node-id`.
* Membership order is deterministic by `node-id` ascending.
* Timing options are positive:
  `ha-lease-renew-ms`, `ha-lease-timeout-ms`,
  `ha-promotion-base-delay-ms`, `ha-promotion-rank-delay-ms`.
* `ha-lease-timeout-ms >= 2 * ha-lease-renew-ms` (recommend `3x`).
* `2 * ha-clock-skew-budget-ms <= (ha-lease-timeout-ms - ha-lease-renew-ms)`.
  The skew hook reports absolute offset from a common time source, so pairwise
  skew across two nodes can approach `2x` the configured budget.
* `ha-max-promotion-lag-lsn` is non-negative.
* Optional `:ha-client-credentials` must include non-blank `:username` and
  `:password`; `:username` must not contain `:`.
* Fencing hook shape is valid (`:cmd`, timeout/retry fields).
* `:group-id`, `:local-peer-id`, and voter `:peer-id` values are unique.
* Local peer appears exactly once in voter list.
* `:peer-id` format is `host:port`; port is valid integer range.
* Control-plane voter set is quorum-capable.

If the server cluster does not use the built-in `datalevin/datalevin` account,
configure `:ha-client-credentials` on every node so follower snapshot copy,
replica-floor updates, watermark probes, and WAL fetches authenticate with the
same credentials as normal client traffic.
* Every promotable `ha-node-id` maps to exactly one promotable voter.
* Witness voters may be unmapped to `ha-members` and are always
  non-promotable.
* Local derived `ha-membership-hash` (from canonicalized members + promotable
  voter mapping) must match authoritative hash at startup in consensus mode.

Dependency contract:

* Add direct runtime dependency `com.alipay.sofa/jraft-core "1.4.0"` in both
  `deps.edn` and `project.clj`.
* Do not add direct `jraft-parent`, `bolt`, or `hessian` entries unless tests
  prove a classpath override is required.
* Keep runtime classpath free of test-only `log4j` artifacts.

## Lease Authority Model

Authoritative lease state is stored in the consensus control plane and keyed by:

* `lease-key(group-id, db-identity) => "/<group-id>/ha/v2/db/<db-identity>/lease"`
* `membership-hash-key(group-id) => "/<group-id>/ha/v2/membership-hash"`

V2 does not mirror lease authority into `kv-info`.

Authoritative lease record shape:

```clojure
{:db-identity "7a9f1f8d-cf5a-4fd6-a5a0-6db4a74a6f6f"
 :leader-node-id 2
 :leader-endpoint "10.0.0.12:8898"
 :term 41
 :lease-until-ms 1760001234567
 :lease-renew-ms 5000
 :updated-ms 1760001229567
 :leader-last-applied-lsn 987654}
```

Semantics:

* `term` strictly increases on each successful acquisition.
* `lease-renew-ms` records configured renew cadence.
* `updated-ms` is the last authoritative renew/acquire time.
* Lease is valid while `now-ms < lease-until-ms`.
* `leader-last-applied-lsn` is refreshed by leader renew loop.
* `db-identity` must match local DB identity or the node fails closed.

Lease operations:

* `read-lease` for promotion/startup checks (linearizable)
* `try-acquire-lease` as CAS on observed lease/version
* `renew-lease` guarded by owner + term
* `current-term`/`owner` helper reads
* `read-membership-hash` / `init-membership-hash!` (CAS init, compare-only
  thereafter)

CAS semantics:

* Compare against observed lease record/version
* Ensure expired or absent at authority
* Compute `new-term = max(observed-term, 0) + 1`
* Atomically write `db-identity`, owner/term/lease window
* Return winner term + lease record

## Replica Progress

Replica progress uses existing API:

* `txlog-update-replica-floor! db replica-id applied-lsn`
* `txlog-clear-replica-floor! db replica-id`

For HA, `replica-id = ha-node-id` (positive integer).

This keeps WAL retention safe for lagging replicas and reuses existing replica
floor staleness TTL handling.

## State Machine (Per DB)

Local role states:

* `:follower`
* `:candidate`
* `:leader`
* `:demoting`

### Follower loop

1. Discover leader endpoint from authoritative lease + membership.
2. Pull WAL from `next-lsn` via existing `open-tx-log` transport.
3. Apply records in order via trusted replay path.
4. Persist applied checkpoint with `txlog-update-replica-floor!`.
5. On lease expiry, transition to `:candidate`.
6. On WAL gap, run snapshot/bootstrap catch-up before steady replication.

WAL gap bootstrap source order:

* Current leader snapshot endpoint first
* Then followers advertising sufficient `last-applied-lsn` (tie-break: highest
  `last-applied-lsn`, then lowest `node-id`)
* If no valid source exists, enter explicit degraded state (operator action
  required)

Snapshot/bootstrap protocol:

* Source returns manifest:
  `db-name`, `db-identity`, `snapshot-last-applied-lsn`, `snapshot-checksum`
* Follower verifies `db-name` and `db-identity` before install
* Snapshot is installed to staging path, checksum-verified, then atomically
  swapped into active DB path
* Post-install local applied checkpoint is set to `snapshot-last-applied-lsn`
* WAL replay resumes from `snapshot-last-applied-lsn + 1`
* First replayed WAL record must equal expected next LSN; otherwise reject
  source and try next deterministic source
* Snapshot with mismatched `db-identity` is rejected before install

### Candidate promotion flow

1. Compute deterministic delay:
   `delay-ms = ha-promotion-base-delay-ms + rank-index * ha-promotion-rank-delay-ms`,
   where `rank-index` is 0-based in ascending `node-id`.
2. Verify authoritative membership hash matches local derived hash.
3. After delay, perform lag guard:
   `leader-last-applied-lsn - local-last-applied-lsn <= ha-max-promotion-lag-lsn`.
4. Execute fencing hook unless bootstrap acquisition from empty lease
   (`term=0`, no owner).
5. Re-check lag guard immediately before CAS using fresh authoritative lease
   read; if leader endpoint is reachable, fetch fresh `txlog-watermarks` and use
   `max(authoritative-lease-lsn, leader-watermark-lsn)` as guard input unless
   the lease is already expired and the old owner reports a lower watermark, in
   which case use the highest currently reachable member watermark instead.
6. If leader endpoint is unreachable, delay CAS by at least one additional renew
   interval beyond lease expiry before final guard re-check.
7. Attempt authoritative lease CAS acquisition.
8. On CAS loss, return immediately to `:follower`.

Promotion safety chain is strict:

* membership-hash check
* lag guard
* fencing hook
* lag guard re-check
* authoritative lease CAS

### Leader renew and write admission

1. Renew authoritative lease every `ha-lease-renew-ms`, including
   `leader-last-applied-lsn`.
2. Write admission requires:
   * local role is `:leader`
   * cached authoritative owner node-id equals local `ha-node-id`
   * local cached authoritative lease window valid (`now-ms < local-lease-until-ms`)
   * local leader term equals last successful authoritative renew term
   * local role is not `:demoting`
3. Write guard uses cached authority from successful renew/read loop; it does
   not perform linearizable control-plane reads on every write.
4. Demote immediately on:
   * renew failure
   * owner/term mismatch
   * renew loop stall beyond `ha-lease-timeout-ms`
   * authoritative membership hash mismatch

### Demotion

1. Transition `:leader -> :demoting` and stop write admission first.
2. Clear leader-only runtime state (term, renew loop, leader-only handles).
3. Transition `:demoting -> :follower`.
4. Re-enter follower replication loop automatically.

## Write Safety and Fencing

Two safety layers are required:

1. External fence action via hook before promotion.
2. Fencing-token write admission (`term` must match local authoritative leader
   term).

If guard fails, write is rejected with explicit not-leader/fenced error.

## Fencing Hook Contract

The promoting node executes the hook locally before lease takeover.

Input:

* Command vector from config (`:ha-fencing-hook :cmd`)
* Exact environment variable names:
  * `DTLV_DB_NAME`
  * `DTLV_OLD_LEADER_NODE_ID`
  * `DTLV_OLD_LEADER_ENDPOINT`
  * `DTLV_NEW_LEADER_NODE_ID`
  * `DTLV_TERM_CANDIDATE`
  * `DTLV_TERM_OBSERVED`
  * `DTLV_FENCE_OP_ID` (`db-name + observed-term + candidate-node-id`; stable
    across retries on one candidate)
  * `DTLV_FENCE_SHARED_OP_ID` (`db-name + observed-term`; shared by every
    candidate fencing the same observed lease)

Execution:

* Timeout: `:timeout-ms`
* Retry: `:retries` with `:retry-delay-ms`
* Multiple candidates can execute the hook concurrently before the final
  authority re-observation and lease CAS resolve a single winner.
* Retries/replays with the same `DTLV_FENCE_OP_ID` must be idempotent.
* Destructive fencing must also be idempotent across nodes for the same
  `DTLV_FENCE_SHARED_OP_ID`; `DTLV_FENCE_OP_ID` alone is not sufficient for
  cross-candidate dedupe.
* If the external fencing backend cannot provide that guarantee directly,
  wrap it in an intent/confirm or equivalent two-phase protocol keyed by
  `DTLV_FENCE_SHARED_OP_ID`.

Result:

* Exit `0`: fencing succeeded, promotion may continue
* Non-zero/timeout/unimplemented hook: promotion aborted
* CAS loss after successful fencing leaves node in follower role even though
  the fencing action already ran

## Failure Behavior

### Leader crash or partition

* Followers detect expiry and run deterministic promotion.
* First eligible candidate to pass safety chain and CAS lease wins.
* Partitioned old leader stops writes immediately after renew failure.

### Control-plane quorum loss

* New promotions are blocked.
* Safe write behavior is enforced (no new authoritative renew, no unsafe writes).

### Old leader restart

* Rejoins as follower only.
* No auto-failback, no preemption.

### Replica lag

* With `ha-max-promotion-lag-lsn=0`, lagging nodes never auto-promote.
* They continue catch-up until fully synchronized.

### Membership/config drift

* Nodes with authoritative membership-hash mismatch fail closed.
* Active leader demotes and stops writes until config is reconciled.

## Migration and Rollback

Migration from storage-lease style deployments is explicit and operator-driven.
No automatic lease-state conversion is provided.

Cutover:

1. Take pre-cutover backup/snapshot of each HA database.
2. Pause writes and verify followers are caught up to leader LSN.
3. Stop all promotable nodes except intended cutover leader.
4. Start control-plane quorum and verify voter quorum health.
5. Start intended leader with `:ha-mode :consensus-lease` and bootstrap empty
   authority lease (`term=1`).
6. Start remaining nodes as followers and verify catch-up + replica floors.
7. Re-enable writes only after renew loop health and follower progress checks.

Rollback:

1. If cutover fails before consensus leader accepts writes: restart with prior
   HA-off/storage-lease-compatible config.
2. If cutover fails after consensus writes are accepted: stop full cluster and
   restore pre-cutover backup/snapshot; do not run mixed lease authorities.

## Client Behavior

* Clients use fixed endpoint lists.
* Retry policy should follow deterministic member order.
* Stale-leader write failures are distinguishable and retryable.

## Operational Procedures

### Membership-hash drift recovery

1. Freeze writes and auto-failover on the affected database.
2. Read the authoritative membership hash from the control plane and compare it
   with each node's derived hash from `:ha-members` plus
   `:ha-control-plane :voters`.
3. Reconcile config on every promotable node and witness so the promotable
   voter mapping and member order are identical everywhere.
4. Restart/rejoin nodes only after the local derived hash matches the
   authoritative control-plane hash.
5. Re-enable writes after one leader renew succeeds and followers resume
   replica-floor updates.

### Fencing hook deployment verification

1. Ensure the configured hook command path exists and is executable on every
   promotable node.
2. Confirm the hook can tolerate replay with the same `DTLV_FENCE_OP_ID` and
   concurrent execution on different candidates sharing the same
   `DTLV_FENCE_SHARED_OP_ID`.
3. Run `../dtlvtest/script/ha/fencing-hook-verify` before staging or production cutover to
   confirm the promotion path exports `DTLV_DB_NAME`,
   `DTLV_OLD_LEADER_NODE_ID`, `DTLV_OLD_LEADER_ENDPOINT`,
   `DTLV_NEW_LEADER_NODE_ID`, `DTLV_TERM_OBSERVED`,
   `DTLV_TERM_CANDIDATE`, `DTLV_FENCE_OP_ID`, and
   `DTLV_FENCE_SHARED_OP_ID`.
4. Verify the recorded `DTLV_FENCE_OP_ID` matches
   `db-name:observed-term:candidate-node-id` and
   `DTLV_FENCE_SHARED_OP_ID` matches `db-name:observed-term` before enabling
   automatic promotion.

### Repeated fencing failures

1. Do not bypass the fencing hook; failed fencing must leave the node in
   follower role.
2. Remove the affected node from promotion eligibility or stop it while the
   hook is repaired.
3. Verify hook command path, timeout, retry budget, and idempotent handling of
   both `DTLV_FENCE_OP_ID` and `DTLV_FENCE_SHARED_OP_ID`.
4. Confirm the hook succeeds in the target environment before re-enabling
   promotion.
5. Reintroduce the node only after a normal follower catch-up cycle completes.

### Follower-only rejoin

1. Rejoin with the same `:db-identity`, `:ha-node-id`, `:ha-members`, and
   control-plane voter config used by the active cluster.
2. Start the node as a follower and allow normal WAL replay or snapshot
   bootstrap; do not force promotion during rejoin.
3. Verify the node reports follower role, observes the current leader term, and
   refreshes its replica floor on the leader.
4. Re-enable promotion eligibility only after the node is caught up and no
   degraded follower state remains.

### Degraded follower recovery

1. Treat `:ha-follower-degraded?` as non-promotable until a valid WAL or
   snapshot source is restored.
2. Inspect the recorded `:source-order`, gap error, and snapshot bootstrap
   errors to identify whether the failure is endpoint reachability, stale
   snapshot data, or identity mismatch.
3. Restore a valid source or replace the local data directory from a verified
   snapshot with matching `db-identity`.
4. Restart or rejoin the follower and confirm immediate post-snapshot WAL
   resume succeeds before considering the node healthy again.

### Control-plane quorum loss recovery

1. Treat loss of linearizable control-plane reads or renew failures as a write
   safety event, not a transient availability-only issue.
2. Verify the leader has demoted or aged out of write admission before
   redirecting traffic.
3. Restore voter quorum first; do not force promotion while the control plane
   is below quorum.
4. After quorum returns, confirm one node renews successfully, followers resume
   replica-floor updates, and client retry targets converge on the new leader
   endpoint.

### Staging and failure-drill checklist

1. Run a 3-voter failover drill with all promotable voters.
2. Run a witness-topology drill with one promotable voter stopped and confirm
   renew still succeeds with witness quorum.
3. Run a control-plane quorum-loss drill and confirm promotion is blocked and
   write admission rejects demoting/stale leaders.
4. Run a WAL-gap drill and confirm snapshot bootstrap plus immediate WAL resume.
5. Run a fencing-failure drill and confirm repeated attempts reuse the same
   `DTLV_FENCE_OP_ID`.
6. Run a fencing-hook-verify drill and confirm the deployed hook executes with
   populated `DTLV_DB_NAME`, leader identity, endpoint, term, and
   `DTLV_FENCE_OP_ID` plus `DTLV_FENCE_SHARED_OP_ID` metadata before
   promotion.
7. Run a follower-only rejoin drill and confirm the restarted former leader
   rejoins as follower, catches up, and refreshes its replica floor on the
   active leader.
8. Run a clock-skew-pause drill and confirm followers stay non-promotable
   while skew exceeds budget, then promote only after skew returns within
   budget.
9. Record observed leader terms, retry endpoints, follower replica floors, and
   recovery times before promoting the configuration beyond staging.

### Local drill scripts

For fast local rehearsal, use:

* `../dtlvtest/script/ha/failover`
* `../dtlvtest/script/ha/follower-rejoin`
* `../dtlvtest/script/ha/rejoin-bootstrap`
* `../dtlvtest/script/ha/membership-hash-drift`
* `../dtlvtest/script/ha/fencing-hook-verify`
* `../dtlvtest/script/ha/clock-skew-pause`
* `../dtlvtest/script/ha/degraded-mode-no-valid-source`
* `../dtlvtest/script/ha/wal-gap`
* `../dtlvtest/script/ha/fencing-failure`

These scripts start a disposable 3-node localhost cluster under a temporary
work directory, provision a HA Datalog store on each node, and exercise the current
runtime behavior. They are useful operator checks, but they are not a
replacement for Jepsen-style network partition and clock-fault testing.
The Datalevin-specific Jepsen scaffold for that next layer lives under
`jepsen/`.

Additional local control-plane drills are available through the shared harness:

* `clojure -M:dev ../dtlvtest/script/ha/drill.clj witness-topology --control-backend sofa-jraft`
* `clojure -M:dev ../dtlvtest/script/ha/drill.clj control-quorum-loss --control-backend sofa-jraft`

These two scenarios exercise standalone JRaft authorities rather than a full
DB cluster. They validate quorum behavior directly and keep the focus on
control-plane quorum behavior.

By default the local harness uses the in-process `:in-memory` control-plane
backend for fast deterministic rehearsal. Pass
`--control-backend sofa-jraft` when you specifically want to debug the JRaft
adapter in a localhost setup.

### How to run local drills

Requirements:

* Run from the repository root.
* Use the dev alias so the drill harness can load local namespaces:
  `clojure -M:dev ../dtlvtest/script/ha/drill.clj --help`
* Localhost ports starting at `19001` must be free unless you override
  `--port-base`.

Common options:

* `-k` or `--keep-work-dir`: keep the temporary node directories after success.
* `-w DIR` or `--work-dir DIR`: use a specific empty directory for drill data.
* `-b in-memory|sofa-jraft` or `--control-backend ...`: choose control-plane backend.
* `-p PORT` or `--port-base PORT`: move the 3-node cluster to a different port range.
* `-v` or `--verbose`: enable more server and HA logging during the run.

Failover drill:

* Command: `../dtlvtest/script/ha/failover`
* Expected success signals:
  `Seed write replicated across all three nodes`
  `Post-failover leader:`
  `Scenario: failover`
  `initial-leader-endpoint:` and `new-leader-endpoint:` must differ
* Failure behavior:
  the script exits non-zero and prints `HA drill failed:`; the work directory is
  kept automatically so node state can be inspected.

Follower-rejoin drill:

* Command: `../dtlvtest/script/ha/follower-rejoin`
* Expected success signals:
  `Stopping leader for follower-only rejoin:`
  `Failover leader:`
  `Restarted node for follower-only rejoin:`
  `Rejoined node is following leader:`
  `Scenario: follower-rejoin`
* Purpose:
  confirm a restarted former leader rejoins with follower role, catches up
  under the current leader, and refreshes its replica floor before being
  considered healthy again.
* Failure behavior:
  the script exits non-zero if the restarted node promotes, fails to rejoin as
  follower, does not converge on post-rejoin data, or never refreshes its
  replica floor on the active leader.

Rejoin-bootstrap drill:

* Command: `../dtlvtest/script/ha/rejoin-bootstrap`
* Expected success signals:
  `Stopping leader to force rejoin bootstrap:`
  `Forced WAL GC on surviving nodes for rejoin bootstrap:`
  `Restarted former leader for rejoin bootstrap:`
  `Rejoined node bootstrapped from:`
  `Scenario: rejoin-bootstrap`
* Purpose:
  confirm a restarted former leader can only rejoin via snapshot bootstrap
  after its retained WAL has been intentionally gapped away, then immediately
  resume WAL replication and refresh its replica floor.
* Failure behavior:
  the script exits non-zero if the stopped leader promotes on restart, no real
  WAL gap is created before restart, snapshot bootstrap never completes, or the
  node fails to converge under the active leader after bootstrap.

Membership-hash-drift drill:

* Command: `../dtlvtest/script/ha/membership-hash-drift`
* Expected success signals:
  `Rejected membership-drift update on leader:`
  `Restored authoritative members on node:`
  `Recovered leader after membership reconciliation:`
  `Scenario: membership-hash-drift`
* Purpose:
  confirm a drifted `:ha-members` update is rejected, promotion/write safety
  remains frozen during the mismatch, and restoring the authoritative config
  restarts HA cleanly.
* Failure behavior:
  the script exits non-zero if the drifted update succeeds, HA does not
  recover after restoring the original members, or post-reconcile replication
  does not converge.

Fencing-hook-verify drill:

* Command: `../dtlvtest/script/ha/fencing-hook-verify`
* Expected success signals:
  `Stopping leader to verify fencing hook contract:`
  `Verified fencing hook contract on promoted node:`
  `Scenario: fencing-hook-verify`
  `verified-entry:` with populated `:db-name`, `:old-node-id`,
  `:old-leader-endpoint`, `:new-node-id`, `:observed-term`,
  `:candidate-term`, `:fence-op-id`, and `:fence-shared-op-id`
* Purpose:
  confirm the real hook command path executes during promotion and receives the
  full fencing environment contract needed for deployment-time integration.
* Failure behavior:
  the script exits non-zero if no promoted-node fence entry appears, required
  metadata is blank or mismatched, or failover cannot complete after a
  successful fence.

Clock-skew-pause drill:

* Command: `../dtlvtest/script/ha/clock-skew-pause`
* Expected success signals:
  `Configured follower clock skew breach; stopping leader`
  `Clock-skew pause blocked automatic failover on followers:`
  `Clock-skew cleared; follower promoted:`
  `Cleared skew budget on remaining follower:`
  `Scenario: clock-skew-pause`
* Purpose:
  confirm followers refuse promotion while the clock-skew hook reports a
  budget breach, failover resumes only after at least one follower returns
  within budget, and the remaining paused follower rejoins cleanly once its
  skew is cleared.
* Failure behavior:
  the script exits non-zero if a skew-paused follower promotes, no follower
  promotes after skew recovery, or the cleared follower fails to rejoin
  replication under the new leader.

Degraded-mode-no-valid-source drill:

* Command: `../dtlvtest/script/ha/degraded-mode-no-valid-source`
* Expected success signals:
  `Restarted follower with no valid snapshot source:`
  `Follower entered degraded mode:`
  `Healthy nodes kept serving writes while follower stayed degraded:`
  `Valid source restored; degraded follower recovered:`
  `Scenario: degraded-mode-no-valid-source`
* Purpose:
  confirm a gapped follower enters degraded mode when no valid snapshot source
  is available, stays out of the healthy replication set while the remaining
  nodes keep serving writes, and recovers only after a valid source returns.
* Failure behavior:
  the script exits non-zero if the follower never enters degraded mode,
  leaves degraded mode before source recovery, or fails to bootstrap and resume
  replication after source recovery.

WAL-gap drill:

* Command: `../dtlvtest/script/ha/wal-gap`
* Expected success signals:
  `Created baseline snapshot at LSN:`
  `Forced WAL GC on leader:`
  `Restarted follower:`
  `Follower bootstrapped from:`
  `Scenario: wal-gap`
* Purpose:
  confirm a stopped follower can recover from a true WAL retention gap by
  installing a snapshot and immediately resuming WAL replication on restart.
* Failure behavior:
  the script exits non-zero if WAL GC never advances beyond the stopped
  follower floor, snapshot bootstrap never completes, or post-bootstrap WAL
  replication does not converge across the cluster.

Fencing-failure drill:

* Command: `../dtlvtest/script/ha/fencing-failure`
* Expected success signals:
  `Updated follower fencing hooks to fail; stopping current leader`
  `Scenario: fencing-failure`
  `fence-retries:` with repeated `DTLV_FENCE_OP_ID` values for follower attempts
  `probe-snapshot:` showing no surviving node in `:leader` status
* Failure behavior:
  the script exits non-zero if any follower promotes despite failed fencing, or
  if the expected retry evidence never appears.

Witness-topology drill:

* Command:
  `clojure -M:dev ../dtlvtest/script/ha/drill.clj witness-topology --control-backend sofa-jraft`
* Expected success signals:
  `Initial control-plane leader:`
  `Stopped promotable voter: 2`
  `Scenario: witness-topology`
  `renew-result:` with `:ok? true`
* Purpose:
  confirm one promotable voter can stop while the remaining promotable node and
  non-promotable witness still retain control-plane quorum.

Control-quorum-loss drill:

* Command:
  `clojure -M:dev ../dtlvtest/script/ha/drill.clj control-quorum-loss --control-backend sofa-jraft`
* Expected success signals:
  `Initial control-plane leader:`
  `Scenario: control-quorum-loss`
  `read-error:` with `:error` in
  `:ha/control-timeout`, `:ha/control-read-failed`, or
  `:ha/control-node-unavailable`
* Purpose:
  confirm linearizable reads fail once the control plane loses voter quorum.

Debugging a failed local drill:

* Re-run with `-k` to preserve the generated node directories.
* Use `-w /tmp/<dir>` when you want a stable location for repeated inspection.
* For fencing drills, inspect the follower hook logs in the kept work directory.
* For HA state inspection, review the printed `probe-snapshot:` and
  `watermarks:` maps before looking at lower-level LMDB or txlog files.

## Operational Requirements

* Membership updates are manual and coordinated.
* Control-plane voter management and quorum-loss procedures are required.
* Promotable data-plane nodes must map one-to-one to promotable control-plane
  voters.
* Witness voters may remain unmapped and never promote.
* Membership-hash drift procedure is required (freeze promotion/writes, reconcile
  config, re-enable consensus mode).
* Fencing hook deployment and verification checklist is required.
* Procedure for repeated fencing failures is required.
* Procedure for follower-only rejoin is required.
* Monitor clock skew budget `<100ms`; pause auto-failover when violated.
* Treat consensus-lease HA as bounded-clock safety, not arbitrary-clock safety;
  larger pairwise skew can create a brief dual-leader belief window until the
  lease window closes.
* Degraded-mode procedure is required when no valid WAL/snapshot source exists.

## Implementation Phases

1. Config/validation and HA control-plane interface
2. Consensus lease authority adapter + CAS semantics
3. Replica follower worker
4. Promotion safety chain
5. Leader renew + write admission guard
6. Client failover behavior docs
7. End-to-end HA tests and rollout controls

## Test Matrix

Required tests include:

* authoritative lease CAS single winner promotion
* transient renew jitter does not fail over when timeout > renew interval
* lag guard block/allow behavior and pre-CAS re-check
* fencing hook failure blocks promotion
* successful fencing hook execution exports DB name, leader identity,
  endpoint, terms, and per-candidate/shared fence-op-id contract before
  promotion
* write admission rejects follower/stale-term writes
* write admission rejects owner-node mismatch (`owner != local ha-node-id`)
* leader demotion on renew failure or owner/term loss
* leader demotion on renew-loop stall past `ha-lease-timeout-ms`
* partitioned leader loses write admission after renew failure
* old leader restart stays follower (no auto-failback)
* quorum loss blocks promotion and unsafe writes
* membership-hash mismatch blocks promotion/startup and demotes active leader
* fencing hook retries with same `DTLV_FENCE_OP_ID` are idempotent
* fencing hook contract exposes `DTLV_FENCE_SHARED_OP_ID` for cross-candidate
  fencing dedupe
* witness voter topology remains non-promotable and still quorum-capable
* retention remains safe while replica floors stay fresh
* WAL gap recovery triggers snapshot/bootstrap path
* snapshot install enforces checksum + LSN continuity on resume
* snapshot with mismatched `db-identity` is rejected before install
* bootstrap from empty lease acquires term `1` without old-leader fence
* reachable-leader watermark refresh is used in promotion lag guard path
* deterministic promotion rank/order is identical for equivalent `ha-members`
  regardless of control-plane peer ID format
* auto-failover pauses on clock-skew budget breach and resumes only after budget
  is back within limit

## Rollout Gates

1. Ship behind `:ha-mode :consensus-lease` opt-in only
2. Validate HA-off compatibility (no behavior change)
3. Run 3-voter failover soak tests (including witness topology)
4. Enable staging with runbook
5. Promote to production after soak and failure drills pass
