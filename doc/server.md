# Datalevin Server/Client

## Usage

Using the same native command line tool, `dtlv serv` will run in the server mode
and accepts network connections on `127.0.0.1:8898` by default.

* Use option `--host` to specify the address that the server listens on. The
  default is `127.0.0.1`. Binding to a non-loopback address, such as
  `0.0.0.0`, requires setting `DATALEVIN_DEFAULT_PASSWORD`.
* Use option `-p` to specify an alternative port number that the server listens
  to. Proper firewall settings is needed to allow remote access to the port.
* `-r` option can be used to specify a root directory path on the server, where
  all data reside under. The default path is `/var/lib/datalevin` on Posix
  systems, `C:\ProgramData\Datalevin` on Windows. User should
  make sure read/write file permissions are set on the directory path for the
  user running the server.
* `-v` option enables verbose server debug logs. Datalevin server writes logs to
  stdout.

Although `dtlv` command line tool starts up fast and use less memory, it may not
be suitable for highly concurrent and demanding use cases. The reason is that
the community version of GraalVM that `dtlv` native command tool is built with
uses only SerialGC, which limits the application throughout greatly. For such
use cases, it is recommended to run the JVM version of Datalevin as the server,
e.g. run this command:

```
DATALEVIN_DEFAULT_PASSWORD=secret java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar datalevin-0.9.10-standalone.jar serv --host 0.0.0.0 -v -r /data/dtlv
```

The JVM version of Datalevin may use more memory, but it supports much higher
throughout, is more suitable as a long running process, and the usual JVM
monitoring tools can also be leveraged.

The user is recommended to run the server process as a daemon or service using
the preferred operation system tools, e.g. systemd on Linux, Launch Daemon on
MacOS, or sc.exe on Windows. Packagers are welcomed to package Datalevin server
on the preferred platforms.

There is a default builtin user `datalevin` with a default password `datalevin`.
This is a system account that can do everything on the server. The default
password can be changed by passing in a `DATALEVIN_DEFAULT_PASSWORD` environment
variable when starting the server. This environment variable is required when
binding the server to a non-loopback address. Another option is to set it on the
REPL or in code:

1. Start the server, maybe as a sudo user, to access the default data root directory
```console
# dtlv serv
```
2. Start Datalevin REPL in another terminal
```console
$ dtlv
```
3. Type the following in the REPL

```console
user> (def client (new-client "dtlv://datalevin:datalevin@localhost"))
#'user/client
user> (reset-password client "datalevin" "new-password")
nil
```

It is suggested to create different users for access to the server (see below).
Leave the `datalevin` user for server administration purpose only.

For remote access, username and password is required on the connection URI. Make
sure username and password are URL encoded strings on the URI.

When a client (for now, just the Datalevin library itself) opens a Datalevin database
using a connection URI, i.e.
`dtlv://<username>:<password>@<hostname>:<port>/<db-name>?store=datalog|kv`,
instead of a local path name, a connection to the server is attempted.

`db-name` should be unique on the server. `store` parameter is optional, default
is `datalog`, the other option is `kv` for key-value store. A database will be
created if it does not yet exist. `port` is optional, default is `8898`.

If you are using both the `datalog` store and the `kv` store on the same server, these are two different databases
and thus must have different names. The `db-name` cannot be the same for the `datalog` store and
the `kv` store running within one datalevin server instance. In fact, `db-name`
must be unique across the whole server.

The same functions for local databases work on the remote databases, i.e. any
function that takes a `dir` argument can also take a connection URI string,
e.g. `(get-conn "dtlv://datalevin:datalevin@localhost/mydb")`. The remote access
is transparent to the function callers.

## Bulk loading Datalog data

Use `init-db` with a `dtlv://` URI to load trusted datoms into a server database,
or `fill-db` to add them to an open remote database. Entity IDs must already be
assigned and the datoms must match the schema; these APIs bypass transaction
processing such as tempid resolution and uniqueness checks.

```clojure
(require '[datalevin.core :as d]
         '[datalevin.constants :as c])

(def loaded
  (binding [c/*fill-db-batch-size* 10000]
    (d/init-db (map #(d/datom % :item/value %) (range 1 100001))
               (System/getenv "DATALEVIN_URI")
               {:item/value {:db/valueType :db.type/long}})))

(def filled (d/fill-db loaded [(d/datom 100001 :item/value 100001)]))
(d/close-db filled)
```

Set `DATALEVIN_URI` to the authenticated connection URI for the target database.
The user needs permission to alter that database, and create permission if the
database does not exist yet.

`datalevin.constants/*fill-db-batch-size*` controls datoms per batch (default
2,097,152). `init-db` commits all batches in one transaction; a failed load
rolls back all of its datom writes. Database creation and schema setup precede
the load and are not rolled back. For `fill-db` outside an explicit transaction,
each batch commits separately, so earlier batches remain committed if a later
batch fails. Inside
`with-transaction`, loads participate in that transaction's commit or rollback.
Remote loading preserves the server's WAL and durability settings. Loaded data
is available to queries and subsequent transactions after commit.

## Reopening remote databases

The server keeps Datalog stores open after clients close their connections.
Reopening with `create-conn` applies explicitly supplied database options to the
cached store and persists the changes across server restarts. Unspecified
options retain their values. Changing options requires `:datalevin.server/alter`
permission on the database; unchanged options and connection-only `:client-opts`
remain available with view permission.

## Embedded server lifecycle

When using `datalevin.server` from Clojure, `create` binds the listening port
and `start` begins serving requests. Always call `stop` after successful
creation, even if `start` was never called. Calling `start` on a running instance
or calling `stop` repeatedly is harmless.

Stopping or failing to start is permanent for that server instance. A later
`start` throws an exception with `:error :server/stopped`. To restart, call
`create` with the same root and port after `stop` completes, then start the new
instance. Stored databases and sessions are retained.

The server retries recoverable network failures automatically. A fatal failure
stops the server; restarting requires a new instance.

`create` accepts these execution limits:

* `:transaction-threads`: maximum simultaneous explicit transactions. Default
  four times the CPU count, bounded between 16 and 128. Further transaction
  opens are rejected immediately when this limit is reached.
* `:background-threads`: maximum simultaneous HA and replica background tasks,
  with the same default. Allow capacity for all configured tasks.
* `:transaction-lock-timeout-ms`: maximum wait to acquire database write access,
  default `1000` milliseconds. Use `0` for immediate rejection.

The legacy `:worker-threads` option supplies defaults for transaction and
background limits when those options are omitted. `:worker-queue-size` is
accepted for compatibility but has no effect.

Exhausted transaction capacity and expired write-access waits return
`:error :server/busy` with `:retryable? true` before the operation executes.
The client automatically retries these responses, including without HA, with
exponential backoff from 25 ms up to 200 ms. Retries share the request's
`:time-out` budget and release the pooled connection between attempts.
Other connections, including transaction commit and abort, continue independently.

## Database change notifications

`listen-db!` subscribes to a remote database by the database name in its
connection URI. Writes from any client connected to that database on the server
can notify the subscriber:

```clojure
(def conn (d/create-conn "dtlv://datalevin:datalevin@localhost/app"))

(d/listen-db! conn :refresh
  (fn [{:keys [type db-name error]}]
    (case type
      :db-changed (refresh-view! db-name (d/q query @conn))
      :subscription-error (report-error! error))))

(d/unlisten-db! conn :refresh)
```

The subscription is ready when `listen-db!` returns. Its callback runs on a
dedicated thread and may query through the original connection, even with a
one-connection pool. Closing the connection stops its subscriptions. Registering
the same key replaces the previous callback.

Events contain `{:type :db-changed :db-name "app"}`. Datalog and KV transaction
writes notify after commit, including mixed explicit transactions. Aborts,
failed transactions, and simulated writes do not notify. Several commits may
coalesce into one event; use it to refresh current state rather than count
transactions. These subscriptions are ephemeral and scoped to one server, with
no durable history or cross-node delivery guarantee. A terminal transport or
authorization error produces a `:subscription-error` event and stops the
subscription; register again to resume. Database view permission is required
and is rechecked before delivery.

`listen!` continues to provide transaction reports only for its own connection.

## Remote access

Queries with a single remote data source run on the server to reduce network
traffic. Queries involving other data sources may also process data on the
client. The same database APIs work in both cases.

Clients cache query, pull, and pull-many results alongside index reads, using
the Datalog `:cache-limit` (default `512`; `0` disables retention). Ordinary and
prepared reads share entries. Clients check for remote database changes before
reusing cached data; writes through the connection invalidate cached results
immediately. Explicit transactions, pull visitors, pull transforms using `rand`
or `rand-int`, mutable inputs/results, and queries using UDFs or qualified
functions bypass result caching. Keep client and server clocks synchronized,
for example with NTP or chrony.

KV point, range, list, rank, and batch reads also have a bounded client cache.
Cache hits validate a committed LMDB revision, which detects updates even when
the number of entries stays the same. Random sampling and visitor calls always
execute. KV caching requires an updated server; older servers fall back to
ordinary reads automatically.

## Configuration

Most deployments work well with defaults. The following options control common
latency and throughput trade-offs.

### Client cache freshness vs remote round trips

`datalevin.constants/*remote-db-last-modified-check-interval-ms*` (default
`0`) sets the minimum interval between checks for remote database changes.

* `0` validates every cache hit against the server (strict freshness, more
  network round trips).
* A positive value reuses the previous freshness check inside the interval
  (fewer round trips, but remote updates may be observed later, up to the
  interval).

Example:

```clojure
(require '[datalevin.constants :as c]
         '[datalevin.core :as d])

(def conn (d/get-conn "dtlv://user:pass@db-host:8898/app"))

(binding [c/*remote-db-last-modified-check-interval-ms* 250]
  (dotimes [_ 100]
    (d/pull @conn [:name :email] 1)))
```

Bind the interval around the reads. Within the interval, repeated cached reads
need no RPC. With the default `0`, a cache hit still checks database metadata,
but skips the query or pull RPC. Updates from other connections become visible
at the next freshness check.

Cached Datalog queries and pulls still check local connection and store closure,
including prepared readers and retained DB views, regardless of the freshness interval.

Direct Datalog schema reads and lookup-reference validation, including `entid`,
honor this interval without requiring an explicit `db?` call. `schema-snapshot`
always fetches a fresh snapshot.

### Client connection pool

`datalevin.client/new-client` accepts:

* `:pool-size` (default `3`): max pooled connections per client instance.
* `:read-cache-limit` (default `512`): client KV and schema cache entries per
  database. `0` disables this cache. Datalog query and pull results use the
  database's `:cache-limit`.
* `:read-cache-check-interval-ms`: freshness interval for cached KV and schema
  reads. When omitted, these reads use
  `c/*remote-db-last-modified-check-interval-ms*` (default `0`). A positive
  interval allows repeated hits without an RPC; writes through the same client
  invalidate immediately. Other clients' writes are observed at the next check.
  Datalog store schema access follows
  `*remote-db-last-modified-check-interval-ms*` independently of this KV option.
* `:time-out` (default `60000` ms): time allowed for one request, including
  connection setup, automatic recovery and retries of retryable `:server/busy`
  responses. Recovery and retries do not restart this timeout.
* `:ha-write-retry-timeout-ms` (default `min(:time-out, 17000)` with current HA
  defaults): extra wall clock budget for retryable HA write failover after an
  endpoint rejects a write because leadership changed. The default is derived
  from HA lease timeout and promotion timing.
* `:ha-write-retry-delay-ms` (default `100` ms): sleep between HA write retry
  rounds.

Example:

```clojure
(require '[datalevin.client :as cl])

(def client
  (cl/new-client "dtlv://user:pass@db-host:8898"
                 {:pool-size 12
                  :time-out 120000
                  :ha-write-retry-timeout-ms 8000
                  :ha-write-retry-delay-ms 150}))
```

Close a client with `cl/close-client` when the application owns its lifecycle.
Remote KV and Datalog handles reserve a separate connection for explicit
transactions, including when their ordinary pool size is one.

Keep the same client and remote database, search, and vector handles across
server outages. The client reconnects, renews expired sessions, and reopens
resources automatically. Calls can fail while the server is unavailable; later
calls can reuse the original handles once service returns. Recovery requires
the original resources to still exist; dropping and recreating a resource with
the same name does not preserve an old handle.

Each request shares its timeout across recovery attempts and HA endpoints.
Synchronous DNS resolution and custom codecs can delay completion beyond the
deadline, but their elapsed time counts toward it. A request whose encoding
finishes after expiry is never sent. A successful response already decoded is
preserved.

Cancelling a request stops its retries and preserves the thread's interrupt
status. Other concurrent requests can continue recovering within their own
timeouts. Explicit `cl/close-client` or `cl/disconnect` permanently closes the
client and cancels pending network requests. Create a new client to connect again.

If a mutation's acknowledgment is lost, the mutation may already have committed.
An error with `:error :ha/write-indeterminate`, `:outcome :unknown`,
`:indeterminate? true`, and `:retryable? false` reports this uncertainty;
`:termination-reason` explains why recovery stopped. Check the outcome before
issuing the mutation again. When operation metadata is available, preserve it
for any supported receipt lookup or replay. Calls known not to have executed
return the ordinary timeout, cancellation, or closure error.

### HA write failover

In HA deployments, the client retries writes rejected because leadership changed
against other known endpoints. Retries stop when a write succeeds, the request
is cancelled, or the `:ha-write-retry-timeout-ms` allowance is exhausted.
`:ha-write-retry-delay-ms` sets the minimum delay between retry rounds; the server
may request a longer delay.

Automatic failover also applies when opening explicit remote write transactions.
Once opened, a transaction stays on that connection and cannot migrate to a new
leader.

A transport failure after a write starts can hide a committed mutation. Without
supported deduplication metadata, the client stops with
`:error :ha/write-indeterminate` rather than resending the mutation. Reads and
requests carrying complete client operation metadata for `:tx-data`,
`:tx-data+db-info`, or `:transact-kv` can recover from a lost reply. Metadata on
other commands, including `:add-vec`, does not enable deduplication. Connection
setup failures before sending a request can fall back to another endpoint.

### HA static membership changes

Consensus HA membership is static and operator managed. Datalevin does not
discover nodes automatically, but a cluster can be reconfigured without wiping
the control-plane Raft log by committing a new authoritative membership hash:

```clojure
(require '[datalevin.client :as cl])

(def client
  (cl/new-client "dtlv://admin:pass@node-a:8898"))

(cl/ha-update-membership!
 client
 "app"
 {:ha-members
  [{:node-id 1 :endpoint "dtlv://node-a:8898/app"}
   {:node-id 2 :endpoint "dtlv://node-b:8898/app"}
   {:node-id 3 :endpoint "dtlv://node-c-new:8898/app"}]
  :ha-control-plane
  {:voters [{:peer-id "node-a:9098" :promotable? true :ha-node-id 1}
            {:peer-id "node-b:9098" :promotable? true :ha-node-id 2}
            {:peer-id "node-c-new:9098" :promotable? true :ha-node-id 3}]}})
```

The same operation is exposed as `client.haUpdateMembership("app", spec)` in
Java and JavaScript, and as `client.ha_update_membership("app", spec)` in
Python. Use normal language maps/objects for `spec`; keyword-like string keys
such as `":ha-members"` are accepted by the non-Clojure bindings.

The update saves the new HA options on the target server and changes the
authoritative cluster membership. By default it also replaces the Raft voter set
and requires write leadership to be reacquired. Nodes whose local configuration
does not match the authoritative membership cannot become leaders.

Useful request keys:

* `:ha-members`: replacement static member list.
* `:ha-control-plane {:voters [...]}` or `:ha-control-plane-voters`:
  replacement control-plane voters. Other control-plane keys are not changed by
  this live API.
* `:expected-membership-hash`: optional CAS guard. If omitted, the server reads
  the current authoritative hash before applying the update.
* `:replace-voters?`: defaults to `true`; set to `false` only if the
  control-plane voter set was changed separately.
* `:clear-leases?`: defaults to `true`; this forces write leadership to be
  reacquired under the new membership.
* `:timeout-ms`: optional control-plane operation timeout.

Run the same update on each surviving or newly staged server, or otherwise open
those servers with the same new HA options, so their local persisted options
match the authoritative hash. Subsequent calls with the same desired membership
are idempotent and can be used to persist local options on nodes that were
staged before the first cluster-wide hash update. Removed nodes should be shut
down and left out of the new member and voter lists.

### HA replica reads

In HA deployments, a client can read from a replica/follower by connecting to
that node's endpoint with the normal client APIs. There is currently no
separate "read-only replica client" API or URI flag.

For example, both of the following are valid ways to read from a replica:

```clojure
(require '[datalevin.client :as cl]
         '[datalevin.core :as d])

(def client
  (cl/new-client "dtlv://user:pass@replica-host:8898"))

(def conn
  (d/create-conn "dtlv://user:pass@replica-host:8898/app"))
```

The existing client is sufficient for replica reads. In HA mode, write
operations may be retried or routed to the authoritative leader. Ordinary read
requests remain follower-eligible, and if the connected node becomes
unreachable, the client can retry the read against other known HA endpoints for
that database. A successful failover read may therefore be served by a follower
rather than the former leader.

If you need enforced read-only access, use RBAC rather than a special client
type: grant the user or role only `:datalevin.server/view` permission on the
database, and do not grant `:datalevin.server/alter`,
`:datalevin.server/create`, or `:datalevin.server/control`.

### Non-HA async read replicas

Datalevin also supports asynchronous read-only replicas without enabling HA. This
is for a simple topology with one primary writer server and one or more
read-only replica servers. It does not provide automatic promotion, fencing,
quorum, leader election, or write failover.

The primary database must have WAL enabled. `:wal-durability-profile :strict` is
recommended for the primary; the replica sync loop only tails records up to the
source durable LSN.

A replica is configured when the database is opened on the replica server:

```clojure
(require '[datalevin.core :as d]
         '[datalevin.client :as cl])

(def schema
  {:name {:db/valueType :db.type/string
          :db/cardinality :db.cardinality/one}})

(def replica-conn
  (d/create-conn
   "dtlv://replica-admin:pass@replica-host:8898/app"
   schema
   {:replica/read-only? true
    :replica/source "dtlv://replicator:pass@primary-host:8898/app"
    :replica/id "xia-replica-us-west"
    :replica/poll-ms 250
    :replica/report-ms 5000
    :wal? true
    :wal-durability-profile :strict}))
```

The first open copies the primary database if no local database exists, then
applies subsequent changes asynchronously. An existing local database is reused;
use a fresh local data directory to force a new copy.

Replica options:

* `:replica/read-only? true`: enables non-HA replica mode.
* `:replica/source`: required primary database URI.
* `:replica/id`: stable identity used for primary WAL retention floors. If
  omitted, a host/UUID value is generated.
* `:replica/poll-ms`: source polling interval, default `250`.
* `:replica/report-ms`: floor reporting interval, default `5000`.
* `:replica/batch-records`: max WAL records fetched per sync batch, default
  `256`.
* `:replica/request-timeout-ms`: source request timeout, default `60000`.
* `:replica/client-opts`: extra client options for the source connection.

The user in `:replica/source` must be able to open/copy the primary database,
read the tx log, and update the replica floor on the primary. In practice, grant
that replicator user database `:datalevin.server/alter` permission. The user
opening the local replica needs permission to create/open the database on the
replica server.

All normal read APIs work against the replica URI. All user writes are rejected
by the replica server, even if the authenticated user otherwise
has write permission:

```clojure
(d/q '[:find [?name ...] :where [?e :name ?name]] @replica-conn)

;; Throws: Replica is read-only
(d/transact! replica-conn [{:db/id -1 :name "blocked"}])
```

Replica status is available from the client:

```clojure
(def client
  (cl/new-client "dtlv://replica-admin:pass@replica-host:8898"))

(cl/replica-status client "app")
```

The same status call is exposed as `client.replicaStatus("app")` in Java and
JavaScript, and as `client.replica_status("app")` in Python.

The returned map includes `:replica/read-only?`, `:replica/source`,
`:replica/id`, `:replica-applied-lsn`, `:replica-source-durable-lsn`,
`:replica-source-committed-lsn`, `:replica-lag-lsn`,
`:replica-last-sync-ms`, `:replica-degraded-reason`, and
`:replica-last-error`.

### Server idle session timeout

CLI option `--idle-timeout` (default `172800000` ms, i.e. 48 hours) controls
when inactive sessions are disconnected to reclaim resources.

Server downtime counts toward session expiry. After a crash, recent activity
may not have been saved, so a session can expire earlier than expected. The
bundled client renews expired sessions automatically on its next request.

```console
dtlv serv -r /data/dtlv --idle-timeout 3600000
```

### Wire compression

Connections use uncompressed transport by default. Enable wire compression in
the connection's `:client-opts`. It applies to both requests and responses and
is retained when pooled connections are replaced,
authentication is renewed, or transaction and HA clients are created.

```clojure
(d/open-kv "dtlv://user:password@host/database")

(d/create-conn "dtlv://user:password@host/database" schema
               {:client-opts {:wire-compression :zstd
                              :wire-compression-threshold 8192
                              :wire-compression-level 3}})
```

* `:wire-compression`: `:none` (default) or `:zstd`.
* `:wire-compression-threshold`: minimum uncompressed serialized payload size
  in bytes. Messages are compressed only when doing so reduces their size.
* `:wire-compression-level`: Zstd level; default `3`.
* `:max-message-size`: maximum received payload size in bytes; default
  `67108864` (64 MiB). It bounds both the transmitted payload and its decompressed
  size. The limit is retained across reconnections.

These options can also be passed directly to `datalevin.client/new-client`.
Different clients connecting to the same server can use different settings.
They affect only the wire protocol, independently of storage compression.

The dynamic vars `datalevin.constants/*wire-compression-threshold*` (default
`8192`) and `datalevin.constants/*wire-compression-level*` (default `3`) supply
creation-time defaults when compression is enabled. A current server accepts
the client's settings for both directions. Older servers honor the on/off
negotiation but use their own threshold and level for responses. Peers without
compression support exchange uncompressed messages.

Choose the threshold from the latency crossover for your payloads and network.
Smaller output alone does not establish a latency benefit.

### Runtime UDFs

Remote transaction UDFs run where the transaction executes: on the server.
Descriptor-backed `:db/udf` therefore requires server-side runtime setup.

Embedded/local databases can be opened with:

```clojure
{:runtime-opts {:udf-registry registry}}
```

In server mode, the equivalent runtime registry or resolver must be installed in
the server process when databases are opened or reopened. Client-local runtime
registries are not consulted for remote transaction execution.

Remote Datalog queries use a server-safe function resolver. Query predicates and
functions sent over `:q` or `:explain` may use built-in query functions and the
`udf` query function, or sandboxed `inter-fn` values supplied as query inputs,
but they do not resolve arbitrary fully qualified Clojure symbols, reflective
dot forms, or raw function values supplied by the client. Custom server-side
query predicates should be registered as UDFs with kind `:query-fn` or
`:predicate` and called through `udf`.

Stored Clojure `:db/fn` transaction functions defined with `inter-fn` continue
to work as before. Non-Clojure or host-managed UDFs should use `:db/udf`, where
the database stores only a descriptor such as:

```clojure
{:udf/lang :java
 :udf/kind :tx-fn
 :udf/id   :user/bootstrap}
```

In HA deployments, optional runtime setting `:ha-require-udf-ready? true` makes
the leader reject writes until all installed `:db/udf` transaction descriptors
can be resolved by the server runtime.

At present, the standalone `dtlv serv` CLI does not provide a dedicated flag
for installing foreign-language registries, so non-Clojure server-side UDFs
need programmatic server setup.

Functions defined with `inter-fn` run in the server sandbox. They cannot use host
file I/O, namespace mutation, dynamic eval/load, thread or agent APIs, or Java
interop, except for `Thread/sleep` calls of at most 1000 ms. Public functions from
application namespaces already loaded on the server can be called by fully
qualified symbol. Those helpers run as trusted server code and must be on the
server classpath.

## Networking

Large values can temporarily grow socket and compression buffers. Idle owners
retain buffers of at most 1 MiB each; larger socket buffers return to 64 KiB once
their pending frames have been consumed. Large buffers and wire compression
contexts return to a shared cache for reuse across operations and workers.
The cache retains at most 64 MiB of buffer capacity in total and expires entries
after 30 seconds without reuse. A daemon sweeps expired entries even when no
requests arrive. Native compression contexts expire with their cached buffers.

Set JVM properties `-Ddatalevin.buffer-cache-bytes=134217728` and
`-Ddatalevin.buffer-cache-idle-ms=30000` to change the shared budget and idle
timeout; a zero byte budget disables caching. Values larger than the budget
are still supported, but their buffers are freed after use. The budget covers
idle buffer capacity, not active requests or native codec working memory.

Storage readers allocate decompression scratch on demand and return large
scratch when the outermost read ends. Large storage write buffers return after
their native put/delete completes. Writers keep their small fallback buffer and
use the previous large size only as a cache hint, so repeated large writes can
reuse allocations without making every worker retain the largest value.

Explicit transactions are pinned to the connection that opens them. Its thread
owns the native transaction through commit or abort. Another socket cannot use
that transaction, even within the same authenticated session. Clients using the
wire protocol directly must keep open, transaction operations and close/abort on
one socket. The bundled KV and Datalog clients use a dedicated one-connection
transaction client when their ordinary pool has more than one connection. A lost
connection aborts its transaction; replacing the socket cannot resume or commit it.

The server limits connections, message sizes, and network waits. Connections
beyond the configured limit are closed immediately. Configure these options
through `server/create` or the corresponding CLI flags:

| Option | Default | Meaning |
| --- | --- | --- |
| `:max-connections` / `--max-connections` | `1024` | All admitted connections, including anonymous sockets. |
| `:max-unauthenticated-connections` / `--max-unauthenticated-connections` | `64` | Connections awaiting successful authentication. |
| `:max-message-size` / `--max-message-size` | `67108864` | Maximum received payload bytes, both on the wire and after decompression, excluding the five-byte frame header. |
| `:authentication-timeout-ms` / `--authentication-timeout-ms` | `10000` | Absolute time from acceptance to authentication. Anonymous traffic does not extend it. |
| `:read-timeout-ms` / `--read-timeout-ms` | `60000` | Time to receive each complete message, including waiting for its first byte except between requests in an active explicit transaction. Partial progress does not extend it. |
| `:write-timeout-ms` / `--write-timeout-ms` | `60000` | Time to send a complete response, including bulk transfers. Partial progress does not extend it. |

All values must be positive 32-bit integers; `:max-message-size` must also leave
five bytes for the frame header. The message-size limit applies to every
connection, including bulk transfers. Raise it explicitly for workloads that
require larger individual messages. The unauthenticated connection limit is
capped at the total connection limit.

Password hashing for authentication, user creation, and password resets shares a
memory limit across servers in the same JVM. With the default hashing settings,
every 512 MiB of maximum JVM heap permits one concurrent hash, up to four. Heaps
below 512 MiB cannot admit the default hash. Custom hashing memory settings affect
these requirements. Insufficient capacity returns `:server/busy` with
`:reason :password-hashing-capacity`. The bundled client retries transient
capacity errors within the authentication deadline. Non-retryable capacity
errors require a larger heap or smaller hashing memory setting.

Read timeouts bound network waits, including each batch of a bulk upload. They
do not include ordinary request decoding or handler execution. An idle connection
can expire while its session remains live; the bundled client reconnects on the
next call. Explicit transactions are exempt while waiting for the first byte of
the next request, but once data arrives the complete message must arrive within
`:read-timeout-ms`.

Write timeouts cover response encoding and transmission, with one deadline for
an entire bulk download. On expiry, the server closes the connection and aborts
any active transaction. These network timeouts do not preempt synchronous
application or native execution.

```console
dtlv serv -r /data/dtlv --max-connections 512 --max-unauthenticated-connections 32 \
  --authentication-timeout-ms 5000 --read-timeout-ms 60000 --write-timeout-ms 60000
```

Explicit transactions stay on the connection that opens them. A lost connection
aborts its transaction; reconnecting cannot resume or commit it. The bundled
clients manage the transaction connection automatically. Custom clients must use
one connection from transaction open through commit or abort.

Pauses between transaction requests have no default timeout. Use the explicit
transaction `:timeout-ms` option or `set-explicit-transaction-timeout!` to bound
the transaction body; see [transaction timeouts](transact.md#explicit-synchronous-transaction).

## Security

Datalevin server implements full-fledged role based access control (RBAC).
Permissions are granted to roles, and roles are assigned to users. User access
is secured by password.

A permission consists of three pieces of information:

* `:permission/act` indicates the permitted actions, and it can be one of
  `:datalevin.server/view`, `:datalevin.server/alter`,
  `:datalevin.server/create`, or `:datalevin.server/control`, in increasing
  level of privilege, and the latter implies the former.
* `:permission/obj` indicates the object type of the securable, and it can be
  one of `:datalevin.server/user`, `:datalevin.server/role`,
  `:datalevin.server/database`, or `:datalevin.server/server`, with the last one
  implies all others.
* `:permission/tgt` refers to the concrete target of the securable. It could be
  a username, a role keyword, a database name or `nil`, depending on
  `permission/obj`. All these target names uniquely identify securable objects.
  When the target is `nil`, the permission applies to all objects of that type.


Each user has a corresponding built-in unique role, with a role keyword
`:datalevin.role/<username>`. For example, the default user `datalevin`  has a
built-in role `:datalevin.role/datalevin`. This role is granted the permission
`{:permission/act :datalevin.server/control, :permission/obj
:datalevin.server/server}`, which permits the role to do everything on the
server.

In the command line REPL, after connecting to a server, issue `(create-user
...)` to create a user, `(create-role ...)` to create a role, `(assign-role
...)` to assign a role to a user, `(grant-permission ...)` to grant a permission
to a role.

User password is stored as a salt and a hash. The password hashing algorithm
takes the recommended more than 0.5 seconds to run on a modern server class
machine, so it can defeat a brutal force cracking effort.
