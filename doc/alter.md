# Alternatives

Datalevin stands on the shoulder of giants. Datascript is developed by [Nikita
Prokopov](https://tonsky.me/) that "is built totally from scratch and is not
related by any means to" Datomic®. Datalevin started out as a port of Datascript
to LMDB storage, but has since deviated from that origin signficantly.

## Differences from Datascript

* Not just a JVM Clojure library, Datalevin also works as a command line tool, a
  networked server and a babashka pod, as native image on all major
  server/desktop platforms.

* Datalevin can be used a fast key-value store.

* Datalevin can be used a vector database.

* Datalevin can be used as a fulltext search engine.

* Datalevin Datalog engine has a cost-based query optimizer, so queries are
  truly declarative and clause ordering does not affect query performance.

* Datalevin has a modern rule engine that leverage query optimizer. It handles
  recursive rules efficiently.

* Datalevin is not an immutable database, and there is no
  "database as a value" feature. Since history is not kept, individual datom
  transaction IDs are not stored. The current transaction counter is persisted
  as database metadata, including for successful transactions with no changed
  datoms.

* Datoms in a transaction are committed together as a batch, rather than being
  saved by `with-datom` one at a time.

* ACID transaction and rollback are supported.

* Support asynchronous transaction.

* Support transactoble Entity.

* Lazy results set and spill to disk are supported.

* Entity id and transaction integer ids are 64 bits long, instead of 32 bits, to
  support much larger DB.

* Respects `:db/valueType`. Currently, most [Datomic® value
  types](https://docs.datomic.com/schema/schema-reference.html#db-valuetype) are
  supported, except uri. Values of the attributes that
  are not defined in the schema or have unspecified types are treated as
  [EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) blobs, and are
  de/serialized with [nippy](https://github.com/ptaoussanis/nippy).

* In addition to composite tuples, Datalevin also supports heterogeneous and
  homogeneous tuples.

* Support more query functions, such as `like` and `not-like` that are similar
  to LIKE and NOT LIKE operators in SQL; `in` and `not-in` that are similar to
  IN and NOT IN operators in SQL, among others. `<`, `>` etc. work on other
  types such as `Date`.

* Extended Datalog query syntax to add clauses such as `:having`, `:limit`,
  `:offset`, `:order-by`.

* Allow math functions in `:find`.

* The attribute and value leading index (AVE) is enabled for all datoms, so
  there is no need to specify `:db/index`, similar to Datomic® Cloud. Does not
  have AEV index, in order to save storage and improve write speed.

* Stored transaction functions of `:db/fn` should be defined with `inter-fn`, for
  function serialization requires special care in order to support GraalVM
  native image. It is the same for functions that need to be passed over the
  wire to server or babashka.

* Attributes are stored in indices as integer ids, thus attributes in index
  access are returned in attribute creation order, not in lexicographic order
  (i.e. do not expect `:b` to come after `:a`). This is the same as Datomic®.

* Has no features that are applicable only for in-memory DBs, such as DB as an
  immutable data structure, DB pretty print, etc.

* Does not support Clojurescript.

## Other Alternatives

Datalevin is the fastest Datalog database in the Clojure world. However, if you
are interested in using the dialect of Datalog pioneered by Datomic®, and want
to explore other options, there are a few:

* If you need time travel and cloud features backed by the company that
  maintains Clojure, and there is no need to see the source code, you may try
  [Datomic®](https://www.datomic.com).

* If you need features such as bi-temporal models and SQL, you may try
  [XTDB](https://github.com/xtdb/xtdb).

* If you need a graph database with an open world assumption, you may try
  [Asami](https://github.com/threatgrid/asami).

* If you need a durable store with some storage choices, you may try
  [Datahike](https://github.com/replikativ/datahike).

* There was also [Eva](https://github.com/Workiva/eva/), a distributed store,
  but it is no longer in active development.
