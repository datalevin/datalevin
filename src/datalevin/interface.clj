;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.interface
  "Some common protocols, e.g. shared between local and remote"
  (:refer-clojure :exclude [sync]))

(defprotocol IList
  (list-dbi? [db dbi-name])
  (put-list-items [db list-name k vs k-type v-type]
    "put some list items by a key")
  (del-list-items
    [db list-name k k-type]
    [db list-name k vs k-type v-type]
    "delete a list or some items of a list by the key")
  (get-list [db list-name k k-type v-type] "get a list by key")
  (visit-list
    [db list-name visitor k k-type]
    [db list-name visitor k k-type v-type]
    [db list-name visitor k k-type v-type raw-pred?]
    "visit a list, presumably for side effects")
  (list-count [db list-name k k-type]
    "return the number of items in the list of a key")
  (in-list? [db list-name k v k-type v-type]
    "return true if an item is in the value list of the key")
  (near-list [db list-name k v k-type v-type]
    "return the value buf that is equal to or greater than the given
     value of the given key, return nil if not found")
  (list-range
    [db list-name k-range k-type v-range v-type]
    "Return a seq of key-values in the specified value range of the
     specified key range")
  (list-range-first
    [db list-name k-range k-type v-range v-type]
    "Return the first key-value pair in the specified value range of the
     specified key range")
  (list-range-first-n
    [db list-name n k-range k-type v-range v-type]
    "Return the first n key-value pairs in the specified value range of the
     specified key range")
  (list-range-count
    [db list-name k-range k-type v-range v-type]
    "Return the approximate number of key-values in the specified key range,
     ignoring the value range boundary")
  (list-range-filter
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return a seq of key-values in the specified value range of the
     specified key range, filtered by pred call")
  (list-range-keep
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return the non-nil results of pred calls in
     the specified value range of the specified key range")
  (list-range-some
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return the first logical true result of pred calls in
     the specified value range of the specified key range")
  (list-range-filter-count
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return the count of key-values in the specified value range of the
     specified key range for those pred call is true")
  (visit-list-range
    [db list-name visitor k-range k-type v-range v-type]
    [db list-name visitor k-range k-type v-range v-type raw-pred?]
    "visit a list range, presumably for side effects of vistor call")
  (visit-list-key-range
    [db list-name visitor k-range k-type v-type]
    [db list-name visitor k-range k-type v-type raw-pred?]
    "visit a list key range, presumably for side effects of vistor call")
  (visit-list-sample
    [db list-name indices visitor k-range k-type v-type]
    [db list-name indices visitor k-range k-type v-type raw-pred?]
    "visit a list range, presumably for side effects of vistor call"))

(defprotocol ILMDB
  (check-ready [db] "check if db is ready to be operated on")
  (close-kv [db] "Close this LMDB env")
  (closed-kv? [db] "Return true if this LMDB env is closed")
  (env-dir [db] "Return the directory path of LMDB env")
  (kv-info [db]
    "Return internal kv runtime state holder (a volatile map) when available, nil otherwise")
  (env-opts [db] "Return the option map of LMDB env")
  (dbi-opts [db dbi-name] "Return option map of a given DBI")
  (max-val-size [db] "Return the max size for value buffer")
  (set-max-val-size [db size] "Set the max size for value buffer")
  (key-compressor [db] "Return the key compressor of this LMDB env")
  (set-key-compressor [db c] "Set the key compressor of this LMDB env")
  (val-compressor [db] "Return the value compressor of this LMDB env")
  (set-val-compressor [db c] "Set the value compressor of this LMDB env")
  (open-dbi
    [db dbi-name]
    [db dbi-name opts]
    "Open a named DBI (i.e. sub-db) in the LMDB env")
  (open-list-dbi
    [db list-name]
    [db list-name opts]
    "Open a named inverted list, a special dbi, that permits a list of
     values for the same key, with some corresponding special functions")
  (clear-dbi [db dbi-name]
    "Clear data in the DBI (i.e sub-db), but leave it open")
  (drop-dbi [db dbi-name]
    "Clear data in the DBI (i.e. sub-db), then delete it")
  (get-dbi [db dbi-name] [db dbi-name create?])
  (list-dbis [db] "List the names of the sub-databases")
  (copy
    [db dest]
    [db dest compact?]
    "Copy the database to a destination directory path, optionally compact
     while copying, default not compact. ")
  (stat
    [db]
    [db dbi-name]
    "Return the statitics of the unnamed top level database or a named DBI
     (i.e. sub-database) as a map")
  (entries [db dbi-name]
    "Get the number of data entries in a DBI (i.e. sub-db)")
  (get-rtx [db])
  (return-rtx [db rtx])
  (open-transact-kv [db] "open an explicit read/write rtx, return writing db")
  (close-transact-kv [db] "close and commit the read/write rtx")
  (abort-transact-kv [db] "abort the explicit read/write rtx")
  (transact-kv
    [db txs]
    [db dbi-name txs]
    [db dbi-name txs k-type]
    [db dbi-name txs k-type v-type]
    "Update DB, insert or delete key value pairs. 2-arity variation's txs can be a seq of KVTxData")
  (set-env-flags [db ks on-off]
    "Set env flags, ks are flags to be changed, on-off is true to set, false to clear.")
  (get-env-flags [db]
    "return the set of flags that are set")
  (sync [db] [db force]
    "force synchronous flush to disk if force (int) is non-zero, otherwise respect env flags")
  (get-value
    [db dbi-name k]
    [db dbi-name k k-type]
    [db dbi-name k k-type v-type]
    [db dbi-name k k-type v-type ignore-key?]
    "Get kv pair of the specified key `k`. ")
  (get-rank
    [db dbi-name k]
    [db dbi-name k k-type]
    "Get the rank (0-based position) of the key `k` in the sorted key order.
     Returns nil if the key does not exist.")
  (get-by-rank
    [db dbi-name rank]
    [db dbi-name rank k-type]
    [db dbi-name rank k-type v-type]
    [db dbi-name rank k-type v-type ignore-key?]
    "Get the key-value pair at the given rank (0-based position) in sorted order.
     Returns nil if the rank is out of bounds.")
  (sample-kv
    [db dbi-name n]
    [db dbi-name n k-type]
    [db dbi-name n k-type v-type]
    [db dbi-name n k-type v-type ignore-key?]
    "Return n random samples of key-value pairs from the dbi.")
  (get-first
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    "Return the first kv pair in the specified key range;")
  (get-first-n
    [db dbi-name n k-range]
    [db dbi-name n k-range k-type]
    [db dbi-name n k-range k-type v-type]
    [db dbi-name n k-range k-type v-type ignore-key?]
    "Return the first n kv pairs in the specified key range;")
  (range-seq
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    [db dbi-name k-range k-type v-type ignore-key? opts]
    "Return a lazy seq of kv pairs in the specified key range;")
  (get-range
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    "Return an eager seq of kv pairs in the specified key range;")
  (key-range
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    "Return an eager seq of keys in the specified key range, does not read
values;")
  (visit-key-range
    [db dbi-name visitor k-range]
    [db dbi-name visitor k-range k-type]
    [db dbi-name visitor k-range k-type raw-pred?]
    "Visit keys in the specified key range for side effects. Return nil.")
  (key-range-count
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    "Return the number of keys in the specified key range, does not read
values;")
  (key-range-list-count
    [db dbi-name k-range k-type]
    "Return the total number of list items in the specified key range, does not read values;")
  (range-count
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    "Return the number of kv pairs in the specified key range, does not process
     the kv pairs.")
  (get-some
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    "Return the first kv pair that has logical true value of pred call")
  (range-filter
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    "Return a seq of kv pair in the specified key range, for only those
     return true value for pred call.")
  (range-keep
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type raw-pred?]
    "Return a seq of non-nil results of pred calls in the specified key
     range.")
  (range-some
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type raw-pred?]
    "Return the first logical true result of pred calls in the specified key
     range.")
  (range-filter-count
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type raw-pred?]
    "Return the number of kv pairs in the specified key range, for only those
     return true value for pred call")
  (visit
    [db dbi-name visitor k-range]
    [db dbi-name visitor k-range k-type]
    [db dbi-name visitor k-range k-type v-type]
    [db dbi-name visitor k-range k-type v-type raw-pred?]
    "Call `visitor` function on each k, v pairs in the specified key range,
     presumably for side effects of visitor call. Return nil.")
  (visit-key-sample
    [db dbi-name indices visitor k-range k-type]
    [db dbi-name indices visitor k-range k-type raw-pred?]
    "visit a key range, presumably for side effects of vistor call"))

(defprotocol ITxLog
  (txlog-watermarks [db]
    "Return txn-log runtime watermarks and related metadata.")
  (open-tx-log
    [db from-lsn]
    [db from-lsn upto-lsn]
    "Return txn-log records in LSN order, optionally capped by `upto-lsn`.
     Records include derived `:tx-kind` classification (e.g. `:user`,
     `:vector-checkpoint`).")
  (force-txlog-sync! [db]
    "Force txn-log fsync/fdatasync on the active segment and advance durable LSN.")
  (force-lmdb-sync! [db]
    "Force LMDB env sync (`mdb_env_sync`) for the store.")
  (create-snapshot! [db]
    "Create/rotate durable LMDB snapshot (`current`/`previous`) and update snapshot floor bookkeeping.")
  (list-snapshots [db]
    "List available snapshots and their metadata.")
  (snapshot-scheduler-state [db]
    "Return snapshot scheduler runtime state.")
  (read-commit-marker [db]
    "Read LMDB dual-slot commit marker state.")
  (verify-commit-marker! [db]
    "Verify commit marker integrity and txlog-reference validity.")
  (txlog-retention-state [db]
    "Return computed txn-log retention floors, GC safety watermark, and pressure state.")
  (gc-txlog-segments!
    [db]
    [db retain-floor-lsn]
    "Run txn-log segment GC. Optional `retain-floor-lsn` keeps records from that floor and newer.")
  (txlog-update-snapshot-floor!
    [db snapshot-lsn]
    [db snapshot-lsn previous-snapshot-lsn]
    "Update txn-log snapshot floor bookkeeping in kv-info.
     2-arity rotates previous <- old current. 3-arity sets both explicitly.")
  (txlog-clear-snapshot-floor!
    [db]
    "Clear txn-log snapshot floor bookkeeping in kv-info.")
  (txlog-update-replica-floor!
    [db replica-id applied-lsn]
    "Upsert replica heartbeat floor (`applied-lsn`) for txn-log retention safety.")
  (txlog-clear-replica-floor!
    [db replica-id]
    "Remove replica heartbeat floor for txn-log retention safety.")
  (txlog-pin-backup-floor!
    [db pin-id floor-lsn]
    [db pin-id floor-lsn expires-ms]
    "Upsert backup/snapshot pin floor for txn-log retention safety. Optional `expires-ms` auto-expires the pin.")
  (txlog-unpin-backup-floor!
    [db pin-id]
    "Remove backup/snapshot pin floor."))

(defprotocol IAdmin
  "Some administrative functions"
  (re-index [db opts] [db schema opts] "dump and reload the data"))

(defprotocol ISearchEngine
  (add-doc [this doc-ref doc-text] [this doc-ref doc-text check-exist?])
  (remove-doc [this doc-ref])
  (clear-docs [this])
  (doc-indexed? [this doc-ref])
  (doc-count [this])
  (search [this query] [this query opts]))

(defprotocol IVectorIndex
  (add-vec [this vec-ref vec-data] "add vector to in memory index")
  (remove-vec [this vec-ref] "remove vector from in memory index")
  (get-vec [this vec-ref] "retrieve the vectors of a vec-ref")
  (persist-vecs [this] "persistent index on disk")
  (close-vecs [this] "free the in memory index")
  (vec-closed? [this] "return true if this index is closed")
  (clear-vecs [this] "close and remove this index")
  (vecs-info [this] "return a map of info about this index")
  (vec-indexed? [this vec-ref] "test if a rec-ref is in the index")
  (search-vec [this query-vec] [this query-vec opts]
    "search vector, return found vec-refs"))

(defprotocol IStore
  (opts [this] "Return the opts map")
  (assoc-opt [this k v] "Set an option")
  (db-name [this] "Return the db-name")
  (dir [this] "Return the data file directory")
  (close [this] "Close storage")
  (closed? [this] "Return true if the storage is closed")
  (last-modified [this]
    "Return the unix timestamp of when the store is last modified")
  (max-gt [this])
  (advance-max-gt [this])
  (max-tx [this])
  (advance-max-tx [this])
  (max-aid [this])
  (schema [this] "Return the schema map")
  (rschema [this] "Return the reverse schema map")
  (set-schema [this new-schema]
    "Update the schema of open storage, return updated schema")
  (attrs [this] "Return the aid -> attr map")
  (init-max-eid [this] "Initialize and return the max entity id")
  (datom-count [this index] "Return the number of datoms in the index")
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update the properties of an attribute, f is similar to that of swap!")
  (del-attr [this attr]
    "Delete an attribute, throw if there is still datom related to it")
  (rename-attr [this attr new-attr] "Rename an attribute")
  (load-datoms [this datoms] "Load datams into storage")
  (fetch [this datom] "Return [datom] if it exists in store, otherwise '()")
  (populated? [this index low-datom high-datom]
    "Return true if there exists at least one datom in the given boundary (inclusive)")
  (size [this index low-datom high-datom]
    "Return the approximate number of datoms within the given range (inclusive)")
  (e-size [this e]
    "Return the numbers of datoms with the given e value")
  (a-size [this a]
    "Return the number of datoms with the given attribute, an estimate")
  (start-sampling [this])
  (stop-sampling [this])
  (analyze [this a])
  (e-sample [this a]
    "Return a sample of eids sampled from full value range of an attribute")
  (default-ratio [this a] "default fan-out, i.e. size / cardinality")
  (v-size [this v]
    "Return the numbers of ref datoms with the given v value")
  (av-size [this a v]
    "Return the numbers of datoms with the given a and v value")
  (av-range-size [this a lv hv]
    "Return the numbers of datoms with given a and v range")
  (cardinality [this a]
    "Return the number of distinct values of an attribute")
  (head [this index low-datom high-datom]
    "Return the first datom within the given range (inclusive)")
  (tail [this index high-datom low-datom]
    "Return the last datom within the given range (inclusive)")
  (slice
    [this index low-datom high-datom]
    [this index low-datom high-datom n]
    "Return a range of datoms within the given range (inclusive).")
  (rslice
    [this index high-datom low-datom]
    [this index high-datom low-datom n]
    "Return a range of datoms in reverse within the given range (inclusive)")
  (e-datoms [this e] "Return datoms with given e value")
  (e-first-datom [this e] "Return the first datom with given e value")
  (av-datoms [this a v] "Return datoms with given a and v value")
  (av-first-datom [this a v] "Return the first datom with given a and value")
  (ea-first-datom [this e a] "Return first datom with given e and a")
  (ea-first-v [this e a] "Return first value with given e and a")
  (av-first-e [this a v] "Return the first e of the given a and v")
  (v-datoms [this v] "Return datoms with given v, for ref attribute only")
  (size-filter
    [this index pred low-datom high-datom]
    "Return the number of datoms within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (head-filter [this index pred low-datom high-datom]
    "Return the first datom within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (tail-filter [this index pred high-datom low-datom]
    "Return the last datom within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (slice-filter [this index pred low-datom high-datom]
    "Return a range of datoms within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (rslice-filter [this index pred high-datom low-datom]
    "Return a range of datoms in reverse for the given range (inclusive)
    that return true for (pred x), where x is the datom")
  (ave-tuples
    [this out attr val-range]
    [this out attr val-range vpred]
    [this out attr val-range vpred get-v?]
    [this out attr val-range vpred get-v? indices]
    "emit tuples to out")
  (ave-tuples-list [this attr val-range vpred get-v?]
    "Return list of tuples of e or e and v using :ave index")
  (sample-ave-tuples [this out attr mcount val-range vpred get-v?]
    "emit sample tuples to out")
  (sample-ave-tuples-list [this attr mcount val-range vpred get-v?]
    "Return sampled tuples of e or e and v using :ave index")
  (eav-scan-v
    [this in out eid-idx attrs-v]
    "emit tuples to out")
  (eav-scan-v-list
    [this in eid-idx attrs-v]
    "Return a list of scanned values merged into tuples using :eav index.
     Expect in to be a list also")
  (val-eq-scan-e [this in out v-idx attr] [this in out v-idx attr bound]
    "emit tuples to out")
  (val-eq-scan-e-list [this in v-idx attr] [this in v-idx attr bound]
    "Return tuples with eid as the last column for given attribute values")
  (val-eq-filter-e [this in out v-idx attr f-idx]
    "emit tuples to out")
  (val-eq-filter-e-list [this in v-idx attr f-idx]
    "Return tuples filtered by the given attribute values"))

(defprotocol ICompressor
  (method [this] "compression method, a keyword")
  (compress [this obj] "compress into byte array")
  (uncompress [this obj] "takes a byte array")
  (bf-compress [this src-bf dst-bf] "compress between byte buffers")
  (bf-uncompress [this src-bf dst-bf] "decompress between byte buffers"))
