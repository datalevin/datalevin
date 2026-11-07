(ns datalevin.binding.unindexed-write-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datalevin.binding.cpp :as cpp]
            [datalevin.binding.cpp.write :as write]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.util :as u])
  (:import [datalevin.binding.cpp CppLMDB Rtx]
           [datalevin.cpp Util$MapFullException]
           [datalevin.lmdb DatomKVTxData]
           [datalevin.utl RowRegions]))

(use-fixtures :each
  (fn [f] (binding [c/*db-background-sampling?* false] (f))))

(defn- with-native
  ([f] (with-native {} f))
  ([opts f]
   (let [dir (u/tmp-dir (str "unindexed-write-" (random-uuid)))
         db (l/open-kv dir opts)]
     (try
       (i/open-list-dbi db c/eav)
       (i/open-list-dbi db c/ave)
       (i/open-dbi db "marker")
       (f db (kv/raw-lmdb db))
       (finally (i/close-kv db) (u/delete-files dir))))))

(defn- row [[op e v]]
  (case op
    :clear (l/kv-tx :clear c/eav nil nil)
    :del (l/kv-tx :del c/eav e :id)
    :marker (l/kv-tx :put "marker" :last v)
    :indexed-put (DatomKVTxData. (+ 98 e) (byte-array [v]) true false)
    :indexed-del (DatomKVTxData. (+ 98 e) (byte-array [v]) false false)
    (l/kv-tx op c/eav e
             (if (= :put op) (byte-array [v]) [(byte-array [v])]) :id :raw)))

(defn- model [ops]
  (reduce (fn [state [op e v]]
            (case op
              :put (update state e (fnil conj #{}) v)
              :del-list (update state e disj v)
              :indexed-put (update state (+ 98 e) (fnil conj #{}) v)
              :indexed-del (update state (+ 98 e) disj v)
              :del (dissoc state e)
              :clear {}
              :marker state)) {} ops))

(defn- regions [rows]
  (let [out (RowRegions.)]
    (doseq [part (partition-all 5 rows)] (.append out (vec part)))
    out))

(defn- apply-rows!
  [^CppLMDB wdb rows mode]
  (case mode
    :prepared
    (let [dbis (.-dbis wdb)
          ^Rtx rtx @(l/write-txn wdb)
          ;; :clear is an internal row, rejected by public KV preparation.
          ops (if (some #(and (not (instance? DatomKVTxData %))
                              (= :clear (.-op ^datalevin.lmdb.KVTxData %))) rows)
                (object-array
                  (mapcat (fn [tx]
                            [(when-not (instance? DatomKVTxData tx)
                               (.get ^java.util.HashMap dbis
                                     (.-dbi-name ^datalevin.lmdb.KVTxData tx))) tx]) rows))
                (write/prepare-kvtx-ops rows dbis nil :data :data))]
      (write/transact-prepared-ops* ops dbis (.-txn rtx)))
    :validated
    (let [^Rtx rtx @(l/write-txn wdb)]
      (write/transact* rows (.-dbis wdb) (.-txn rtx)))
    ((cpp/prepared-row-applier wdb) (if (= :regions mode) (regions rows) rows))))

(deftest eav-runs-preserve-order-across-regions-and-kv-barriers
  (with-native
    (fn [db raw]
      (let [result
            (tc/quick-check 100
              (prop/for-all [ops (gen/vector
                                  (gen/tuple (gen/elements [:put :put :del-list :del :clear :marker])
                                             (gen/choose 1 4) (gen/choose 1 8)) 0 80)
                             regions? gen/boolean]
                (let [;; Repeated/missing deletes must preserve the following key.
                      prefix [[:put 1 1] [:put 1 2] [:del-list 1 8] [:put 2 3]
                              [:del-list 1 1] [:put 1 1] [:del-list 1 1]]
                      ops (into prefix ops)
                      rows (mapv row (cons [:clear 0 0] ops))
                      expected (model ops)]
                  (cpp/apply-native-once!
                    raw (fn [wdb]
                          ((cpp/prepared-row-applier wdb)
                           (if regions? (regions rows) rows))) nil)
                  (every? (fn [e]
                            (= (or (get expected e) #{})
                               (set (map #(aget ^bytes % 0)
                                         (i/get-list db c/eav e :id :raw)))))
                          (range 1 5))))
              :seed 42)]
        (is (:pass? result) (pr-str result))))))

(deftest indexed-and-unindexed-rows-share-atomic-native-application
  (with-native
    (fn [db raw]
      (let [avg (byte-array [50])
            indexed (DatomKVTxData. 99 avg true false)
            rows (into [(DatomKVTxData. 99 avg false false) indexed]
                       (map row [[:put 1 1] [:put 1 2]
                                          [:del-list 1 7] [:put 2 3]
                                          [:put 1 4] [:del-list 1 2]]))
            apply! (fn [wdb] ((cpp/prepared-row-applier wdb) (regions rows)))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"abort test"
              (cpp/apply-native-once! raw apply!
                (fn [_ _] (throw (ex-info "abort test" {}))))))
        (is (zero? (i/entries db c/eav)))
        (is (zero? (i/entries db c/ave)))
        (cpp/apply-native-once! raw apply! nil)
        (is (= [[1] [4]] (mapv vec (i/get-list db c/eav 1 :id :raw))))
        (is (= [[3]] (mapv vec (i/get-list db c/eav 2 :id :raw))))
        (is (= [[50]] (mapv vec (i/get-list db c/eav 99 :id :raw))))
        (is (= [99] (vec (i/get-list db c/ave avg :raw :id))))
        ;; Unindexed values never acquire AVE entries.
        (is (= 1 (i/entries db c/ave)))))))

(deftest scalar-eav-runs-preserve-order-around-indexed-and-kv-operations
  (with-native
    (fn [db raw]
      (binding [c/*ordered-datom-writes?* false]
        (let [result
              (tc/quick-check 100
                (prop/for-all
                  [tail (gen/vector
                          (gen/tuple (gen/elements [:put :put :del-list :del :clear
                                                    :marker :indexed-put :indexed-del])
                                     (gen/choose 1 4) (gen/choose 1 8)) 0 80)
                   mode (gen/elements [:rows :regions :prepared :validated])]
                  (let [prefix [[:indexed-del 1 8]
                                [:put 1 1] [:put 1 2] [:del-list 1 8] [:put 2 3]
                                [:indexed-put 1 7]
                                [:del-list 1 1] [:put 1 1] [:del-list 1 1] [:put 1 4]
                                [:marker 0 77] [:indexed-del 1 7]]
                        ops (into prefix (if (= mode :validated)
                                           (remove #(= :clear (first %)) tail) tail))
                        expected (model ops)
                        ave (model (map (fn [[op e v]]
                                          (case op
                                            :indexed-put [:put v (+ 98 e)]
                                            :indexed-del [:del-list v (+ 98 e)]
                                            [:marker 0 0])) ops))
                        rows (mapv row ops)]
                    (cpp/apply-native-once!
                      raw
                      (fn [wdb]
                        (apply-rows! wdb [(l/kv-tx :clear c/eav nil nil)
                                         (l/kv-tx :clear c/ave nil nil)] :rows)
                        (apply-rows! wdb rows mode)) nil)
                    (and
                      (every? (fn [e]
                                (= (or (get expected e) #{})
                                   (set (map #(aget ^bytes % 0)
                                             (i/get-list db c/eav e :id :raw)))))
                              (concat (range 1 5) (range 99 103)))
                      (every? (fn [v]
                                (= (or (get ave v) #{})
                                   (set (i/get-list db c/ave (byte-array [v]) :raw :id))))
                              (range 1 9))
                      (= (nth (last (filter #(= :marker (first %)) ops)) 2)
                         (i/get-value db "marker" :last)))))
                :seed 43)]
          (is (:pass? result) (pr-str result)))))))

(deftest scalar-eav-runs-use-the-shared-writer-at-the-existing-threshold
  (doseq [n [3 4 8] regions? [false true]]
    (with-native
      (fn [db raw]
        (let [batched-reads (atom 0)
              rows (into [(DatomKVTxData. 99 (byte-array [50]) false false)]
                         (map #(row [:put % %]) (range 1 (inc n))))
              observed (proxy [java.util.AbstractList java.util.RandomAccess] []
                         (size [] (count rows))
                         (get [index]
                           (when (some #(.startsWith (.getClassName ^StackTraceElement %)
                                                    "datalevin.binding.cpp.write$put_eav_rows")
                                       (.getStackTrace (Thread/currentThread)))
                             (swap! batched-reads inc))
                           (nth rows index)))]
          (binding [c/*ordered-datom-writes?* false]
            (cpp/apply-native-once! raw
              (fn [wdb]
                (apply-rows! wdb (if regions?
                                  (doto (RowRegions.) (.append observed)) observed)
                             :rows)) nil))
          (is (= (>= n 4) (pos? @batched-reads)))
          (is (= n (i/entries db c/eav)))
          (is (zero? (i/entries db c/ave))))))))

(deftest scalar-eav-runs-respect-flagged-row-barriers
  (doseq [mode [:rows :prepared]]
    (with-native
      (fn [db raw]
        (let [rows (conj (into [(DatomKVTxData. 99 (byte-array [50]) false false)]
                               (map #(row [:put % %]) (range 1 5)))
                         (l/kv-tx :put c/eav 1 (byte-array [1]) :id :raw [:nodupdata]))]
          (binding [c/*ordered-datom-writes?* false]
            (is (thrown? Exception
                         (cpp/apply-native-once! raw
                           (fn [wdb] (apply-rows! wdb rows mode)) nil))))
          (is (zero? (i/entries db c/eav)))
          (is (zero? (i/entries db c/ave))))))))

(deftest eav-batch-closes-its-cursor-before-map-growth-retry
  (doseq [mode [:rows :prepared]]
    (with-native {:mapsize 1}
      (fn [db raw]
        (let [value (byte-array 511 (byte 7))
              rows (into [(DatomKVTxData. 99999 (byte-array [50]) false false)]
                         (map #(l/kv-tx :put c/eav % value :id :raw) (range 1 5001)))
              calls (atom 0)
              apply! (fn [wdb] (apply-rows! wdb rows mode))
              failure (try
                        (cpp/apply-native-once!
                          raw (fn [wdb] (swap! calls inc) (apply! wdb)) nil)
                        nil
                        (catch Exception e e))]
          (is (if (= mode :prepared)
                (instance? Util$MapFullException failure)
                (:resized (ex-data failure))))
          (is (= 1 @calls))
          (is (zero? (i/entries db c/eav)))
          (if (= mode :prepared)
            (i/transact-kv raw rows)
            (cpp/apply-native-range! raw apply! nil))
          (is (= 5000 (i/entries db c/eav)))
          (is (= 511 (alength ^bytes (first (i/get-list db c/eav 5000 :id :raw)))))
          (is (zero? (i/entries db c/ave))))))))
