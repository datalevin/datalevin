(ns datalevin.binding.unindexed-write-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.util :as u])
  (:import [datalevin.lmdb DatomKVTxData]
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
    (l/kv-tx op c/eav e
             (if (= :put op) (byte-array [v]) [(byte-array [v])]) :id :raw)))

(defn- model [ops]
  (reduce (fn [state [op e v]]
            (case op
              :put (update state e (fnil conj #{}) v)
              :del-list (update state e disj v)
              :del (dissoc state e)
              :clear {}
              :marker state)) {} ops))

(defn- regions [rows]
  (let [out (RowRegions.)]
    (doseq [part (partition-all 5 rows)] (.append out (vec part)))
    out))

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
            rows (into [indexed] (map row [[:put 1 1] [:put 1 2]
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

(deftest eav-batch-closes-its-cursor-before-map-growth-retry
  (with-native {:mapsize 1}
    (fn [db raw]
      (let [value (byte-array 511 (byte 7))
            rows (mapv #(l/kv-tx :put c/eav % value :id :raw) (range 1 5001))
            calls (atom 0)
            apply! (fn [wdb] ((cpp/prepared-row-applier wdb) rows))
            failure (try
                      (cpp/apply-native-once!
                        raw (fn [wdb] (swap! calls inc) (apply! wdb)) nil)
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
        (is (:resized (ex-data failure)))
        (is (= 1 @calls))
        (is (zero? (i/entries db c/eav)))
        (cpp/apply-native-range! raw apply! nil)
        (is (= 5000 (i/entries db c/eav)))
        (is (= 511 (alength ^bytes (first (i/get-list db c/eav 5000 :id :raw)))))
        (is (zero? (i/entries db c/ave)))))))
