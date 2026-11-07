(ns datalevin.storage-staging-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.storage :as s]
            [datalevin.test.core :refer [db-fixture]])
  (:import [datalevin.storage Store WriteGroup]
           [datalevin.utl WriterContext]
           [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:number {:db/valueType :db.type/long}
   :text {:db/valueType :db.type/string}
   :blob {:db/valueType :db.type/bytes}})

(defn- with-group [f]
  (with-open [conn (d/create-conn nil schema
                    {:wal? false :background-sampling? false :snapshot-scheduler? false})]
    (l/with-transaction-kv [writer (kv/raw-lmdb (d/datalog-kv conn))]
      (let [store (s/transfer (:store @conn) writer)
            ^WriteGroup group (s/write-group writer)
            ^WriterContext context (l/writer-context writer)
            notices (atom 0)
            flush! (fn []
                     (when-let [rows (s/take-group-storage-rows! group writer)]
                       (i/transact-kv writer rows)))]
        (.setRequestContext context {:datalog-prepare? true})
        (.setCallbacks context nil #(swap! notices inc) flush! nil nil)
        (binding [s/*write-group* group c/*ordered-datom-writes?* false]
          (f {:store store :group group :writer writer :notices notices}))))))

(defn- pending [^WriteGroup group]
  (or (aget ^objects (.-storage-context group) 1) (.-storage-datoms group)))

(defn- metadata-state [^Store store ^WriteGroup group]
  [(vec (.-metadata group)) (i/max-tx store)
   (aget ^objects (.-storage-context group) 0)])

(deftest successful-staging-traverses-the-input-once
  (with-group
    (fn [{:keys [store group notices]}]
      (let [datoms [(datom/datom 1 :number 1) (datom/datom 1 :text "one")
                    (datom/datom 2 :number 2)]
            traversals (atom 0)
            input (reify Iterable
                    (iterator [_]
                      (swap! traversals inc)
                      (.iterator ^List datoms)))]
        (is (number? (s/stage-group-datoms! store input)))
        (is (= 1 @traversals))
        (is (= datoms (vec (pending group))))
        (is (= 1 @notices))
        (let [before-tx (i/max-tx store)]
          (is (number? (s/stage-group-datoms! store [])))
          (is (= (inc (long before-tx)) (i/max-tx store)))
          (is (= datoms (vec (pending group))))
          (is (= 1 @notices)))))))

(deftest rejected-or-throwing-input-restores-the-existing-prefix
  (doseq [detached? [false true]]
    (testing (str "detached pending region " detached?)
      (with-group
        (fn [{:keys [store group notices]}]
          (when detached?
            (aset ^objects (.-storage-context ^WriteGroup group) 1 (FastList.)))
          (let [prefix [(datom/datom 1 :number 1)]
                good (datom/datom 2 :number 2)]
            (s/stage-group-datoms! store prefix)
            (let [before (metadata-state store group)]
              (doseq [bad [(datom/datom 3 :unknown 3)
                           (datom/datom 3 :blob (byte-array [1 2 3]))
                           (datom/datom 3 :text (.repeat "x" 101))]
                      position [0 4]]
                (is (nil? (s/stage-group-datoms! store
                             (into (vec (repeat position good)) [bad good]))))
                (is (= prefix (vec (pending group))))
                (is (= before (metadata-state store group)))
                (is (= 1 @notices)))
              (is (thrown-with-msg? clojure.lang.ExceptionInfo #"stream failed"
                    (s/stage-group-datoms! store
                      (lazy-seq (cons good (lazy-seq (throw (ex-info "stream failed" {}))))))))
              (is (= prefix (vec (pending group))))
              (is (= before (metadata-state store group)))
              (is (= 1 @notices))
              (is (number? (s/stage-group-datoms! store [good])))
              (is (= (conj prefix good) (vec (pending group))))
              (is (= 2 @notices)))))))))

(deftest global-rejection-does-not-touch-input-or-group-state
  (with-group
    (fn [{:keys [store group notices]}]
      (s/stage-group-datoms! store [(datom/datom 1 :number 1)])
      (let [before (metadata-state store group)
            prefix (vec (pending group))
            input (lazy-seq (throw (ex-info "must not read input" {})))]
        (binding [c/*ordered-datom-writes?* true]
          (is (nil? (s/stage-group-datoms! store input))))
        (is (= prefix (vec (pending group))))
        (is (= before (metadata-state store group)))
        (is (= 1 @notices))))))

(deftest fallback-drains-only-the-previously-accepted-datoms
  (with-group
    (fn [{:keys [store group writer notices]}]
      (let [prefix [(datom/datom 1 :number 1)]
            bytes (byte-array [1 2 3])
            fallback [(datom/delete (first prefix))
                      (datom/datom 1 :number 2) (datom/datom 1 :blob bytes)]]
        (s/stage-group-datoms! store prefix)
        (is (nil? (s/stage-group-datoms! store fallback)))
        (is (= prefix (vec (pending group))))
        (is (= 1 @notices))
        (s/load-datoms-with-plan! store fallback nil)
        (is (empty? (pending group)))
        (is (= 2 (i/ea-first-v store 1 :number)))
        (is (= [1 2 3] (vec (i/ea-first-v store 1 :blob))))
        (is (= 2 (i/entries writer c/eav)))))))
