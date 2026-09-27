(ns datalevin.kv.txlog-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.kv.txlog :as sut]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u])
  (:import
   [java.io RandomAccessFile]
   [java.nio.channels FileChannel]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "bounded-txlog-" (random-uuid)))]
      (u/create-dirs *dir*)
      (try (f) (finally (u/delete-files *dir*))))))

(defn- append! [id lsns]
  (let [path (txlog/segment-path *dir* id)]
    (with-open [^FileChannel ch (txlog/open-segment-channel path)]
      (doseq [lsn lsns]
        (txlog/append-record!
          ch (txlog/encode-commit-row-payload lsn 1 [[:put "dbi" lsn lsn]]))))
    (.length (io/file path))))

(defn- state []
  {:dir *dir* :txlog-records-cache (volatile! {})})

(defn- lsns [state from upto]
  (mapv :lsn (sut/txlog-records state from upto)))

(deftest bounded-catch-up-does-not-collect-the-log-tip
  (doseq [^long id (range 1 11)]
    (append! id (range (inc (* (dec id) 1000)) (inc (* id 1000)))))
  (let [state (state)
        cache (:txlog-records-cache state)
        segments (txlog/segment-files *dir*)]
    (testing "cold reads scan a bounded prefix and only probe later segment starts"
      (is (= (vec (range 101 111)) (lsns state 101 110)))
      (is (< (reduce + (map #(count (:records %)) (vals @cache))) 120))
      (is (= 111 (-> @cache (get 1) :records peek :lsn))))
    (testing "catch-up extends the cached closed prefix instead of rescanning it"
      (let [first-summary (-> @cache (get 1) :records first)]
        (is (= (vec (range 111 121)) (lsns state 111 120)))
        (is (identical? first-summary (-> @cache (get 1) :records first)))
        (is (= 121 (-> @cache (get 1) :records peek :lsn)))))
    (testing "warm full indexes also collect only the requested validation window"
      (is (= 10000 (count (sut/txlog-records state))))
      (let [selected (#'sut/txlog-segments-through state segments cache 110)
            records (vec (#'sut/collect-txlog-records
                           state selected cache 101 110 true))]
        (is (= [1 2] (mapv :id selected)))
        (is (<= (count records) 13))
        (is (= (vec (range 101 111)) (lsns state 101 110)))))
    (testing "a page can straddle segments"
      (is (= (vec (range 995 1005)) (lsns state 995 1004))))))

(deftest bounded-segment-scan-stops-before-unrequested-corruption
  (append! 1 (range 1 101))
  (append! 2 (range 101 201))
  (append! 3 (range 201 301))
  ;; Neither the target segment's tail nor the tails of boundary probes are
  ;; part of this page. Corruption there must only be encountered by later reads.
  (doseq [[id record-index] [[1 50] [2 1] [3 1]]]
    (let [path (txlog/segment-path *dir* id)
          records (:records (txlog/scan-segment path))
          offset (:offset (nth records record-index))]
      (with-open [raf (RandomAccessFile. ^String path "rw")]
        (.seek raf offset)
        (.write raf (int 0)))))
  (doseq [state [(state) {:dir *dir*}]]
    (is (= [10 11] (lsns state 10 11)))
    (is (thrown? clojure.lang.ExceptionInfo (sut/txlog-records state 10)))))

(deftest bounded-reader-preserves-overlaps-and-skips-empty-segments
  (doseq [[id records] [[1 [1 2 3]] [2 []] [3 [3 4 5]]
                        [4 [3 4 5]] [5 []] [6 [6 7]] [7 []]]]
    (append! id records))
  (doseq [state [(state) {:dir *dir*}]]
    (is (= [2 3 4 5 6] (lsns state 2 6)))
    (is (= [4 4 4] (mapv :segment-id (sut/txlog-records state 3 5))))
    (is (= [7] (lsns state 7 9)))
    (is (= [] (lsns state 8 9)))
    (is (= [] (lsns state 0 0)))
    (is (= [1 2 3 4 5 6 7] (lsns state 0 100)))))

(deftest bounded-reader-keeps-gap-validation-at-both-ends
  (append! 1 [1 2 4 5])
  (append! 2 [7 8])
  (doseq [[from upto] [[4 5] [1 2] [4 6]]]
    (let [error (try (sut/txlog-records (state) from upto)
                     (catch clojure.lang.ExceptionInfo e e))]
      (is (= :txlog/corrupt (:type (ex-data error)))))))

(deftest bounded-cache-remains-correct-as-active-segments-grow-and-close
  (let [offset (append! 1 [1 2 3 4])
        state (assoc (state) :segment-id (volatile! 1)
                             :segment-offset (volatile! offset))]
    (is (= [1] (lsns state 1 1)))
    (vreset! (:segment-offset state) (append! 1 [5 6]))
    (is (= [4 5] (lsns state 4 5)))
    (vreset! (:segment-offset state) (append! 2 [7 8]))
    (vreset! (:segment-id state) 2)
    (is (= [5 6 7] (lsns state 5 7)))
    (is (= (vec (range 1 9)) (mapv :lsn (sut/txlog-records state))))))

(deftest bounded-cache-does-not-hide-truncation
  (append! 1 [1 2 3 4 5])
  (let [state (state)]
    (is (= [1 2 3] (lsns state 1 3)))
    (let [offset (-> @(:txlog-records-cache state) (get 1) :records second
                     :next-offset)]
      (with-open [raf (RandomAccessFile. ^String (txlog/segment-path *dir* 1) "rw")]
        (.setLength raf offset)))
    (is (= [1 2] (lsns state 1 3)))))

(deftest lagging-follower-retains-its-prefix-in-a-small-cache
  (doseq [^long id (range 1 11)]
    (append! id (range (inc (* (dec id) 10)) (inc (* id 10)))))
  (binding [c/*wal-records-cache-segments* 2]
    (let [state (state)
          cache (:txlog-records-cache state)]
      (is (= [99 100] (lsns state 99 100)))
      (is (= [2 3] (lsns state 2 3)))
      (let [first-summary (-> @cache (get 1) :records first)]
        (is (some? first-summary))
        (is (= [4 5] (lsns state 4 5)))
        (is (identical? first-summary (-> @cache (get 1) :records first)))
        (is (<= (count @cache) 2))))))

(deftest recovery-still-reads-the-full-retained-prefix-and-tail
  (append! 1 [1 2 3 4])
  (append! 2 [5 6 7 8])
  (let [state (state)]
    (is (= [2] (lsns state 2 2)))
    (is (= (vec (range 1 9))
           (mapv :lsn (sut/txlog-records-for-recovery state 3))))
    (is (= [5 6 7 8]
           (mapv :lsn (sut/txlog-records-for-recovery state 6))))))

(deftest bounded-reader-includes-maximum-lsn
  (append! 1 [(dec Long/MAX_VALUE) Long/MAX_VALUE])
  (is (= [(dec Long/MAX_VALUE) Long/MAX_VALUE]
         (lsns (state) (dec Long/MAX_VALUE) Long/MAX_VALUE))))

(deftest scan-callback-can-stop-without-reporting-a-partial-tail
  (append! 1 [1 2 3])
  (doseq [collect? [true false]]
    (let [seen (atom [])
          result (txlog/scan-segment
                   (txlog/segment-path *dir* 1)
                   {:collect-records? collect?
                    :on-record (fn [record]
                                 (swap! seen conj record)
                                 (reduced nil))})]
      (is (= 1 (count @seen)))
      (is (= collect? (boolean (seq (:records result)))))
      (is (:stopped? result))
      (is (false? (:partial-tail? result)))
      (is (= (:next-offset (first @seen)) (:valid-end result)))
      (is (< (:valid-end result) (:size result))))))
