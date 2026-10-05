(ns datalevin.prepared-native-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.util :as u])
  (:import [java.util Arrays]))

(defn- row [op dbi key value]
  (l/kv-tx op dbi key value :raw :raw))

(deftest prepared-native-rows-preserve-order-and-duplicate-deletes
  (let [dir (u/tmp-dir (str "prepared-native-" (random-uuid)))
        db (l/open-kv dir {})
        raw (kv/raw-lmdb db)
        key (byte-array [1])
        a (byte-array [2])
        b (byte-array [3])]
    (try
      (i/open-dbi db "data")
      (i/open-list-dbi db "items")
      (is (thrown? clojure.lang.ExceptionInfo (cpp/prepared-row-applier raw)))
      (cpp/apply-native-once!
       raw
       (fn [wdb]
         (let [apply! (cpp/prepared-row-applier wdb)]
           (apply! [(row :put "data" key a) (row :put "items" key a)
                    (row :put "items" key b)])
           (apply! [(row :del "data" key nil) (row :put "data" key b)
                    ;; The missing duplicate must not corrupt the native key.
                    (row :del-list "items" key [(byte-array [4])])
                    (row :del-list "items" key [a])])
           (is (Arrays/equals b ^bytes (i/get-value wdb "data" key :raw :raw)))
           (is (= [[3]] (mapv vec (i/get-list wdb "items" key :raw :raw))))))
       nil)
      (is (Arrays/equals b ^bytes (i/get-value db "data" key :raw :raw)))
      (is (= [[3]] (mapv vec (i/get-list db "items" key :raw :raw))))
      (finally (i/close-kv db) (u/delete-files dir)))))

(deftest prepared-native-large-values-persist-buffer-size-and-abort-as-a-unit
  (let [dir (u/tmp-dir (str "prepared-native-large-" (random-uuid)))
        db (l/open-kv dir {})
        raw (kv/raw-lmdb db)
        key (byte-array [1])
        value (byte-array 131072 (byte 7))]
    (try
      (i/open-dbi db "data")
      (cpp/apply-native-once!
       raw (fn [wdb] ((cpp/prepared-row-applier wdb) [(row :put "data" key value)])) nil)
      (is (<= (alength value) (long (i/get-value db c/kv-info :max-val-size))))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"policy failure"
           (cpp/apply-native-once!
            raw
            (fn [wdb]
              (let [apply! (cpp/prepared-row-applier wdb)]
                (apply! [(row :del "data" key nil)])
                (apply! [(row :put "data" (byte-array [2]) value)])))
            (fn [_ _] (throw (ex-info "policy failure" {}))))))
      (is (Arrays/equals value ^bytes (i/get-value db "data" key :raw :raw)))
      (is (nil? (i/get-value db "data" (byte-array [2]) :raw :raw)))
      (i/close-kv db)
      (let [reopened (l/open-kv dir {})]
        (try
          (is (Arrays/equals value ^bytes (i/get-value reopened "data" key :raw :raw)))
          (finally (i/close-kv reopened))))
      (finally (i/close-kv db) (u/delete-files dir)))))

(deftest prepared-native-map-growth-does-not-repeat-a-body
  (let [dir (u/tmp-dir (str "prepared-native-resize-" (random-uuid)))
        db (l/open-kv dir {:mapsize 1})
        raw (kv/raw-lmdb db)
        calls (atom 0)
        rows [(row :put "data" (byte-array [1]) (byte-array 2097152))]]
    (try
      (i/open-dbi db "data")
      (let [failure (try
                      (cpp/apply-native-once!
                       raw (fn [wdb]
                             (swap! calls inc)
                             ((cpp/prepared-row-applier wdb) rows)) nil)
                      (catch clojure.lang.ExceptionInfo e e))]
        (is (:resized (ex-data failure)))
        (is (= 1 @calls))
        (is (nil? (i/get-value db "data" (byte-array [1]) :raw :raw))))
      ;; Immutable application may retry map growth, with a fresh applier.
      (cpp/apply-native-range!
       raw (fn [wdb] ((cpp/prepared-row-applier wdb) rows)) nil)
      (is (= 2097152 (alength ^bytes (i/get-value db "data" (byte-array [1]) :raw :raw))))
      (finally (i/close-kv db) (u/delete-files dir)))))
