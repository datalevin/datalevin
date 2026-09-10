(ns datalevin.kv-dump-test
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pp]
            [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [taoensso.nippy :as nippy])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream
            DataInputStream DataOutputStream PushbackReader StringReader]
           [java.util UUID]))

(use-fixtures :each
  (fn [f]
    (binding [c/*db-background-sampling?* false] (f))))

(defn- dump-data [kv dbi binary? legacy?]
  (if binary?
    (let [out (ByteArrayOutputStream.)]
      (with-open [data (DataOutputStream. out)]
        (if dbi (l/dump-dbi kv dbi data) (l/dump-all kv data)))
      (if legacy?
        (let [sections (nippy/thaw-from-in!
                         (DataInputStream. (ByteArrayInputStream. (.toByteArray out))))
              old (mapv (fn [[header rows]] [(dissoc header :opts) rows]) sections)
              out (ByteArrayOutputStream.)]
          (with-open [data (DataOutputStream. out)] (nippy/freeze-to-out! data old))
          (.toByteArray out))
        (.toByteArray out)))
    (let [text (with-out-str (if dbi (l/dump-dbi kv dbi) (l/dump-all kv)))]
      (if legacy?
        (with-open [in (PushbackReader. (StringReader. text))]
          (with-out-str
            (doseq [form (take-while #(not= ::eof %)
                                     (repeatedly #(edn/read {:eof ::eof} in)))]
              (pp/pprint (if (map? form) (dissoc form :opts) form)))))
        text))))

(defn- restore! [kv dbi data binary?]
  (with-open [in (if binary?
                  (DataInputStream. (ByteArrayInputStream. data))
                  (PushbackReader. (StringReader. data)))]
    (if dbi (l/load-dbi kv dbi in binary?) (l/load-all kv in binary?))))

(deftest duplicate-dump-preserves-native-dbi-options
  (let [dir (u/tmp-dir (str "kv-dump-options-" (UUID/randomUUID)))
        source (d/open-kv (str dir "/source") {:wal? false})
        values (vec (range 1 1025))]
    (try
      (d/open-list-dbi source "fixed" {:flags #{:create :dupsort :dupfixed}
                                       :key-size 8 :val-size 8})
      (d/open-list-dbi source "empty" {:flags #{:create :dupsort :dupfixed}})
      (d/open-dbi source "plain" {:key-size 8})
      (d/put-list-items source "fixed" 1 values :id :id)
      (d/transact-kv source "plain" [[:put 1 "payload"]] :id :string)
      (doseq [binary? [false true]
              [dbi legacy?] [[nil false] [nil true] ["fixed" false]]]
        (let [path (str dir "/dest-" binary? "-" dbi "-" legacy?)
              dest (d/open-kv path {:wal? false})
              data (dump-data source dbi binary? legacy?)]
          (try
            (restore! dest dbi data binary?)
            (is (= (count values) (d/list-count dest "fixed" 1 :id)))
            (is (= values (vec (d/get-list dest "fixed" 1 :id :id))))
            (is (= #{:create :dupsort :dupfixed} (:flags (i/dbi-opts dest "fixed"))))
            (is (= 8 (:key-size (i/dbi-opts dest "fixed"))))
            (when-not dbi
              (is (= 0 (d/list-count dest "empty" 1 :id)))
              (is (= "payload" (d/get-value dest "plain" 1 :id :string))))
            (finally (d/close-kv dest)))
          (let [reopened (d/open-kv path {:wal? false})]
            (try
              (d/open-list-dbi reopened "fixed")
              (is (= values (vec (d/get-list reopened "fixed" 1 :id :id))))
              (finally (d/close-kv reopened))))))
      (finally (d/close-kv source) (u/delete-files dir)))))
