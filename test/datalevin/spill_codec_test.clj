(ns datalevin.spill-codec-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.protocol :as p]
   [datalevin.spill :as spill]
   [datalevin.test.core :refer [db-fixture]]
   [taoensso.nippy :as nippy]
   [taoensso.nippy.io :as nio]
   [taoensso.nippy.schema :as schema])
  (:import
   [datalevin.spill SpillableVector]
   [datalevin.utl UniqueVectorSet]
   [java.io ByteArrayOutputStream DataOutputStream]
   [java.nio ByteBuffer BufferOverflowException]
   [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(defn- legacy-bytes [value]
  (let [bytes (ByteArrayOutputStream.)
        out (DataOutputStream. bytes)]
    (nippy/with-cache
      (when-let [metadata (and nippy/*incl-metadata?* (not-empty (meta value)))]
        (.writeByte out schema/id-meta)
        (nippy/freeze-to-out! out metadata))
      (nio/write-typed-din value out))
    (.toByteArray bytes)))

(defn- buffer [direct? capacity]
  (if direct? (ByteBuffer/allocateDirect capacity) (ByteBuffer/allocate capacity)))

(deftest spillable-buffer-writers-preserve-legacy-readers-and-metadata
  (let [cached (nippy/cache {:keyword-key [:same :same]})
        factories [(fn [opts] (spill/new-spillable-vector
                               [cached cached nil false "東京 👋"
                                (with-meta [:nested] {:source :inner})] opts))
                   (fn [opts] (spill/new-spillable-map
                               {:first cached :second cached nil false
                                :nested (with-meta [:nested] {:source :inner})} opts))
                   (fn [opts] (spill/new-spillable-set
                               #{:first :second nil false [1 2]} opts))]]
    (doseq [factory factories
            spilled? [false true]
            :let [value (with-meta (factory {:spill-threshold (if spilled? -1 100)})
                          {:source :outer})]]
      (try
        (doseq [metadata? [false true]
                direct? [false true]]
          (binding [nippy/*incl-metadata?* metadata?]
            (let [^ByteBuffer out (buffer direct? 16384)
                  legacy (legacy-bytes value)]
              (p/write-nippy-bf out value)
              (.flip out)
              (let [bytes (byte-array (.remaining out))
                    _ (.get out bytes)
                    expected (nippy/fast-thaw legacy)
                    actual (nippy/fast-thaw bytes)]
                (is (= expected actual))
                (is (= (class value) (class actual)))
                (is (= (when metadata? {:source :outer}) (meta actual)))
                (when (instance? SpillableVector value)
                  (is (= (seq legacy) (seq bytes)))
                  (is (= (when metadata? {:source :inner}) (meta (nth actual 5)))))
                (when (map? actual)
                  (is (= (when metadata? {:source :inner}) (meta (:nested actual)))))))))
        (finally (empty value))))))

(deftest spillable-writers-retry-without-poisoning-caches
  (let [cached (nippy/cache (.repeat "cached" 15000))]
    (doseq [make [spill/new-spillable-vector spill/new-spillable-map spill/new-spillable-set]
            direct? [false true]
            :let [value (condp = make
                          spill/new-spillable-vector (make [cached cached])
                          spill/new-spillable-map (make {:first cached :second cached})
                          (make #{[:first cached] [:second cached]}))
                  message {:result value :next cached}]]
      (testing (str (class value) " direct=" direct?)
        (let [^ByteBuffer small (buffer direct? 32)]
          (.putInt small 42)
          (is (thrown? BufferOverflowException (p/write-message-bf small message)))
          (is (= 4 (.position small)))
          (is (= 42 (.getInt small 0))))
        (let [^ByteBuffer large (buffer direct? 300000)
              expected (nippy/fast-thaw (nippy/fast-freeze message))]
          (p/write-message-bf large message)
          (is (= expected (first (p/receive-one-message large))))
          (is (zero? (.position large))))))))

(deftest unique-vector-set-keeps-its-spillable-set-wire-type
  (let [rows (doto (FastList.)
               (.add (object-array [1 :one]))
               (.add (object-array [2 :two])))
        value (with-meta (UniqueVectorSet/fromUniqueTuples rows) {:source :query})
        out (ByteBuffer/allocate 4096)]
    (p/write-nippy-bf out value)
    (.flip out)
    (let [actual (p/read-nippy-bf out)]
      (is (= #{[1 :one] [2 :two]} actual))
      (is (= {:source :query} (meta actual)))
      (is (= (class (spill/new-spillable-set)) (class actual))))))
