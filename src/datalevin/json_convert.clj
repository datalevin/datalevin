;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.json-convert
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [datalevin.datom :as dd]
   [jsonista.core :as json])
  (:import
   [java.io ByteArrayOutputStream OutputStream]
   [java.math BigDecimal BigInteger]
   [java.nio.charset StandardCharsets]
   [java.time Instant]
   [java.util Base64 Date UUID]))

(def ^:const max-js-safe-int 9007199254740991)
(def ^:const min-js-safe-int (- max-js-safe-int))

(def ^:private tag-keys
  #{"~str" "~i64" "~map" "~set" "~date" "~uuid" "~bytes" "~bigdec"
    "~bigint" "~edn" "~handle" "~pull"})

(def ^:private read-mapper
  (json/object-mapper {:decode-key-fn identity}))

(def ^:private write-mapper
  (json/object-mapper {:encode-key-fn identity}))

(defn tagged-value
  [tag value]
  {::tag tag
   ::value value})

(defn tagged-value?
  ([x]
   (and (map? x)
        (keyword? (::tag x))
        (contains? x ::value)))
  ([x tag]
   (and (tagged-value? x)
        (= (::tag x) tag))))

(defn tagged-value-value
  [x]
  (::value x))

(defn handle-ref?
  [x]
  (tagged-value? x :handle))

(defn handle-ref
  [h]
  (tagged-value :handle h))

(defn handle-ref-value
  [x]
  (tagged-value-value x))

(defn pull-form?
  [x]
  (tagged-value? x :pull))

(defn pull-form
  [x]
  (tagged-value :pull x))

(defn pull-form-value
  [x]
  (tagged-value-value x))

(defn reserved-tag?
  [s]
  (contains? tag-keys s))

(defn- decode-string
  [x]
  (if (and (string? x) (str/starts-with? x ":"))
    (keyword (subs x 1))
    x))

(declare decode-json-value)

(defn- decode-map-pairs
  [pairs]
  (into {}
        (map (fn [[k v]]
               [(decode-json-value k) (decode-json-value v)]))
        pairs))

(defn- decode-tagged-value
  [tag value]
  (case tag
    "~str" value
    "~i64" (Long/parseLong ^String value)
    "~map" (decode-map-pairs value)
    "~set" (into #{} (map decode-json-value) value)
    "~date" (Date/from (Instant/parse ^String value))
    "~uuid" (UUID/fromString ^String value)
    "~bytes" (.decode (Base64/getDecoder) ^String value)
    "~bigdec" (BigDecimal. ^String value)
    "~bigint" (BigInteger. ^String value)
    "~edn" (edn/read-string ^String value)
    "~handle" (handle-ref value)
    "~pull" (pull-form (decode-json-value value))
    (throw (ex-info "Unsupported JSON tag."
                    {:code :invalid-json-tag
                     :tag  tag}))))

(defn decode-json-value
  [x]
  (cond
    (or (nil? x) (string? x) (boolean? x))
    (if (string? x) (decode-string x) x)

    (map? x)
    (if (and (= 1 (count x))
             (contains? tag-keys (ffirst x)))
      (decode-tagged-value (ffirst x) (val (first x)))
      (into {}
            (map (fn [[k v]]
                   [(decode-string k) (decode-json-value v)]))
            x))

    (instance? java.util.Map x)
    (decode-json-value (into {} x))

    (vector? x)
    (mapv decode-json-value x)

    (instance? java.util.List x)
    (mapv decode-json-value x)

    :else
    x))

(defn read-json-string
  [^String s]
  (try
    (decode-json-value (json/read-value s read-mapper))
    (catch Exception e
      (throw (ex-info "Invalid JSON request."
                      {:code :invalid-json}
                      e)))))

(defn- js-safe-int?
  [^long n]
  (<= min-js-safe-int n max-js-safe-int))

(declare json-ready)

(defn- encode-map
  [m]
  (let [entries (mapv (fn [[k v]]
                        [(json-ready k) (json-ready v)])
                      m)
        keys    (map first entries)]
    (if (and (every? string? keys)
             (not-any? reserved-tag? keys))
      (into {} entries)
      {"~map" entries})))

(defn- encode-datom
  [d]
  {"e"     (json-ready (dd/datom-e d))
   "a"     (json-ready (dd/datom-a d))
   "v"     (json-ready (dd/datom-v d))
   "tx"    (json-ready (dd/datom-tx d))
   "added" (json-ready (dd/datom-added d))})

(defn json-ready
  [x]
  (cond
    (nil? x) nil
    (string? x) x
    (keyword? x) (str x)
    (symbol? x) {"~edn" (pr-str x)}
    (boolean? x) x
    (instance? Long x) (if (js-safe-int? x) x {"~i64" (str x)})
    (instance? Integer x) x
    (instance? Short x) (int x)
    (instance? Byte x) (int x)
    (instance? BigInteger x) {"~bigint" (str x)}
    (instance? clojure.lang.BigInt x) {"~bigint" (str x)}
    (instance? BigDecimal x) {"~bigdec" (str x)}
    (instance? Double x) x
    (instance? Float x) (double x)
    (instance? Date x) {"~date" (.toString (.toInstant ^Date x))}
    (instance? Instant x) {"~date" (.toString ^Instant x)}
    (instance? UUID x) {"~uuid" (str x)}
    (bytes? x) {"~bytes" (.encodeToString (Base64/getEncoder) ^bytes x)}
    (dd/datom? x) (encode-datom x)
    (handle-ref? x) {"~handle" (handle-ref-value x)}
    (pull-form? x) {"~pull" (json-ready (pull-form-value x))}
    (map? x) (encode-map x)
    (instance? java.util.Map x) (encode-map (into {} x))
    (set? x) {"~set" (mapv json-ready x)}
    (vector? x) (mapv json-ready x)
    (sequential? x) (mapv json-ready x)
    (instance? java.util.Collection x) (mapv json-ready x)
    (integer? x) {"~bigint" (str x)}
    :else
    (throw (ex-info "Unsupported value for JSON conversion."
                    {:code  :json-unsupported-type
                     :class (str (class x))}))))

(defn- limited-output-stream
  [^long max-bytes]
  (let [baos  (ByteArrayOutputStream.)
        count (volatile! 0)]
    {:buffer baos
     :stream
     (proxy [OutputStream] []
       (write
         ([b]
          (let [n (inc ^long @count)]
            (when (> n max-bytes)
              (throw (ex-info "Result exceeds max-response-bytes limit."
                              {:code  :result-too-large
                               :kind  :bytes
                               :limit max-bytes})))
            (vreset! count n)
            (.write baos (int b))))
         ([b off len]
          (let [n (+ ^long @count ^long len)]
            (when (> n max-bytes)
              (throw (ex-info "Result exceeds max-response-bytes limit."
                              {:code  :result-too-large
                               :kind  :bytes
                               :limit max-bytes})))
            (vreset! count n)
            (.write baos ^bytes b (int off) (int len)))))
       (flush []
         (.flush baos))
       (close []
         (.close baos)))}))

(defn write-json-ready-string
  ([value]
   (json/write-value-as-string value write-mapper))
  ([value {:keys [max-response-bytes]}]
   (if (nil? max-response-bytes)
     (write-json-ready-string value)
     (let [{:keys [buffer stream]} (limited-output-stream max-response-bytes)]
       (json/write-value stream value write-mapper)
       (.toString ^ByteArrayOutputStream buffer StandardCharsets/UTF_8)))))

(defn write-json-string
  ([value]
   (write-json-ready-string (json-ready value)))
  ([value limits]
   (write-json-ready-string (json-ready value) limits)))
