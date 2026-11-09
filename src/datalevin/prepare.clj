;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.prepare
  "Prepare phase of transaction processing."
  (:require
   [datalevin.interface :as i
    :refer [schema opts]]
   [datalevin.index :as idx]
   [datalevin.custom-datalog :as cd]
   [datalevin.datom :as d]
   [datalevin.util :refer [raise]]
   [datalevin.bits :as b])
  (:import
   [datalevin.datom Datom]
   [java.util Date]))

;; ---- Interpreter schema snapshot ----

(defrecord PrepareCtx [schema rschema opts])

(defn make-prepare-ctx
  "Capture the schema and options shared by one interpreter attempt."
  [db]
  (let [store (:store db)]
    (->PrepareCtx (schema store) (i/rschema store) (opts store))))

;; ---- Type coercion and value correction ----

(defn coerce-inst
  "Coerce a value to java.util.Date."
  [v]
  (cond
    (instance? java.time.Instant v) (Date. (long (inst-ms v)))
    (inst? v)    v ; java.sql.Date and java.sql.Timestamp are java.util.Date
    (integer? v) (Date. (long v))
    :else        (raise "Expect java.util.Date" {:input v})))

(defn coerce-uuid
  "Coerce a value to java.util.UUID."
  [v]
  (cond
    (uuid? v)   v
    (string? v) (if-let [u (parse-uuid v)]
                  u
                  (raise "Unable to parse string to UUID" {:input v}))
    :else       (raise "Expect java.util.UUID" {:input v})))

(defn type-coercion
  "Coerce a value to the appropriate type based on value type."
  [vt v]
  (case vt
    :db.type/string              (if (string? v) v (str v))
    :db.type/bigint              (biginteger v)
    :db.type/bigdec              (bigdec v)
    (:db.type/long :db.type/ref) (if (instance? Long v) v (long v))
    :db.type/float               (if (instance? Float v) v (float v))
    :db.type/double              (if (instance? Double v) v (double v))
    (:db.type/bytes :bytes)      (if (bytes? v) v (byte-array v))
    (:db.type/keyword :keyword)  (keyword v)
    (:db.type/symbol :symbol)    (symbol v)
    (:db.type/boolean :boolean)  (boolean v)
    (:db.type/instant :instant)  (coerce-inst v)
    (:db.type/uuid :uuid)        (coerce-uuid v)
    :db.type/tuple               (vec v)
    v))

(defn- valid-prepared-value?
  "Check canonical scalar classes directly; other values retain full validation."
  [v vt]
  (case vt
    :db.type/string (string? v)
    (:db.type/long :db.type/ref)
    (or (instance? Long v) (b/valid-data? v vt))
    ;; These classes also contain NaN and infinities, which validation rejects.
    :db.type/float (if (instance? Float v)
                     (Float/isFinite (float v))
                     (b/valid-data? v vt))
    :db.type/double (if (instance? Double v)
                      (Double/isFinite (double v))
                      (b/valid-data? v vt))
    (b/valid-data? v vt)))

(defn correct-datom*
  "Reuse a datom whose corrected value is identical; otherwise reconstruct it."
  [^Datom datom v]
  (if (identical? (.-v datom) v)
    datom
    (d/datom (.-e datom) (.-a datom) v
             (d/datom-tx datom) (d/datom-added datom))))

(declare correct-value)

(defn correct-datom
  "Correct value in datom via type validation and coercion."
  [store ^Datom datom]
  (correct-datom* datom (correct-value store (.-a datom) (.-v datom))))

(defn ^:no-doc correct-value-with-props
  "Validate and coerce a value using already-fetched store options and
  attribute properties."
  [store-opts props a v]
  (let [vt (idx/value-type props)]
    (when (and (contains? props :db/tupleType)
               (some? v)
               (seqable? v)
               (empty? v))
      (raise "Cannot store an empty homogeneous tuple for attribute " a
               {:error      :transact/syntax
                :attribute  a
                :value      v
                :tuple-type (:db/tupleType props)}))
    (if (identical? vt :db.type/idoc)
      ((requiring-resolve 'datalevin.idoc/parse-value)
       a props store-opts v)
      (do
        (or (not (store-opts :validate-data?))
            (cd/custom-type? vt)
            (valid-prepared-value? v vt)
            (raise "Invalid data, expecting" vt " got " v {:input v}))
        (type-coercion vt v)))))

(defn ^:no-doc correct-scalar-value
  "Validate and coerce a non-nil value after preparation has excluded
  tuples, unresolved references, idoc and custom types. `vt` is the built-in type
  or nil for untyped attributes; value-dependent validation is never cached."
  [validate? vt v]
  (let [vt (or vt :data)]
    (or (not validate?)
        (valid-prepared-value? v vt)
        (raise "Invalid data, expecting" vt " got " v {:input v}))
    (type-coercion vt v)))

(defn correct-value
  "Validate type and coerce value for an attribute."
  [store a v]
  (correct-value-with-props (opts store) ((schema store) a) a v))
