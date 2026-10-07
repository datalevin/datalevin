;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.wal
  "IWalBranch adapter over the WAL-only txlog interface.

  Each sealed descriptor carries an owned `:wal-body`, either encoded bytes or
  a frozen encoding plan. Sealing stores those bodies in a flat
  array. The adapter appends that array as one group record under a single exclusive
  WAL ownership span, so a force cannot claim ownership between the append and
  its policy completion. The WAL runtime must have a bound runtime control
  (`txlog/bind-runtime-control!`)."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec])
  (:import [java.util ArrayList List]))

(definterface ^:private IBodyPlan
  (^bytes encodeBody []))

(defn- encode-parts
  [^List parts encode-body]
  (if (= 1 (.size parts))
    (let [part (.get parts 0)]
      (if (bytes? part) part (encode-body part {})))
    (let [encoded (ArrayList. (.size parts))]
      (doseq [part parts]
        (.add encoded (if (bytes? part) part (encode-body part {}))))
      (codec/combine-commit-row-payloads encoded))))

(deftype ^:private BodyPlan [^List parts encode-body]
  IBodyPlan
  (encodeBody [_] (encode-parts parts encode-body)))

(defn ^:redef prepare-body
  "Freeze ordered row regions and existing payloads for the WAL owner's encoder.
   Neither the parts nor their rows may be changed after dispatch."
  ([parts encode-body] (BodyPlan. parts encode-body))
  ([parts encode-body defer?]
   (if defer? (prepare-body parts encode-body) (encode-parts parts encode-body))))

(defn- encoded-bodies
  [b]
  (let [^objects bodies (batch/wal-bodies b)]
    (if (loop [idx 0]
          (cond (= idx (alength bodies)) false
                (instance? BodyPlan (aget bodies idx)) true
                :else (recur (inc idx))))
      (do
        (phase/phase! :wal-encode-start b)
        (let [encoded (object-array (alength bodies))]
          (dotimes [idx (alength bodies)]
            (let [body (aget bodies idx)]
              (aset encoded idx (if (instance? BodyPlan body)
                                  (.encodeBody ^BodyPlan body) body))))
          (phase/phase! :wal-encode-complete b)
          encoded))
      bodies)))

(defn branch
  "Adapt a private WAL runtime state to the executor's `IWalBranch` and
  `IWalMaintenance`.

  `opts`:
  - `:append-fn` overrides the five-argument `txlog/begin-prepared-group!`
    (state, LSN, body array, absolute deadline, phase context); injected by tests.
  - `:complete-fn` overrides `txlog/finish-prepared-group!`; injected by tests.
  - `:maintenance-deadline-fn` overrides `txlog/maintenance-deadline-ns`.
  - `:service-maintenance-fn` overrides `txlog/service-maintenance!`."
  ([state] (branch state nil))
  ([state {:keys [append-fn complete-fn
                  maintenance-deadline-fn service-maintenance-fn]}]
   (let [custom-append? (some? append-fn)
         append-fn (or append-fn wal/begin-prepared-group!)
         complete-fn (or complete-fn wal/finish-prepared-group!)
         deadline-fn (or maintenance-deadline-fn wal/maintenance-deadline-ns)
         service-fn (or service-maintenance-fn wal/service-maintenance!)]
     (reify
       executor/IWalBranch
       (append-group! [_ batch lsn]
         (let [bodies (encoded-bodies batch)]
           (if custom-append?
             (append-fn state lsn bodies (batch/batch-cutoff batch) batch)
             (let [write-count
                   (loop [idx 0 total 0]
                     (if (< idx (batch/batch-count batch))
                       (let [data (batch/data (batch/batch-at batch idx))]
                         (recur (inc idx)
                                (if (or (:storage-staged? data)
                                        (if (or (instance? datalevin.tx_group.batch.DatalogMemberData data)
                                                (contains? data :rows))
                                          (seq (:rows data)) (:wal-body data)))
                                  (inc total) total)))
                       total))]
               (wal/begin-prepared-group! state lsn bodies
                                          (batch/batch-cutoff batch) batch
                                          write-count)))))
       (complete-policy! [_ token deadline-ns]
         (complete-fn state token deadline-ns))
       executor/IWalMaintenance
       (maintenance-deadline-ns [_]
         (deadline-fn state))
       (service-maintenance! [_ deadline-ns]
         (service-fn state deadline-ns))))))
