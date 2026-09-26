;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.entity
  "Scalar entity projections from EAV under a shared cursor and snapshot."
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.custom-datalog :as cd]
            [datalevin.index :as idx]
            [datalevin.lmdb :as l]
            [datalevin.scan :as scan]
            [datalevin.timeout :as timeout])
  (:import [java.lang AutoCloseable]))

(defn- projected-entity
  [lmdb iter eid ^objects names ^longs aids id?]
  (timeout/assert-time-left)
  (when-some [eid eid]
    (let [n (alength names)
          result
          (loop [index 0
                 next? (and (pos? n) (l/seek-key iter eid :id))
                 entity (transient (if id? {:db/id eid} {}))]
            (if (and next? (< index n))
              (let [buffer (l/next-val iter)
                    aid (b/avg->aid buffer)
                    index (long (loop [j (long index)]
                                  (if (and (< j n) (< (aget aids j) aid))
                                    (recur (inc j)) j)))
                    match? (and (< index n) (= aid (aget aids index)))
                    entity (if match?
                             (assoc! entity (aget names index)
                                     (idx/avg-buffer->v lmdb buffer))
                             entity)
                    index (if match? (inc index) index)]
                (recur index (and (< index n) (l/has-next-val iter)) entity))
              (not-empty (persistent! entity))))]
      (timeout/assert-time-left)
      result)))

(defn select-entities
  "Project scalar attributes for resolved IDs with one reusable EAV cursor.
  Names and aids are parallel arrays ordered by aid. Preserve order, duplicates
  and nil lookup results, including pull's :db/id behavior for missing IDs."
  [lmdb ids names aids id?]
  (if (seq ids)
    (cd/with-snapshot lmdb
      (scan/scan lmdb c/eav
        (with-open [^AutoCloseable iter
                    (l/val-iterator (l/iterate-list-val-full dbi rtx cur))]
          (mapv #(projected-entity lmdb iter % names aids id?) ids))
        (throw e)))
    []))
