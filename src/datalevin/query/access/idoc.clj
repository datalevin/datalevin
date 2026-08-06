;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns ^:no-doc datalevin.query.access.idoc
  "Complete unordered access paths for indexed-document matches."
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.idoc :as idoc]
   [datalevin.interface :refer [schema]]
   [datalevin.query.access :as access]
   [datalevin.query.access.function :as function]
   [datalevin.storage :as st]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^:const ^long default-batch-size 512)

(defn- argument-slots
  [args]
  (case (count args)
    1 [(nth args 0) nil nil]
    2 [(nth args 0) (nth args 1) nil]
    3 [(nth args 0) (nth args 1) (nth args 2)]
    nil))

(defn- attr-domain
  [^Store store attr]
  (let [props ((schema store) attr)]
    (when (identical? (:db/valueType props) :db.type/idoc)
      (or (:db/domain props) (u/keyword->string attr)))))

(defn- planning-domains
  [^Store store indices args]
  (when-let [[arg1 arg2 arg3] (argument-slots args)]
    (let [attr?    (keyword? arg1)
          domain   (when attr? (attr-domain store arg1))
          domains0 (if attr? [domain] (:domains arg2))
          opts     (if attr? arg3 arg2)
          domains  (or (when (map? opts) (:domains opts))
                       domains0
                       (keys indices))]
      (when (and (or (not attr?) domain)
                 (every? indices domains))
        (vec domains)))))

(defn- estimated-output-rows
  ^long [^long scan-rows]
  (if (zero? scan-rows)
    0
    (max 1
         (long
           (Math/ceil
             (* (double scan-rows)
                (double c/magic-scan-ratio)))))))

(defn- open-match-cursor
  [^DB source spec resume]
  (let [[arg1 arg2 arg3]
        (or (argument-slots (function/resolve-arguments spec))
            (throw
              (ex-info "Invalid idoc access function arity"
                       {:function (:function spec)
                        :args     (:args spec)})))
        needed  (get-in spec [:projection :needed])
        request (built-ins/idoc-match-request
                  source arg1 arg2 arg3 needed)]
    (built-ins/prepare-idoc-match-cursor request resume)))

(deftype IdocCursor
    [^DB source spec ^long batch-size max-candidates resume state closed?]
  access/IAccessCursor
  (-next-batch [_]
    (if @closed?
      (access/->AccessBatch (FastList.) nil true)
      (let [{:keys [match-cursor scanned]} @state
            scanned      (long scanned)
            remaining    (when (some? max-candidates)
                           (max 0 (- (long max-candidates) scanned)))
            page-size    (long
                           (if (some? remaining)
                             (min batch-size (long remaining))
                             batch-size))]
        (if (zero? page-size)
          (access/->AccessBatch (FastList.) nil false)
          (let [match-cursor
                (or match-cursor
                    (open-match-cursor source spec resume))
                {:keys [tuples continuation exhausted? scanned]}
                (built-ins/next-idoc-match-cursor-batch
                  match-cursor page-size)
                scanned (long scanned)
                frontier
                (when continuation
                  (access/->AccessFrontier continuation nil))]
            (vreset! state
                     {:match-cursor match-cursor
                      :scanned      (+ (long (:scanned @state)) scanned)})
            (when exhausted?
              (vreset! closed? true)
              (built-ins/close-idoc-match-cursor match-cursor))
            (assoc
              (access/->AccessBatch tuples frontier exhausted?)
              :scanned scanned))))))

  (-close-cursor [_]
    (when-not @closed?
      (vreset! closed? true)
      (when-let [match-cursor (:match-cursor @state)]
        (built-ins/close-idoc-match-cursor match-cursor)))))

(defrecord IdocAccessMethod []
  function/IFunctionAccessBackend
  (-function-access-plans
    [this _planning-context spec]
    (let [source (:source-value spec)]
      (if (and (empty? (:requires spec))
               (= 3 (get-in spec [:projection :source-width]))
               (instance? DB source)
               (instance? Store (.-store ^DB source)))
        (let [^Store store (.-store ^DB source)
              indices      (st/store-idoc-indices store)
              args         (function/resolve-arguments spec)]
          (if-some [domains (planning-domains store indices args)]
            (let [scan-rows
                  (long
                    (reduce
                      (fn [^long total domain]
                        (+ total
                           (idoc/doc-count (indices domain))))
                      0 domains))
                  output-rows (estimated-output-rows scan-rows)
                  startup     1.0
                  per-row     (double c/magic-cost-pred)
                  cost        (if (zero? scan-rows)
                                0.0
                                (+ startup
                                   (* (double scan-rows) per-row)))
                  expr        (function/access-expr
                                spec :idoc (:produces spec))
                  path
                  (assoc
                    (access/->AccessPath
                      :idoc this :complete-scan nil #{:complete :resumable}
                      {:spec spec :domains domains}
                      (access/access-policy :none :execution))
                    :quality :exact)
                  estimate
                  (assoc
                    (access/->AccessEstimate
                      startup per-row output-rows cost :low)
                    :range-rows scan-rows
                    :scan-rows scan-rows
                    :output-rows output-rows
                    :conventional-cost cost)]
              [(access/->AccessPlan
                 expr path (access/source-bounds)
                 (access/access-work default-batch-size) estimate)])
            []))
        [])))

  access/IAccessMethod
  (-access-plans [_ _planning-context]
    [])

  (-open-access [_ path _demand _bounds work source]
    (let [spec   (get-in path [:options :spec])
          source (or source (:source-value spec))]
      (when-not (instance? DB source)
        (throw
          (ex-info "Idoc access requires a database source"
                   {:source source})))
      (IdocCursor.
        source spec
        (max 1 (long (or (:batch-size work) default-batch-size)))
        (:max-candidates work)
        (:resume work)
        (volatile! {:match-cursor nil
                    :scanned      (long (or (:emitted work) 0))})
        (volatile! false))))

  (-frontier-satisfies? [_ _path _demand _frontier _cutoff]
    false))

(def access-method (->IdocAccessMethod))
