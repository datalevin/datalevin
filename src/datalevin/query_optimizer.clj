;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query-optimizer
  "Compatibility facade over datalevin.query.optimizer.*"
  (:require
   [datalevin.query.optimizer.plan-build :as qpb
    :refer [alternative-satisfies? build-plan* estimate-scan-v-cost
            merge-pred-options multi-key-result-size]]
   [datalevin.query.optimizer.estimates :refer [estimate-hash-join-cost]]
   [datalevin.constants :as c])
  (:import
   [datalevin.utl LRUCache]))

(def ^:dynamic *plan-cache* (LRUCache. c/query-result-cache-size))

(qpb/set-plan-cache-provider! (fn [] *plan-cache*))

;; Public API aliases kept for callers of `datalevin.query-optimizer`.
(def access-sample-cost-budget qpb/access-sample-cost-budget)
(def build-graph qpb/build-graph)
(def build-plan qpb/build-plan)
(def build-property-memo qpb/build-property-memo)
(def combine-ranges qpb/combine-ranges)
(def estimated-plan-cost qpb/estimated-plan-cost)
(def fast-clause-count qpb/fast-clause-count)
(def find-index qpb/find-index)
(def flip-ranges qpb/flip-ranges)
(def intersect-ranges qpb/intersect-ranges)
(def materialize-input-bound-patterns qpb/materialize-input-bound-patterns)
(def materialize-selective-rule-anchors qpb/materialize-selective-rule-anchors)
(def materialize-selective-value-lookups qpb/materialize-selective-value-lookups)
(def plan-access-joins qpb/plan-access-joins)
(def plan-not-joins qpb/plan-not-joins)
(def plugin-inputs qpb/plugin-inputs)
(def propagate-physical-properties qpb/propagate-physical-properties)
(def properties-satisfy? qpb/properties-satisfy?)
(def property-memo-summary qpb/property-memo-summary)
(def push-down-equality-disjunctions qpb/push-down-equality-disjunctions)
(def retain-property-alternative qpb/retain-property-alternative)
(def rewrite-unused-vars qpb/rewrite-unused-vars)
(def schedule-correlated-access qpb/schedule-correlated-access)
(def selected-alternative qpb/selected-alternative)
(def unused-var-replacements qpb/unused-var-replacements)
(def writing? qpb/writing?)
