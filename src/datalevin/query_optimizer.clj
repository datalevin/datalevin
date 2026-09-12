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
   [datalevin.constants :as c]
   [datalevin.query.optimizer.access-plan :as qaccess]
   [datalevin.query.optimizer.bound-patterns :as qbound]
   [datalevin.query.optimizer.estimates :refer [estimate-hash-join-cost]]
   [datalevin.query.optimizer.plan-build :as qpb
    :refer [build-plan* merge-pred-options multi-key-result-size]]
   [datalevin.query.optimizer.plan-cost :as qcost
    :refer [estimate-scan-v-cost]]
   [datalevin.query.optimizer.properties :as qprops
    :refer [alternative-satisfies?]]
   [datalevin.query.optimizer.rewrite :as qrewrite]
   [datalevin.query.optimizer.selective :as qselective])
  (:import
   [datalevin.utl LRUCache]))

(def ^:dynamic *plan-cache* (LRUCache. c/query-result-cache-size))

(qpb/set-plan-cache-provider! (fn [] *plan-cache*))

;; Public API aliases kept for callers of `datalevin.query-optimizer`.
(def access-sample-cost-budget qcost/access-sample-cost-budget)
(def build-graph qrewrite/build-graph)
(def build-plan qpb/build-plan)
(def build-property-memo qprops/build-property-memo)
(def combine-ranges qrewrite/combine-ranges)
(def estimated-plan-cost qcost/estimated-plan-cost)
(def fast-clause-count qcost/fast-clause-count)
(def find-index qpb/find-index)
(def flip-ranges qrewrite/flip-ranges)
(def intersect-ranges qrewrite/intersect-ranges)
(def materialize-input-bound-patterns qbound/materialize-input-bound-patterns)
(def materialize-selective-rule-anchors
  qselective/materialize-selective-rule-anchors)
(def materialize-selective-value-lookups
  qselective/materialize-selective-value-lookups)
(def plan-access-joins qaccess/plan-access-joins)
(def plan-not-joins qpb/plan-not-joins)
(def plugin-inputs qrewrite/plugin-inputs)
(def propagate-physical-properties qprops/propagate-physical-properties)
(def properties-satisfy? qprops/properties-satisfy?)
(def property-memo-summary qprops/property-memo-summary)
(def push-down-equality-disjunctions qrewrite/push-down-equality-disjunctions)
(def retain-property-alternative qprops/retain-property-alternative)
(def rewrite-unused-vars qrewrite/rewrite-unused-vars)
(def schedule-correlated-access qaccess/schedule-correlated-access)
(def selected-alternative qprops/selected-alternative)
(def unused-var-replacements qrewrite/unused-var-replacements)
(def writing? qpb/writing?)
