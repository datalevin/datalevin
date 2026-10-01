(ns datalevin.ha-control-startup-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.ha.control :as ctrl]
            [datalevin.util :as u])
  (:import [com.alipay.sofa.jraft Node NodeManager]
           [com.alipay.sofa.jraft.entity PeerId]
           [java.net ServerSocket]))

(deftest failed-listener-start-releases-raft-node-for-retry
  (let [socket (ServerSocket. 0)
        peer (str "127.0.0.1:" (.getLocalPort socket))
        peer-id (doto (PeerId.) (.parse peer))
        group (str "startup-retry-" (random-uuid))
        dir (u/tmp-dir group)
        manager (NodeManager/getInstance)
        authority (ctrl/new-sofa-jraft-authority
                   {:group-id group :local-peer-id peer
                    :voters [{:peer-id peer :ha-node-id 1 :promotable? true}]
                    :raft-dir dir :rpc-timeout-ms 600
                    :election-timeout-ms 1200 :operation-timeout-ms 12000})]
    (u/create-dirs dir)
    (try
      (is (thrown? IllegalStateException (ctrl/start-authority! authority)))
      (is (false? (:running? (ctrl/authority-diagnostics authority))))
      (is (nil? (.get manager group peer-id))
          "A failed listener must not leave the initialized Raft node alive")
      (is (false? (.serverExists manager (.getEndpoint peer-id))))
      (.close socket)
      ;; Avoid starting another node over leaked resources when running this
      ;; regression against the broken implementation.
      (when-not (.get manager group peer-id)
        (ctrl/start-authority! authority)
        (is (true? (:running? (ctrl/authority-diagnostics authority)))))
      (finally
        (.close socket)
        (ctrl/stop-authority! authority)
        ;; Also clean up the baseline's leaked node after reporting the failure.
        (when-let [^Node node (.get manager group peer-id)]
          (.shutdown node)
          (.join node))
        (.removeAddress manager (.getEndpoint peer-id))
        (u/delete-files dir)))))
