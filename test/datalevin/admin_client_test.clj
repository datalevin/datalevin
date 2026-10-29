(ns datalevin.admin-client-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.protocol :as protocol])
  (:import [datalevin.client ConnectionPool]
           [java.nio ByteBuffer]
           [java.nio.channels SocketChannel]
           [java.nio.channels.spi SelectorProvider]))

(deftest status-reopen-response-never-sends-an-open
  (let [frame (ByteBuffer/allocate 65536)
        sent (atom 0)
        _ (protocol/write-message-bf frame {:type :reopen :db-name "closed" :db-type "kv"}
                                     c/message-format-nippy)
        _ (.flip frame)
        channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (write [^ByteBuffer src]
                    (swap! sent inc)
                    (let [n (.remaining src)] (.position src (.limit src)) n))
                  (read [^ByteBuffer dst]
                    (if (and (pos? @sent) (.hasRemaining frame))
                      (let [n (.remaining frame)] (.put dst frame) n)
                      0))
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        connection (client/->Connection channel 5000 (ByteBuffer/allocate 65536))
        ^ConnectionPool pool (#'client/connection-pool "localhost" 19001 nil 1 5000)
        _ (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) connection)
        transport (client/->Client "user" "password" "localhost" 19001 1 5000 nil pool)]
    (try
      (let [error (try (client/admin-status transport "closed") nil (catch Exception e e))]
        (is (= :admin/unexpected-reopen (:error (ex-data error))))
        (is (= 1 @sent)))
      (finally (client/close-pool pool)))))
