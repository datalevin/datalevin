(ns datalevin.client-pool-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.client :as sut]
            [datalevin.constants :as c]
            [datalevin.protocol :as p])
  (:import [java.nio ByteBuffer]
           [java.nio.channels SocketChannel ServerSocketChannel]
           [java.net InetSocketAddress]
           [java.util UUID]
           [java.util.concurrent ConcurrentLinkedQueue]))

(deftest request-holds-connection-during-copy-out-test
  (let [copy-started (promise)
        server       (doto (ServerSocketChannel/open)
                       (.bind (InetSocketAddress. "127.0.0.1" 0)))
        port         (.getLocalPort (.socket server))
        server-fut   (future
                       (with-open [^SocketChannel server-ch (.accept server)]
                         (let [^ByteBuffer read-bf  (ByteBuffer/allocate c/+buffer-size+)
                               ^ByteBuffer write-bf (ByteBuffer/allocate c/+buffer-size+)]
                           (p/receive-ch server-ch read-bf)
                           (p/write-message-blocking server-ch write-bf
                                                     {:type :copy-out-response})
                           (deliver copy-started true)
                           ;; Keep copy-out in progress long enough for a second borrow attempt.
                           (Thread/sleep 1500)
                           (p/write-message-blocking server-ch write-bf [:payload])
                           (p/write-message-blocking server-ch write-bf
                                                     {:type :copy-done})))
                       :server-done)
        client-id    (UUID/randomUUID)
        available    (ConcurrentLinkedQueue.)
        used         (ConcurrentLinkedQueue.)
        ch           (doto (SocketChannel/open)
                       (.connect (InetSocketAddress. "127.0.0.1" port)))
        conn         (sut/->Connection ch (ByteBuffer/allocate c/+buffer-size+))
        pool         (sut/->ConnectionPool "127.0.0.1" port client-id
                                           1 5000 available used)
        client       (sut/->Client "datalevin" "datalevin"
                                   "127.0.0.1" port
                                   1 5000 client-id pool)]
    (.add available conn)
    (try
      (let [request-fut (future (sut/request client {:type :test-copy-out}))]
        (is (true? (deref copy-started 2000 false)))
        (let [borrow-started (promise)
              borrow-fut     (future
                               (deliver borrow-started true)
                               (sut/get-connection pool))]
          (is (true? (deref borrow-started 1000 false)))
          ;; While copy-out is still streaming, no second borrow should complete.
          (Thread/sleep 300)
          (is (false? (realized? borrow-fut)))

          (let [request-result (deref request-fut 5000 ::timeout)]
            (is (not= ::timeout request-result))
            (is (= :command-complete (:type request-result)))
            (is (= [:payload] (:result request-result))))
          (let [borrowed-conn (deref borrow-fut 5000 ::timeout)]
            (is (not= ::timeout borrowed-conn))
            (sut/release-connection pool borrowed-conn)))
        (is (= :server-done (deref server-fut 3000 ::timeout))))
      (finally
        (.close ch)
        (.close server)))))

(deftest close-pool-closes-all-connections-test
  (let [client-id (UUID/randomUUID)
        available (ConcurrentLinkedQueue.)
        used      (ConcurrentLinkedQueue.)
        ch1       (SocketChannel/open)
        ch2       (SocketChannel/open)
        conn1     (sut/->Connection ch1 (ByteBuffer/allocate c/+buffer-size+))
        conn2     (sut/->Connection ch2 (ByteBuffer/allocate c/+buffer-size+))
        pool      (sut/->ConnectionPool "localhost" c/default-port client-id
                                        2 1000 available used)]
    (.add available conn1)
    (.add used conn2)
    (sut/close-pool pool)
    (is (false? (.isOpen ch1)))
    (is (false? (.isOpen ch2)))
    (is (sut/closed-pool? pool))))

(deftest copy-out-response-propagates-extra-metadata-test
  (let [server     (doto (ServerSocketChannel/open)
                     (.bind (InetSocketAddress. "127.0.0.1" 0)))
        port       (.getLocalPort (.socket server))
        server-fut (future
                     (with-open [^SocketChannel server-ch (.accept server)]
                       (let [^ByteBuffer read-bf  (ByteBuffer/allocate c/+buffer-size+)
                             ^ByteBuffer write-bf (ByteBuffer/allocate c/+buffer-size+)]
                         (p/receive-ch server-ch read-bf)
                         (p/write-message-blocking server-ch write-bf
                                                   {:type           :copy-out-response
                                                    :db-info        {:max-eid 10
                                                                     :max-tx  9}
                                                    :new-attributes [:x/new]
                                                    :copy-meta      {:duration-ms 3}})
                         (p/write-message-blocking server-ch write-bf [[:payload]])
                         (p/write-message-blocking server-ch write-bf
                                                   {:type          :copy-done
                                                    :checksum      "abc"
                                                    :checksum-algo :sha-256
                                                    :bytes         7})))
                     :server-done)
        client-id  (UUID/randomUUID)
        available  (ConcurrentLinkedQueue.)
        used       (ConcurrentLinkedQueue.)
        ch         (doto (SocketChannel/open)
                     (.connect (InetSocketAddress. "127.0.0.1" port)))
        conn       (sut/->Connection ch (ByteBuffer/allocate c/+buffer-size+))
        pool       (sut/->ConnectionPool "127.0.0.1" port client-id
                                         1 1000 available used)
        client     (sut/->Client "datalevin" "datalevin"
                                 "127.0.0.1" port
                                 1 1000 client-id pool)]
    (.add available conn)
    (try
      (let [{:keys [type result db-info new-attributes copy-meta
                    checksum checksum-algo bytes]}
            (sut/request client {:type :test-copy-out-with-meta})]
        (is (= :command-complete type))
        (is (= [[:payload]] result))
        (is (= {:max-eid 10 :max-tx 9} db-info))
        (is (= [:x/new] new-attributes))
        (is (= {:duration-ms 3} copy-meta))
        (is (= "abc" checksum))
        (is (= :sha-256 checksum-algo))
        (is (= 7 bytes))
        (is (= :server-done (deref server-fut 3000 ::timeout))))
      (finally
        (.close ch)
        (.close server)))))
