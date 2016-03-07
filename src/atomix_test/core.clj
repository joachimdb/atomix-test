(ns atomix-test.core
  (:import (io.atomix Atomix AtomixClient AtomixReplica)
           (io.atomix.catalyst.transport Address NettyTransport)
           (io.atomix.copycat.server.storage Storage StorageLevel)
           (java.util Collection UUID)
           (java.net InetAddress)
           (java.util.concurrent CompletableFuture)
           (io.atomix.collections DistributedMap)
           (io.atomix.variables DistributedValue)
           ;; (io.atomix.group DistributedGroup)
           ;; (io.atomix.concurrent.DistributedLock) ;; OK
           ;; (io.atomix.messaging DistributedTopic) ;; OK
           ;; (io.atomix.messaging DistributedTaskQueue) ;; OK
           ;; (io.atomix.group DistributedGroup) ;; not OK
           ))

(defn replica
  "Returns an `io.atomix.AtomixReplica` for the `port` and `remote-nodes`.
  * `port` should be the localhost port for the replica to listen on.
  * `nodes` should be a `seq` of `map`s containing `:host` and `:port` values.
  * `config` should be a `map` containing:
      * `:storage` - The `io.atomix.copycat.server.storage.Storage` instance for the replica to use.
      * `:transport` - The `io.atomix.catalyst.transport.Transport` instance for the client to use. Defaults to `NettyTransport`."
  ^AtomixReplica
  ([port nodes]
   (replica port nodes (empty {})))
  ([port nodes config]
   (let [localhost (-> (InetAddress/getLocalHost)
                       (.getHostName))
         local-address (Address. localhost port)
         ^Collection addresses (map #(Address. (:host %) (:port %))
                                           nodes)
         storage (get config :storage (Storage.))
         transport (get config :transport (NettyTransport.))
         replica (-> (AtomixReplica/builder local-address addresses)
                     (.withTransport transport)
                     (.withStorage storage)
                     (.build))]
     replica)))

(defn open!
  "Opens the `atomix` client or replica."
  [^Atomix atomix]
  (-> (.open atomix)
      (.get)))

(defn open-async!
  "Asynchronously opens the `atomix` client or replica."
  ^CompletableFuture
  [^Atomix atomix]
  (.open atomix))

(defn close!
  "Closes the `atomix` client or replica."
  [^Atomix atomix]
  (-> (.close atomix)
      (.get)))

(defn close-async!
  "Asynchronously closes the `atomix` client or replica."
  ^CompletableFuture
  [^Atomix atomix]
  (.close atomix))

(defn get-map
  "Gets a distributed map for the `atomix` instance on the resource `key`."
  ^DistributedMap
  [^Atomix atomix key]
  (-> atomix
      (.getMap key)
      (.get)))

(defn get-map-async
  "Asynchronously gets a distributed map for the `atomix` instance on the resource `key`."
  ^CompletableFuture
  [^Atomix atomix key]
  (-> atomix
      (.getMap key)))

(defn get-group
  "Gets a distributed group for the `atomix` instance on the group `key`."
  ;; ^DistributedGroup
  [^Atomix atomix key]
  (-> atomix
      (.getGroup key)
      (.get)))

(defn get-group-async
  "Asynchronously gets a distributed group for the `atomix` instance on the group `key`."
  ^CompletableFuture
  [^Atomix atomix key]
  (-> atomix
      (.getGroup key)))

(comment 

  ;; on guest machine:
  (def r (replica 8010 [{:host "localhost" :port 8011}]))

  ;; in vagrant:
  (def r (replica 8011 [{:host "localhost" :port 8010}]))

  (def o (open-async! r))
  (.isDone o1)
  
  )

