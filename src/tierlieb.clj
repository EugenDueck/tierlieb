(ns tierlieb
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException))
  (:use [clojure.tools.logging])
  (:require [clojure.set :as set]
            [tierlieb.listener :as listener]
            [zookeeper :as zk]
            [zookeeper.util :as util]
            [zookeeper.data :as data]))

(defn- make-path
  [& segments]
  (apply str \/ (interpose \/ segments)))

(defn- get-last-path-element
  [path]
  (.substring path (inc (.lastIndexOf path "/"))))

(defn- deserialize-data
  [bytes]
  (if bytes (read-string (data/to-string bytes)) nil))

(defn- serialize-data
  [data]
  (data/to-bytes (pr-str data)))

(defn- zk-alter-data
  "fun will be called with a byte array as argument and should return
   [new-data x]. Where x can be anything and the value of the final
   invocation is returned by zk-alter-data."
  ([client path fun] (zk-alter-data client path fun nil 0))
  ([client path fun data version]
     (let [ret (try
                 (let [[new-data new-data-deserialized] (fun data)]
                   (zk/set-data client path new-data version)
                   new-data-deserialized)
                 (catch KeeperException$BadVersionException e
                   e))]
     (if (instance? KeeperException$BadVersionException ret)
       (let [{:keys [data stat]} (zk/data client path)]
         (warn "RETRY ALTER DATA - TODO: optimize away for common case, by caching previous version, also: prevent double (de)serialization")
         (recur client path fun data (:version stat)))
       ret))))

(defn alter-data
  [cluster fun]
  (let [cluster-val @cluster
        new-data (zk-alter-data
                  (:client cluster-val)
                  (:cluster-path cluster-val)
                  (fn data-alterer [bytes]
                    (let [new-data (fun (deserialize-data bytes))]
                      [(serialize-data new-data) new-data])))]
    (swap! cluster assoc :data new-data)
    new-data))

(defn- member-set
  [& keys]
;; TODO: (into (hash-set) keys) vs. (apply hash-set keys)
  (apply sorted-set-by (fn member-set-comp [x y] (compare (util/extract-id x) (util/extract-id y))) keys))

(defn- member-watcher
  [cluster {:keys [event-type path] :as all-data}]
  (let [cluster-val @cluster
        old-member-set (:member-set cluster-val)
        new-member-set (apply member-set (zk/children (:client cluster-val) (:cluster-path cluster-val) :watcher (partial member-watcher cluster)))]

    ;; update cluster atom first
    (swap! cluster assoc :member-set new-member-set)

    ;; callback for every member that left
    (let [member-left-listeners (:member-left-listeners @cluster)]
      (when-not (empty? member-left-listeners)
        (let [gone-members (set/difference old-member-set new-member-set)]
          (doseq [gone-member gone-members]
            (doseq [member-left-listener member-left-listeners]
              (member-left-listener cluster gone-member))))))

    ;; callback for every new member
    (let [member-joined-listeners (:member-joined-listeners @cluster)]
      (when-not (empty? member-joined-listeners)
        (let [new-members (set/difference new-member-set old-member-set)]
          (doseq [new-member new-members]
            (doseq [member-joined-listener member-joined-listeners]
              (member-joined-listener cluster new-member))))))))

(defn- cluster-data-watcher
  [cluster {:keys [event-type path] :as all-data}]
  (let [cluster-val @cluster
        cluster-changed-listeners (:cluster-changed-listeners cluster-val)
        new-data-raw (:data (zk/data (:client cluster-val) (:cluster-path cluster-val) :watcher (partial cluster-data-watcher cluster)))
        new-data (deserialize-data new-data-raw)
        old-data (:data cluster-val)]

    ;; update cluster atom first
    (swap! cluster assoc :data new-data)

    (listener/invoke cluster-changed-listeners cluster old-data new-data)))

(defn add-cluster-changed-listener
  [cluster callback]
  (swap! cluster #(update-in % [:cluster-changed-listeners] listener/add callback)))
(defn remove-cluster-changed-listener
  [cluster callback]
  (swap! cluster #(update-in % [:cluster-changed-listeners] listener/remove callback)))

(defn add-member-joined-listener
  [cluster callback]
  (swap! cluster #(update-in % [:member-joined-listeners] listener/add callback)))
(defn remove-member-joined-listener
  [cluster callback]
  (swap! cluster #(update-in % [:member-joined-listeners] listener/remove callback)))

(defn add-member-left-listener
  [cluster callback]
  (swap! cluster #(update-in % [:member-left-listeners] listener/add callback)))
(defn remove-member-left-listener
  [cluster callback]
  (swap! cluster #(update-in % [:member-left-listeners] listener/remove callback)))

(defn join-cluster
  "Joins a member to a zookeeper cluster and returns a cluster object
   to be provided to functions such as leave-cluster. This cluster
   object is an atom containing a map that you may add entries to,
   preferably using namespace-qualified keys so as to avoid colliding
   with the cluster metadata stored therein.

   Any or all of the listeners provided can be nil. Non-nil listener
   functions will be called on ZooKeeper's 'event thread' (see
   http://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html),
   i.e.  they won't be called concurrently.

   Parameters the listener functions get called with:
   (cluster-changed-listener cluster old-data new-data)
   (member-left-listener cluster name-of-member)
   (member-joined-listener cluster name-of-member)

   Both member-left-listener and member-joined-listener will be called
   for both other members and the member that invoked join-cluster
   itself"
  
  ([client cluster-name my-member-name] (join-cluster client cluster-name my-member-name nil nil nil))
  ([client cluster-name my-member-name cluster-changed-listener member-joined-listener member-left-listener]
     (let [cluster-path (make-path cluster-name)
           cluster (atom {:client client
                                     :cluster-path cluster-path
                                     :my-member-name my-member-name
                                     :cluster-changed-listeners (if cluster-changed-listener [cluster-changed-listener] [])
                                     :member-joined-listeners (if member-joined-listener [member-joined-listener] [])
                                     :member-left-listeners (if member-left-listener [member-left-listener] [])
                                     :member-set (hash-set)})]

       ;; ensure cluster path exists
       (zk/create client cluster-path :persistent? true)

       ;; create ephemeral node path
       ;; TODO: what if the node already existed? zk/create has a bug that
       ;; prevents us from knowing: http://github.com/liebke/zookeeper-clj/pull/9
       (swap! cluster assoc :my-member-name (get-last-path-element (zk/create client (make-path cluster-name (str my-member-name "-")) :sequential? true)))

       ;; watch cluster members and notify joined/left listeners
       (member-watcher cluster nil)

       ;; watch cluster data and notify listeners
       (cluster-data-watcher cluster nil)

       cluster)))

(defn leave-cluster
  [cluster]
  (doseq [member-left-listener (:member-left-listeners @cluster)]
    (member-left-listener cluster (:my-member-name @cluster))))
