(ns tierlieb.consistent-hash
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException))
  (:use [clojure.tools.logging])
  (:require [tierlieb :as cluster]
            [tierlieb.listener :as listener]
            [tierlieb.util :as tierlieb-util]
            [zookeeper :as zk]
            [zookeeper.util :as util]
            [zookeeper.data :as data]))

(def calculate-effective-hashcode-map
  (tierlieb-util/memoize-last
   (fn calculate-effective-hashcode-map-inner
     [hashcode-map member-set]
     (info "calculate-effective-hashcode-map-inner" hashcode-map member-set)
     (apply sorted-map (flatten (filter #(member-set (second %)) hashcode-map))))))

(defn get-hashcode-map
  [cluster]
  (or (:last-ret (calculate-effective-hashcode-map)) {}))

(defn load-balance
  "Returns the name of the cluster node that is responsible to handle the given
   hashcode. Returns nil if no cluster node is alive (should only happen after
   we left the cluster, otherwise there's at least us who's alive in the cluster."
  [cluster hashcode]
  (let [effective-hashcode-map (get-hashcode-map cluster)]
    (info "load-balance" @cluster effective-hashcode-map)
    (second (first (or (subseq effective-hashcode-map >= hashcode) effective-hashcode-map)))))

(defn- rand-int-where
  ([f] (rand-int-where f Integer/MAX_VALUE))
  ([f n]
     (first (filter f (repeatedly #(rand-int n))))))

(defn- add-consistent-hash-points
  "Adds a total number of 'weight' random integers, all mapping to
   member-name, to smap.  These random numbers will be made not to clash
   with existing keys in smap."
  [smap member-name weight]
  (if (zero? weight)
    smap
    (recur (assoc (or smap (sorted-map)) (rand-int-where #(not (contains? smap %))) member-name) member-name (dec weight))))

(defn- recalc-effective-hashcode-map-and-callback
  [cluster]
  (let [cluster-val @cluster
        hashcode-map-listeners (:tierlieb.consistent-hash/hashcode-map-listeners cluster-val)
        old-effective-hashcode-map (:last-ret (calculate-effective-hashcode-map))
        new-effective-hashcode-map (calculate-effective-hashcode-map (get-in cluster-val [:data :tierlieb.consistent-hash/hashcode-map]) (:member-set cluster-val))]
    (when (not= old-effective-hashcode-map new-effective-hashcode-map)
      (listener/invoke hashcode-map-listeners old-effective-hashcode-map new-effective-hashcode-map))))

(defn add-hashcode-map-listener
  [cluster callback]
  (swap! cluster #(update-in % [:tierlieb.consistent-hash/hashcode-map-listeners] listener/add callback)))

(defn remove-hashcode-map-listener
  [cluster callback]
  (swap! cluster #(update-in % [:tierlieb.consistent-hash/hashcode-map-listeners] listener/remove callback)))

(defn setup-consistent-hashing
  "Configures a cluster to be used in a consistent hash setting.

   weight must contain the number of hashcodes in the map for this new
   member. Note that both relative and absolute values of the weights
   matter, as the points are chosen randomly and if you choose, say, a
   1:1 ratio, both hashcodes may lie close to each other, resulting in
   one member getting almost all the load and the other one getting
   almost none.  Parameters the callback functions get called with:

   The callbacks may be nil. If set, it will be called on ZooKeeper's
   'event thread' (see
   http://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html),
   i.e.  it won't be called multiple times concurrently.

   (hashcode-map-listener old-hashcode-map new-hashcode-map)"
  [cluster weight hashcode-map-listener]
  (let [my-member-name (:my-member-name @cluster)]
    
    ;; add hashcode-map-listener
    (swap! cluster assoc :tierlieb.consistent-hash/hashcode-map-listeners (if hashcode-map-listener [hashcode-map-listener] []))

    ;; add ourselves as cluster-changed listener
    (cluster/add-cluster-changed-listener cluster (fn hashcode-map-calculator [cluster old-data new-data]
                                                  (info "cluster-changed-listener" new-data)
                                                  (recalc-effective-hashcode-map-and-callback cluster)))

    ;; add hashcode points for ourself to cluster data
    (cluster/alter-data cluster (fn add-hashcode-points [current-data]
                                  (update-in current-data [:tierlieb.consistent-hash/hashcode-map] add-consistent-hash-points my-member-name weight)))

    (cluster/add-member-joined-listener cluster (fn member-joined-listener [cluster new-member]
                                                  ;; (info "member-joined-listener" new-member)
                                                  (recalc-effective-hashcode-map-and-callback cluster)))

    (cluster/add-member-left-listener cluster (fn member-left-listener [cluster old-member]
                                                  ;; (info "member-left-listener" old-member)
                                                  (recalc-effective-hashcode-map-and-callback cluster)))

    cluster))
