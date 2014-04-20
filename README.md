tierlieb
========

A clojure library that makes writing (consistent-hashing based) clusters using zoo-keeper easy.

In general, what do you need in order to run a clustered server?

* maintain a list of members
* determine what member is resposponsible for doing what

*tierlieb* uses *Apache ZooKeeper* to maintain such a member list and makes that list available to its members. Members can join and leave (orderly or because of a problem) clusters at any time.

In addition to managing simple membership in a cluster, *tierlieb* can also create and manage mappings of keys to members to be used for consistent hashing. It's still your job to make use of those keys, e.g. by distributing the mapping to clients of your cluster.

# Usage #
---------

To the list of `:dependencies` in your `project.clj` file, please add

```
[eugendueck/tierlieb "0.2"]
```

Use it like so:

```clojure
(require 'zookeeper 'tierlieb 'tierlieb.consistent-hash)

;; configuration

(def zookeeper-address "127.0.0.1:2181")
(def cluster-name "xmycluster")
(def my-host "192.168.40.50") ;; my own ip address
(def my-port 12345)
(def member-name (str my-host \: my-port))
(def number-of-consistent-hash-keys 20) ; more keys relative to other members means bigger piece of the hash pie

;; connect to zookeeper
(def zk-client (zookeeper/connect zookeeper-address))

;; setup cluster
(def tierlieb-cluster (tierlieb/join-cluster zk-client cluster-name member-name
   (fn cluster-changed-callback [cluster old-data new-data] (prn "** cluster changed"))
   (fn member-joined-callback [cluster member-name] (prn "** joined" member-name))
   (fn member-left-callback [cluster member-name] (prn "** left" member-name))))

;; setup consistent hashing
(tierlieb.consistent-hash/setup-consistent-hashing tierlieb-cluster number-of-consistent-hash-keys
 (fn hashcode-map-changed-callback [& params]
  (prn "hashcode-map" (tierlieb.consistent-hash/get-hashcode-map tierlieb-cluster))))
```

Now it is your job to use the hashcode-map, that you can retrieve via `get-hashcode-map`, see the last line in the snippet above. You could e.g. make the map known to clients of your cluster.

The hashcode map is a simple clojure map that contains hashcode to cluster member entries. If you chose "3" instead of "20" like we did in the sample, running the sample with a single member you might end up with the following:

```clojure
{1782634599 "192.168.40.50:12345-0000000000",
 1958226340 "192.168.40.50:12345-0000000000",
 1959512508 "192.168.40.50:12345-0000000000"}
```

The part of the member information after the hyphen is the member index as assigned by apache zookeeper. It is not necessary for clients connecting to the cluster to know this, but members of the cluster may use it e.g. to determine who is eldest and assigning a special role to that member.

The above hashcode-map btw is a good example why "3" is probably too low, because the randomly chosen hash keys might occasionally end up being really close, like they do in this case, resulting in an uneven distribution (once you add more members).
