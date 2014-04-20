(ns tierlieb.listener)

(defn- remove-from
  "Removes item from the collection c (could  a vector, a set or a map)"
  [c item]
  (into (empty c) (remove #(= % item) c)))

(defn add
  [listener-v listener]
  (conj listener-v listener))

(defn remove
  [listener-v listener]
  (remove-from listener-v listener))

(defmacro invoke
  [listeners & params]
  `(doseq [listener# ~listeners]
     (listener# ~@params)))

(defn anyone-listening
  [listeners]
  (first listeners))
