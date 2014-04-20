(ns tierlieb.util)

(defn memoize-last
  [f]
  (let [last (atom {})]
    (fn memoizer [& params]
      (if (nil? params)
        @last
        (let [last-val @last]
          (if (and (contains? last-val :last-args) (= (:last-args last-val) params))
            (:last-ret last-val)
            (:last-ret (swap! last assoc :last-args params :last-ret (apply f params)))))))))
