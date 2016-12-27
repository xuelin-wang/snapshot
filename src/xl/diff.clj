(ns xl.diff)

(def default-dbl-tolerate 0.0000001)

(defn comp-dbl [tolerate val1 val2]
    (let [diff (- val1 val2)]
      (cond
          (<= (Math/abs diff) tolerate) (int 0)
          (< diff 0.0) (int -1)
          :else (int 1))))

(defn comp-nilable [comp-nonnilable]
  (fn [val1 val2]
      (cond
          (nil? val1) (if (nil? val2) (int 0) (int -1))
          (nil? val2) (int 1)
          :else (comp-nonnilable val1 val2))))

(def default-comp
  (fn [val1 val2]
;    (println (str "type:" (type val1) ", val1:" val1))
;    (println (str "type:" (type val2) ", val2:" val2))
    (.compareTo val1 val2)))

(def default-nilable-comp
  (comp-nilable default-comp))

(defn diff-list [list1 list2 comp-item]
    (let [sorted-l1 (sort comp-item list1)
          sorted-l2 (sort comp-item list2)]

        (loop [result [] l1 sorted-l1 l2 sorted-l2]
            (cond
                (empty? l1) (concat result (map (fn [item] [:right item]) l2))
                (empty? l2) (concat result (map (fn [item] [:left item]) l1))
                :else
                  (let [
                        first1 (first l1)
                        first2 (first l2)
                        check-first (comp-item first1 first2)]
                    (cond
                      (zero? check-first) (recur (conj result [:both first1]) (rest l1) (rest l2))
                      (pos? check-first) (recur (conj result [:right first2]) l1 (rest l2))
                      :else (recur (conj result [:left first1]) (rest l1) l2)))))))
