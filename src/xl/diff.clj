(ns xl.diff
  (:require [clojure.core.async :as async]))

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

(defn diff [chan1 chan2 comp-item output-diff-only?]
  (let [ch (async/chan)
        process
        (fn []
          (let [new-item
                (fn [item ch chan-done]
                  (if (or (some? item) chan-done) item (async/<!! ch)))]

            (loop [item1 nil item2 nil chan1-done false chan2-done false]
              (let [new-item1 (new-item item1 chan1 chan1-done)
                    new-item2 (new-item item2 chan2 chan2-done)]
                (cond
                  (and (nil? new-item1) (nil? new-item2)) (async/close! ch)
                  (nil? new-item1) (do (async/>!! ch [:right new-item2]) (recur nil nil true false))
                  (nil? new-item2) (do (async/>!! ch [:left new-item1]) (recur nil nil false true))
                  :else (let [check (comp-item new-item1 new-item2)]
                          (cond
                            (zero? check) (do (if-not output-diff-only? (async/>!! ch [:both new-item1]))
                                            (recur nil nil false false))
                            (pos? check) (do (async/>!! ch [:right new-item2])
                                           (recur new-item1 nil false false))
                            :else (do (async/>!! ch [:left new-item1])
                                    (recur nil new-item2 false false)))))))))]
    (async/thread (process))
    ch))
