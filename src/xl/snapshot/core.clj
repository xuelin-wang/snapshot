(ns xl.snapshot.core
  (:require
      [clojure.java.jdbc :as j]
      [xl.diff :as diff]
      [xl.io]
      [xl.snapshot.db :as db])
  (:gen-class))

(defn create-test-t3 [db start batch-size batch-count]
  (let [cols ["c3_0_i" "c3_1_f" "c3_2_f"]]
    (dotimes [n batch-count]
      (let [
            curr-start (+ start (* batch-size n))
            vals (map (fn [mm] [mm (inc mm) (+ mm 2)]) (range curr-start (+ curr-start batch-size)))]
        (j/insert-multi! db "t3" cols vals)))))

(defn update-test-t3 [db] (j/execute! db ["update t3 set c3_1_f = c3_1_f + 1.1 where c3_0_i % 10 = 1"]))

(defn snapshot-test-t3 [db]
  (db/load-table-snapshot db "t3" ["c3_0_i" "c3_1_f"] "c3_0_i > ?" 1000000))


(defn -main
  "entry point for snapshot"
  [& args]
  (let [conn (db/db-conn)
        metadatas (db/get-tables-metadata ["t3"])
        metadata (j/result-set-seq (first metadatas))
        first-col (first metadata)
        ;        rows (j/query conn ["select * from t3 where c3_0_i > ?" 1000000] {:as-arrays? true})
        out-file1 "/Users/xuelin/t3.txt"
        out-file2 "/Users/xuelin/t3a.txt"]
    ;        rows2 [{:c3_0_i (int 3), :c3_1_f (float 3.15), :c3_2_f nil}
    ;               {:c3_0_i (int 4), :c3_1_f (float 4.5567), :c3_2_f (float 1800.0)})
    ;        diff (db/diff-rows metadata rows rows2 diff/default-dbl-tolerate)]

    (println metadata)
    (println (:column_name first-col))
    (println (:table_name first-col))
    (println (:type_name first-col))
    (println (:is_nullable first-col))
    (spit out-file1 (snapshot-test-t3 conn))
    (update-test-t3 conn)
    (spit out-file2 (snapshot-test-t3 conn))
    (let [rows1 (read-string (slurp out-file1))
          rows2 (read-string (slurp out-file2))
          diff (db/diff-rows metadata (rest rows1) (rest rows2) [:c3_0_i :c3_1_f] true diff/default-dbl-tolerate)]
      (println diff))))

;      (println diff)
      ;(create-test-t3 conn 1000 1000 1000)
