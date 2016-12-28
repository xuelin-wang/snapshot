(ns xl.snapshot.db
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource)
           (java.io Writer))
  (:require
      [clojure.java.jdbc :as j]
      [clojure.string :as str]
      [clojure.java.io :as io]
      [clojure.core.async :as async]
      [clojure.core.async.lab :as asynclab]
      [xl.diff :as diff]))

(def db-spec
  {
      :classname "org.mariadb.jdbc.Driver"
      :subprotocol "mariadb"
      :subname "//localhost:3306/test_core"
      :user "ts1"
      :password "pw_ts1"})

(defn pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 5 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 5 60))
               ;; expire connections after 20 minutes of inactivity:
               (.setMaxIdleTime (* 20 60)))]
    {:datasource cpds}))

(def pooled-db (delay (pool db-spec)))

(defn db-conn [] @pooled-db)

(defn get-tables-metadata [table-names]
    (let [conn (db-conn)
          metadata (.getMetaData (.getConnection (:datasource conn)))]
        (map #(.getColumns metadata nil nil % nil) table-names)))

(defn get-type-comp [type-name dbl-tolerate]
  (case type-name
    ("REAL" "FLOAT" "DOUBLE") (partial diff/comp-dbl dbl-tolerate)
    diff/default-comp))

(defn get-row-comp [table-metadata ordered-cols0 is-row-array? dbl-tolerate]
    (let [
          ordered-cols (if (nil? ordered-cols0)
                         (map #(keyword (:column_name %)) table-metadata)
                         ordered-cols0)
          get-col (if is-row-array?
                    (let [col-to-index (into {} (map-indexed (fn [idx col] [col idx]) ordered-cols))]
                      (fn [row col]
                        (nth row (col-to-index col))))
                    (fn [row col] (col row)))

          col-comp-fns
            (into {}
              (map
                (fn [colmeta]
                  [(keyword (:column_name colmeta))
                   (diff/comp-nilable (get-type-comp (:type-name colmeta) dbl-tolerate))])
                table-metadata))]
        (fn [row1 row2]
          (reduce
              (fn [result col]
                  (let [comp-fn (col col-comp-fns)
                        col1 (get-col row1 col)
                        col2 (get-col row2 col)
                        check-col (comp-fn col1 col2)]
                      (if (zero? check-col) check-col (reduced check-col))))
              (int 0)
              ordered-cols))))

(defn diff-chans [table-metadata ordered-cols is-row-array? dbl-tolerate chan1 chan2 output-diff-only?]
    (let [row-comp (get-row-comp table-metadata ordered-cols is-row-array? dbl-tolerate)]
      (diff/diff chan1 chan2 row-comp output-diff-only?)))

(defn diff-rows [table-metadata ordered-cols is-row-array? dbl-tolerate rows1 rows2 sorted? output-diff-only?]
    (let [row-comp (get-row-comp table-metadata ordered-cols is-row-array? dbl-tolerate)
          sorted-rows1 (if sorted? rows1 (sort row-comp rows1))
          sorted-rows2 (if sorted? rows2 (sort row-comp rows2))
          chan1 (asynclab/spool sorted-rows1)
          chan2 (asynclab/spool sorted-rows2)
          ch (diff-chans table-metadata ordered-cols is-row-array? dbl-tolerate chan1 chan2 output-diff-only?)
          result-ch (async/into [] ch)
          result (async/<!! result-ch)]
      (do (async/close! result-ch) result)))

(defn diff-files [table-name ordered-cols is-row-array? dbl-tolerate file-path1 file-path2 output-diff-only?]
  (let [table-metadatas (get-tables-metadata [table-name])
        table-metadata (j/result-set-seq (first table-metadatas))
        chan1 (xl.io/read-edn-chan file-path1)
        chan2 (xl.io/read-edn-chan file-path2)]
    (diff-chans table-metadata ordered-cols is-row-array? dbl-tolerate chan1 chan2 output-diff-only?)))

(defn load-table-snapshot [conn table-name colnames where-clauses & params]
  (let [cols-str (str/join "," colnames)
        sql (str "select " cols-str " from " table-name
                 (if (str/blank? where-clauses) "" (str " where "  where-clauses))
                 " order by " cols-str)
        query-params (into [] (concat [sql] params))]
    (j/query conn query-params {:as-arrays? true})))

(defn store-table-snapshot [file-path conn table-name colnames where-clauses & params]
  (let [cols-str (str/join "," colnames)
        sql (str "select " cols-str " from " table-name
                 (if (str/blank? where-clauses) "" (str " where "  where-clauses))
                 " order by " cols-str)
        query-params (into [] (concat [sql] params))]
    (with-open [^Writer writer (io/writer file-path)]
      (j/query conn query-params {:as-arrays? true :row-fn #(.write writer (pr-str %))}))))
