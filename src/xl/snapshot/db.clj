(ns xl.snapshot.db
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)

  (:require
      [clojure.java.jdbc :as j]
      [clojure.string :as str]
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

(defn diff-rows [table-metadata rows1 rows2 ordered-cols is-row-array? dbl-tolerate]
    (let [row-comp (get-row-comp table-metadata ordered-cols is-row-array? dbl-tolerate)]
      (diff/diff-list rows1 rows2 row-comp)))

(defn load-table-snapshot [conn table-name colnames where-clauses & params]
  (let [sql (str "select " (str/join "," colnames) " from " table-name (if (str/blank? where-clauses) "" (str " where "  where-clauses)))
        query-params (into [] (concat [sql] params))]
    (j/query conn query-params {:as-arrays? true})))
