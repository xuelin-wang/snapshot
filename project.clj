(defproject snapshot "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.clojure/clojure "1.9.0-alpha14"]
                 [com.mchange/c3p0 "0.9.5.2"]
                 [org.mariadb.jdbc/mariadb-java-client "1.5.5"]
                 [org.clojure/java.jdbc "0.7.0-alpha1"]
                 [org.clojure/test.check "0.9.0"]]

  :main ^:skip-aot xl.snapshot.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
