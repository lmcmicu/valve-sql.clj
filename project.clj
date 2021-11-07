(defproject valve-sql.clj "0.1.0-SNAPSHOT"
  :description "VALVE is A Lightweight Validation Engine"
  :url "https://github.com/lmcmicu/valve-sql.clj"
  :license {:name "BSD 3-Clause License"
            :url "https://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.github.seancorfield/honeysql "2.1.818"]
                 [com.github.seancorfield/next.jdbc "1.2.737"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]
                 [instaparse "1.4.10"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :main ^:skip-aot valve-sql.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
