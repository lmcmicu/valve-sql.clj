(ns valve-sql.core
  (:require [next.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [valve-sql.log :as log])
  (:import [valve_sql Sqlite])
  (:gen-class))

(def db
  {:dbtype "sqlite"
   :dbname "example.db"})

(def conn
  (-> db (jdbc/get-datasource) (jdbc/get-connection)))

(defn parse-condition
  "TODO: Add a docstring here"
  [{:keys [conditions/table conditions/column conditions/condition]}]
  ;; To be implemented ...
  (log/debug "Parsing condition:" condition "for table.column:" (str table "." column))
  {:table table
   :column column
   :pre-parsed condition})

(defn -main
  "TODO: Add a docstring here."
  [& args]

  ;; TODO:
  ;; 1. Read in the `conditions` table and iterate through the conditions
  ;; 2. Write a `parse` function (see valve.clj) that will parse the condition using an instaparse
  ;;    grammar and then send back the parsed condition.
  ;; 3. Generate the SQL statements needed to validate the parsed condition.
  ;; 4. Run the SQL statements.

  (let [rows (jdbc/execute! conn ["select * from conditions"])]
    (->> rows (map parse-condition) (pprint))))
