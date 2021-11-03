(ns valve-sql.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [instaparse.core :as insta]
            [next.jdbc :as jdbc]
            [valve-sql.log :as log])
  (:import [valve_sql Sqlite])
  (:gen-class))

(def db
  {:dbtype "sqlite"
   :dbname "example.db"})

(def conn
  (-> db (jdbc/get-datasource) (jdbc/get-connection)))

(defn- postprocess
  "Given the result of parsing a given condition using VALVE's grammar, generate a map containing
  the condition's type and its value, where the latter can in general be composed of further
  conditions which are postprocessed recursively."
  [result]
  (let [node-type (first result)
        node-values (drop 1 result)
        first-value (first node-values)]
    (cond
      (some #(= node-type %) '(:start :expression :label :function-name
                                      :argument :regex :regex-pattern))
      (postprocess first-value)

      (= node-type :string)
      {:type "string"
       :value (postprocess first-value)}

      (= node-type :ALPHANUM)
      (->> result (drop 1) (string/join ""))

      ;; Whitespace:
      (= node-type :_)
      (when (> (count result) 1)
        {:type "space"
         :value (->> result (drop 1) (string/join ""))})

      ;; Double-quoted string:
      (= node-type :dqstring)
      (->> result (drop 1) (string/join ""))

      (= node-type :function)
      {:type "function"
       :name (postprocess first-value)
       :args (postprocess (nth node-values 1))}

      (= node-type :arguments)
      (->> node-values (map postprocess) (vec))

      (= node-type :field)
      {:type "field"
       :table (postprocess first-value)
       :column (postprocess (nth node-values 1))}

      (= node-type :named-arg)
      {:type "named-arg"
       :key (postprocess first-value)
       :value (postprocess (nth node-values 1))}

      (= node-type :regex-sub)
      {:type "regex"
       :pattern (postprocess first-value)
       :replace (postprocess (nth node-values 1))
       :flags (postprocess (nth node-values 2))}

      (= node-type :regex-match)
      {:type "regex"
       :pattern (postprocess first-value)
       :flags (postprocess (nth node-values 1))}

      (= node-type :regex-unescaped)
      (->> result (drop 1) (string/join ""))

      (= node-type :regex-flag)
      (->> result (drop 1) (vec))

      :else
      first-value)))

(defn parse
  "TODO: Add a docstring here"
  [{:keys [conditions/table conditions/column conditions/condition]}]
  (log/debug "Parsing condition:" condition "for table.column:" (str table "." column))
  (let [condition-parser (-> "valve_grammar.ebnf"
                             (io/resource)
                             (insta/parser))]
    (let [result (-> condition (condition-parser))]
      (if (insta/failure? result)
        (log/error "Parsing failed due to reason:" (insta/get-failure result))
        ;; Drop the :start keyword and iterate over the remaining nodes in the tree:
        (->> (drop 1 result)
             (map postprocess)
             (remove nil?)
             (vec)
             (remove #(= (:type %) "space"))
             ;; After removing whitespace, the result list should contain only one entry,
             ;; corresponding to the `expression` to be parsed (see resources/valve_grammar.ebnf):
             (#(if (> (count %) 1)
                 (throw (Exception. (str "Unable to parse condition: '" condition
                                         "'. Got result vector with more than one entry: " result)))
                 (first %))))))))

(defn -main
  "TODO: Add a docstring here."
  [& args]

  ;; TODO:
  ;; 1. Read in the `conditions` table and iterate through the conditions
  ;;    - DONE
  ;;
  ;; 2. Write a `parse` function (see valve.clj) that will parse the condition using an instaparse
  ;;    grammar and then send back the parsed condition.
  ;;    - DONE (simply re-used grammar from valve.clj)
  ;;
  ;; 3. Generate the SQL statements needed to validate the parsed condition.
  ;;
  ;; 4. Run the SQL statements.
  ;;

  (let [rows (jdbc/execute! conn ["select * from conditions"])]
    (->> rows (map parse) (pprint))))
