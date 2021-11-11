(ns valve-sql.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [honey.sql :as sql]
            [honey.sql.helpers :refer [select select-distinct from where union union-all intersect
                                       with with-recursive]]
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
                 {:table table
                  :column column
                  :pre-parsed condition
                  :condition (first %)})))))))

(defn gen-sql-in
  "TODO: Add a docstring here"
  [table column args negate?]
  (log/debug "Generating SQL to validate function: 'in' with args:" args "against table.column:"
             (str table "." column))
  (letfn [(field-condition [{child-table :table
                             child-column :column}]
            (let [base-condition [:in (keyword column) (-> (select (keyword child-column))
                                                           (from (keyword child-table)))]]
              (if negate?
                base-condition
                [:not base-condition])))]
    (->> args
         (map field-condition)
         (#(if negate?
             (apply conj [:and] %)
             (apply conj [:or] %))))))

(defn gen-sql
  "TODO: Add a docstring here"
  [{table :table
    column :column
    pre-parsed :pre-parsed
    {cond-type :type
     cond-name :name
     cond-args :args
     negate? :negate?} :condition}]
  (log/debug "Generating SQL for cond-name:" cond-name "of type:" cond-type "with args:" cond-args
             "against table.column:" (str table "." column))
  (cond
    (= cond-type "function")
    (cond
      (= cond-name "in")
      (-> (select :* [pre-parsed :failed-condition])
          (from (keyword table))
          (where (gen-sql-in table column cond-args negate?)))

      (= cond-name "all")
      (let [inner-sql (->> cond-args
                           (map #(gen-sql {:table table :column column :pre-parsed pre-parsed
                                           :condition (assoc % :negate? negate?)}))
                           (remove nil?)
                           (#(if negate?
                               (apply intersect %)
                               (apply union %))))]
        {:select [:*]
         :from inner-sql})

      (= cond-name "any")
      (let [inner-sql (->> cond-args
                           (map #(gen-sql {:table table :column column :pre-parsed pre-parsed
                                           :condition (assoc % :negate? negate?)}))
                           (remove nil?)
                           (#(if negate?
                               (apply union %)
                               (apply intersect %))))]
        {:select [:*]
         :from inner-sql})

      ;; not is a unary operator so we can just take the first argument from the list of args
      ;; and associate a negation to it:
      (= cond-name "not")
      (let [operand (first cond-args)]
        (gen-sql {:table table :column column :pre-parsed pre-parsed
                  :condition (-> operand (assoc :negate? true))}))

      (= cond-name "list")
      (if-not (-> cond-args (first) :type (= "string"))
        (log/error "Invalid delimiter argument:" (first cond-args) "to list()")
        (let [delim-arg (-> cond-args (first) :value (string/replace #"^\"|\"$" ""))
              list-cond (second cond-args)
              table-k (keyword table)
              column-k (keyword column)
              split-table-k (-> table (str "_split") (keyword))
              split-column-k (keyword (str column "||'" delim-arg "'"))
              inner-sql (gen-sql {:table split-table-k :column column-k :pre-parsed pre-parsed
                                  :condition (assoc list-cond :negate? negate?)})]
          (-> inner-sql
              (with [[split-table-k {:columns [column-k]}]
                     (-> (with-recursive [[split-table-k {:columns [column-k :str]}]
                                          (union-all
                                           (-> (select "" split-column-k)
                                               (from table-k))
                                           (-> (select [[:substr :str 0 [[:instr :str delim-arg]]]]
                                                       [[:substr :str
                                                         [:+ [[:instr :str delim-arg]] 1]]])
                                               (from split-table-k)
                                               (where [:<> :str ""])))])
                         (select-distinct column-k)
                         (from split-table-k)
                         (where [:<> column-k ""]))]))))

      :else
      (log/error "Function:" cond-name "not yet supported by gen-sql."))

    :else
    (log/error "Condition type:" cond-type "not yet supported by gen-sql.")))

(defn sqlify-condition
  "TODO: Add a docstring here"
  [condition]
  (-> condition
      (parse)
      (gen-sql)
      (sql/format)))

(defn validate-condition
  "TODO: Add a docstring here"
  [condition]
  (-> condition
      (sqlify-condition)
      (#(do
          (log/debug "Executing SQL:" %)
          (jdbc/execute! conn %)))))

(defn -main
  "TODO: Add a docstring here."
  [& args]
  (let [rows (jdbc/execute! conn ["select * from conditions"])]
    (->> rows
         (map validate-condition))))
