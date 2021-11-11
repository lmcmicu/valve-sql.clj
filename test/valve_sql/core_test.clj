(ns valve-sql.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [valve-sql.core :refer [sqlify-condition]]))

(defn- remove-ws
  [string]
  (-> string
      (string/replace #"([\s\(])\s+" "$1")
      (string/replace #"\s+\)" ")")))

(deftest test-in
  (let [condition "in(lookup_t.lookup_c)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(str "SELECT *, ? AS failed_condition "
                   "FROM target_t "
                   "WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t)")
              "in(lookup_t.lookup_c)"])))))

(deftest test-not-in
  (let [condition "not(in(lookup_t.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(str "SELECT *, ? AS failed_condition "
                   "FROM target_t "
                   "WHERE (target_c IN (SELECT lookup_c FROM lookup_t))")
              "not(in(lookup_t.lookup_c))"])))))

(deftest test-in-2
  (let [condition "in(lookup_t1.lookup_c, lookup_t2.lookup_c)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT *, ? AS failed_condition "
                       "FROM target_t "
                       "WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t1) "
                       "   OR NOT target_c IN (SELECT lookup_c FROM lookup_t2)")
                  (remove-ws))
              "in(lookup_t1.lookup_c, lookup_t2.lookup_c)"])))))

(deftest test-not-in-2
  (let [condition "not(in(lookup_t1.lookup_c, lookup_t2.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT *, ? AS failed_condition "
                       "FROM target_t "
                       "WHERE (target_c IN (SELECT lookup_c FROM lookup_t1)) "
                       "  AND (target_c IN (SELECT lookup_c FROM lookup_t2))")
                  (remove-ws))
              "not(in(lookup_t1.lookup_c, lookup_t2.lookup_c))"])))))

(deftest test-any
  (let [condition "any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                       "  INTERSECT "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_2)"
                       ")")
                  (remove-ws))
              "any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"
              "any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"])))))

(deftest test-not-any
  (let [condition "not(any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "  UNION "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              "not(any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"
              "not(any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"])))))

(deftest test-any-2
  (let [condition "any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition}))
          [(-> (str "SELECT * FROM ("
                    "  SELECT *, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_2) "
                    "  INTERSECT "
                    "  SELECT *, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_3) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_4))")
               (remove-ws))
           "any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"
           "any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"]))))

(deftest test-not-any-2
  (let [condition "not(any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_2)) "
                       "  UNION "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_3)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_4)))")
                  (remove-ws))
              "not(any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"
              "not(any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"])))))

(deftest test-all
  (let [condition "all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t",
                                            :column "target_c",
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                       "  UNION "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_2)"
                       ")")
                  (remove-ws))
              "all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"
              "all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"])))))

(deftest test-not-all
  (let [condition "not(all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "  INTERSECT "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              "not(all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"
              "not(all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"])))))

(deftest test-all-2
  (let [condition "all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition}))
          [(-> (str "SELECT * FROM ("
                    "  SELECT *, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_2) "
                    "  UNION "
                    "  SELECT *, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_3) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_4))")
               (remove-ws))
           "all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"
           "all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"]))))

(deftest test-not-all-2
  (let [condition "not(all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_2)) "
                       "  INTERSECT "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_3)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_4)))")
                  (remove-ws))
              "not(all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"
              "not(all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"])))))

(deftest test-all-3
  (let [condition "all(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  UNION "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_2) "
                       "  UNION "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_3)"
                       ")")
                  (remove-ws))
              "all(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"
              "all(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"
              "all(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"])))))

(deftest test-any-3
  (let [condition "any(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  INTERSECT "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_2) "
                       "  INTERSECT "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_3)"
                       ")")
                  (remove-ws))
              "any(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"
              "any(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"
              "any(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"])))))

(deftest test-all-mixed
  (let [condition "all(in(lookup_t_1.c), not(in(lookup_t_2.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  UNION "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              "all(in(lookup_t_1.c), not(in(lookup_t_2.c)))"
              "all(in(lookup_t_1.c), not(in(lookup_t_2.c)))"])))))

(deftest test-any-mixed
  (let [condition "any(in(lookup_t_1.c), not(in(lookup_t_2.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  INTERSECT "
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              "any(in(lookup_t_1.c), not(in(lookup_t_2.c)))"
              "any(in(lookup_t_1.c), not(in(lookup_t_2.c)))"])))))

(deftest test-any-all
  (let [condition "any(all(in(lookup_t1.c), in(lookup_t2.c)), all(in(lookup_t3.c), in(lookup_t4.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT * FROM ("
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t1) "
                       "    UNION "
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t2)"
                       "  )"
                       "  INTERSECT "
                       "  SELECT * FROM ("
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t3) "
                       "    UNION "
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t4)"
                       "  )"
                       ")")
                  (remove-ws))
              "any(all(in(lookup_t1.c), in(lookup_t2.c)), all(in(lookup_t3.c), in(lookup_t4.c)))"
              "any(all(in(lookup_t1.c), in(lookup_t2.c)), all(in(lookup_t3.c), in(lookup_t4.c)))"
              "any(all(in(lookup_t1.c), in(lookup_t2.c)), all(in(lookup_t3.c), in(lookup_t4.c)))"
              "any(all(in(lookup_t1.c), in(lookup_t2.c)), all(in(lookup_t3.c), in(lookup_t4.c)))"])))))

(deftest test-all-any
  (let [condition "all(any(in(lookup_t1.c), in(lookup_t2.c)), any(in(lookup_t3.c), in(lookup_t4.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT * FROM ("
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t1) "
                       "    INTERSECT "
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t2)"
                       "  )"
                       "  UNION "
                       "  SELECT * FROM ("
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t3) "
                       "    INTERSECT "
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t4)"
                       "  )"
                       ")")
                  (remove-ws))
              "all(any(in(lookup_t1.c), in(lookup_t2.c)), any(in(lookup_t3.c), in(lookup_t4.c)))"
              "all(any(in(lookup_t1.c), in(lookup_t2.c)), any(in(lookup_t3.c), in(lookup_t4.c)))"
              "all(any(in(lookup_t1.c), in(lookup_t2.c)), any(in(lookup_t3.c), in(lookup_t4.c)))"
              "all(any(in(lookup_t1.c), in(lookup_t2.c)), any(in(lookup_t3.c), in(lookup_t4.c)))"])))))

(deftest test-mixed-nested
  (let [condition "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT *, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t1) "
                       "     OR NOT target_c IN (SELECT c FROM lookup_t2) "
                       "  UNION "
                       "  SELECT * FROM ("
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE (target_c IN (SELECT c FROM lookup_t3)) "
                       "    INTERSECT "
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE (target_c IN (SELECT c FROM lookup_t4))"
                       "  )"
                       "  UNION "
                       "  SELECT * FROM ("
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t5) "
                       "    INTERSECT "
                       "    SELECT *, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE (target_c IN (SELECT c FROM lookup_t6))"
                       "  )"
                       ")")
                  (remove-ws))
              "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"
              "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"
              "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"
              "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"
              "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"])))))

(deftest test-list
  (let [condition "list(\"@\", in(lookup_t.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (target_c) AS ("
                       "  WITH RECURSIVE target_t_split (target_c, str) AS ("
                       "    SELECT ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT DISTINCT target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT *, ? AS failed_condition "
                       "FROM target_t_split "
                       "WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t)")
                  (remove-ws))
              ""
              0
              "@"
              "@"
              1
              ""
              ""
              "list(\"@\", in(lookup_t.lookup_c))"])))))

(deftest test-not-list
  (let [condition "not(list(\"@\", in(lookup_t.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (target_c) AS ("
                       "  WITH RECURSIVE target_t_split (target_c, str) AS ("
                       "    SELECT ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT DISTINCT target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT *, ? AS failed_condition "
                       "FROM target_t_split "
                       "WHERE (target_c IN (SELECT lookup_c FROM lookup_t))")
                  (remove-ws))
              ""
              0
              "@"
              "@"
              1
              ""
              ""
              "not(list(\"@\", in(lookup_t.lookup_c)))"])))))

(deftest test-list-not
  (let [condition "list(\"@\", not(in(lookup_t.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (target_c) AS ("
                       "  WITH RECURSIVE target_t_split (target_c, str) AS ("
                       "    SELECT ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT DISTINCT target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT *, ? AS failed_condition "
                       "FROM target_t_split "
                       "WHERE (target_c IN (SELECT lookup_c FROM lookup_t))")
                  (remove-ws))
              ""
              0
              "@"
              "@"
              1
              ""
              ""
              "list(\"@\", not(in(lookup_t.lookup_c)))"])))))
