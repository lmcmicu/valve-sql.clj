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
             [(str "SELECT rowid, target_c, ? AS failed_condition "
                   "FROM target_t "
                   "WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t)")
              condition])))))

(deftest test-not-in
  (let [condition "not(in(lookup_t.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(str "SELECT rowid, target_c, ? AS failed_condition "
                   "FROM target_t "
                   "WHERE (target_c IN (SELECT lookup_c FROM lookup_t))")
              condition])))))

(deftest test-in-2
  (let [condition "in(lookup_t1.lookup_c, lookup_t2.lookup_c)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT rowid, target_c, ? AS failed_condition "
                       "FROM target_t "
                       "WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t1) "
                       "   OR NOT target_c IN (SELECT lookup_c FROM lookup_t2)")
                  (remove-ws))
              condition])))))

(deftest test-not-in-2
  (let [condition "not(in(lookup_t1.lookup_c, lookup_t2.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT rowid, target_c, ? AS failed_condition "
                       "FROM target_t "
                       "WHERE (target_c IN (SELECT lookup_c FROM lookup_t1)) "
                       "  AND (target_c IN (SELECT lookup_c FROM lookup_t2))")
                  (remove-ws))
              condition])))))

(deftest test-any
  (let [condition "any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                       "  INTERSECT "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_2)"
                       ")")
                  (remove-ws))
              condition
              condition])))))

(deftest test-not-any
  (let [condition "not(any(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "  UNION "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              condition
              condition])))))

(deftest test-any-2
  (let [condition "any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition}))
          [(-> (str "SELECT * FROM ("
                    "  SELECT rowid, target_c, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_2) "
                    "  INTERSECT "
                    "  SELECT rowid, target_c, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_3) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_4))")
               (remove-ws))
           condition
           condition]))))

(deftest test-not-any-2
  (let [condition "not(any(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_2)) "
                       "  UNION "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_3)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_4)))")
                  (remove-ws))
              condition
              condition])))))

(deftest test-all
  (let [condition "all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t",
                                            :column "target_c",
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                       "  UNION "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_2)"
                       ")")
                  (remove-ws))
              condition
              condition])))))

(deftest test-not-all
  (let [condition "not(all(in(lookup_t_1.lookup_c), in(lookup_t_2.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "  INTERSECT "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              condition
              condition])))))

(deftest test-all-2
  (let [condition "all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition}))
          [(-> (str "SELECT * FROM ("
                    "  SELECT rowid, target_c, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_1) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_2) "
                    "  UNION "
                    "  SELECT rowid, target_c, ? AS failed_condition "
                    "  FROM target_t "
                    "  WHERE NOT target_c IN (SELECT lookup_c FROM lookup_t_3) "
                    "     OR NOT target_c IN (SELECT lookup_c FROM lookup_t_4))")
               (remove-ws))
           condition
           condition]))))

(deftest test-not-all-2
  (let [condition "not(all(in(lookup_t_1.lookup_c, lookup_t_2.lookup_c), in(lookup_t_3.lookup_c, lookup_t_4.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_1)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_2)) "
                       "  INTERSECT "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT lookup_c FROM lookup_t_3)) "
                       "    AND (target_c IN (SELECT lookup_c FROM lookup_t_4)))")
                  (remove-ws))
              condition
              condition])))))

(deftest test-all-3
  (let [condition "all(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  UNION "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_2) "
                       "  UNION "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_3)"
                       ")")
                  (remove-ws))
              condition
              condition
              condition])))))

(deftest test-any-3
  (let [condition "any(in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  INTERSECT "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_2) "
                       "  INTERSECT "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_3)"
                       ")")
                  (remove-ws))
              condition
              condition
              condition])))))

(deftest test-all-mixed
  (let [condition "all(in(lookup_t_1.c), not(in(lookup_t_2.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  UNION "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              condition
              condition])))))

(deftest test-any-mixed
  (let [condition "any(in(lookup_t_1.c), not(in(lookup_t_2.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t_1) "
                       "  INTERSECT "
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE (target_c IN (SELECT c FROM lookup_t_2))"
                       ")")
                  (remove-ws))
              condition
              condition])))))

(deftest test-any-all
  (let [condition "any(all(in(lookup_t1.c), in(lookup_t2.c)), all(in(lookup_t3.c), in(lookup_t4.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT * FROM ("
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t1) "
                       "    UNION "
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t2)"
                       "  )"
                       "  INTERSECT "
                       "  SELECT * FROM ("
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t3) "
                       "    UNION "
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t4)"
                       "  )"
                       ")")
                  (remove-ws))
              condition
              condition
              condition
              condition])))))

(deftest test-all-any
  (let [condition "all(any(in(lookup_t1.c), in(lookup_t2.c)), any(in(lookup_t3.c), in(lookup_t4.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT * FROM ("
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t1) "
                       "    INTERSECT "
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t2)"
                       "  )"
                       "  UNION "
                       "  SELECT * FROM ("
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t3) "
                       "    INTERSECT "
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t4)"
                       "  )"
                       ")")
                  (remove-ws))
              condition
              condition
              condition
              condition])))))

(deftest test-all-any-not-mixed-nested
  (let [condition "all(in(lookup_t1.c, lookup_t2.c), not(all(in(lookup_t3.c), in(lookup_t4.c))), any(in(lookup_t5.c), not(in(lookup_t6.c))))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "SELECT * FROM ("
                       "  SELECT rowid, target_c, ? AS failed_condition "
                       "  FROM target_t "
                       "  WHERE NOT target_c IN (SELECT c FROM lookup_t1) "
                       "     OR NOT target_c IN (SELECT c FROM lookup_t2) "
                       "  UNION "
                       "  SELECT * FROM ("
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE (target_c IN (SELECT c FROM lookup_t3)) "
                       "    INTERSECT "
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE (target_c IN (SELECT c FROM lookup_t4))"
                       "  )"
                       "  UNION "
                       "  SELECT * FROM ("
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE NOT target_c IN (SELECT c FROM lookup_t5) "
                       "    INTERSECT "
                       "    SELECT rowid, target_c, ? AS failed_condition "
                       "    FROM target_t "
                       "    WHERE (target_c IN (SELECT c FROM lookup_t6))"
                       "  )"
                       ")")
                  (remove-ws))
              condition
              condition
              condition
              condition
              condition])))))

(deftest test-list
  (let [condition "list(\"@\", in(lookup_t.lookup_c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, target_c, str) AS ("
                       "    SELECT rowid, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT rowid, target_c, ? AS failed_condition "
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
              condition])))))

(deftest test-not-list
  (let [condition "not(list(\"@\", in(lookup_t.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, reference, id, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, reference, id, target_c, str) AS ("
                       "    SELECT rowid, target_c, ?, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT rowid, reference, ? AS failed_condition "
                       "FROM target_t_split "
                       "GROUP BY rowid, reference "
                       "HAVING COUNT(?) = SUM(CASE WHEN (target_c IN (SELECT lookup_c FROM lookup_t)) THEN ? ELSE ? END)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              condition
              1
              1
              0])))))

(deftest test-list-not
  (let [condition "list(\"@\", not(in(lookup_t.lookup_c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, target_c, str) AS ("
                       "    SELECT rowid, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT rowid, target_c, ? AS failed_condition "
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
              condition])))))

(deftest test-not-list-not
  (let [condition "not(list(\"@\", not(in(lookup_t.lookup_c))))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, reference, id, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, reference, id, target_c, str) AS ("
                       "    SELECT rowid, target_c, ?, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       ") "
                       "SELECT rowid, reference, ? AS failed_condition "
                       "FROM target_t_split "
                       "GROUP BY rowid, reference "
                       "HAVING COUNT(?) = SUM(CASE WHEN NOT target_c IN (SELECT lookup_c FROM lookup_t) THEN ? ELSE ? END)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              condition
              1
              1
              0])))))

(deftest test-split
  (let [condition "split(\"@\", 3, in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, reference, id, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, reference, id, target_c, str) AS ("
                       "    SELECT rowid, target_c, ?, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM target_t_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_3) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       ") "
                       "SELECT rowid, reference, ? AS failed_condition "
                       "FROM results "
                       "WHERE (? = count_invalid) "
                       "  OR (? = col1_invalid) "
                       "  OR (? = col2_invalid) "
                       "  OR (? = col3_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              3
              1
              2
              3
              condition
              1
              1
              1
              1])))))

(deftest test-split-not
  (let [condition "split(\"@\", 3, in(lookup_t_1.c), not(in(lookup_t_2.c)), in(lookup_t_3.c))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, reference, id, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, reference, id, target_c, str) AS ("
                       "    SELECT rowid, target_c, ?, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM target_t_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, (target_c IN (SELECT c FROM lookup_t_2)) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_3) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       ") "
                       "SELECT rowid, reference, ? AS failed_condition "
                       "FROM results "
                       "WHERE (? = count_invalid) "
                       "  OR (? = col1_invalid) "
                       "  OR (? = col2_invalid) "
                       "  OR (? = col3_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              3
              1
              2
              3
              condition
              1
              1
              1
              1])))))

(deftest test-not-split
  (let [condition "not(split(\"@\", 3, in(lookup_t_1.c), in(lookup_t_2.c), in(lookup_t_3.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, reference, id, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, reference, id, target_c, str) AS ("
                       "    SELECT rowid, target_c, ?, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM target_t_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_3) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       ") "
                       "SELECT rowid, reference, ? AS failed_condition "
                       "FROM results "
                       "WHERE (? = count_invalid) "
                       "  AND (? = col1_invalid) "
                       "  AND (? = col2_invalid) "
                       "  AND (? = col3_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              3
              1
              2
              3
              condition
              0
              0
              0
              0])))))

(deftest test-not-split-not
  (let [condition "not(split(\"@\", 3, in(lookup_t_1.c), not(in(lookup_t_2.c)), in(lookup_t_3.c)))"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH target_t_split (rowid, reference, id, target_c) AS ("
                       "  WITH RECURSIVE target_t_split (rowid, reference, id, target_c, str) AS ("
                       "    SELECT rowid, target_c, ?, ?, target_c||'@' "
                       "    FROM target_t "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM target_t_split WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, target_c "
                       "  FROM target_t_split "
                       "  WHERE target_c <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM target_t_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, (target_c IN (SELECT c FROM lookup_t_2)) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT target_c IN (SELECT c FROM lookup_t_3) AS invalid "
                       "  FROM target_t_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       ") "
                       "SELECT rowid, reference, ? AS failed_condition "
                       "FROM results "
                       "WHERE (? = count_invalid) "
                       "  AND (? = col1_invalid) "
                       "  AND (? = col2_invalid) "
                       "  AND (? = col3_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              3
              1
              2
              3
              condition
              0
              0
              0
              0])))))

(deftest test-concat
  (let [condition "concat(\"Prefer \", in(lookup_t_1.c), \" over \", in(lookup_t_1.c), \" and \", in(lookup_t_2.c), \" over \", in(lookup_t_2.c))"
        regexp "Prefer (\\w+) over (\\w+) and (\\w+) over (\\w+)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH captures_split (rowid, reference, id, capture) AS ("
                       "  WITH RECURSIVE captures_split (rowid, reference, id, capture, str) AS ("
                       "    SELECT rowid, capture, ?, ?, capture||'@' "
                       "    FROM captures "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM captures_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, capture "
                       "  FROM captures_split "
                       "  WHERE capture <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM captures_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM captures_split WHERE id = ?"
                       "), col4_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid, col4_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid, "
                       "    col4_invalid.invalid AS col4_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col4_invalid ON "
                       "    (col4_invalid.rowid = count_invalid.rowid) AND (col4_invalid.reference = count_invalid.reference) "
                       "), captures (rowid, target_c, capture) AS ("
                       "  SELECT rowid, target_c, REGEXP_MATCHES(target_c, ?) "
                       "  FROM target_t"
                       ") "
                       "SELECT captures.rowid, target_c, ? AS failed_condition "
                       "FROM captures "
                       "LEFT JOIN results ON "
                       "  (captures.rowid = results.rowid) AND (captures.capture = results.reference) "
                       "WHERE (captures.capture = ?) "
                       "   OR (? = col1_invalid) "
                       "   OR (? = col2_invalid) "
                       "   OR (? = col3_invalid) "
                       "   OR (? = col4_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              4
              1
              2
              3
              4
              regexp
              condition
              ""
              1
              1
              1
              1])))))

(deftest test-concat-not
  (let [condition "concat(\"Prefer \", in(lookup_t_1.c), \" over \", in(lookup_t_1.c), \" and \", not(in(lookup_t_2.c)), \" over \", in(lookup_t_2.c))"
        regexp "Prefer (\\w+) over (\\w+) and (\\w+) over (\\w+)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH captures_split (rowid, reference, id, capture) AS ("
                       "  WITH RECURSIVE captures_split (rowid, reference, id, capture, str) AS ("
                       "    SELECT rowid, capture, ?, ?, capture||'@' "
                       "    FROM captures "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM captures_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, capture "
                       "  FROM captures_split "
                       "  WHERE capture <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM captures_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, (capture IN (SELECT c FROM lookup_t_2)) AS invalid "
                       "  FROM captures_split WHERE id = ?"
                       "), col4_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid, col4_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid, "
                       "    col4_invalid.invalid AS col4_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col4_invalid ON "
                       "    (col4_invalid.rowid = count_invalid.rowid) AND (col4_invalid.reference = count_invalid.reference) "
                       "), captures (rowid, target_c, capture) AS ("
                       "  SELECT rowid, target_c, REGEXP_MATCHES(target_c, ?) "
                       "  FROM target_t"
                       ") "
                       "SELECT captures.rowid, target_c, ? AS failed_condition "
                       "FROM captures "
                       "LEFT JOIN results ON "
                       "  (captures.rowid = results.rowid) AND (captures.capture = results.reference) "
                       "WHERE (captures.capture = ?) "
                       "   OR (? = col1_invalid) "
                       "   OR (? = col2_invalid) "
                       "   OR (? = col3_invalid) "
                       "   OR (? = col4_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              4
              1
              2
              3
              4
              regexp
              condition
              ""
              1
              1
              1
              1])))))

(deftest test-not-concat
  (let [condition "not(concat(\"Prefer \", in(lookup_t_1.c), \" over \", in(lookup_t_1.c), \" and \", in(lookup_t_2.c), \" over \", in(lookup_t_2.c)))"
        regexp "Prefer (\\w+) over (\\w+) and (\\w+) over (\\w+)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH captures_split (rowid, reference, id, capture) AS ("
                       "  WITH RECURSIVE captures_split (rowid, reference, id, capture, str) AS ("
                       "    SELECT rowid, capture, ?, ?, capture||'@' "
                       "    FROM captures "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM captures_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, capture "
                       "  FROM captures_split "
                       "  WHERE capture <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM captures_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM captures_split WHERE id = ?"
                       "), col4_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid, col4_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid, "
                       "    col4_invalid.invalid AS col4_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col4_invalid ON "
                       "    (col4_invalid.rowid = count_invalid.rowid) AND (col4_invalid.reference = count_invalid.reference) "
                       "), captures (rowid, target_c, capture) AS ("
                       "  SELECT rowid, target_c, REGEXP_MATCHES(target_c, ?) "
                       "  FROM target_t"
                       ") "
                       "SELECT captures.rowid, target_c, ? AS failed_condition "
                       "FROM captures "
                       "LEFT JOIN results ON "
                       "  (captures.rowid = results.rowid) AND (captures.capture = results.reference) "
                       "WHERE (captures.capture <> ?) "
                       "  AND (? = col1_invalid) "
                       "  AND (? = col2_invalid) "
                       "  AND (? = col3_invalid) "
                       "  AND (? = col4_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              4
              1
              2
              3
              4
              regexp
              condition
              ""
              0
              0
              0
              0])))))

(deftest test-not-concat-not
  (let [condition "not(concat(\"Prefer \", in(lookup_t_1.c), \" over \", in(lookup_t_1.c), \" and \", not(in(lookup_t_2.c)), \" over \", in(lookup_t_2.c)))"
        regexp "Prefer (\\w+) over (\\w+) and (\\w+) over (\\w+)"]
    (testing (str "target_t.target_c " condition)
      (is (= (sqlify-condition #:conditions{:table "target_t"
                                            :column "target_c"
                                            :condition condition})
             [(-> (str "WITH captures_split (rowid, reference, id, capture) AS ("
                       "  WITH RECURSIVE captures_split (rowid, reference, id, capture, str) AS ("
                       "    SELECT rowid, capture, ?, ?, capture||'@' "
                       "    FROM captures "
                       "    UNION ALL "
                       "    SELECT rowid, reference, id + ?, SUBSTR(str, ?, (INSTR(str, ?))), SUBSTR(str, (INSTR(str, ?)) + ?) "
                       "    FROM captures_split "
                       "    WHERE str <> ?"
                       "  ) "
                       "  SELECT rowid, reference, id, capture "
                       "  FROM captures_split "
                       "  WHERE capture <> ?"
                       "), count_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, COUNT(?) <> ? AS invalid "
                       "  FROM captures_split "
                       "  GROUP BY rowid, reference"
                       "), col1_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col2_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_1) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), col3_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, (capture IN (SELECT c FROM lookup_t_2)) AS invalid "
                       "  FROM captures_split WHERE id = ?"
                       "), col4_invalid (rowid, reference, invalid) AS ("
                       "  SELECT rowid, reference, NOT capture IN (SELECT c FROM lookup_t_2) AS invalid "
                       "  FROM captures_split "
                       "  WHERE id = ?"
                       "), results (rowid, reference, count_invalid, col1_invalid, col2_invalid, col3_invalid, col4_invalid) AS ("
                       "  SELECT "
                       "    count_invalid.rowid AS rowid, "
                       "    count_invalid.reference AS reference, "
                       "    count_invalid.invalid AS count_invalid, "
                       "    col1_invalid.invalid AS col1_invalid, "
                       "    col2_invalid.invalid AS col2_invalid, "
                       "    col3_invalid.invalid AS col3_invalid, "
                       "    col4_invalid.invalid AS col4_invalid "
                       "  FROM count_invalid "
                       "  LEFT JOIN col1_invalid ON "
                       "    (col1_invalid.rowid = count_invalid.rowid) AND (col1_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col2_invalid ON "
                       "    (col2_invalid.rowid = count_invalid.rowid) AND (col2_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col3_invalid ON "
                       "    (col3_invalid.rowid = count_invalid.rowid) AND (col3_invalid.reference = count_invalid.reference) "
                       "  LEFT JOIN col4_invalid ON "
                       "    (col4_invalid.rowid = count_invalid.rowid) AND (col4_invalid.reference = count_invalid.reference) "
                       "), captures (rowid, target_c, capture) AS ("
                       "  SELECT rowid, target_c, REGEXP_MATCHES(target_c, ?) "
                       "  FROM target_t"
                       ") "
                       "SELECT captures.rowid, target_c, ? AS failed_condition "
                       "FROM captures "
                       "LEFT JOIN results ON "
                       "  (captures.rowid = results.rowid) AND (captures.capture = results.reference) "
                       "WHERE (captures.capture <> ?) "
                       "  AND (? = col1_invalid) "
                       "  AND (? = col2_invalid) "
                       "  AND (? = col3_invalid) "
                       "  AND (? = col4_invalid)")
                  (remove-ws))
              0
              ""
              1
              0
              "@"
              "@"
              1
              ""
              ""
              1
              4
              1
              2
              3
              4
              regexp
              condition
              ""
              0
              0
              0
              0])))))
