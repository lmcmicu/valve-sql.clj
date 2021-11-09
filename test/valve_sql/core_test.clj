(ns valve-sql.core-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [valve-sql.core :refer [sqlify-condition]]))

(deftest test-in
  (testing "meals.second_course in(available_meats.name)"
    (is (= (sqlify-condition #:conditions{:table "meals"
                                          :column "second_course"
                                          :condition "in(available_meats.name)"})
           [(str "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT second_course IN (SELECT name FROM available_meats)")
            "in(available_meats.name)"]))))

(deftest test-not-in
  (testing "meals.second_course not(in(recalled_meats.name))"
    (is (= (sqlify-condition #:conditions{:table "meals"
                                          :column "second_course"
                                          :condition "not(in(recalled_meats.name))"})
           [(str "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE (second_course IN (SELECT name FROM recalled_meats))")
            "not(in(recalled_meats.name))"]))))

(deftest test-any
  (testing "meals.soup any(in(cold_soups.name), in(sweet_soups.name))"
    (is (= (sqlify-condition #:conditions{:table "meals"
                                          :column "soup"
                                          :condition "any(in(cold_soups.name), in(sweet_soups.name))"})
           [(-> (str "SELECT * FROM ("
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE NOT soup IN (SELECT name FROM cold_soups) "
                     "  INTERSECT "
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE NOT soup IN (SELECT name FROM sweet_soups)"
                     ")")
                (string/replace #"([\s\(])\s+" "$1")
                (string/replace #"\s+\)" ")"))
            "any(in(cold_soups.name), in(sweet_soups.name))"
            "any(in(cold_soups.name), in(sweet_soups.name))"]))))

(deftest test-not-any
  (testing "meals.soup not(any(in(cold_soups.name), in(sweet_soups.name)))"
    (is (= (sqlify-condition #:conditions{:table "meals"
                                          :column "soup"
                                          :condition "not(any(in(cold_soups.name), in(sweet_soups.name)))"})
           [(-> (str "SELECT * FROM ("
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE (soup IN (SELECT name FROM cold_soups)) "
                     "  UNION "
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE (soup IN (SELECT name FROM sweet_soups))"
                     ")")
                (string/replace #"([\s\(])\s+" "$1")
                (string/replace #"\s+\)" ")"))
            "not(any(in(cold_soups.name), in(sweet_soups.name)))"
            "not(any(in(cold_soups.name), in(sweet_soups.name)))"]))))

(deftest test-all
  (testing "meals.soup all(in(soups.name), in(available_soups.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "soup",
                                          :condition
                                          "all(in(soups.name), in(available_soups.name))"})
           [(-> (str "SELECT * FROM ("
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE NOT soup IN (SELECT name FROM soups) "
                     "  UNION "
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE NOT soup IN (SELECT name FROM available_soups)"
                     ")")
                (string/replace #"([\s\(])\s+" "$1")
                (string/replace #"\s+\)" ")"))
            "all(in(soups.name), in(available_soups.name))"
            "all(in(soups.name), in(available_soups.name))"]))))

(deftest test-not-all
  (testing "meals.dessert not(all(in(bad_tasting_desserts.name), in(unhealthy_desserts.name)))"
    (is (= (sqlify-condition #:conditions{:table "meals"
                                          :column "dessert"
                                          :condition
                                          "not(all(in(bad_tasting_desserts.name), in(unhealthy_desserts.name)))"})
           [(-> (str "SELECT * FROM ("
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE (dessert IN (SELECT name FROM bad_tasting_desserts)) "
                     "  INTERSECT "
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE (dessert IN (SELECT name FROM unhealthy_desserts))"
                     ")")
                (string/replace #"([\s\(])\s+" "$1")
                (string/replace #"\s+\)" ")"))
            "not(all(in(bad_tasting_desserts.name), in(unhealthy_desserts.name)))"
            "not(all(in(bad_tasting_desserts.name), in(unhealthy_desserts.name)))"]))))

(deftest test-alls-nested-in-any
  (testing "meals.second_course any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "second_course",
                                          :condition
                                          "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"}))
        [(-> (str "SELECT * FROM ("
                  "  SELECT * FROM ("
                  "    SELECT *, ? AS failed_condition "
                  "    FROM meals "
                  "    WHERE NOT second_course IN (SELECT name FROM meats) "
                  "    UNION "
                  "    SELECT *, ? AS failed_condition "
                  "    FROM meals "
                  "    WHERE NOT second_course IN (SELECT name FROM available_meats)"
                  "  )"
                  "  INTERSECT "
                  "  SELECT * FROM ("
                  "    SELECT *, ? AS failed_condition "
                  "    FROM meals "
                  "    WHERE NOT second_course IN (SELECT name FROM veggie_mains) "
                  "    UNION "
                  "    SELECT *, ? AS failed_condition "
                  "    FROM meals "
                  "    WHERE NOT second_course IN (SELECT name FROM available_veggie_mains)"
                  "  )"
                  ")")
             (string/replace #"([\s\(])\s+" "$1")
             (string/replace #"\s+\)" ")"))
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"])))

(deftest test-any-and-in-nested-in-all
  (testing "meals.wine all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "wine",
                                          :condition
                                          "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"})
           [(-> (str "SELECT * FROM ("
                     "  SELECT * FROM ("
                     "    SELECT *, ? AS failed_condition "
                     "    FROM meals "
                     "    WHERE NOT wine IN (SELECT name FROM white_wines) "
                     "    INTERSECT "
                     "    SELECT *, ? AS failed_condition "
                     "    FROM meals "
                     "    WHERE NOT wine IN (SELECT name FROM red_wines)"
                     "  ) "
                     "  UNION "
                     "  SELECT *, ? AS failed_condition "
                     "  FROM meals "
                     "  WHERE NOT wine IN (SELECT name FROM available_wines)"
                     ")")
                (string/replace #"([\s\(])\s+" "$1")
                (string/replace #"\s+\)" ")"))
            "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"
            "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"
            "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"]))))
