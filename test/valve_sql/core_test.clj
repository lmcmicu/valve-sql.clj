(ns valve-sql.core-test
  (:require [clojure.test :refer :all]
            [clojure.string :refer [trim]]
            [valve-sql.core :refer [sqlify-condition]]))

(deftest test-sqlify-condition-1
  (testing "Validating SQL for condition: meals.soup all(in(soups.name), in(available_soups.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "soup",
                                          :condition
                                          "all(in(soups.name), in(available_soups.name))"})
           [(str "SELECT * FROM ("
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT soup IN (SELECT name FROM soups) "
                 "UNION "
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT soup IN (SELECT name FROM available_soups)"
                 ")")
            "all(in(soups.name), in(available_soups.name))"
            "all(in(soups.name), in(available_soups.name))"]))))

(deftest test-sqlify-condition-2
  (testing "Validating SQL for condition: meals.first_course all(in(pastas.name), in(available_pastas.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "first_course",
                                          :condition
                                          "all(in(pastas.name), in(available_pastas.name))"})
           [(str "SELECT * FROM ("
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT first_course IN (SELECT name FROM pastas) "
                 "UNION "
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT first_course IN (SELECT name FROM available_pastas)"
                 ")")
            "all(in(pastas.name), in(available_pastas.name))"
            "all(in(pastas.name), in(available_pastas.name))"]))))

(deftest test-sqlify-condition-3
  (testing "Validating SQL for condition: meals.second_course any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "second_course",
                                          :condition
                                          "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"}))
        [(str "SELECT * FROM ("
              "SELECT * FROM ("
              "SELECT *, ? AS failed_condition "
              "FROM meals "
              "WHERE NOT second_course IN (SELECT name FROM meats) "
              "UNION "
              "SELECT *, ? AS failed_condition "
              "FROM meals "
              "WHERE NOT second_course IN (SELECT name FROM available_meats)"
              ")"
              "INTERSECT "
              "SELECT * FROM ("
              "SELECT *, ? AS failed_condition "
              "FROM meals "
              "WHERE NOT second_course IN (SELECT name FROM veggie_mains) "
              "UNION "
              "SELECT *, ? AS failed_condition "
              "FROM meals "
              "WHERE NOT second_course IN (SELECT name FROM available_veggie_mains)"
              ")"
              ")")
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"
         "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))"])))

(deftest test-sqlify-condition-4
  (testing "Validating SQL for condition: meals.bread all(in(breads.name), in(available_breads.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "bread",
                                          :condition
                                          "all(in(breads.name), in(available_breads.name))"}))
        [(str "SELECT * FROM ("
              "SELECT *, ? AS failed_condition "
              "FROM meals "
              "WHERE NOT bread IN (SELECT name FROM breads) "
              "UNION "
              "SELECT *, ? AS failed_condition "
              "FROM meals "
              "WHERE NOT bread IN (SELECT name FROM available_breads)"
              ")")
         "all(in(breads.name), in(available_breads.name))"
         "all(in(breads.name), in(available_breads.name))"])))

(deftest test-sqlify-condition-5
  (testing "Validating SQL for condition: meals.side all(in(vegetables.name), in(available_vegetables.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "side",
                                          :condition
                                          "all(in(vegetables.name), in(available_vegetables.name))"})
           [(str "SELECT * FROM ("
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT side IN (SELECT name FROM vegetables) "
                 "UNION "
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT side IN (SELECT name FROM available_vegetables)"
                 ")")
            "all(in(vegetables.name), in(available_vegetables.name))"
            "all(in(vegetables.name), in(available_vegetables.name))"]))))

(deftest test-sqlify-condition-6
  (testing "Validating SQL for condition: meals.dessert all(in(desserts.name), in(available_desserts.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "dessert",
                                          :condition
                                          "all(in(desserts.name), in(available_desserts.name))"})
           [(str "SELECT * FROM ("
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT dessert IN (SELECT name FROM desserts) "
                 "UNION "
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT dessert IN (SELECT name FROM available_desserts)"
                 ")")
            "all(in(desserts.name), in(available_desserts.name))"
            "all(in(desserts.name), in(available_desserts.name))"]))))

(deftest test-sqlify-condition-7
  (testing "Validating SQL for condition: meals.wine all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"
    (is (= (sqlify-condition #:conditions{:table "meals",
                                          :column "wine",
                                          :condition
                                          "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"})
           [(str "SELECT * FROM ("
                 "SELECT * FROM ("
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT wine IN (SELECT name FROM white_wines) "
                 "INTERSECT "
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT wine IN (SELECT name FROM red_wines)"
                 ") "
                 "UNION "
                 "SELECT *, ? AS failed_condition "
                 "FROM meals "
                 "WHERE NOT wine IN (SELECT name FROM available_wines)"
                 ")")
            "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"
            "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"
            "all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))"]))))
