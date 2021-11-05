(ns valve-sql.core-test
  (:require [clojure.test :refer :all]
            [valve-sql.core :refer [validate-conditions]]))

(deftest test-validate-conditions
  (testing "Validate conditions"
    (let [result (validate-conditions)]
      (is (= result
             (list [;; Validation results for:
                    ;; 'meals', 'soup', 'all(in(soups.name), in(available_soups.name))'),
                    {:failed_condition "all(in(soups.name), in(available_soups.name))",
                     :meals/first_course "farfalle",
                     :meals/wine "chianti",
                     :meals/side "vegetarian tacos",
                     :meals/dessert "chocolate cake",
                     :meals/soup "cream of spinach",
                     :meals/wedding_couple "Harry and Victor",
                     :meals/bread "pumpernickel",
                     :meals/second_course "filet mignon"}]
                   ;; Validation results for:
                   ;; 'meals', 'first_course', 'all(in(pastas.name), in(available_pastas.name))'
                   []
                   ;; Validation results for:
                   ;; 'meals', 'second_course', 'any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))'
                   [{:failed_condition
                     "any(all(in(meats.name), in(available_meats.name)), all(in(veggie_mains.name), in(available_veggie_mains.name)))",
                     :meals/first_course "spaghetti",
                     :meals/wine "merlot",
                     :meals/side "spinach",
                     :meals/dessert "sundae",
                     :meals/soup "leek and potato",
                     :meals/wedding_couple "Mork and Mindy",
                     :meals/bread "panini",
                     :meals/second_course "Ork burgers"}]
                   ;; Validation results for:
                   ;; ('meals', 'bread', 'all(in(breads.name), in(available_breads.name))'
                   []
                   ;; Validation results for:
                   ;; ('meals', 'side', 'all(in(vegetables.name), in(available_vegetables.name))'
                   [{:failed_condition
                     "all(in(vegetables.name), in(available_vegetables.name))",
                     :meals/first_course "farfalle",
                     :meals/wine "chianti",
                     :meals/side "vegetarian tacos",
                     :meals/dessert "chocolate cake",
                     :meals/soup "cream of spinach",
                     :meals/wedding_couple "Harry and Victor",
                     :meals/bread "pumpernickel",
                     :meals/second_course "filet mignon"}]
                   ;; Validation results for:
                   ;; 'meals', 'dessert', 'all(in(desserts.name), in(available_desserts.name))'
                   [{:failed_condition "all(in(desserts.name), in(available_desserts.name))",
                     :meals/first_course "rigatoni",
                     :meals/wine "chianti",
                     :meals/side "spinach",
                     :meals/dessert "cheesecake",
                     :meals/soup "tomato",
                     :meals/wedding_couple "Sam and Diane",
                     :meals/bread "baguette",
                     :meals/second_course "vegan chili"}]
                   ;; Validation results for:
                   ;; 'meals', 'wine', 'all(any(in(white_wines.name), in(red_wines.name)), in(available_wines.name))'
                   []))))))
