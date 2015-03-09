(ns metabase.driver.generic-sql.query-processor-test
  (:require [expectations :refer :all]
            [metabase.test-data :refer [db-id table->id field->id]]
            [metabase.driver.generic-sql.query-processor :refer :all]))

;; ## "COUNT" AGGREGATION
(expect {:status :completed
         :row_count 1
         :data {:rows [[100]]
                :columns ["count"]
                :cols [{:base_type "IntegerField"
                        :special_type "number"
                        :name "count"
                        :id nil
                        :table_id nil
                        :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter [nil nil]
                                  :aggregation ["count"]
                                  :breakout [nil]
                                  :limit nil}}))

;; ## "SUM" AGGREGATION
(expect {:status :completed
         :row_count 1
         :data {:rows [[203]]
                :columns ["sum"]
                :cols [{:base_type "IntegerField"
                        :special_type nil
                        :name "sum"
                        :id nil
                        :table_id nil
                        :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter [nil nil]
                                  :aggregation ["sum" (field->id :venues :price)]
                                  :breakout [nil]
                                  :limit nil}}))

;; ## "DISTINCT COUNT" AGGREGATION
(expect {:status :completed
         :row_count 1
         :data {:rows [[15]]
                :columns ["count"]
                :cols [{:base_type "IntegerField"
                        :special_type "number"
                        :name "count"
                        :id nil
                        :table_id nil
                        :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :checkins)
                                  :filter [nil nil]
                                  :aggregation ["distinct" (field->id :checkins :user_id)]
                                  :breakout [nil]
                                  :limit nil}}))

;; ## "AVG" AGGREGATION
;; TODO - try this with an integer field. (Should the average of an integer field be a float or an int?)
(expect {:status :completed,
         :row_count 1,
         :data {:rows [[35.745891999999984]]
                :columns ["avg"]
                :cols [{:base_type "FloatField"
                        :special_type nil
                        :name "avg"
                        :id nil
                        :table_id nil
                        :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter [nil nil]
                                  :aggregation ["avg" (field->id :venues :latitude)]
                                  :breakout [nil]
                                  :limit nil}}))

;; ## "STDDEV" AGGREGATION
(expect {:status :completed
         :row_count 1
         :data {:rows [[2.2851266195132554]]
                :columns ["stddev"]
                :cols [{:base_type "FloatField"
                        :special_type nil
                        :name "stddev"
                        :id nil
                        :table_id nil
                        :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter [nil nil]
                                  :aggregation ["stddev" (field->id :venues :latitude)]
                                  :breakout [nil]
                                  :limit nil}}))

;; ## "ROWS" AGGREGATION
;; Test that a rows aggregation just returns rows as-is.
(expect {:status :completed,
         :row_count 10,
         :data
         {:rows [[1 4 3 -118.374 34.0646 "Red Medicine"]
                 [2 11 2 -118.329 34.0996 "Stout Burgers & Beers"]
                 [3 11 2 -118.428 34.0406 "The Apple Pan"]
                 [4 29 2 -118.465 33.9997 "Wurstküche"]
                 [5 20 2 -118.261 34.0778 "Brite Spot Family Restaurant"]
                 [6 20 2 -118.324 34.1054 "The 101 Coffee Shop"]
                 [7 44 2 -118.305 34.0689 "Don Day Korean Restaurant"]
                 [8 11 2 -118.342 34.1015 "25°"]
                 [9 71 1 -118.301 34.1018 "Krua Siri"]
                 [10 20 2 -118.292 34.1046 "Fred 62"]],
          :columns ["ID" "CATEGORY_ID" "PRICE" "LONGITUDE" "LATITUDE" "NAME"],
          :cols [{:special_type nil, :base_type "BigIntegerField", :description nil, :name "ID", :table_id (table->id :venues), :id (field->id :venues :id)}
                 {:special_type nil, :base_type "IntegerField", :description nil, :name "CATEGORY_ID", :table_id (table->id :venues), :id (field->id :venues :category_id)}
                 {:special_type nil, :base_type "IntegerField", :description nil, :name "PRICE", :table_id (table->id :venues), :id (field->id :venues :price)}
                 {:special_type nil, :base_type "FloatField", :description nil, :name "LONGITUDE", :table_id (table->id :venues), :id (field->id :venues :longitude)}
                 {:special_type nil, :base_type "FloatField", :description nil, :name "LATITUDE", :table_id (table->id :venues), :id (field->id :venues :latitude)}
                 {:special_type nil, :base_type "TextField", :description nil, :name "NAME", :table_id (table->id :venues), :id (field->id :venues :name)}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter nil
                                  :aggregation ["rows"]
                                  :breakout [nil]
                                  :limit 10
                                  :order_by [[(field->id :venues :id) "ascending"]]}}))

;; ## "PAGE" CLAUSE
;; Test that we can get "pages" of results.

;; Get the first page
(expect {:status :completed
         :row_count 5
         :data {:rows [[1 "African"]
                       [2 "American"]
                       [3 "Artisan"]
                       [4 "Asian"]
                       [5 "BBQ"]]
                :columns ["ID", "NAME"]
                :cols [{:special_type nil, :base_type "BigIntegerField", :description nil, :name "ID", :table_id (table->id :categories), :id (field->id :categories :id)}
                       {:special_type nil, :base_type "TextField", :description nil, :name "NAME", :table_id (table->id :categories), :id (field->id :categories :name)}]}}
  (process-and-run {:type :query
                    :database @db-id
                    :query {:source_table (table->id :categories)
                            :aggregation ["rows"]
                            :page {:items 5
                                   :page 1}
                            :order_by [[(field->id :categories :name) "ascending"]]}}))

;; Get the second page
(expect {:status :completed
         :row_count 5
         :data {:rows [[6 "Bakery"]
                       [7 "Bar"]
                       [8 "Beer Garden"]
                       [9 "Breakfast / Brunch"]
                       [10 "Brewery"]]
                :columns ["ID", "NAME"]
                :cols [{:special_type nil, :base_type "BigIntegerField", :description nil, :name "ID", :table_id (table->id :categories), :id (field->id :categories :id)}
                       {:special_type nil, :base_type "TextField", :description nil, :name "NAME", :table_id (table->id :categories), :id (field->id :categories :name)}]}}
  (process-and-run {:type :query
                    :database @db-id
                    :query {:source_table (table->id :categories)
                            :aggregation ["rows"]
                            :page {:items 5
                                   :page 2}
                            :order_by [[(field->id :categories :name) "ascending"]]}}))

;; ## "FIELDS" CLAUSE
;; Test that we can restrict the Fields that get returned to the ones specified, and that results come back in the order of the IDs in the `fields` clause
(expect {:status :completed,
         :row_count 10,
         :data {:rows [["Red Medicine" 1]
                       ["Stout Burgers & Beers" 2]
                       ["The Apple Pan" 3]
                       ["Wurstküche" 4]
                       ["Brite Spot Family Restaurant" 5]
                       ["The 101 Coffee Shop" 6]
                       ["Don Day Korean Restaurant" 7]
                       ["25°" 8]
                       ["Krua Siri" 9]
                       ["Fred 62" 10]],
                :columns ["NAME" "ID"],
                :cols [{:special_type nil, :base_type "TextField", :description nil, :name "NAME", :table_id (table->id :venues), :id (field->id :venues :name)}
                       {:special_type nil, :base_type "BigIntegerField", :description nil, :name "ID", :table_id (table->id :venues), :id (field->id :venues :id)}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter [nil nil]
                                  :aggregation ["rows"]
                                  :fields [(field->id :venues :name)
                                           (field->id :venues :id)]
                                  :breakout [nil]
                                  :limit 10
                                  :order_by [[(field->id :venues :id) "ascending"]]}}))

;; ## "ORDER_BY" CLAUSE
;; Test that we can tell the Query Processor to return results ordered by multiple fields
(expect {:status :completed,
         :row_count 10,
         :data {:rows [[1 12 375] [1 9 139] [1 1 72] [2 15 129] [2 12 471] [2 11 325] [2 9 590] [2 9 833] [2 8 380] [2 5 719]],
                :columns ["VENUE_ID" "USER_ID" "ID"],
                :cols [{:special_type nil, :base_type "IntegerField", :description nil, :name "VENUE_ID", :table_id (table->id :checkins), :id (field->id :checkins :venue_id)}
                       {:special_type nil, :base_type "IntegerField", :description nil, :name "USER_ID", :table_id (table->id :checkins), :id (field->id :checkins :user_id)}
                       {:special_type nil, :base_type "BigIntegerField", :description nil, :name "ID", :table_id (table->id :checkins), :id (field->id :checkins :id)}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :checkins)
                                  :aggregation ["rows"]
                                  :limit 10
                                  :fields [(field->id :checkins :venue_id)
                                           (field->id :checkins :user_id)
                                           (field->id :checkins :id)]
                                  :order_by [[(field->id :checkins :venue_id) "ascending"]
                                             [(field->id :checkins :user_id) "descending"]
                                             [(field->id :checkins :id) "ascending"]]}}))

;; ## "FILTER" CLAUSE
(expect {:status :completed,
         :row_count 5,
         :data
         {:rows [[55 67 4 -118.096 33.983 "Dal Rae Restaurant"]
                 [61 67 4 -118.376 34.0677 "Lawry's The Prime Rib"]
                 [77 40 4 -74.0045 40.7318 "Sushi Nakazawa"]
                 [79 40 4 -73.9736 40.7514 "Sushi Yasuda"]
                 [81 40 4 -73.9533 40.7677 "Tanoshi Sushi & Sake Bar"]],
          :columns ["ID" "CATEGORY_ID" "PRICE" "LONGITUDE" "LATITUDE" "NAME"],
          :cols [{:special_type nil, :base_type "BigIntegerField", :description nil, :name "ID", :table_id (table->id :venues), :id (field->id :venues :id)}
                 {:special_type nil, :base_type "IntegerField", :description nil, :name "CATEGORY_ID", :table_id (table->id :venues), :id (field->id :venues :category_id)}
                 {:special_type nil, :base_type "IntegerField", :description nil, :name "PRICE", :table_id (table->id :venues), :id (field->id :venues :price)}
                 {:special_type nil, :base_type "FloatField", :description nil, :name "LONGITUDE", :table_id (table->id :venues), :id (field->id :venues :longitude)}
                 {:special_type nil, :base_type "FloatField", :description nil, :name "LATITUDE", :table_id (table->id :venues), :id (field->id :venues :latitude)}
                 {:special_type nil, :base_type "TextField", :description nil, :name "NAME", :table_id (table->id :venues), :id (field->id :venues :name)}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :venues)
                                  :filter ["AND"
                                           [">" (field->id :venues :id) 50]
                                           [">=" (field->id :venues :price) 4]]
                                  :aggregation ["rows"]
                                  :breakout [nil]
                                  :limit nil}}))

;; ## "BREAKOUT"
(expect {:status :completed,
         :row_count 15,
         :data {:rows [[1 31] [2 70] [3 75] [4 77] [5 69] [6 70] [7 76] [8 81] [9 68] [10 78] [11 74] [12 59] [13 76] [14 62] [15 34]],
                :columns ["USER_ID" "count"],
                :cols [{:special_type nil, :base_type "IntegerField", :description nil, :name "USER_ID", :table_id (table->id :checkins) :id (field->id :checkins :user_id)}
                       {:base_type "IntegerField", :special_type "number", :name "count", :id nil, :table_id nil, :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :checkins)
                                  :filter [nil nil]
                                  :aggregation ["count"]
                                  :breakout [(field->id :checkins :user_id)]
                                  :order_by [[(field->id :checkins :user_id) "ascending"]]
                                  :limit nil}}))

;; ## "BREAKOUT" - MULTIPLE COLUMNS
(expect {:status :completed,
         :row_count 10,
         :data {:rows [[2 15 1] [3 15 1] [7 15 1] [14 15 1] [16 15 1] [18 15 1] [22 15 1] [23 15 2] [24 15 1] [27 15 1]],
                :columns ["VENUE_ID" "USER_ID" "count"],
                :cols [{:special_type nil, :base_type "IntegerField", :description nil, :name "VENUE_ID", :table_id (table->id :checkins), :id (field->id :checkins :venue_id)}
                       {:special_type nil, :base_type "IntegerField", :description nil, :name "USER_ID", :table_id (table->id :checkins), :id (field->id :checkins :user_id)}
                       {:base_type "IntegerField", :special_type "number", :name "count", :id nil, :table_id nil, :description nil}]}}
        (process-and-run {:type :query
                          :database @db-id
                          :query {:source_table (table->id :checkins)
                                  :limit 10
                                  :aggregation ["count"]
                                  :breakout [(field->id :checkins :user_id)
                                             (field->id :checkins :venue_id)]
                                  :order_by [[(field->id :checkins :user_id) "descending"]
                                             [(field->id :checkins :venue_id) "ascending"]]}}))

;; ## EMPTY QUERY
;; Just don't barf
(expect {:status :completed, :row_count 0, :data {:rows [], :columns [], :cols []}}
        (process-and-run {:type :query
                          :database @db-id
                          :native {}
                          :query {:source_table 0
                                  :filter [nil nil]
                                  :aggregation ["rows"]
                                  :breakout [nil]
                                  :limit nil}}))