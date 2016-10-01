(ns metabase.permissions-test
  "A test suite around permissions. Nice!"
  (:require [clojure.string :as s]
            [expectations :refer :all]
            [metabase.db :as db]
            (metabase.models [card :refer [Card]]
                             [dashboard :refer [Dashboard]]
                             [dashboard-card :refer [DashboardCard]]
                             [database :refer [Database]]
                             [permissions :refer [Permissions], :as perms]
                             [permissions-group :refer [PermissionsGroup], :as group]
                             [permissions-group-membership :refer [PermissionsGroupMembership]]
                             [table :refer [Table]])
            [metabase.query-processor.expand :as ql]
            [metabase.sync-database :as sync]
            [metabase.test.data :as data]
            [metabase.test.data.datasets :as datasets]
            [metabase.test.data.users :as test-users]
            [metabase.test.util :as tu]
            [metabase.util :as u]))

;; 3 users:
;; crowberto, member of Admin, All Users
;; rasta, member of All Users
;; lucky, member of All Users, Ops


;;; ------------------------------------------------------------ Ops Group ------------------------------------------------------------

;; ops group is a group with only one member: lucky
(def ^:private ^:dynamic *ops-group*)

(defn- with-ops-group [f]
  (fn []
    (tu/with-temp* [PermissionsGroup           [group {:name "Operations"}]
                    PermissionsGroupMembership [_     {:group_id (u/get-id group), :user_id (test-users/user->id :lucky)}]]
      (binding [*ops-group* group]
        (f)))))


;;; ------------------------------------------------------------ DBs ------------------------------------------------------------

(defn- test-db [db-name]
  (let [db (db/select-one [Database :details :engine] :id (data/id))]
    {:name         db-name
     :engine       (:engine db)
     :is_full_sync false
     :details      (assoc (:details db)
                       :short-lived? true)}))

(defn- with-db [db-name f]
  (fn []
    (tu/with-temp Database [db (test-db db-name)]
      ;; syncing is slow as f**k so just manually insert the Tables
      (doseq [table-name ["venues" "users" "checkins"]]
        (db/insert! Table :db_id (u/get-id db), :active true, :name table-name))
      (f db))))

(def ^:private ^:dynamic *db1*)
(def ^:private ^:dynamic *db2*)

(defn- table [db table-name]
  (db/select-one Table
    :%lower.name (s/lower-case (name table-name))
    :db_id       (u/get-id db)))

(defn- with-db-1 [f]
  (with-db "DB One" (fn [db]
                      (binding [*db1* db]
                        (f)))))

(defn- with-db-2 [f]
  (with-db "DB Two" (fn [db]
                      ;; all-users has no access to DB 2
                      (perms/revoke-permissions! (group/all-users) (u/get-id db))
                      ;; ops group only has access venues table + *reading* SQL
                      (when *ops-group*
                        (perms/revoke-permissions! *ops-group* (u/get-id db))
                        (perms/grant-native-read-permissions! *ops-group* (u/get-id db))
                        (let [venues-table (table db :venues)]
                          (perms/grant-permissions! *ops-group* (u/get-id db) (:schema venues-table) (u/get-id venues-table))))
                      (binding [*db2* db]
                        (f)))))


;;; ------------------------------------------------------------ Cards ------------------------------------------------------------

(defn- count-card [db table-name card-name]
  (let [table (table db table-name)]
    {:name          card-name
     :database_id   (u/get-id db)
     :table_id      (u/get-id table)
     :dataset_query {:database (u/get-id db)
                     :type     "query"
                     :query    (ql/query
                                 (ql/source-table (u/get-id table))
                                 (ql/aggregation (ql/count)))}}))

(defn- sql-count-card [db table-name card-name]
  (let [table (table db table-name)]
    {:name          card-name
     :database_id   (u/get-id db)
     :table_id      (u/get-id table)
     :dataset_query {:database (u/get-id db)
                     :type     "native"
                     :query    (format "SELECT count(*) FROM \"%s\";" (:name table))}}))


(def ^:private ^:dynamic *card:db1-count-of-venues*)
(def ^:private ^:dynamic *card:db1-count-of-users*)
(def ^:private ^:dynamic *card:db1-count-of-checkins*)
(def ^:private ^:dynamic *card:db1-sql-count-of-users*)
(def ^:private ^:dynamic *card:db2-count-of-venues*)
(def ^:private ^:dynamic *card:db2-count-of-users*)
(def ^:private ^:dynamic *card:db2-count-of-checkins*)
(def ^:private ^:dynamic *card:db2-sql-count-of-users*)

(defn- all-cards []
  #{*card:db1-count-of-venues*
    *card:db1-count-of-users*
    *card:db1-count-of-checkins*
    *card:db1-sql-count-of-users*
    *card:db2-count-of-venues*
    *card:db2-count-of-users*
    *card:db2-count-of-checkins*
    *card:db2-sql-count-of-users*})

(defn- all-card-ids []
  (set (map :id (all-cards))))

(defn- with-cards [f]
  (fn []
    (tu/with-temp* [Card [db1-count-of-venues    (count-card     *db1* :venues   "DB 1 Count of Venues")]
                    Card [db1-count-of-users     (count-card     *db1* :users    "DB 1 Count of Users")]
                    Card [db1-count-of-checkins  (count-card     *db1* :checkins "DB 1 Count of Checkins")]
                    Card [db1-sql-count-of-users (sql-count-card *db1* :venues   "DB 1 SQL Count of Users")]
                    Card [db2-count-of-venues    (count-card     *db2* :venues   "DB 2 Count of Venues")]
                    Card [db2-count-of-users     (count-card     *db2* :users    "DB 2 Count of Users")]
                    Card [db2-count-of-checkins  (count-card     *db2* :checkins "DB 2 Count of Checkins")]
                    Card [db2-sql-count-of-users (sql-count-card *db2* :venues   "DB 2 SQL Count of Users")]]
      (binding [*card:db1-count-of-venues*    db1-count-of-venues
                *card:db1-count-of-users*     db1-count-of-users
                *card:db1-count-of-checkins*  db1-count-of-checkins
                *card:db1-sql-count-of-users* db1-sql-count-of-users
                *card:db2-count-of-venues*    db2-count-of-venues
                *card:db2-count-of-users*     db2-count-of-users
                *card:db2-count-of-checkins*  db2-count-of-checkins
                *card:db2-sql-count-of-users* db2-sql-count-of-users]
        (f)))))


;;; ------------------------------------------------------------ Dashboards ------------------------------------------------------------

(def ^:private ^:dynamic *dash:db1-all*)
(def ^:private ^:dynamic *dash:db2-all*)
(def ^:private ^:dynamic *dash:db2-private*)

(defn- all-dashboards []
  #{*dash:db1-all*
    *dash:db2-all*
    *dash:db2-private*})

(defn- all-dashboard-ids []
  (set (map :id (all-dashboards))))

(defn- add-cards-to-dashboard! {:style/indent 1} [dashboard & cards]
  (doseq [card cards]
    (db/insert! DashboardCard
      :dashboard_id (u/get-id dashboard)
      :card_id      (u/get-id card))))

(defn- with-dashboards [f]
  (fn []
    (tu/with-temp* [Dashboard [db1-all     {:name "All DB 1"}]
                    Dashboard [db2-all     {:name "All DB 2"}]
                    Dashboard [db2-private {:name "Private DB 2"}]]
      (add-cards-to-dashboard! db1-all
        *card:db1-count-of-venues*
        *card:db1-count-of-users*
        *card:db1-count-of-checkins*
        *card:db1-sql-count-of-users*)
      (add-cards-to-dashboard! db2-all
        *card:db2-count-of-venues*
        *card:db2-count-of-users*
        *card:db2-count-of-checkins*
        *card:db2-sql-count-of-users*)
      (add-cards-to-dashboard! db2-private
        *card:db2-count-of-users*)
      (binding [*dash:db1-all*     db1-all
                *dash:db2-all*     db2-all
                *dash:db2-private* db2-private]
        (f)))))


;;; ------------------------------------------------------------ TODO Pulses ------------------------------------------------------------

;;; ------------------------------------------------------------ TODO Metrics ------------------------------------------------------------

;;; ------------------------------------------------------------ TODO Segments ------------------------------------------------------------

;;; ------------------------------------------------------------ with everything! ------------------------------------------------------------


(defn- -do-with-test-data [f]
  (((comp with-ops-group with-db-2 with-db-1 with-cards with-dashboards) f)))

(defmacro ^:private with-test-data {:style/indent 0} [& body]
  `(-do-with-test-data (fn []
                         ~@body)))

(defmacro ^:private expect-with-test-data {:style/indent 0} [expected actual]
  `(expect
     ~expected
     (with-test-data
       ~actual)))

;; this test just for tuning purposes, NOCOMMIT
(expect
  nil
  (do (println "\nHow long does it take to set up test data for each permissions test?")
      (with-test-data :ok) ; do a couple dry runs to prime things / create tests users if needed
      (with-test-data :ok)
      (time (with-test-data :ok))
      (println "Cool, let's get started...\n")))

;;; +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
;;; |                                                                              TESTS                                                                             |
;;; +----------------------------------------------------------------------------------------------------------------------------------------------------------------+


;;; ------------------------------------------------------------ TODO Query Builder / Metadata ------------------------------------------------------------

;;; TODO tables in DB 1

;;; TODO tables in DB 2

;;; TODO can ask SQL questions for DB 1?

;;; TODO can ask SQL questions for DB 2?

;;; TODO metrics

;;; TODO segments


;;; ------------------------------------------------------------ GET /api/card ------------------------------------------------------------

(defn- GET-card [username]
  (vec (for [card  ((test-users/user->client username) :get 200 "card")
             :when (contains? (all-card-ids) (u/get-id card))]
         (:name card))))

;; Admin should be able to see all 8 questions
(expect-with-test-data
  ["DB 1 Count of Checkins"
   "DB 1 Count of Users"
   "DB 1 Count of Venues"
   "DB 1 SQL Count of Users"
   "DB 2 Count of Checkins"
   "DB 2 Count of Users"
   "DB 2 Count of Venues"
   "DB 2 SQL Count of Users"]
  (GET-card :crowberto))

;; All Users should only be able to see questions in DB 1
(expect-with-test-data
  ["DB 1 Count of Checkins"
   "DB 1 Count of Users"
   "DB 1 Count of Venues"
   "DB 1 SQL Count of Users"]
  (GET-card :rasta))

;; Ops should be able to see questions in DB 1, and DB 2 venues & SQL questions
(expect-with-test-data
  ["DB 1 Count of Checkins"
   "DB 1 Count of Users"
   "DB 1 Count of Venues"
   "DB 1 SQL Count of Users"
   "DB 2 Count of Venues"
   "DB 2 SQL Count of Users"]
  (GET-card :lucky))


;;; ------------------------------------------------------------ GET /api/card/:id ------------------------------------------------------------

;; just return true/false based on whether they were allowed to see the card
(defn- GET-card-id [username card]
  (not= ((test-users/user->client username) :get (str "card/" (u/get-id card)))
        "You don't have permissions to do that."))

;; make API calls for all 8 at once since loading the test data is slow, etc.
(expect-with-test-data true  (GET-card-id :crowberto *card:db1-count-of-venues*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db1-count-of-users*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db1-count-of-checkins*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db1-sql-count-of-users*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db2-count-of-venues*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db2-count-of-users*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db2-count-of-checkins*))
(expect-with-test-data true  (GET-card-id :crowberto *card:db2-sql-count-of-users*))

(expect-with-test-data true  (GET-card-id :rasta *card:db1-count-of-venues*))
(expect-with-test-data true  (GET-card-id :rasta *card:db1-count-of-users*))
(expect-with-test-data true  (GET-card-id :rasta *card:db1-count-of-checkins*))
(expect-with-test-data true  (GET-card-id :rasta *card:db1-sql-count-of-users*))
(expect-with-test-data false (GET-card-id :rasta *card:db2-count-of-venues*))
(expect-with-test-data false (GET-card-id :rasta *card:db2-count-of-users*))
(expect-with-test-data false (GET-card-id :rasta *card:db2-count-of-checkins*))
(expect-with-test-data false (GET-card-id :rasta *card:db2-sql-count-of-users*))

(expect-with-test-data true  (GET-card-id :lucky *card:db1-count-of-venues*))
(expect-with-test-data true  (GET-card-id :lucky *card:db1-count-of-users*))
(expect-with-test-data true  (GET-card-id :lucky *card:db1-count-of-checkins*))
(expect-with-test-data true  (GET-card-id :lucky *card:db1-sql-count-of-users*))
(expect-with-test-data true  (GET-card-id :lucky *card:db2-count-of-venues*))
(expect-with-test-data false (GET-card-id :lucky *card:db2-count-of-users*))
(expect-with-test-data false (GET-card-id :lucky *card:db2-count-of-checkins*))
(expect-with-test-data true  (GET-card-id :lucky *card:db2-sql-count-of-users*))


;;; ------------------------------------------------------------ GET /api/dashboard ------------------------------------------------------------

(defn- GET-dashboard [username]
  (vec (for [dashboard ((test-users/user->client username) :get 200 "dashboard")
             :when     (contains? (all-dashboard-ids) (u/get-id dashboard))]
         (:name dashboard))))

;; Admin should be able to see all dashboards
(expect-with-test-data
  ["All DB 1"
   "All DB 2"
   "Private DB 2"]
  (GET-dashboard :crowberto))

;; All Users should only be able to see All DB 1.
;; Shouldn't see either one of the DB 2 dashboards because they have no access to DB 2
(expect-with-test-data
  ["All DB 1"]
  (GET-dashboard :rasta))

;; Ops should be able to see All DB 1 & All DB 2
;; Shouldn't see DB 2 Private because they have no access to the db2-count-of-users card, its only card
(expect-with-test-data
  ["All DB 1"
   "All DB 2"]
  (GET-dashboard :lucky))


;;; ------------------------------------------------------------ TODO GET /api/dashboard/:id  ------------------------------------------------------------


;;; ------------------------------------------------------------ TODO Pulses ------------------------------------------------------------

;;; TODO visible pulses


;;; ------------------------------------------------------------ TODO Data Reference ------------------------------------------------------------

;;; TODO visible metrics

;;; TODO visible segments

;;; TODO visible tables
