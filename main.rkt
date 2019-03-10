#lang typed/racket/base

;; TODO: some arbitrary limitations that we may be able to fix
;; include the fact that we don't allow @, / and : in column names
;; and that we don't allow duplicate selections.
;; Currently we rely on these limitations (or, in the case of @,
;; planning to rely on them). But they're all easy fixes.

;; A few notes on our experiments:
;; See, in PostgreSQL, we actually CAN have our cake and eat it too,
;; in the case of making joins but not increasing row count,
;; while not losing information, and keeping efficiency, i.e.
;; not adding redundant data in selects, by simply grouping
;; by the original table's id, and doing array_agg(joined_table.*)
;; which results in a proper array of records/rows that
;; are accessible using the usual SQL stuff.
;; But the column type of the array_agg is an oid that
;; Racket doesn't recognize, and Racket throws an error.
;; Python's psycopg2 fares only slightly better, in that
;; it returns a string instead, but that's still bad because
;; we wanted vectors or namedtuples or dicts or something.
;; So we could either go the django route (multiple queries,
;; with the joined ones literally selecting against concrete ids)
;; or the sqlalchemy one, which groups in the library rather
;; than the db.
;; Our solution in the end is similar to sqlalchemy,
;; which resolves groups in-python.
;; But we'll be more robust, I think.
;; What we're going to be doing is listed below in the TODO.
;; We will tie ourselves somewhat to the db package,
;; as opposed to tying ourselves to PostgreSQL's sql
;; capabilities. Each would have its own issues.
;; We COULD just shift to plisqin's method of using group
;; by and adding an array_agg or something, but I don't think
;; that works very well with the `db` package.
;; Additionally, I don't think we can nest array_agg calls, and then
;; we'd have to do render more selects and nest, and that
;; will quickly get just as weird as this. It may still be better.

;; A couple things to be wary of during grouping is that
;; you probably always want to select ids and that you probably
;; want to take care around impure db functions.

;; The best approach would involving somehow telling Racket
;; about the array of records type (which is slightly difficult
;; because the record's type is the table's type, and not just row or record.

;; TODO:

;; Test and fix relations that need to depend on another relation
;; as well as the base table. I think I tested that they work but
;; someone ran into some trouble.

;; How to do the limits thing:
;; - apply limits to base query
;; - turn base query into a table expression for a FROM clause
;; - apply joins against this limited-table FROM clause
;; - apply filters
;; - get rows
;; - group rows in racket using base query's ID.

;; How to do permissions (This is vague-er than we hoped):
;; - One approach is to:
;;   - Have separate permissions descriptions, totally separate from all this
;;   - Modify the query at the very end to add where clauses to all the tables
;;     and relations according to the permission descriptions.
;; - AND/OR,
;;   - Make a subtype of relation that uses equal? of the original
;;     but adds a non-essential part to the on clause
;;   - Leverage this non-essential one for permissions.

;; How to do aggregates:
;; - Basically just copy django's aggregates+values thing.
;;   (Ditch the table->query thing that has all columns pre-selected,
;;    or just "exclude" all the selectables.)
;; - The only thing to make easier is the grouping by id without
;;   being forced to select it.
;; - Also, since we haven't gone the explicit "Grouped-Join" route
;;   that plisqin takes, and we do the grouping in racket, we have some
;;   additional steps
;;   - an aggregation should trigger a group by or "DISTINCT ON" on the
;;     pre-agg query
;;   - then the function will be applied using that query as a sub-query
;;   - The plan for limit, where the limit would be applied to an inner
;;     query that wouldn't have joins, doesn't apply when aggregating,
;;     because we'll already have a group-by, we can apply the limit
;;     at the end.

;; Far into the future:
;; - TopN relations, i.e. relations with limits, may be achieved using
;;   lateral joins in postgresql. TopN might be a non-essential condition
;;   on the discussed relation subtype.

(provide (all-from-out "private/tables.rkt"
                       "private/query.rkt"
                       "private/grouping.rkt"
                       "private/grouping-v2.rkt"))

(require "private/tables.rkt"
         "private/query.rkt"
         (prefix-in grouping-old: "private/grouping.rkt")
         "private/grouping-v2.rkt")
