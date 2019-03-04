#lang typed/racket/base

;; TODO: some arbitrary limitations that we may be able to fix
;; include the fact that we don't allow @, / and : in column names
;; and that we don't allow duplicate selections.
;; Currently we rely on these limitations (or, in the case of @,
;; planning to rely on them).

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

;; A couple things to be wary of during grouping is that
;; you probably always want to select ids and that you probably
;; want to take care around impure db functions.

;; The best approach would involving somehow telling Racket
;; about the array of records type (which is slightly difficult
;; because the record's type is the table's type, and not just row or record.
;;
;; Also, it just occurred to me that the kind of programming needed in
;; this library may be better done in Racklog.

;; TODO:

;; How to do the limits thing:
;; - apply limits to base query
;; - turn base query into a table expression for a FROM clause
;; - apply joins against this limited-table FROM clause
;; - apply filters
;; - get rows
;; - group rows in racket using base query's ID.

;; How to do the grouping thing:
;; - Preconditions
;;   - No aggregate functions... Actually we'll be able to support them soon.
;;   - All columns must be given unique names.
;;     - Stick join table names as prefixes to related column names.
;;   - We must know which relations are 1-1 and which are 1-N
;;   - We must be able to get QualifiedColumns from functions
;;     - Simply add the init args as a field on the class.
;;   - Basically the grouping level of a function is
;;     the longest rel-path in any qualified column
;;     it has.
;; - If there are no relations, no groupings.
;; - If there are any relations, all TableColumns must be grouped.
;; - We really ought to be threading SelRef's around everywhere and
;;   not Selectable's. Should've converted to SelRef's in query and use those
;;   everywhere.

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

;; Far into the future:
;; - TopN relations, i.e. relations with limits, may be achieved using
;;   lateral joins in postgresql. TopN might be a non-essential condition
;;   on the discussed relation subtype.
;; - We may want to factor out some of the stuff
;;   to-sql does, namely the mapping generation,
;;   and store/update it on the query itself,
;;   incrementally, during initial creation and
;;   includes. The thing to take care of would
;;   be preservation of order across include/exclude.
;;   This way to-sql will be free to have different
;;   implementations, and query will have all the
;;   information any implementation may need.
;;   For now, we'll make do with to-sql returning
;;   the required groupings.

(provide (all-from-out "private/tables.rkt"
                       "private/query.rkt"
                       "private/grouping.rkt"
                       "private/grouping-v2.rkt"))

(require "private/tables.rkt"
         "private/query.rkt"
         (prefix-in grouping-old: "private/grouping.rkt")
         "private/grouping-v2.rkt")
