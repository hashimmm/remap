#lang typed/racket/base

(provide (all-defined-out))

(require racket/list
         "tables.rkt"
         "query.rkt"
         "utils.rkt"
         "fancy-select.rkt")

(struct PreparedDelete ([table : Table]
                        [where : (U False WhereClause)])
  #:transparent)

(: delete (-> Table Col-Arg-Item PreparedDelete))
(define (delete tbl where)
  (define where-clause
    (if (not where)
        #f
        (select-cols tbl (list where))))
  (when (not (= (length where-clause) 1))
    (raise-arguments-error
     'update
     "Where clause must return a single expression."
     "where-clause" where-clause))
  (PreparedDelete tbl
                  (if where-clause
                      (cast
                       ((inst first Selectable)
                        where-clause)
                       WhereClause)
                      #f)))
