#lang typed/racket/base

(provide (all-defined-out))

(require racket/string
         "tables.rkt"
         "query.rkt"
         "creates-and-updates.rkt")

(define-type SQL-Clause (U Query
                           PreparedQuery
                           LiteralQuery
                           PreparedInsert
                           PreparedUpdate))

(: to-sql (-> SQL-Clause String))
(define (to-sql sql-clause)
  (cond [(LiteralQuery? sql-clause)
         (format "SELECT ~a"
                 (string-join (map
                               (λ([x : (U Any-SQL-Literal SQL-Param)])
                                 (cond [(SQL-Literal? x)
                                        (render-sel x '() '())]
                                       [else
                                        (render-sql-param)]))
                               (LiteralQuery-selectables sql-clause))
                              ", "))]
        [(Query? sql-clause)
         (to-sql (prepare-query sql-clause))]
        [(PreparedInsert? sql-clause)
         (define pi sql-clause)
         (define rendered-cols
           (string-join
            (map escape-ident-symbol (PreparedInsert-cols pi))
            "\n, "))
         (define rendered-col-clause
           (string-join (list "(" rendered-cols ")") "\n"))
         (define rendered-select-stmt
           (to-sql (PreparedInsert-query pi)))
         (define rendered-tbl-name (render-tbl-name (PreparedInsert-table pi)))
         (format "INSERT INTO ~a ~a~n~a"
                 rendered-tbl-name rendered-col-clause rendered-select-stmt)]
        [(PreparedUpdate? sql-clause)
         (define pu sql-clause)
         (define rendered-tbl-name
           (render-tbl-name (PreparedUpdate-table pu)))
         (define rendered-update-clauses
           (string-join
            (map (λ([clause : (Pairof Symbol (U Any-SQL-Literal SQL-Param))])
                   (format "~a = ~a"
                           (escape-ident-symbol (car clause))
                           (render-sel (cdr clause) '())))
                 (PreparedUpdate-clauses pu))
            "\n, "))
         (define where (PreparedUpdate-where pu))
         (define rendered-where
           (if where
               (format "~nWHERE ~a"
                       (render-sel where '()))
               ""))
         (format "UPDATE ~a SET ~a~a"
                 rendered-tbl-name
                 rendered-update-clauses
                 rendered-where)]
        [else
         (define pq sql-clause)
         (define rendered-sels
           (render-sel-refs (PreparedQuery-sel-refs-map pq)))
         (define rendered-from-clause
           (render-tbl-name (Query-from (PreparedQuery-query pq))))
         (define rendered-joins
           (render-joins (PreparedQuery-rel-refs pq)))
         (define where-clause (Query-where (PreparedQuery-query pq)))
         (define rendered-where-clause
           (if where-clause
               (format "WHERE ~a"
                       (render-sel where-clause
                                   (PreparedQuery-rel-refs pq)))
               ""))
         (define rest-clauses (list rendered-joins rendered-where-clause))
         (define rendered-rest-clauses
           (string-join (filter (λ([s : String]) (not (string=? s "")))
                                rest-clauses)
                        "\n"))
         (define rendered-stmt
           (format "SELECT ~a~nFROM ~a~n~a"
                   rendered-sels
                   rendered-from-clause
                   rendered-rest-clauses))
         rendered-stmt]))
