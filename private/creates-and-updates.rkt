#lang typed/racket/base

(provide (all-defined-out))

(require racket/list
         "tables.rkt"
         "query.rkt"
         "utils.rkt")

(define-type Col-Arg-Item (U Symbol Col-List-Arg))
(define-type Col-Arg (Listof Col-Arg-Item))
(define-type Col-List-Arg (U Col-Rel-Arg Col-Func-Arg Col-Literal-Arg))
(define-type Col-Rel-Arg (Pairof Symbol (AtLeastOne Col-Arg-Item)))
(define-type Col-Func-Arg (Pairof '@ (Pairof Symbol (Listof Col-Arg-Item))))
(define-type Col-Literal-Arg (List '? SQL-Literal-Types))

(require/typed "fancy-select.rkt"
               (select-cols (->* (Table Col-Arg)
                                 ((Listof Relation))
                                 (Listof Selectable))))

#|
There's much to do here, for now we're just going
with the basics, but we'll try to end up enabling
our motivating examples.

Motivating examples:

'(insert
  acquirable #:returning () #:from #f
  (name [something]
   created-by (insert user #:returning (id) #:from department
                      (name [meow]
                       department
                       (where
                        (select id)
                        (equal name [HR]))))))

Hints on how to make the query:
products=# create temporary table foo (id int, name text);
products=# insert into foo (id, name) select 1, 'qwe';
products=# with usr_tmp as (insert into vanilla.users (name) select 'mew' returning id)
 insert into foo (id, name) select 1, id from usr_tmp ;

... So basically, for both update and insert, we can either provide
a list of FuncAnyParam (Selectable) as "returning", in which case we'll
have to make sure that all params only use columns from the affected table.

Or, we can simply provide a query, in which case, the insert should turn into
part of a WITH expression, return all columns, and then the query will use
it as an alias.
|#

(struct PreparedInsert ([table : Table]
                        [cols : (Listof Symbol)]
                        [query : (U LiteralQuery)])
  #:transparent)

(define-type InsertArg (List Symbol (U SQL-Literal-Types '?)))

(: insert (-> Table (Listof InsertArg) PreparedInsert))
(define (insert tbl args)
  (define allowed-cols (map ColIdent-name (Table-columns tbl)))
  (define cols
    (map (λ([col-n-lit : InsertArg]) (first col-n-lit))
         args))
  (when (not (andmap (λ([x : Symbol])
                       (member x allowed-cols))
                     cols))
    (raise-arguments-error 'insert
                           "Columns must exist in table."
                           "cols" cols
                           "allowed-cols" allowed-cols
                           "tbl" tbl))
  (define vals
    (map (λ([col-n-lit : InsertArg]) (second col-n-lit))
         args))
  (define lits (map (λ([x : (U SQL-Literal-Types '?)])
                      (cond [(symbol? x)
                             sql-param]
                            [else
                             (SQL-Literal x)]))
                    vals))
  (PreparedInsert tbl
                  cols
                  (LiteralQuery lits)))


(struct PreparedUpdate ([table : Table]
                        [clauses : (Listof (Pairof Symbol
                                                   (U Any-SQL-Literal SQL-Param)))]
                        [where : (U False WhereClause)])
  #:transparent)

(define-type UpdateArg (List Symbol (U SQL-Literal-Types '?)))

(: update (-> Table (Listof UpdateArg) Col-Arg-Item PreparedUpdate))
(define (update tbl args where)
  (define allowed-cols (map ColIdent-name (Table-columns tbl)))
  (define cols
    (map (λ([col-n-lit : UpdateArg]) (first col-n-lit))
         args))
  (when (not (andmap (λ([x : Symbol])
                       (member x allowed-cols))
                     cols))
    (raise-arguments-error 'update
                           "Columns must exist in table."
                           "cols" cols
                           "allowed-cols" allowed-cols
                           "tbl" tbl))
  (define where-clause
    (if (not where)
        #f
        (select-cols tbl (list where))))
  (when (not (= (length where-clause) 1))
    (raise-arguments-error
     'update
     "Where clause must return a single expression."
     "where-clause" where-clause))
  (define clauses
    (map (λ([x : UpdateArg])
           (cons (first x)
                 (if (symbol? (second x))
                     sql-param
                     (SQL-Literal (second x)))))
         args))
  (PreparedUpdate tbl
                  clauses
                  (if where-clause
                      (cast
                       ((inst first Selectable)
                        where-clause)
                       WhereClause)
                      #f)))
