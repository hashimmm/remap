#lang typed/racket/base

(require racket/list
         "tables.rkt"
         "query.rkt")

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

|#

(struct PreparedInsert ([table : Table]
                        [cols : (Listof Symbol)]
                        [query : (U LiteralQuery)]))

(define-type Arg (List Symbol SQL-Literal-Types))

(: insert (-> Table (Listof Arg) PreparedInsert))
(define (insert tbl args)
  (define cols
    (map (λ([col-n-lit : Arg]) (first col-n-lit))
         args))
  (define vals
    (map (λ([col-n-lit : Arg]) (second col-n-lit))
         args))
  (define lits (map (inst SQL-Literal SQL-Literal-Types)
                    vals))
  (PreparedInsert tbl
                  cols
                  (LiteralQuery lits)))
