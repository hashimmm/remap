#lang typed/racket/base

(require racket/list
         (only-in typed/db sql-null)
         "tables.rkt"
         "query.rkt"
         "grouping.rkt"
         "utils.rkt"
         "creates-and-updates.rkt"
         "to-sql.rkt"
         (prefix-in gv2: "grouping-v2.rkt"))

(require/typed "fancy-select.rkt"
               [select-from-2 (-> Table (Listof Any) Query)])

(module+ test
  (require typed/rackunit))

(module+ test
  (check-equal?
   (to-sql (LiteralQuery (list (SQL-Literal 1)
                               (SQL-Literal "two")
                               (SQL-Literal #f))))
   "SELECT 1, 'two', false")
  (define tbl1
    (make-table 't1
                'sch
                (list (UuidColumn 'id)
                      (TextColumn 'name)
                      (BooleanColumn 'approved)
                      (UuidColumn 'parent))
                (λ([self : Table])
                  (define rel (Relation 'parent-rel self (SQL-Literal #f)))
                  (define on-clause
                    (Equal
                     (TableColumn self
                                  (UuidColumn 'parent))
                     (RelatedColumn rel
                                    (Rel (UuidColumn 'id)))))
                  (set-Relation-on! rel on-clause)
                  (list rel))))
  (define tbl2
    (make-table 't2
                'sch
                (list (UuidColumn 'id)
                      (TextColumn 'name)
                      (UuidColumn 't1-id))
                (λ([self : Table])
                  (define rel (Relation 't1-rel tbl1 (SQL-Literal #f)))
                  (define on-clause
                    (Equal
                     (TableColumn self
                                  (UuidColumn 't1-id))
                     (RelatedColumn rel
                                    (Rel (UuidColumn 'id)))))
                  (set-Relation-on! rel on-clause)
                  (list rel))))
  (define sel
    (select-from tbl2
                 (list (TableColumn tbl2 (UuidColumn 'id)))))
  (define rel-t2-to-t1
    (first (Table-relations tbl2)))
  (define rel-t1-self
    (first (Table-relations tbl1)))
  (define fk (RelatedColumn rel-t2-to-t1
                            (Rel (UuidColumn 'id))))
  (define fk-lvl-2
    (RelatedColumn
     rel-t2-to-t1
     (RelatedColumn rel-t1-self
                    (Rel (UuidColumn 'id)))))
  (define jg-1 (create-graph-from-list (list rel-t2-to-t1)))
  (define merged-jg-1
    (merge-into-graph jg-1
                      (list rel-t1-self)))
  (define sel-with-rel
    (include sel fk))
  (define sel-with-rel-2
    (include sel-with-rel fk-lvl-2))
  (define selected (Query-selectables sel-with-rel-2)) 
  (define selected-joins
    (Query-joins sel-with-rel-2))

  (define (not-false x) (not (equal? x #f)))

  (check-equal?
   (JoinGraph rel-t2-to-t1 '()) jg-1)
  (check-equal?
   merged-jg-1
   (JoinGraph rel-t2-to-t1
              (list (JoinGraph rel-t1-self '()))))
  (check-equal?
   merged-jg-1
   (merge-into-graph merged-jg-1
                     (list)))
  (check-equal?
   merged-jg-1
   (merge-into-graph merged-jg-1
                     (list rel-t1-self)))

  (check-equal? (length selected) 3)
  (check-not-false (member fk selected))
  (check-not-false (member fk-lvl-2 selected))
  (check-not-false (member (TableColumn tbl2 (UuidColumn 'id))
                           selected))
  (check-equal?
   selected-joins
   (list
    merged-jg-1))
  (define-values (sql-1 group-1) (to-sql-with-groupings sel-with-rel))
  (define-values (sql-2 group-2) (to-sql-with-groupings sel-with-rel-2))
  (check-equal?
   sql-1
   #<<EOF
SELECT "t1-rel"."id" AS "t1-rel:id"
, "t2"."id" AS "id"
FROM "sch"."t2"
LEFT OUTER JOIN "sch"."t1" AS "t1-rel" ON "t2"."t1-id" = "t1-rel"."id"
EOF
   )
  (check-equal?
   sql-2
   #<<EOF
SELECT "t1-rel/parent-rel"."id" AS "t1-rel/parent-rel:id"
, "t1-rel"."id" AS "t1-rel:id"
, "t2"."id" AS "id"
FROM "sch"."t2"
LEFT OUTER JOIN "sch"."t1" AS "t1-rel" ON "t2"."t1-id" = "t1-rel"."id"
LEFT OUTER JOIN "sch"."t1" AS "t1-rel/parent-rel" ON "t1-rel"."parent" = "t1-rel/parent-rel"."id"
EOF
   )
  
  (define tbl3
    (make-table 'tbl3 #f
                (list
                 (UuidColumn 'id))
                (λ(x) '())))
  (define tbl4
    (make-table 'tbl4 #f
                (list
                 (UuidColumn 'id)
                 (UuidColumn 't5-id))
                (λ(x) '())))
  (define tbl5
    (make-table 'tbl5 #f
                (list
                 (UuidColumn 'id)
                 (UuidColumn 't4-id))
                (λ(x) '())))
  (define rel:t4->t5
    (OneToOneRel 't5 tbl5 (SQL-Literal #f)))
  (set-Relation-on!
   rel:t4->t5
   (Equal (TableColumn tbl4
                       (UuidColumn 't5-id))
          (RelatedColumn rel:t4->t5
                         (Rel (UuidColumn 'id)))))
  (set-Table-relations!
   tbl4
   (list rel:t4->t5))
  (define rel:t5->t4
    (OneToOneRel 't4 tbl4 (SQL-Literal #f)))
  (set-Relation-on!
   rel:t5->t4
   (Equal (TableColumn tbl5
                       (UuidColumn 't4-id))
          (RelatedColumn rel:t5->t4
                         (Rel (UuidColumn 'id)))))
  (define rel:t5->t4-set
    (Relation 't4-set tbl4 (SQL-Literal #f)))
  (set-Relation-on!
   rel:t5->t4-set
   (Equal (TableColumn tbl5 (UuidColumn 'id))
          (RelatedColumn rel:t5->t4-set
                         (Rel
                          (UuidColumn 't5-id)))))
  (set-Table-relations!
   tbl5
   (list rel:t5->t4
         rel:t5->t4-set))
  (define sel-t5
    (select-from tbl5
                 (list (TableColumn tbl5
                                    (UuidColumn 'id)))))
  (define sel-t5-with-t4
    (include sel-t5
             (RelatedColumn rel:t5->t4
                            (Rel (UuidColumn 'id)))))
  (define sel-t5-with-t4-and-t4-set
    (include sel-t5-with-t4
             (RelatedColumn rel:t5->t4-set
                            (Rel
                             (UuidColumn 'id)))))
  (define-values
    (with-relations-query-str with-relations-groupings)
    (to-sql-with-groupings sel-t5-with-t4-and-t4-set))

  (check-equal?
   with-relations-query-str
   #<<EOF
SELECT "t4-set"."id" AS "t4-set:id"
, "t4"."id" AS "t4:id"
, "tbl5"."id" AS "id"
FROM "tbl5"
LEFT OUTER JOIN "tbl4" AS "t4-set" ON "tbl5"."id" = "t4-set"."t5-id"
LEFT OUTER JOIN "tbl4" AS "t4" ON "tbl5"."t4-id" = "t4"."id"
EOF
   )

  ;; Checking to see if selectables, selectable ref mapping,
  ;; and the rendered query are all in the same order.
  (define prep-q (prepare-query sel-t5-with-t4-and-t4-set))
  (check-true
   (andmap (λ(x y) (equal? x y))
           (map (inst car Selectable SelRef)
                (PreparedQuery-sel-refs-map prep-q))
           (Query-selectables (PreparedQuery-query prep-q))))
  (check-equal?
   (render-sel-refs
    (list
     (cons (TableColumn tbl5 (UuidColumn 'id))
           (SelRef "\"tbl5\".\"id\"" "id"))
     (cons (TableColumn tbl5 (UuidColumn 't4-id))
           (SelRef "\"tbl5\".\"t4-id\"" "t4-id"))))
   #<<EOF
"tbl5"."id" AS "id"
, "tbl5"."t4-id" AS "t4-id"
EOF
   )

  (define results-sel-t5-with-t4-and-t4-set
    '(#("t4-set-1" "t4-1" 1)
      #("t4-set-2" "t4-1" 1)
      #("t4-set-3" "t4-1" 1)
      #("t4-set-1" "t4-2" 2)
      #("t4-set-2" "t4-2" 2)
      #("t4-set-4" "t4-3" 3)
      #("t4-set-5" "t4-3" 3)
      #("t4-set-5" "t4-3" 4)))

  (check-equal?
   (name-pq-results prep-q results-sel-t5-with-t4-and-t4-set)
   (list
    (Record
     (list
      '(id . 1)
      (cons 't4 (Record '((t4:id . "t4-1"))))
      (list
       't4-set
       (Record '((t4-set:id . "t4-set-3")))
       (Record '((t4-set:id . "t4-set-2")))
       (Record '((t4-set:id . "t4-set-1"))))))
    (Record
     (list
      '(id . 2)
      (cons 't4 (Record '((t4:id . "t4-2"))))
      (list
       't4-set
       (Record '((t4-set:id . "t4-set-2")))
       (Record '((t4-set:id . "t4-set-1"))))))
    (Record
     (list
      '(id . 3)
      (cons 't4 (Record '((t4:id . "t4-3"))))
      (list
       't4-set
       (Record '((t4-set:id . "t4-set-5")))
       (Record '((t4-set:id . "t4-set-4"))))))
    (Record
     (list
      '(id . 4)
      (cons 't4 (Record '((t4:id . "t4-3"))))
      (list 't4-set (Record '((t4-set:id . "t4-set-5"))))))))
  (check-equal?
   (gv2:group-rows (PreparedQuery-groupings prep-q)
                   (PreparedQuery-sel-refs-map prep-q)
                   results-sel-t5-with-t4-and-t4-set)
   (list
    (Record
     (list
      (list
       't4-set
       (Record '((id . "t4-set-1")))
       (Record '((id . "t4-set-2")))
       (Record '((id . "t4-set-3"))))
      (cons 't4 (Record '((id . "t4-1"))))
      '(id . 1)))
    (Record
     (list
      (list
       't4-set
       (Record '((id . "t4-set-1")))
       (Record '((id . "t4-set-2"))))
      (cons 't4 (Record '((id . "t4-2"))))
      '(id . 2)))
    (Record
     (list
      (list
       't4-set
       (Record '((id . "t4-set-4")))
       (Record '((id . "t4-set-5"))))
      (cons 't4 (Record '((id . "t4-3"))))
      '(id . 3)))
    (Record
     (list
      (list 't4-set (Record '((id . "t4-set-5"))))
      (cons 't4 (Record '((id . "t4-3"))))
      '(id . 4)))))

  (define stuff-tbl
    (make-table 'stuff
                'sch
                (list (UuidColumn 'id)
                      (TextColumn 'name)
                      (BooleanColumn 'approved)
                      (UuidColumn 'parent-id))
                (λ([self : Table])
                  '())))
  (define users-tbl
    (make-table 'users
                'sch
                (list (UuidColumn 'id)
                      (TextColumn 'name)
                      (UuidColumn 'stuff-id))
                (λ([self : Table])
                  '())))
  (define phone-numbers-tbl
    (make-table 'phone-numbers
                'sch
                (list (UuidColumn 'id)
                      (TextColumn 'country)
                      (TextColumn 'number)
                      (UuidColumn 'user-id))
                (λ([self : Table])
                  '())))
  (define stuff-rels
    (list
     (make-relation 'users
                    users-tbl
                    (λ(tbl rel)
                      (Equal (TableColumn
                              tbl
                              (UuidColumn 'id))
                             (RelatedColumn
                              rel
                              (Rel (UuidColumn 'stuff-id))))))
     (make-1-1-relation 'parent
                        stuff-tbl
                        (λ(tbl rel)
                          (Equal (TableColumn
                                  tbl
                                  (UuidColumn 'parent-id))
                                 (RelatedColumn
                                  rel
                                  (Rel (UuidColumn 'id))))))))

  (set-Table-relations! stuff-tbl stuff-rels)

  (define user-rels
    (list
     (make-relation 'phone-numbers
                    phone-numbers-tbl
                    (λ(tbl rel)
                      (Equal (TableColumn
                              tbl
                              (UuidColumn 'id))
                             (RelatedColumn
                              rel
                              (Rel (UuidColumn 'user-id))))))))

  (set-Table-relations! users-tbl user-rels)

  (define 1-N-N-query
    (include
     (include
      (include
       (include
        (select-from stuff-tbl
                     (list
                      (TableColumn stuff-tbl (UuidColumn 'id))
                      (TableColumn stuff-tbl (UuidColumn 'parent-id))
                      (TableColumn stuff-tbl (TextColumn 'name))))
        (RelatedColumn
         (second stuff-rels)
         (Rel (UuidColumn 'id))))
       (RelatedColumn
        (first stuff-rels)
        (Rel (UuidColumn 'name))))
      (RelatedColumn
       (first stuff-rels)
       (RelatedColumn
        (first user-rels)
        (Rel (UuidColumn 'id)))))
     (RelatedColumn
      (first stuff-rels)
      (RelatedColumn
       (first user-rels)
       (Rel (TextColumn 'number))))))

  (define 1-N-N-prep-query
    (prepare-query 1-N-N-query))

  (check-equal?
   (to-sql 1-N-N-prep-query)
   #<<EOF
SELECT "users/phone-numbers"."number" AS "users/phone-numbers:number"
, "users/phone-numbers"."id" AS "users/phone-numbers:id"
, "users"."name" AS "users:name"
, "parent"."id" AS "parent:id"
, "stuff"."name" AS "name"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
LEFT OUTER JOIN "sch"."users" AS "users" ON "users"."id" = "users"."stuff-id"
LEFT OUTER JOIN "sch"."phone-numbers" AS "users/phone-numbers" ON "users"."id" = "users/phone-numbers"."user-id"
LEFT OUTER JOIN "sch"."stuff" AS "parent" ON "stuff"."parent-id" = "parent"."id"
EOF
   )

  (: 1-N-N-results gv2:Results)
  (define 1-N-N-results
    `(#("090078601" 1 "somebody" 11 "thing" 11 1)
      #("111532532" 2 "somebody" 11 "thing" 11 1)
      #("111241241" 3 "otherbody" 11 "thing" 11 1)
      #("111244622" 4 "otherbody" 12 "foo" 12 2)
      #("111532532" 2 "somebody" 12 "foo" 12 2)
      #(,sql-null ,sql-null ,sql-null ,sql-null "foo" ,sql-null 2)))

  (check
   equal?
   (gv2:group-rows (PreparedQuery-groupings 1-N-N-prep-query)
                   (PreparedQuery-sel-refs-map 1-N-N-prep-query)
                   1-N-N-results)
   (list
    (Record
     (list
      (list
       'users
       (Record
        (list
         '(name . "somebody")
         (list
          'phone-numbers
          (Record '((id . 1) (number . "090078601")))
          (Record '((id . 2) (number . "111532532"))))))
       (Record
        (list
         '(name . "otherbody")
         (list
          'phone-numbers
          (Record '((id . 3) (number . "111241241")))))))
      (cons 'parent (Record '((id . 11))))
      '(name . "thing")
      '(parent-id . 11)
      '(id . 1)))
    (Record
     (list
      (list
       'users
       (Record
        (list
         '(name . "otherbody")
         (list
          'phone-numbers
          (Record '((id . 4) (number . "111244622"))))))
       (Record
        (list
         '(name . "somebody")
         (list
          'phone-numbers
          (Record '((id . 2) (number . "111532532")))))))
      (cons 'parent (Record '((id . 12))))
      '(name . "foo")
      '(parent-id . 12)
      '(id . 2)))
    (Record
     `((users)
       (parent . ,sql-null)
       (name . "foo")
       (parent-id . ,sql-null)
       (id . 2)))))

  (define where-query-sql-literal
    (where
     (select-from stuff-tbl
                  (list
                   (TableColumn stuff-tbl (UuidColumn 'id))
                   (TableColumn stuff-tbl (BooleanColumn 'approved))
                   (TableColumn stuff-tbl (UuidColumn 'parent-id))
                   (TableColumn stuff-tbl (TextColumn 'name))))
     (SQL-Literal #f)))
  
  (check-equal?
   (to-sql where-query-sql-literal)
   #<<EOF
SELECT "stuff"."name" AS "name"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."approved" AS "approved"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
WHERE false
EOF
   )

  (define where-query-boolean-column
    (where
     (select-from stuff-tbl
                  (list
                   (TableColumn stuff-tbl (UuidColumn 'id))
                   (TableColumn stuff-tbl (BooleanColumn 'approved))
                   (TableColumn stuff-tbl (UuidColumn 'parent-id))
                   (TableColumn stuff-tbl (TextColumn 'name))))
     (TableColumn stuff-tbl (BooleanColumn 'approved))))
  
  (check-equal?
   (to-sql where-query-boolean-column)
   #<<EOF
SELECT "stuff"."name" AS "name"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."approved" AS "approved"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
WHERE "stuff"."approved"
EOF
   )

  (define where-query-func
    (where
     (select-from stuff-tbl
                  (list
                   (TableColumn stuff-tbl (UuidColumn 'id))
                   (TableColumn stuff-tbl (BooleanColumn 'approved))
                   (TableColumn stuff-tbl (UuidColumn 'parent-id))
                   (TableColumn stuff-tbl (TextColumn 'name))))
     (Equal (TableColumn stuff-tbl (BooleanColumn 'approved)) (SQL-Literal #f))))
  
  (check-equal?
   (to-sql where-query-func)
   #<<EOF
SELECT "stuff"."name" AS "name"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."approved" AS "approved"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
WHERE "stuff"."approved" = false
EOF
   )
  
  (define where-query-qualified-col
    (where
     (select-from stuff-tbl
                  (list
                   (TableColumn stuff-tbl (UuidColumn 'id))
                   (TableColumn stuff-tbl (BooleanColumn 'approved))
                   (TableColumn stuff-tbl (UuidColumn 'parent-id))
                   (TableColumn stuff-tbl (TextColumn 'name))))
     (Equal (TableColumn stuff-tbl (UuidColumn 'parent-id))
            (RelatedColumn (second stuff-rels) (Rel (UuidColumn 'id))))))

  (check-equal?
   (to-sql where-query-qualified-col)
   #<<EOF
SELECT "stuff"."name" AS "name"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."approved" AS "approved"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
LEFT OUTER JOIN "sch"."stuff" AS "parent" ON "stuff"."parent-id" = "parent"."id"
WHERE "stuff"."parent-id" = "parent"."id"
EOF
   )

  (define where-query-sql-param
    (where
     (select-from stuff-tbl
                  (list
                   (TableColumn stuff-tbl (UuidColumn 'id))
                   (TableColumn stuff-tbl (BooleanColumn 'approved))
                   (TableColumn stuff-tbl (UuidColumn 'parent-id))
                   (TableColumn stuff-tbl (TextColumn 'name))))
     (Equal (TableColumn stuff-tbl (UuidColumn 'parent-id))
            sql-param)))

  (check-equal?
   (to-sql where-query-sql-param)
   #<<EOF
SELECT "stuff"."name" AS "name"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."approved" AS "approved"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
WHERE "stuff"."parent-id" = ?
EOF
   )

  (check-equal?
   (to-sql (select-from-2 stuff-tbl
                          '(id
                            name
                            parent-id
                            (parent id name))))
   #<<EOF
SELECT "parent"."name" AS "parent:name"
, "parent"."id" AS "parent:id"
, "stuff"."parent-id" AS "parent-id"
, "stuff"."name" AS "name"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
LEFT OUTER JOIN "sch"."stuff" AS "parent" ON "stuff"."parent-id" = "parent"."id"
EOF
   )

  (check-equal?
   (to-sql
    (insert stuff-tbl
            '((name ?)
              (approved ?))))
   #<<EOF
INSERT INTO "sch"."stuff" (
"name"
, "approved"
)
SELECT ?, ?
EOF
   )

  (check-equal?
   (to-sql (select-from-2 stuff-tbl
                          '(id
                            name
                            (? "hello")
                            parent-id
                            (parent id name))))
   #<<EOF
SELECT "parent"."name" AS "parent:name"
, "parent"."id" AS "parent:id"
, "stuff"."parent-id" AS "parent-id"
, 'hello' AS "F4"
, "stuff"."name" AS "name"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
LEFT OUTER JOIN "sch"."stuff" AS "parent" ON "stuff"."parent-id" = "parent"."id"
EOF
   )

  (check-equal?
   (to-sql (select-from-2 stuff-tbl
                          '(id
                            name
                            (? "hello")
                            parent-id
                            (parent id name)
                            (@ Equal name (? "hello")))))
   #<<EOF
SELECT "stuff"."name" = 'hello' AS "F1"
, "parent"."name" AS "parent:name"
, "parent"."id" AS "parent:id"
, "stuff"."parent-id" AS "parent-id"
, 'hello' AS "F5"
, "stuff"."name" AS "name"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
LEFT OUTER JOIN "sch"."stuff" AS "parent" ON "stuff"."parent-id" = "parent"."id"
EOF
   )

  (check-equal?
   (to-sql (select-from-2 stuff-tbl
                          '(id
                            name
                            (? "hello")
                            parent-id
                            (parent id name)
                            (@ Equal name (parent name)))))
   #<<EOF
SELECT "stuff"."name" = "parent"."name" AS "F1"
, "parent"."name" AS "parent:name"
, "parent"."id" AS "parent:id"
, "stuff"."parent-id" AS "parent-id"
, 'hello' AS "F5"
, "stuff"."name" AS "name"
, "stuff"."id" AS "id"
FROM "sch"."stuff"
LEFT OUTER JOIN "sch"."stuff" AS "parent" ON "stuff"."parent-id" = "parent"."id"
EOF
   ))
