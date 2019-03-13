#lang typed/racket/base/no-check

#|
POC for typing this module:
(: apply-eq (-> (Listof FuncAnyParam) BoolFunc))
(define (apply-eq args)
  (apply
   Equal
   (cast args (List FuncAnyParam FuncAnyParam))))
|#

(provide (all-defined-out))

(require "query.rkt"
         "tables.rkt")

(: get-column-by-name (case-> (-> Table Symbol Null (TableColumn ColIdent))
                              (-> Table Symbol (AtLeastOne Relation) (RelatedColumn ColIdent))))
(define (get-column-by-name tbl sym rels)
  (cond [(null? rels)
         (let ([matching-item (find-tbl-col tbl sym)])
           (if (not matching-item)
               (raise-arguments-error 'get-column-by-name
                                      "No such column."
                                      "sym" sym "tbl" tbl)
               (TableColumn tbl matching-item)))]
        [(null? (rest rels))
         (let* ([rel-tbl (Relation-to (first rels))]
                [matching-item (find-tbl-col rel-tbl sym)])
           (if (not matching-item)
               (raise-arguments-error 'get-column-by-name
                                      "No such column."
                                      "sym" sym "rel-tbl" rel-tbl)
               (RelatedColumn (first rels) (Rel matching-item))))]
        [else
         (RelatedColumn (first rels)
                        (get-column-by-name (Relation-to (first rels))
                                            sym
                                            (rest rels)))]))

;(select-from-2 tbl
;               '(id
;                 (parent id)))

(define-type Col-Arg-Item (U Symbol Col-List-Arg))
(define-type Col-Arg (Listof Col-Arg-Item))
(define-type Col-List-Arg (U Col-Rel-Arg Col-Func-Arg Col-Literal-Arg))
(define-type Col-Rel-Arg (Pairof Symbol (AtLeastOne Col-Arg-Item)))
(define-type Col-Func-Arg (Pairof '@ (Pairof Symbol (Listof Col-Arg-Item))))
(define-type Col-Literal-Arg (List '? SQL-Literal-Types))

(: select-cols (->* (Table Col-Arg)
                    ((Listof Relation))
                    (Listof Selectable)))
(define (select-cols tbl cols [prefix '()])
  (cond [(null? cols)
         '()]
        [else
         (let ([first-arg (first cols)])
           (cond [(symbol? first-arg)
                  (let ([rev-prefix (reverse prefix)])
                    (cons (if (null? rev-prefix)
                              (get-column-by-name tbl first-arg rev-prefix)
                              (get-column-by-name tbl first-arg rev-prefix))
                          (select-cols tbl (rest cols) prefix)))]
                 [(list? first-arg)
                  (append
                   (select-col-list-arg tbl first-arg prefix)
                   (select-cols tbl (rest cols) prefix))]))]))

(: select-col-list-arg (->* (Table Col-List-Arg)
                            ((Listof Relation))
                            (Listof Selectable)))
(define (select-col-list-arg tbl col-list-arg [prefix '()])
  (cond [(symbol=? '? (first col-list-arg))
         (select-literal tbl (cast col-list-arg Col-Literal-Arg) prefix)]
        [(symbol=? '@ (first col-list-arg))
         (select-func tbl (cast col-list-arg Col-Func-Arg) prefix)]
        [else
         (select-rel-col tbl (cast col-list-arg Col-Rel-Arg) prefix)]))

(: select-rel-col (->* (Table Col-Rel-Arg)
                       ((Listof Relation))
                       (Listof Selectable)))
(define (select-rel-col tbl col-list-arg [prefix '()])
  (let* ([rel-name (first col-list-arg)]
         [rel (find-tbl-item tbl rel-name)])
    (cond [(TableColumn? rel)
           (raise-arguments-error
            'select-cols
            "First symbol in nested list must match a relation name."
            "rel-name" rel-name
            "col-list-arg" col-list-arg)]
          [else
           (select-cols (Relation-to rel)
                        (rest col-list-arg)
                        (cons rel prefix))])))

(: func-mapping (HashTable Symbol
                           (->* () () #:rest Selectable AnyFunc)))
(define func-mapping
  (hash 'Equal Equal))

(: select-func (->* (Table Col-Func-Arg)
                    ((Listof Relation))
                    (List AnyFunc)))
(define (select-func tbl col-list-arg [prefix '()])
  (define fun-name (second col-list-arg))
  (define fun-args (rest (rest col-list-arg)))
  (define function
    (hash-ref func-mapping fun-name))
  (define arg-list
    (select-cols tbl fun-args prefix))
  (list (apply function arg-list)))

(: select-literal (->* (Table Col-Literal-Arg)
                       ((Listof Relation))
                       (List Any-SQL-Literal)))
(define (select-literal tbl col-list-arg [prefix '()])
  (define val (second col-list-arg))
  (list (SQL-Literal val)))

(: select-from-2 (->* (Table Col-Arg)
                      ((U False Col-Arg-Item))
                      Query))
(define (select-from-2 table columns [where #f])
  (define selections
    (select-cols table columns))
  (define query (select-from table selections))
  (if (not where)
      query
      (let ([where-clause (select-cols table (list where))])
        (cond [(not (= (length where-clause) 1))
               (raise-arguments-error
                'select-from-2
                "Where clause must return a single expression."
                "where-clause" where-clause)]
              [else
               (where query (first where-clause))]))))
