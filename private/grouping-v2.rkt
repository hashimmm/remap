#lang typed/racket/base

;; Provides functions to group a query's results
;; such that relations are collected so that a one
;; to many join doesn't mess up the row count.

;; Designed to work with the shape of results provided
;; by the db pkg/collection.

;; Basically we use the fact that the ordering of
;; groupings and selrefs is the same as selectables
;; and result columns.

;; What we do is run two passes; first, we expand
;; each result value into the final shape.
;; Then we iterate over each row, merge things from
;; the same relations horizontally, and then for each
;; subsequent row merge vertically.

;; TODO: sql-null clearing (i.e. clear one-to-many groups
;; with all nulls).

;; TODO: the double reverse means we're probably being
;; silly somewhere, in the "merge-records" function.

;; TODO: once we add primary key and unique constraints
;; (which for us will be mere tags! we don't do DDL or anything)
;; then we should optimize here.

(provide (all-defined-out))

(module+ test
  (require typed/rackunit))

(require racket/list
         racket/string
         "query.rkt"
         "tables.rkt"
         "utils.rkt")

(require/typed db
               [sql-null? (-> Any Boolean)])

(define-type Any (U String Number Boolean))

(define-type ResultRow (Vectorof Any))
(define-type Results (Listof ResultRow))

(define-type GroupedRowItem
  (Pairof Symbol
          (U GroupedRow  ;; Row?
             GroupedResults  ;; List?
             Any)))

(define-type GroupedRow
  (Record GroupedRowItem))

(define-type UnwrappedGroupedRow
  (Listof GroupedRowItem))

(define-type GroupedResults
  (Listof GroupedRow))

(: group-rows (-> Groupings SelectableNameMap Results GroupedResults))
(define (group-rows groupings sel-ref-map results)
  (cond [(null? results)
         results]
        [else
         (merge-rows
          (expand-rows groupings sel-ref-map results))]))

(: group-query-result (-> (U PreparedQuery Query) Results GroupedResults))
(define (group-query-result pq-or-q res)
  (define pq (if (PreparedQuery? pq-or-q) pq-or-q (prepare-query pq-or-q)))
  (group-rows (PreparedQuery-groupings pq)
              (PreparedQuery-sel-refs-map pq)
              res))

(: expand-rows (-> Groupings SelectableNameMap Results GroupedResults))
(define (expand-rows groupings sel-ref-map results)
  (map (λ([row-vec : (Vectorof Any)]) : GroupedRow
         (Record
          ((inst map
                 GroupedRowItem
                 Any
                 (Pairof Selectable (Listof Relation))
                 (Pairof Selectable SelRef))
           (λ([x : Any]
              [sel-n-rels : (Pairof Selectable (Listof Relation))]
              [sel-n-ref : (Pairof Selectable SelRef)])
             : GroupedRowItem
             (expand-item x sel-n-rels sel-n-ref))
           (vector->list row-vec)
           groupings
           sel-ref-map)))
       results))

(: expand-item (-> Any
                   (Pairof Selectable (Listof Relation))
                   (Pairof Selectable SelRef)
                   GroupedRowItem))
(define (expand-item x sel-n-rels sel-n-ref)
  (define rels (cdr sel-n-rels))
  (define sel (car sel-n-rels))
  (define ref
    (if (null? rels)
        (string->symbol (last
                         (string-split
                          (SelRef-ref (cdr sel-n-ref)) ":")))
        (Relation-name (first rels))))
  (cond [(null? rels)
         (cons ref x)]
        [(OneToOneRel? (first rels))
         (cons ref
               (Record
                (list
                 (expand-item x
                              (cons sel (rest rels))
                              sel-n-ref))))]
        [else
         (cons ref
               (list
                (Record
                 (list
                  (expand-item x
                               (cons sel (rest rels))
                               sel-n-ref)))))]))

(: merge-rows (-> GroupedResults GroupedResults))
(define (merge-rows expanded-rows)
  (define res
    (for/fold : GroupedResults
      ([so-far : GroupedResults '()])
      ([current-row : GroupedRow (in-list expanded-rows)])
      (define record
        (Record
         (reverse
          (for/fold : UnwrappedGroupedRow
            ([horizontally-merged : UnwrappedGroupedRow '()])
            ([current-item : GroupedRowItem (in-list (Record-val current-row))])
            (maybe-merge-items horizontally-merged current-item)))))
      (maybe-merge-rows so-far record)))
  (reverse res))

(: maybe-merge-items (-> UnwrappedGroupedRow
                         GroupedRowItem
                         UnwrappedGroupedRow))
(define (maybe-merge-items row-so-far current-item)
  (cond [(null? row-so-far)
         (cons current-item row-so-far)]
        [else
         (define last-item (first row-so-far))
         (define last-ref (car last-item))
         (define current-ref (car current-item))
         (define last-val (cdr last-item))
         (define current-val (cdr current-item))
         (define rest-row (rest row-so-far))
         (cond [(not (equal? last-ref current-ref))
                (cons current-item row-so-far)]
               [(and (Record? last-val) (Record? current-val))
                (let ([current-sub-val (Record-val current-val)]
                      [last-sub-val (Record-val last-val)])
                  ((inst cons GroupedRowItem)
                   ((inst cons Symbol GroupedRow)
                    last-ref
                    (Record
                     (maybe-merge-items last-sub-val
                                        (first current-sub-val))))
                   rest-row))]
               [(and (list? last-val) (list? current-val))
                ((inst cons GroupedRowItem)
                 ((inst cons Symbol GroupedResults)
                  last-ref
                  (list
                   (Record
                    (maybe-merge-items (Record-val (first last-val))
                                       (first (Record-val (first current-val)))))))
                 rest-row)]
               [else
                (cons current-item row-so-far)])]))

(: check-record-equal? (-> GroupedRow GroupedRow Boolean))
(define (check-record-equal? row1 row2)
  (andmap (λ([item1 : GroupedRowItem]
             [item2 : GroupedRowItem])
            (define val1 (cdr item1))
            (define val2 (cdr item2))
            (cond [(list? val1)
                   #t]
                  [(and (Record? val1) (Record? val2))
                   (check-record-equal? val1 val2)]
                  [else
                   (equal? val1 val2)]))
          (Record-val row1)
          (Record-val row2)))

(: maybe-merge-rows (-> GroupedResults
                        GroupedRow
                        GroupedResults))
(define (maybe-merge-rows so-far new-row)
  (cond [(null? so-far)
         (cons new-row so-far)]
        [(not (check-record-equal? (car so-far) new-row))
         (cons new-row so-far)]
        [else
         (cons (merge-records (car so-far) new-row)
               (rest so-far))]))

(: merge-records (-> GroupedRow GroupedRow GroupedRow))
(define (merge-records row1 row2)
  ((inst Record GroupedRowItem)
   ((inst map GroupedRowItem GroupedRowItem GroupedRowItem)
    (λ([item1 : GroupedRowItem]
       [item2 : GroupedRowItem])
      (define val1 (cdr item1))
      (define val2 (cdr item2))
      (define ref (car item1))
      (cond [(and (list? val1) (list? val2))
             (cons ref
                   (reverse
                    ((inst foldl GroupedRow GroupedResults)
                     (λ ([new-row : GroupedRow]
                         [so-far : GroupedResults])
                       (maybe-merge-rows so-far new-row))
                     (reverse val1)
                     val2)))]
            [else
             item1]))
    (Record-val row1)
    (Record-val row2))))

(define-type HashRowVal
  (U HashRow  ;; Row?
     HashResults  ;; List?
     Any))

(define-type HashRow
  (HashTable Symbol HashRowVal))

(define-type HashResults
  (Listof HashRow))

(: grouped-results->hash-results
   (-> GroupedResults
       HashResults))
(define (grouped-results->hash-results g-res)
  (map hash-row g-res))

(: hash-row (-> GroupedRow HashRow))
(define (hash-row g-row)
  ((inst foldl GroupedRowItem HashRow)
   (λ([p : GroupedRowItem]
      [so-far : HashRow]) : HashRow
     (let* ([ref (car p)]
            [val (cdr p)]
            [processed-val
             (cond [(Record? val)
                    (hash-row val)]
                   [(list? val)
                    (grouped-results->hash-results val)]
                   [else
                    val])])
       (hash-set so-far ref processed-val)))
   #{(hash) : HashRow}
   (Record-val g-row)))
