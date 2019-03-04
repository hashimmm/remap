#lang typed/racket/base

;; DEPRECATED... Kinda, at least.
;; Also, currently returns lists in reverse order.
;; (except for the root list)
;; The tests have been "fixed" to let that pass for now.

(provide (all-defined-out))

(require "query.rkt"
         "utils.rkt"
         racket/list
         "tables.rkt")

(require/typed db
               [sql-null? (-> Any Boolean)])

(: to-sql-with-groupings (-> Query (Values String Groupings)))
(define (to-sql-with-groupings query)
  (define prep-q (prepare-query query))
  (define rendered-stmt (to-sql prep-q))
  (values rendered-stmt (PreparedQuery-groupings prep-q)))

(define-type TaggedResults (Listof (Row (Pairof Selectable Any))))
(define-type Results (Listof (Vectorof Any)))

(: tag-results (-> Results (Listof Selectable) TaggedResults))
(define (tag-results res cols)
  (map (λ([row : (Vectorof Any)])
         (let ([row-list (vector->list row)])
           (when (not (equal? (length row-list) (length cols)))
             (raise-arguments-error
              'name-results
              "Result vectors must be of same length as columns."
              "row-list" row-list
              "cols" cols))
           (Row
            (map (λ([item : Any] [tag : Selectable])
                   (cons tag item))
                 row-list
                 cols))))
       res))

;; TODO:
;; You can see we're not nesting lists inside 1-1's, we always make
;; the list grouping separate.
;; I am not sure if this is the best design choice. What it does is
;; it makes considerably flatter results for deep nesting,
;; but the structure is more dynamic and less predictable, which may
;; offset the advantages we give the user. But only usage will tell.

(define-type GroupedRow
  (Row (U (Pairof Selectable Any)
          (Pairof (Listof Relation)
                  (Row (Pairof Selectable Any)))
          (Pairof (Listof Relation)
                  GroupedResults))))
(define-type GroupedResults
  (Listof GroupedRow))

(: group-rows (-> TaggedResults Groupings GroupedResults))
(define (group-rows all-rows groupings)
  ; sort groupings by relation length, ascending
  ; skim the top for nulls and only-1-to-1's, split the rest.
  ; This skimmed set of groupings is "statics".
  ; in the whole set of rows
  ; - take a set of rows that has same values for "statics".
  ; - make one row out of these, by:
  ;   - taking values for "statics" from just one row.
  ;   - For 1-1's, gather by relation into single rows, so they become nested rows.
  ;   - if there aren't any groupings left, we're DONE.
  ;   - OTHERWISE
  ;   - To deal with the remaining columns
  ;     - take the first of the remaining groupings
  ;     - group all groupings starting with this same relation under this one
  ;       - (should work because already ordered by ascending length)
  ;     - take the remaining groupings, make similar groups of groupings.
  ;     - For each group of grouping, split the rows by selecting only corresponding columns
  ;     - Now we have basically a set of rows and groupings...
  ;     - So just RECUR/LOOP into the original function for them all.
  ;     - Put each result into a list in the original row, keyed against the relation prefix.
  ; END.
  (define (drop-1-1s [rels : (Listof Relation)]) : (Listof Relation)
    (dropf rels (λ([x : Relation]) (OneToOneRel? x))))
  (define (split-sorted-groups-by-static-and-rest
           [groups : Groupings]) : (Values Groupings Groupings)
    (cond [(null? groups)
           (values null null)]
          [else
           (splitf-at
            groups
            (λ([g : (Pairof Selectable (Listof Relation))])
              (null? (drop-1-1s (cdr g)))))]))
  (define sorted-groupings
    (sort groupings (λ([x : (Pairof Selectable (Listof Relation))]
                       [y : (Pairof Selectable (Listof Relation))])
                      (< (length (drop-1-1s (cdr x)))
                         (length (drop-1-1s (cdr y)))))))
  (: extract-col-values (-> (Row (Pairof Selectable Any))
                            (Listof Selectable)
                            (Listof Any)))
  (define (extract-col-values row cols)
    (map (λ([sel : Selectable])
           (alist-ref row sel))
         cols))
  (: grouper (-> TaggedResults Groupings GroupedResults))
  (define (grouper rows all-groupings)
    (cond [(null? rows)
           '()]
          [(or (null? all-groupings)
               (null? (rest all-groupings)))
           rows]
          [else
           (define-values (static-groupings rest-groupings)
             (split-sorted-groups-by-static-and-rest
              all-groupings))
           (define static-cols (map (inst car Selectable (Listof Relation))
                                    static-groupings))
           (let*-values
               ([(first-row) (first rows)]
                [(static-vals)
                 (extract-col-values first-row static-cols)]
                [(matching-set non-matching)
                 (splitf-at
                  rows
                  (λ([row : (Row (Pairof Selectable Any))])
                    (let ([relevant-values
                           (extract-col-values row static-cols)])
                      (andmap equal? relevant-values static-vals))))])
             ((inst cons GroupedRow)
              (process-matching-set matching-set
                                    static-cols
                                    static-vals
                                    static-groupings
                                    rest-groupings)
              (grouper non-matching all-groupings)))]))
  (: process-matching-set
     (-> TaggedResults
         (Listof Selectable)
         (Listof Any)
         Groupings
         Groupings
         GroupedRow))
  (define (process-matching-set matching-set
                                static-cols
                                static-vals
                                static-groups
                                remaining-groups)
    (define row-statics
      (map (inst cons Selectable Any) static-cols static-vals))
    (define-values (row-simple row-1-1)
      (for/fold : (Values
                   (Listof (Pairof Selectable Any))
                   (Listof 
                    (Pairof (Listof Relation) (Row (Pairof Selectable Any)))))
        ([collected-simple : (Listof (Pairof Selectable Any))
                           '()]
         [collected-1-1 : (Listof 
                           (Pairof (Listof Relation) (Row (Pairof Selectable Any))))
                        '()])
        ([sel-n-val (in-list row-statics)])
        (define sel (car sel-n-val))
        (define matching-rel (alist-ref static-groups sel))
        (cond [(null? matching-rel)
               (values (cons sel-n-val collected-simple)
                       collected-1-1)]
              [else
               (values
                collected-simple
                ((inst alist-update (Listof Relation) (Row (Pairof Selectable Any)))
                 collected-1-1
                 matching-rel
                 (λ([sel-n-vals : (Row (Pairof Selectable Any))])
                   (row-cons sel-n-val sel-n-vals))
                 (Row '())))])))
    (define normalized-row-statics
      (Row (append row-simple row-1-1)))
    (cond [(null? remaining-groups)
           normalized-row-statics]
          [else
           (: common-groups (Listof (Pairof (Listof Relation) Groupings)))
           (define common-groups
             (let loop
               ([#{collected : (Listof (Pairof (Listof Relation) Groupings))}
                 '()]
                [sel-n-rels remaining-groups])
               (cond [(null? sel-n-rels) collected]
                     [else
                      (define next-sel-rel (first sel-n-rels))
                      (define prefix (cdr next-sel-rel))
                      (define-values (all-related-to-next remaining-sel-rels)
                        ((inst my-partition
                               (Pairof Selectable (Listof Relation))
                               (Pairof Selectable (Listof Relation))
                               (Pairof Selectable (Listof Relation)))
                         (λ([sel-rel : (Pairof Selectable (Listof Relation))])
                           ((inst list-prefix? Relation Relation) prefix (cdr sel-rel)))
                         sel-n-rels))
                      (loop ((inst cons (Pairof (Listof Relation) Groupings))
                             (cons prefix
                                   all-related-to-next)
                             collected)
                            remaining-sel-rels)])))
           (: sel-and-new-prefix Groupings)
           (define sel-and-new-prefix
             ((inst foldl (Pairof (Listof Relation) Groupings) Groupings)
              (λ([pref-n-groups : (Pairof (Listof Relation) Groupings)]
                 [so-far : Groupings])
                (define pref (car pref-n-groups))
                (define groups (cdr pref-n-groups))
                ((inst append (Pairof Selectable (Listof Relation)))
                 so-far
                 ((inst map (Pairof Selectable (Listof Relation))
                        (Pairof Selectable (Listof Relation)))
                  (λ([sel-n-rels : (Pairof Selectable (Listof Relation))])
                    (define sel (car sel-n-rels))
                    (cons sel pref))
                  groups)))
              '()
              common-groups))
           (: rows-by-prefix (Listof (Pairof (Listof Relation) TaggedResults)))
           (define rows-by-prefix
             (for/fold : (Listof (Pairof (Listof Relation) TaggedResults))
               ([collected : (Listof (Pairof (Listof Relation) TaggedResults)) '()])
               ([row : (Row (Pairof Selectable Any)) (in-list matching-set)])
               (define partitioned-row
                 (make-groups (Row (filter (λ([sel-n-val : (Pairof Selectable Any)])
                                             (not (member (car sel-n-val) static-cols)))
                                           (Row-val row)))
                              (λ([sel-n-val : (Pairof Selectable Any)])
                                (alist-ref sel-and-new-prefix (car sel-n-val)))))
               (foldl (λ([pref-n-row-part : (Pairof (Listof Relation)
                                                    (Row (Pairof Selectable Any)))]
                         [last-collected : (Listof (Pairof (Listof Relation) TaggedResults))])
                        (alist-update last-collected
                                      (car pref-n-row-part)
                                      (λ([res : TaggedResults])
                                        (cons (cdr pref-n-row-part)
                                              res))
                                      '()))
                      collected
                      partitioned-row)))
           (: grouper-args (Listof (List (Listof Relation) TaggedResults Groupings)))
           (define grouper-args
             (map (λ([pref-n-res : (Pairof (Listof Relation) TaggedResults)])
                    (define res (cdr pref-n-res))
                    (define pref (car pref-n-res))
                    (list pref
                          res
                          (alist-map-values (alist-ref common-groups pref)
                                            (λ([rels : (Listof Relation)])
                                              (drop rels (length pref))))))
                  rows-by-prefix))
           (: grouping-results (Listof (Pairof (Listof Relation) GroupedResults)))
           (define grouping-results
             ((inst map (Pairof (Listof Relation) GroupedResults)
                    (List (Listof Relation) TaggedResults Groupings))
              (λ([pref-n-res-n-groups : (List (Listof Relation) TaggedResults Groupings)])
                (cons (first pref-n-res-n-groups)
                      (let ([group-res
                             (grouper (second pref-n-res-n-groups)
                                      (third pref-n-res-n-groups))])
                        (if (and (equal? (length group-res) 1) (andmap sql-null?
                                                                       (Row-val (first group-res))))
                            '()
                            group-res))))
              grouper-args))
           ((inst row-append (U (Pairof Selectable Any)
                                (Pairof (Listof Relation)
                                        (Row (Pairof Selectable Any)))
                                (Pairof (Listof Relation)
                                        GroupedResults)))
            normalized-row-statics (Row grouping-results))]))
  (grouper all-rows sorted-groupings))

(: group-results (-> Results Groupings GroupedResults))
(define (group-results results groupings)
  (define tagged-results
    (tag-results results (map (inst car Selectable (Listof Relation)) groupings)))
  (group-rows tagged-results groupings))

;; TODO: Had we been using sel-refs instead of selectables when tagging
;; results, we wouldn't have had to drag a selectable name map along here.

;; The "A" is for Symbol or String but otherwise this has to be
;; the same as GroupedRow/GroupedResults
(define-type NamedRow (All (A)
                           (Row (U (Pairof A Any)
                                   (Pairof A (Row (Pairof A Any)))
                                   (Pairof A NamedResults)))))
(define-type (NamedResults A) (Listof (NamedRow A)))

(: name-row (-> GroupedRow SelectableNameMap (NamedRow Symbol)))
(define (name-row row sel-ref-map)
  (Row
   (for/list : (Listof (U (Pairof Symbol Any)
                          (Pairof Symbol (Row (Pairof Symbol Any)))
                          (Pairof Symbol (NamedResults Symbol))))
     ([val-pair (in-list (Row-val row))])
     (let ([key (car val-pair)]
           [val (cdr val-pair)])
       (cond [(list? key) ;; listof relation, val is a listof rows or listof row
              (cons (string->symbol (rel-path->name key))
                    (cond [(null? val)
                           (ann '() (NamedResults Symbol))]
                          [(Row? val)
                           (name-row val sel-ref-map)]
                          [else
                           (name-grouped-results val sel-ref-map)]))]
             [else ;; Selectable
              (cons (string->symbol
                     (SelRef-ref (alist-ref sel-ref-map key)))
                    val)])))))

(: name-grouped-results
   (-> GroupedResults SelectableNameMap
       (NamedResults Symbol)))
(define (name-grouped-results g-res sel-ref-map)
  (map (λ([g : GroupedRow]) (name-row g sel-ref-map)) g-res))

(: name-pq-results (-> PreparedQuery Results (NamedResults Symbol)))
(define (name-pq-results pq res)
  (name-grouped-results (group-results res (PreparedQuery-groupings pq))
                        (PreparedQuery-sel-refs-map pq)))

(: name-query-results (-> Query Results (NamedResults Symbol)))
(define (name-query-results q res)
  (define pq (prepare-query q))
  (name-pq-results pq res))
