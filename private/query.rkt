#lang typed/racket/base

(provide (all-defined-out))

(require "tables.rkt"
         "utils.rkt"
         racket/list
         racket/string
         typed/racket/class)

(struct JoinGraph
  ([val : Relation]
   [children : (Listof JoinGraph)])
  #:transparent)

(struct Query
  ([selectables : (Listof Selectable)]
   [from : Table]
   [joins : (Listof JoinGraph)]
   [where : (U WhereClause False)])
  #:transparent)
(define-type Selectable FuncAnyParam)

;; TODO: Defining a "Grouping" type bugs the type system up.
(define-type Groupings (Listof (Pairof Selectable (Listof Relation))))

(struct PreparedQuery
  ([query : Query]
   [groupings : Groupings]
   [sel-refs-map : SelectableNameMap]
   [rel-refs : RelationNameMap])
  #:transparent)

(: select-from (-> Table (Listof QualifiedAnyColumn) Query))
(define (select-from table columns)
  (for/fold ([query (Query '() table '() #f)])
            ([col (in-list columns)])
    (include query col)))

(: add-select (-> Query Selectable Query))
(define (add-select query sel)
  (struct-copy Query query
               [selectables (cons sel (Query-selectables query))]))

(: extract-rel-path (-> RelatedAnyColumn (AtLeastOne Relation)))
(define (extract-rel-path rcol)
  (cons (RelatedColumn-qualifier rcol)
        (let ([rel (RelatedColumn-rel rcol)])
          (cond [(Rel? rel)
                 '()]
                [else
                 (extract-rel-path rel)]))))

(: create-graph-from-list (-> (AtLeastOne Relation) JoinGraph))
(define (create-graph-from-list rels)
  (cond [(null? (cdr rels))
         (JoinGraph (car rels) '())]
        [else
         (JoinGraph (car rels)
                    (list (create-graph-from-list (cdr rels))))]))

(: ensure-path (-> Query (AtLeastOne Relation) Query))
(define (ensure-path query rels)
  (let*-values ([(graphs) (Query-joins query)]
                [(first-rel rest-rels)
                 (values (car rels) (cdr rels))]
                [(found-jg rem-graphs)
                 (extractf (λ([g : JoinGraph])
                             (equal? first-rel (JoinGraph-val g)))
                           graphs)])
    (if (NotFound? found-jg)
        (struct-copy Query query
                     [joins (cons (create-graph-from-list rels)
                                  (Query-joins query))])
        (struct-copy Query query
                     [joins (cons (merge-into-graph found-jg rest-rels)
                                  rem-graphs)]))))

(: merge-into-graph (-> JoinGraph (Listof Relation) JoinGraph))
(define (merge-into-graph jg rels)
  (cond [(null? rels)
         jg]
        [else
         (let*-values
             ([(graphs) (JoinGraph-children jg)]
              [(first-rel rest-rels)
               (values (car rels) (cdr rels))]
              [(found-jg rem-graphs)
               (extractf (λ([g : JoinGraph])
                           (equal? first-rel (JoinGraph-val g)))
                         graphs)])
           (if (NotFound? found-jg)
               (struct-copy JoinGraph jg
                            [children (cons (create-graph-from-list rels)
                                            rem-graphs)])
               (struct-copy JoinGraph jg
                            [children
                             (cons (merge-into-graph found-jg
                                                     rest-rels)
                                   rem-graphs)])))]))

(: include (-> Query QualifiedAnyColumn Query))
(define (include query qc)
  (when (member qc (Query-selectables query))
    (raise-arguments-error
              'include
              "Cannot include same column twice."
              "qc" qc
              "query" query))
  (cond [(TableColumn? qc)
         (if (equal? (Query-from query)
                     (TableColumn-qualifier qc))
             (add-select query qc)
             (raise-arguments-error
              'include
              "TableColumn must be of same table as query."
              "TableColumn-qualifier" (TableColumn-qualifier qc)
              "Query-from" (Query-from query)))]
        [(RelatedColumn? qc)
         (let* ([rel-path (extract-rel-path qc)]
                [query-with-path (ensure-path query rel-path)])
           (add-select query-with-path qc))]))

(: where (-> Query WhereClause Query))
(define (where query where-func)
  (cond [(TableColumn? where-func)
         (if (equal? (Query-from query)
                     (TableColumn-qualifier where-func))
             (struct-copy Query query
                          [where where-func])
             (raise-arguments-error
              'includef
              "TableColumn must be of same table as query."
              "TableColumn-qualifier" (TableColumn-qualifier where-func)
              "Query-from" (Query-from query)))]
        [(RelatedColumn? where-func)
         (let* ([rel-path (extract-rel-path where-func)]
                [query-with-path (ensure-path query rel-path)])
           (struct-copy Query query-with-path
                        [where where-func]))]
        [(SQL-Literal? where-func)
         (struct-copy Query query
                      [where where-func])]
        [else
         (let ([qcs (WhereFunc->Columns where-func)])
           (for/fold : Query
             ([new-q : Query (struct-copy Query query
                                          [where where-func])])
             ([col : QualifiedAnyColumn (in-list qcs)])
             (cond [(TableColumn? col)
                    new-q]
                   [(RelatedColumn? col)
                    (let ([rel-path (extract-rel-path col)])
                      (ensure-path new-q rel-path))])))]))

(define-type RelationNameMap (Listof (Pairof (Listof Relation) String)))

;; Traverse the list of join graphs, and assign a unique name to each node.
(: gen-name-map (-> (Listof JoinGraph) RelationNameMap))
(: flatten-jg (-> JoinGraph (Listof (Listof Relation))))
(define (flatten-jg jg)
  (let ([current-rel (JoinGraph-val jg)])
    (cons (list current-rel)
          ((inst map (Listof Relation) (Listof Relation))
           (λ([rel-list : (Listof Relation)])
             ((inst cons Relation) current-rel rel-list))
           ((inst append-map (Listof Relation) JoinGraph)
            flatten-jg (JoinGraph-children jg))))))
(define (gen-name-map jg-lst)
  (define path-list
    ((inst append-map (Listof Relation) JoinGraph)
     flatten-jg jg-lst))
  (for/list : RelationNameMap
    ([path : (Listof Relation)
           (in-list path-list)])
    (cons path (rel-path->name path))))

(: rel-path->name (-> (Listof Relation) String))
(define (rel-path->name rel-path)
  (string-join
   (for/list : (Listof String)
     ([r (in-list rel-path)])
     (symbol->string (Relation-name r)))
   "/"))

(define (escape-ident-symbol [x : Symbol]) : String
  (format "\"~a\"" (string-replace (symbol->string x) "\"" "\"\"")))

(define (escape-ident-string [x : String]) : String
  (format "\"~a\"" (string-replace x "\"" "\"\"")))

(define (escape-text [x : String]) : String
  (format "'~a'" (string-replace x "'" "''")))

(: TableColumn->RelatedColumn
   (All (A) (-> (TableColumn A)
                (AtLeastOne Relation)
                (RelatedColumn A))))
(define (TableColumn->RelatedColumn tbl-col prefix)
  (RelatedColumn
   (car prefix)
   (let ([remaining (cdr prefix)])
     (if (null? remaining)
         (Rel (TableColumn-column tbl-col))
         (TableColumn->RelatedColumn tbl-col remaining)))))

(: add-prefix
   (All (A) (-> (RelatedColumn A)
                (AtLeastOne Relation)
                (RelatedColumn A))))
(define (add-prefix rel-col prefix)
  (RelatedColumn
   (car prefix)
   (let ([remaining (cdr prefix)])
     (if (null? remaining)
         rel-col
         (add-prefix rel-col remaining)))))

(: get-rel-name-and-col-name (-> RelatedAnyColumn RelationNameMap (Values String String)))
(define (get-rel-name-and-col-name rel-col mapping)
  (let ([rel-name (alist-ref mapping (extract-rel-path rel-col))]
        [col-name (ColIdent-name (get-ident rel-col))])
    (values rel-name (symbol->string col-name))))

(: get-escaped-rel-name-and-col-name (-> RelatedAnyColumn RelationNameMap (Values String String)))
(define (get-escaped-rel-name-and-col-name rel-col mapping)
  (let-values
      ([(rel-name col-name)
        (get-rel-name-and-col-name rel-col mapping)])
    (values
     (escape-ident-string rel-name)
     (escape-ident-string col-name))))

;; Now that we have a map of relations to names, render selectables.
(: render-sel (->* (Selectable RelationNameMap)
                   ((Listof Relation))
                   String))
(define (render-sel sel mapping [prefix '()])
  (cond [(SQL-Literal? sel)
         (let ([lit-val (SQL-Literal-val sel)])
           (cond [(boolean? lit-val)
                  (if lit-val
                      "true"
                      "false")]
                 [(string? lit-val)
                  (escape-text lit-val)]
                 [(number? lit-val)
                  (number->string lit-val)]))]
        [(TableColumn? sel)
         (if (null? prefix)
             (format "~a.~a"
                     (escape-ident-symbol
                      (Table-name
                       (TableColumn-qualifier sel)))
                     (escape-ident-symbol
                      (ColIdent-name
                       (TableColumn-column sel))))
             ;; Apply prefix and then render WITHOUT prefix.
             (render-sel (TableColumn->RelatedColumn sel prefix)
                         mapping
                         '()))]
        [(RelatedColumn? sel)
         (if (null? prefix)
             (let-values
                 ([(rel-name col-name)
                   (get-escaped-rel-name-and-col-name sel mapping)])
               (format "~a.~a" rel-name col-name))
             ;; Apply prefix and then render WITHOUT prefix
             (render-sel (add-prefix sel prefix)
                         mapping
                         '()))]
        [else
         (string-join
          (map (λ([x : (U String FuncAnyParam)])
                 (cond [(string? x) x]
                       [else
                        (render-sel x mapping prefix)]))
               (send sel to-str))
          "")]))

(: get-sel-repr-and-alias
   (->* (Selectable RelationNameMap)
        (Integer)
        (Values String String)))
(define (get-sel-repr-and-alias sel mapping
                                (num-for-func-or-literal 1))
  (cond [(SQL-Literal? sel)
         (let ([repr
                (render-sel sel mapping)])
           (values repr
                   (format
                    "F~a"
                    num-for-func-or-literal)))]
        [(TableColumn? sel)
         (values (render-sel sel mapping)
                 (symbol->string
                  (ColIdent-name (TableColumn-column sel))))]
        [(RelatedColumn? sel)
         (values
          (render-sel sel mapping)
          (let-values
              ([(rel-name col-name)
                (get-rel-name-and-col-name sel mapping)])
             (format "~a:~a" rel-name col-name)))]
        [else
         (values (render-sel sel mapping)
                 (format
                  "F~a"
                  num-for-func-or-literal))]))

(struct SelRef
  ([str : String]
   [ref : String])
  #:transparent)

(: render-sel-ref (-> SelRef String))
(define (render-sel-ref sel-ref)
  (format "~a AS ~a"
          (SelRef-str sel-ref)
          (escape-ident-string
           (SelRef-ref sel-ref))))

(define-type SelectableNameMap
  (Listof (Pairof Selectable SelRef)))

(: generate-sels-map
   (-> (Listof Selectable)
       RelationNameMap
       SelectableNameMap))
(define (generate-sels-map sels mapping)
  (for/list : SelectableNameMap
    ([sel (in-list sels)]
     [num (in-naturals)])
    (let-values ([(sel-str sel-ref)
                  (get-sel-repr-and-alias
                   sel mapping
                   (+ num 1))])
      (cons
       sel
       (SelRef sel-str
               sel-ref)))))

(: render-sel-refs (-> SelectableNameMap
                       String))
(define (render-sel-refs sel-map)
  (string-join
   (map (λ([sel-ref : SelRef]) : String
          (render-sel-ref sel-ref))
        (map (inst cdr Selectable SelRef)
             sel-map))
   "\n, "))

(: render-where (-> WhereClause
                    RelationNameMap
                    String))
(define (render-where where mapping)
  (cond [(or (SQL-Literal? where)
             (TableColumn? where)
             (RelatedColumn? where))
         (render-sel where mapping)]
        [else
         (string-join
          (map (λ([x : (U String WhereParam)])
                 (cond [(string? x) x]
                       [(SQL-Param? x)
                        "?"]
                       [(or (SQL-Literal? x)
                            (TableColumn? x)
                            (RelatedColumn? x))
                        (render-sel x mapping)]
                       [else
                        (render-where x mapping)]))
               (send where to-str))
          "")]))

(: render-tbl-name (-> Table String))
(define (render-tbl-name tbl)
  (let ([schema (Table-schema tbl)])
    (if schema
        (format "~a.~a"
                (escape-ident-symbol
                 schema)
                (escape-ident-symbol
                 (Table-name tbl)))
        (escape-ident-symbol (Table-name tbl)))))

(: render-joins (-> RelationNameMap String))
(define (render-joins mapping)
  (string-join
   (for/list : (Listof String)
     ([rel-list-and-alias (in-list mapping)])
     (let* ([rel-list (car rel-list-and-alias)]
            [alias (cdr rel-list-and-alias)]
            [reversed (reverse rel-list)]
            [rel-to-render (last rel-list)]
            [prefix (reverse (cdr reversed))]
            [tbl (Relation-to rel-to-render)])
       (format "LEFT OUTER JOIN ~a AS ~a ON ~a"
               (render-tbl-name tbl)
               (escape-ident-string alias)
               (render-sel
                (Relation-on rel-to-render)
                mapping
                prefix))))
   "\n"))

(: Func->Columns (-> AnyFunc
                     (Listof QualifiedAnyColumn)))
(define (Func->Columns fun)
  (let*-values
      ([(args) (get-field args fun)]
       [(non-func-args func-args)
        ((inst my-partition
               (U QualifiedAnyColumn Any-SQL-Literal)
               AnyFunc
               FuncAnyParam)
         (λ([x : FuncAnyParam])
           (or (TableColumn? x)
               (RelatedColumn? x)
               (SQL-Literal? x)))
         args)]
       [(col-args)
        (filter (λ([x : (U Any-SQL-Literal QualifiedAnyColumn)])
                  (or (TableColumn? x) (RelatedColumn? x)))
                non-func-args)])
    (cond [(null? func-args)
           col-args]
          [else
           (append
            col-args
            ((inst append-map QualifiedAnyColumn AnyFunc)
             Func->Columns func-args))])))

(: WhereFunc->Columns (-> WhereFunc
                          (Listof QualifiedAnyColumn)))
(define (WhereFunc->Columns fun)
  (let*-values
      ([(args) (get-field args fun)]
       [(non-func-args func-args)
        ((inst my-partition
               (U QualifiedAnyColumn
                  Any-SQL-Literal
                  SQL-Param)
               WhereFunc
               WhereParam)
         (λ([x : WhereParam])
           (or (TableColumn? x)
               (RelatedColumn? x)
               (SQL-Literal? x)
               (SQL-Param? x)))
         args)]
       [(col-args)
        (filter (λ([x : (U QualifiedAnyColumn
                           Any-SQL-Literal
                           SQL-Param)])
                  (or (TableColumn? x)
                      (RelatedColumn? x)))
                non-func-args)])
    (cond [(null? func-args)
           col-args]
          [else
           (append
            col-args
            ((inst append-map QualifiedAnyColumn WhereFunc)
             WhereFunc->Columns func-args))])))

(: make-groupings
   (-> (Listof Selectable)
       (Listof JoinGraph)
       Groupings))
(define (make-groupings sels
                        graphs)
  ;; Basically we clear out functions by turning them
  ;; into their most specific column
  (define (make-grouping-prefixes [sels : (Listof Selectable)]) : Groupings
    (map
     (λ([x : Selectable]) : (Pairof Selectable (Listof Relation))
       (cond [(or (TableColumn? x) (SQL-Literal? x))
              (cons x '())]
             [(RelatedColumn? x)
              (cons x
                    (extract-rel-path x))]
             [else
              (let* ([func-cols (Func->Columns x)]
                     [sorted
                      (sort
                       func-cols
                       (λ([low : (U TableColumn RelatedColumn)]
                          [hi : (U TableColumn RelatedColumn)])
                         (cond [(TableColumn? low) #f]
                               [(TableColumn? hi) #t]
                               [else
                                (> (length (extract-rel-path low))
                                   (length (extract-rel-path hi)))])))])
                (cond [(null? sorted)
                       (cons x '())]
                      [else
                       (let ([repr-col
                              (car sorted)])
                         (cond [(TableColumn? repr-col)
                                (cons x '())]
                               [else
                                (cons x
                                      (extract-rel-path repr-col))]))]))]))
     sels))
  (define grouping-prefix-map (make-grouping-prefixes sels))
  grouping-prefix-map)

(: prepare-query (-> Query PreparedQuery))
(define (prepare-query query)
  (define rel-name-mapping
    (gen-name-map (Query-joins query)))
  (define sel-sel-ref-mapping
    (generate-sels-map (Query-selectables query)
                       rel-name-mapping))
  (define groupings
    (make-groupings (Query-selectables query)
                    (Query-joins query)))
  (PreparedQuery query
                 groupings
                 sel-sel-ref-mapping
                 rel-name-mapping))

(: to-sql (-> (U Query PreparedQuery) String))
(define (to-sql pq-or-q)
  (define pq
    (cond [(Query? pq-or-q)
           (prepare-query pq-or-q)]
          [else pq-or-q]))
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
                (render-where where-clause
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
  rendered-stmt)
