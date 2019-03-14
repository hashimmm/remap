#lang typed/racket/base

(provide (all-defined-out))

(require typed/racket/class
         racket/list
         racket/string)

(define-type TableName Symbol)
(define-type ColumnName Symbol)

(struct ColIdent ([name : ColumnName])
  #:transparent)
(struct BooleanColumn ColIdent ()
  #:transparent)
(struct UuidColumn ColIdent ()
  #:transparent)
(struct TimestampColumn ColIdent ()
  #:transparent)
(struct TextColumn ColIdent ()
  #:transparent)
(struct IntColumn ColIdent ()
  #:transparent)
(struct FloatColumn ColIdent ()
  #:transparent)

;; TODO: a primary key and unique index registry, allowing composites.
;; For us they will just be tags but the will help improve grouping
;; performance.
(: primary-key? (-> Any Boolean))
(define (primary-key? x) #f)

(: get-col (All (A) (-> (FuncParam A) A)))
(define (get-col func-param-or-qual-col)
  (cond [(TableColumn? func-param-or-qual-col)
         (get-ident func-param-or-qual-col)]
        [(RelatedColumn? func-param-or-qual-col)
         (get-ident func-param-or-qual-col)]
        [else
         (get-field returns func-param-or-qual-col)]))

(define-type (BaseFunc<%> A B)
  (Rec Func<%>
       (Class
        (init-field
         [args (Listof B)])
        (field [returns A])
        [to-str (-> (Listof
                     (U String B)))])))

(define-type (Func<%> A)
  (BaseFunc<%> A FuncAnyParam))

(define-type BoolFunc<%> (Func<%> BooleanColumn))
(define-type AnyFunc<%> (Func<%> ColIdent))

(define-type BoolFunc
  (Instance
   (Class #:implements BoolFunc<%>)))
(define-type AnyFunc
  (Instance
   (Class #:implements AnyFunc<%>)))

(define-type (BaseFunc A B)
  (Object
   (field [returns A]
          [args
           (Listof B)])
   [to-str (-> (Listof
                (U String B)))]))

(define-type (Func A)
  (BaseFunc A FuncAnyParam))

(struct SQL-Param () #:prefab)
(define sql-param (SQL-Param))

(define-type WhereClause
  (U BoolFunc
     (QualifiedColumn BooleanColumn)
     Bool-SQL-Literal))

(define-type (FuncParam A)
  (U (Func A)
     (QualifiedColumn A)))

(define-type FuncAnyParam
  (U AnyFunc QualifiedAnyColumn Any-SQL-Literal SQL-Param))

(define-type BoolFuncOrArg
  (U (QualifiedColumn BooleanColumn)
     BoolFunc
     Bool-SQL-Literal))

(struct (A) SQL-Literal ([val : A]) #:transparent)
(define-type SQL-Literal-Types
  (U Boolean String Number 'null))
(define-type Any-SQL-Literal (SQL-Literal SQL-Literal-Types))
(define-type Bool-SQL-Literal (SQL-Literal Boolean))

(define Equal%
  (class object%
    #:forall (A)
    (init-field [args : (Listof A)])
    (super-new)
    (field [returns : BooleanColumn (BooleanColumn 'eq)])
    (define/public (to-str)
      (list (first args)
            " = "
            (second args)))))

(: Equal (-> FuncAnyParam FuncAnyParam BoolFunc))
(define (Equal x y)
  (new (inst Equal% FuncAnyParam) [args (list x y)]))

(: insert-commas (All (A) (-> (Listof A) (Listof (U String A)))))
(define (insert-commas lst)
  (cond [(null? lst)
         '()]
        [else
         (cons (first lst)
               (if (null? (rest lst))
                   '()
                   (cons ","
                         (insert-commas (rest lst)))))]))

(define Coalesce%
  (class object%
    #:forall (A)
    (init-field [args : (Listof (FuncParam A))])
    (super-new)
    (field [returns : A ((inst get-col A) (first args))])
    (define/public (to-str)
      `("Coalesce("
        ,@(insert-commas args)
        ")"))))

(define #:forall (A)
  (Coalesce [val : (FuncParam A)] . [owise : (FuncParam A) *])
  (new (inst Coalesce% A)
       [args ((inst cons (FuncParam A) (Listof (FuncParam A)))
              val owise)]))

(struct (A) TableColumn
  ([qualifier : Table]
   [column : A])
  #:transparent)

(struct (A) RelatedColumn
  ([qualifier : Relation]
   [rel : (U (RelatedColumn A) (Rel A))])
  #:transparent)

;; This only exists to wrap the rel, otherwise the type of
;; RelatedColumn-rel becomes (U (RelatedColumn A) A) which is
;; ambiguous.
(struct (A) Rel ([col : A]) #:transparent)

(define-type QualifiedColumn
  (All (A)
       (U (TableColumn A)
          (RelatedColumn A))))

(define-type QualifiedAnyColumn
  (QualifiedColumn ColIdent))

(define-type RelatedAnyColumn
  (RelatedColumn ColIdent))

(: get-ident (All (A) (-> (QualifiedColumn A) A)))
(define (get-ident qual-com)
  (cond [(TableColumn? qual-com)
         (TableColumn-column qual-com)]
        [(RelatedColumn? qual-com)
         (let ([rel (RelatedColumn-rel qual-com)])
           (cond [(RelatedColumn? rel)
                  (get-ident rel)]
                 [else
                  (Rel-col rel)]))]))

(struct Relation
  ([name : TableName]
   [to : Table]
   [on : BoolFuncOrArg])
  #:transparent #:mutable)

(struct OneToOneRel Relation ()
  #:transparent #:mutable)

;; Invariants: The "on" clauses of relations must
;; always contain either TableColumns for the containing
;; table itself or RelationColumns that start with
;; one of the containing table's relations.
;; REMEMBER, DON'T USE THE "Relation-to" TABLE!
;; Refer to the relation itself.
;; Also, no two tables should have the same name.
;; Not sure how to usefully enforce that, really.
(struct Table
  ([name : TableName]
   [schema : (U False TableName)]
   [columns : (Listof ColIdent)]
   [relations : (Listof Relation)])
  #:transparent #:mutable)

(: make-table (-> TableName
                  (U False TableName)
                  (Listof ColIdent)
                  (-> Table (Listof Relation))
                  Table))
(define (make-table name schema columns relations-func)
  (define tbl
    (Table name schema columns '()))
  (define col-names
    (map ColIdent-name columns))
  (when (check-duplicates col-names)
    (raise-arguments-error
     'make-table
     "Columns must not contain duplicate names."
     "columns" columns))
  (when (member '@id col-names)
    (raise-arguments-error
     'make-table
     "Cannot use @id as a col name, reserved."
     "columns" columns))
  (define relations
    (relations-func tbl))
  (define rel-names (map Relation-name relations))
  (when (check-duplicates rel-names)
    (raise-arguments-error
     'make-table
     "Relation names must not contain duplicate names."
     "rel-names" rel-names))
  (define tbl-and-col-names (cons name col-names))
  (when (ormap (λ([rel-name : TableName])
                  (member rel-name tbl-and-col-names))
         rel-names)
    (raise-arguments-error
     'make-table
     "Relation names must not collide with table or column names."
     "tbl-and-col-names" tbl-and-col-names
     "rel-names" rel-names))
  (when (ormap (λ([name : TableName])
                 (let ([name-str (symbol->string name)])
                   (or (string-contains? name-str "/")
                       (string-contains? name-str ":")
                       (string-contains? name-str "@")
                       (string-contains? name-str "?"))))
               (append rel-names tbl-and-col-names))
    (raise-arguments-error
     'make-table
     "Relation, table, column names may not contain @, ?, / or : characters."
     "rel-names" rel-names
     "tbl-and-col-names" tbl-and-col-names))                  
  (set-Table-relations! tbl relations)
  tbl)

(: make-relation (-> TableName Table Table (-> Table Relation BoolFuncOrArg) Relation))
(define (make-relation name from-tbl to-tbl on-fun)
  (define rel (Relation name to-tbl (SQL-Literal #f)))
  (set-Relation-on! rel
                    (on-fun from-tbl rel))
  rel)

(: make-1-1-relation (-> TableName Table Table (-> Table OneToOneRel BoolFuncOrArg) OneToOneRel))
(define (make-1-1-relation name from-tbl to-tbl on-fun)
  (define rel (OneToOneRel name to-tbl (SQL-Literal #f)))
  (set-Relation-on! rel
                    (on-fun from-tbl rel))
  rel)
