#lang racket/base

(require remap)

(define (create-one-to-one-rel rel-name table-rel table-name ref-col)
  (define rel (OneToOneRel rel-name table-name (SQL-Literal #f)))
  (define on-clause
    (Equal
     (TableColumn table-rel
                  (UuidColumn ref-col))
     (RelatedColumn rel
                    (Rel (UuidColumn 'id)))))
  (set-Relation-on! rel on-clause)
  rel)

;; measurement units
(define Measurement-Units
  (make-table 'measurement_units 'vanilla
    (list
     (UuidColumn 'id))
    (λ(x) 
      (list))))

;; Acquirables
(define Acquirables
  (make-table 'acquirables 'vanilla
    (list
     (UuidColumn 'id)
     (UuidColumn 'measurement_unit)) ; uuid? REFERENCES measurement_units (id)
    (λ(x)
      (define rel-mu (create-one-to-one-rel 'rel-mu x Measurement-Units 'measurement_unit))
      (list rel-mu))))

(define tbls
  (hash "acquirables" Acquirables))

(define tbl->route-names-map
  (for/hash ([(name tbl) (in-hash tbls)])
    (values tbl name)))
