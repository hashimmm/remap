#lang info
(define collection "remap")
(define deps '("base"
               "db-lib"
               "typed-racket-lib"))
(define build-deps '("scribble-lib"
                     "racket-doc"
                     "rackunit-typed"))
(define scribblings '(("scribblings/remap.scrbl" ())))
(define pkg-desc
  (string-append "A 'relational mapper', as in, like an 'Object Relational Mapper' "
                 "but without the objects. "
                 "Currently generates queries specific to PostgreSQL."))
(define version "0.1")
(define pkg-authors '(hashim))
