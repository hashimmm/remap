#lang typed/racket/base

(provide (all-defined-out))

(require racket/list)

(module+ test
  (require typed/rackunit))

(define-type (AtLeastOne A) (Pairof A (Listof A)))
(define-type (AtLeastTwo A) (Pairof A (Pairof A (Listof A))))

(: debug (All (A) (-> String A A)))
(define (debug s x)
  (printf "~a: ~a~n" s x)
  x)

(struct NotFound () #:transparent)

(: extractf (All (A) (-> (-> A (U False Any)) (Listof A) (Values (U NotFound A) (Listof A)))))
(define (extractf pred lst)
  (let loop ([so-far : (Listof A) '()]
             [remaining lst])
    (cond [(null? remaining)
           (values (NotFound) lst)]
          [else
           (let ([next (car remaining)])
             (cond [(pred next)
                    (values next
                            (append (reverse so-far) (cdr remaining)))]
                   [else
                    (loop (cons next so-far)
                          (cdr remaining))]))])))

(module+ test
  (let-values ([(found-or-not rem-list)
                (extractf (λ(x) (equal? x 12))
                          '(10 11 12 13 14))])
    (check-equal? found-or-not 12)
    (check-equal? rem-list '(10 11 13 14)))
  (let-values ([(found-or-not rem-list)
                (extractf (λ(x) (equal? x 10))
                          '(10 11 12 13 14))])
    (check-equal? found-or-not 10)
    (check-equal? rem-list '(11 12 13 14)))
  (let-values ([(found-or-not rem-list)
                (extractf (λ(x) (equal? x 14))
                          '(10 11 12 13 14))])
    (check-equal? found-or-not 14)
    (check-equal? rem-list '(10 11 12 13)))
  (let-values ([(found-or-not rem-list)
                (extractf (λ(x) (equal? x 15))
                          '(10 11 12 13 14))])
    (check-true (NotFound? found-or-not))
    (check-equal? rem-list '(10 11 12 13 14))))

;; Turns out this is almost the same as assoc (facepalm)
;; and I could've used dict-ref (but assoc is typed, so better)
(: alist-ref (All (A B) (-> (U (Listof (Pairof A B)) (Record (Pairof A B)))
                            A
                            B)))
(define (alist-ref mapping key)
  (let
      ([found
        (findf (λ([x : (Pairof A B)])
                 (equal? key ((inst car A B) x)))
               (if (list? mapping) mapping (Record-val mapping)))])
    (if found
        ((inst cdr A B) found)
        (raise-arguments-error
         'alist-ref
         "key not in mapping"
         "key" key
         "mapping" mapping))))

(: alist-update (All (A B)
                     (-> (Listof (Pairof A B))
                         A
                         (-> B B)
                         B
                         (Listof (Pairof A B)))))
(define (alist-update mapping key updater fallback)
  (cond [(null? mapping)
         (list (cons key (updater fallback)))]
        [(equal? (car (first mapping)) key)
         (cons (cons key (updater (cdr (first mapping))))
               (rest mapping))]
        [else
         (cons (first mapping)
               (alist-update (rest mapping) key updater fallback))]))

(: alist-row-update (All (A B)
                         (-> (Record (Pairof A B))
                             A
                             (-> B B)
                             B
                             (Record (Pairof A B)))))
(define (alist-row-update row key updater fallback)
  (Record
   (let ([mapping (Record-val row)])
     (cond [(null? mapping)
            (list (cons key (updater fallback)))]
           [(equal? (car (first mapping)) key)
            (cons (cons key (updater (cdr (first mapping))))
                  (rest mapping))]
           [else
            (cons (first mapping)
                  (alist-update (rest mapping) key updater fallback))]))))

(: alist-map-values (All (A B C)
                         (-> (Listof (Pairof A B))
                             (-> B C)
                             (Listof (Pairof A C)))))
(define (alist-map-values mapping fun)
  (map (λ([k-v : (Pairof A B)])
         (cons (car k-v)
               (fun (cdr k-v))))
       mapping))

(: alist-row-map-values (All (A B C)
                             (-> (Record (Pairof A B))
                                 (-> B C)
                                 (Record (Pairof A C)))))
(define (alist-row-map-values row fun)
  (Record
   (map (λ([k-v : (Pairof A B)])
          (cons (car k-v)
                (fun (cdr k-v))))
        (Record-val row))))

;; This guy's purpose is to save us from the issue of pairs and lists.
;; Vectors are painful too, but we may switch to them for performance later.
(struct (A) Record ([val : (Listof A)]) #:transparent)

(: row-map (All (A B) (-> (-> A B) (Record A) (Record B))))
(define (row-map fun row)
  (Record (map fun (Record-val row))))

(: row-map2 (All (A B C) (-> (-> A B C) (Record A) (Record B) (Record C))))
(define (row-map2 fun row row2)
  (Record (map fun (Record-val row) (Record-val row2))))

(: row-append (All (A) (-> (Record A) (Record A) (Record A))))
(define (row-append row1 row2)
  (Record (append (Record-val row1) (Record-val row2))))

(: my-partition
   (All (a b c)
        (-> (-> c Any : #:+ a #:- b)
            (Listof c)
            (Values (Listof a) (Listof b)))))
(define (my-partition fun lst)
  (for/fold
   : (Values (Listof a) (Listof b))
    ([tru : (Listof a) '()]
     [fal : (Listof b) '()])
    ([x : c (in-list lst)])
    (if (fun x)
        (values (cons x tru) fal)
        (values tru (cons x fal)))))

(: make-groups (All (A B)
                    (-> (Record A)
                        (-> A B)
                        (Listof (Pairof B (Record A))))))
(define (make-groups lst grouper)
  (for/fold : (Listof (Pairof B (Record A)))
              ([collected : (Listof (Pairof B (Record A))) '()])
              ([val : A (in-list (Record-val lst))])
    (define belongs-to-group (grouper val))
    (alist-update collected
                  belongs-to-group
                  (λ([x : (Record A)])
                    (row-cons val x))
                  (Record '()))))

(: row-cons (All (A) (-> A (U (Listof A) (Record A)) (Record A))))
(define (row-cons val row)
  (Record (cons val (if (list? row) row (Record-val row)))))
