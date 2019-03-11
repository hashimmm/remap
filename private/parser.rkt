#lang brag

;(select tbl
;        (id
;         (parent id))))

; We need to go from:

;(select tbl
;        (id
;         ?#f
;         (parent id)
;         {string_concat first_name last_name}))
;        {Equal (parent id) ?"qw"})

; To:

;(select tbl
;        (id
;         (? #f)
;         (parent id)
;         {@ string_concat first_name last_name}))
;        {@ Equal (parent id) (? "qw")})

select-exp : "(select" table-name
                       list-of-col-exp
                       where-clause? ")"
table-name : WORD
list-of-col-exp : "(" col-exp* ")"
col-exp : (col-name | rel-col-arg | literal | fun)
rel-col-arg : "(" rel-name SEP col-exp+ ")"
rel-name : WORD
literal : LITERAL
fun : "{" WORD col-exp* "}"
where-clause : col-exp
col-name : WORD
