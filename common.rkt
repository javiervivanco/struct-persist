#lang racket/base
(require racket/async-channel
         racket/hash
         racket/syntax
         syntax/parse
         racket/stxparam-exptime)
(provide  struct/persistent-id
          prop:struct/persistent-info struct/persistent-info? struct/persistent-info-ref
          struct/persistent-fieldname
          (struct-out struct-persistent-info)
          (struct-out struct-persistent-type)
          (struct-out struct-persistent-field)
          
          )

(struct struct-persistent-field
  (name
   contract
   nullable?
   position
   default
   index?)
  #:transparent)


(struct struct-persistent-info (ctor
                                pred
                                contracts
                                accesors
                                mutators
                                indexes
                                seeker
                                store
                                directory
                                ) #:transparent)

(define-values (prop:struct/persistent-info struct/persistent-info? struct/persistent-info-ref)
  (make-struct-type-property 'struct/persistent-info))

(struct struct-persistent-type (id  ) #:mutable  #:transparent)

(define (struct/persistent-id s)
  (struct-persistent-type-id s))

(define (struct/persistent-fieldname s name)
  (let* ([info ((struct/persistent-info-ref s))]
         [accesors (struct-persistent-info-accesors (info))]
         [acc-fieldname (hash-ref accesors name)])
    (acc-fieldname s)))


