#lang racket/base
(require
  "common.rkt"
  racket/serialize
  buid
  "index.rkt"
  "persist.rkt"
  racket/undefined
  racket/bool
  racket/file
  racket/port
  racket/format
  racket/list
  racket/async-channel
  syntax/parse/define
  racket/stxparam
  syntax/location
  racket/contract
  ;(for-meta 2 racket/base )
  (for-syntax
   racket/base
   ;;racket/stxparam-exptime
   ;;buid
   syntax/parse
   racket/list
   ;;racket/bool
   racket/syntax
   ;;syntax/parse/define
   ;;racket/contract
   ;;racket/stxparam
  ;; "common.rkt"
  ;; "persist.rkt"
  ;; "index.rkt"
   ))
(provide  struct-persistent define-fields
          (all-from-out "common.rkt")
          (all-from-out "index.rkt")
          (all-from-out "persist.rkt")
          (all-from-out  racket/undefined)
          )


(begin-for-syntax

;
;
;
;
;   ;;;;;  ;   ;  ;;;    ;;;;;  ;   ;;        ;;;;;  ;;;;;  ;;;;;  ;      ;;;
;     ;    ;   ;  ;  ;;  ;      ;   ;         ;        ;    ;      ;      ;  ;;
;     ;    ;;  ;  ;   ;  ;       ; ;;         ;        ;    ;      ;      ;   ;
;     ;    ;;; ;  ;   ;; ;       ; ;          ;        ;    ;      ;      ;   ;;
;     ;    ; ; ;  ;    ; ;;;;;    ;           ;;;;;    ;    ;;;;;  ;      ;    ;
;     ;    ; ;;;  ;    ; ;        ;;          ;        ;    ;      ;      ;    ;
;     ;    ;  ;;  ;   ;  ;       ; ;          ;        ;    ;      ;      ;   ;
;     ;    ;  ;;  ;   ;  ;      ;;  ;         ;        ;    ;      ;      ;   ;
;   ;;;;;  ;   ;  ;;;;   ;;;;;  ;   ;;        ;      ;;;;;  ;;;;;  ;;;;;  ;;;;
;
;
;
;

  (define (make-stx-index-struct-persistent  STRUCT-TYPE PLURAL STRUCT-DIR INDEX BUILD-INDEX-ARG )
    (with-syntax ([SEEK-STRUCTS      (format-id STRUCT-TYPE "~a-~a:"              PLURAL       INDEX)]
                  [STORE             (format-id STRUCT-TYPE "$~a"                 PLURAL)]
                  [SEEK-A-STRUCT     (format-id STRUCT-TYPE "~a-~a:"              STRUCT-TYPE  INDEX)]
                  [SEEK-ID%          (format-id STRUCT-TYPE "~a-~a-filter"        PLURAL       INDEX)]
                  [SEEK-ID?          (format-id STRUCT-TYPE "~a-~a?"              PLURAL       INDEX)]
                  [SEEK-ID*          (format-id STRUCT-TYPE "~a-~a*"              PLURAL       INDEX)]
                  [INDEX-FIELD-NAME  (format-id STRUCT-TYPE "index-~a-~a-name"    STRUCT-TYPE  INDEX)]
                  [APPEND-INDEX      (format-id STRUCT-TYPE "index-append-~a-~a"  STRUCT-TYPE  INDEX)]
                  [BUILD-INDEX-EXTRA BUILD-INDEX-ARG]
                  [STRUCT-DIRECTORY  STRUCT-DIR]
                  [INDEX-NAME        INDEX])
      #'(begin
          (define INDEX-FIELD-NAME (symbol->string 'INDEX-NAME))

          (define (APPEND-INDEX a-struct value)
            (index-append! a-struct INDEX-FIELD-NAME value)
            ;; puedo encolar los cambio via hilo?
            (call-with-values
             (λ () (BUILD-INDEX-EXTRA a-struct value))
             (λ args (apply index-append*! (append (list a-struct INDEX-FIELD-NAME) args)))))

          ;(PARAM-MUTATOR )

          (define (SEEK-ID*  ) (index-keys
                                #:directory STRUCT-DIRECTORY
                                #:index INDEX-FIELD-NAME))
          (define (SEEK-ID? proceduce value  #:unique [unique #f])
            (index-compare #:store STORE
                           #:directory STRUCT-DIRECTORY
                           #:index INDEX-FIELD-NAME
                           #:unique unique proceduce value))

          (define (SEEK-ID% filter  #:unique [unique #f] )
            (index-filter
             #:store STORE
             #:directory STRUCT-DIRECTORY
             #:index INDEX-FIELD-NAME
             #:unique unique filter))

          (define (SEEK-A-STRUCT value)
            (let ([result (SEEK-STRUCTS value)])
              (cond
                [(empty? result) undefined]
                [(list? result) (car result)]
                [else result])))

          (define (SEEK-STRUCTS value)
            (index-ref
             #:store STORE
             #:directory STRUCT-DIRECTORY
             #:index INDEX-FIELD-NAME
             value)))))

  (define BUILD-INDEX-DEFAULT #'(λ a '()))

  (define-syntax-class INDEX
    (pattern NAME:id  #:with build #'(λ a '()))
    (pattern (NAME:id BUILD:expr)  #:with build  #'BUILD ))

  (define (syntax->keyword stx)
    (datum->syntax #f (string->keyword (symbol->string (syntax->datum stx)))))

;
;
;
;
;   ;;;;;  ;;;;;  ;;;;;  ;      ;;;
;   ;        ;    ;      ;      ;  ;;
;   ;        ;    ;      ;      ;   ;
;   ;        ;    ;      ;      ;   ;;
;   ;;;;;    ;    ;;;;;  ;      ;    ;
;   ;        ;    ;      ;      ;    ;
;   ;        ;    ;      ;      ;   ;
;   ;        ;    ;      ;      ;   ;
;   ;      ;;;;;  ;;;;;  ;;;;;  ;;;;
;
;
;
;

  (define-splicing-syntax-class FIELD
    (pattern
     (~seq (~datum #:with) ID:id)
     #:with value #`(#,(syntax-parameter-value #'ID))
     #:with NAME    (syntax-parse #'value [(field:FIELD) #'field.NAME])
     #:with expr    (syntax-parse #'value [(field:FIELD) #'[field.NAME field.type/c field.DEFAULT]])
     #:with type/c  (syntax-parse #'value [(field:FIELD) #'field.type/c])
     #:with default (syntax-parse #'value [(field:FIELD) #'field.default])
     #:with DEFAULT (syntax-parse #'value [(field:FIELD) #'field.DEFAULT])
     #:with key     (syntax-parse #'value  [(field:FIELD) #'field.key]))

    (pattern NAME:id
             #:with DEFAULT  #'null
             #:with expr     #'[NAME any/c DEFAULT]
             #:with type/c   #'any/c
             #:with key      (syntax->keyword #'NAME)
             #:with default  #'[NAME null]
             )

    (pattern (NAME:id TYPE/c:expr )

             #:with DEFAULT  #'null
             #:with expr     #'(NAME TYPE/c DEFAULT)
             #:with type/c   #'TYPE/c
             #:with key      (syntax->keyword #'NAME)
             #:with default  #'[NAME null]
             )
    (pattern (NAME:id TYPE/c:expr DEFAULT:expr)

             #:with expr     #'[NAME TYPE/c DEFAULT]
             #:with type/c   #'TYPE/c
             #:with key      (syntax->keyword #'NAME)
             #:with default  #'[NAME DEFAULT]))

  (define-syntax-class TYPE
    (pattern ID:id
             #:with expr #'ID
             #:with plural (format-id #'ID "~as" #'ID)
             #:with prefix #'make)
    (pattern (ID:id PLURAL:id)
             #:with expr #'ID
             #:with plural #'PLURAL
             #:with prefix #'make )
    (pattern (ID:id PLURAL:id PREFIX:id)
             #:with expr #'ID
             #:with plural #'PLURAL
             #:with prefix #'PREFIX)))

(define-syntax (define-fields STX)
  (syntax-parse STX
    [(_  ([(ID:id) FIELD:FIELD] ...))
     #`(begin (define-syntax-parameter ID #'FIELD.expr) ...)]
    [(_  (FIELD:FIELD ...))
     #`(begin (define-syntax-parameter FIELD.NAME  #'FIELD.expr) ...)]))

;
;
;
;
;     ;;  ;;;;;;; ;;;    ;   ;    ;;  ;;;;;;;
;   ;;  ;    ;    ;  ;;  ;   ;  ;;  ;    ;
;   ;   ;;   ;    ;   ;  ;   ;  ;   ;;   ;
;   ;        ;    ;   ;  ;   ;  ;        ;
;    ;;;     ;    ;  ;;  ;   ;  ;        ;
;      ;;    ;    ;;;;   ;   ;  ;        ;
;       ;;   ;    ;  ;   ;   ;  ;   ;;   ;
;   ;   ;;   ;    ;   ;  ;   ;  ;   ;    ;
;    ;;;;    ;    ;   ;;  ;;;    ;;;     ;
;
;
;
;

(define-syntax-parser struct-persistent
  [(_ TYPE:TYPE  (FIELD:FIELD ...)  )
   #'(struct-persistent TYPE (FIELD.expr ...) ())]
  [(_ TYPE:TYPE  (FIELD:FIELD ...)   (INDEX:INDEX ...))

   #:with SUFIX-RAW       #'*
   #:with PLURAL          #'TYPE.plural
   #:with STRUCT-TYPE     #'TYPE.ID
   #:with PREFIX-MAKE     #'TYPE.prefix
   #:with PREDICATE       (format-id #'STRUCT-TYPE "~a?"            #'STRUCT-TYPE )
   #:with STRUCT-INIT!    (format-id #'STRUCT-TYPE "~a-init!"       #'STRUCT-TYPE )
   #:with STRUCT-INFO     (format-id #'STRUCT-TYPE "~a-info"        #'STRUCT-TYPE)
   #:with STRUCT-DIRECTORY(format-id #'STRUCT-TYPE "~a-directory"   #'STRUCT-TYPE)
   #:with STRUCT-CHANNEL  (format-id #'STRUCT-TYPE "~a-channel"     #'STRUCT-TYPE)

   #:with TOTAL              (format-id #'STRUCT-TYPE "~a->total"   #'PLURAL)
   #:with ALL-IDS            (format-id #'STRUCT-TYPE "~a->id"      #'PLURAL)
   #:with SEEK-ALL           (format-id #'STRUCT-TYPE "~a:"         #'PLURAL)
   #:with STORE              (format-id #'STRUCT-TYPE "$~a"         #'PLURAL)
   #:with TAKE-DROP          (format-id #'STRUCT-TYPE "~a-offset/limit:" #'PLURAL)
   #:with LIST-OF-STRUCT     (format-id #'STRUCT-TYPE "~a?"         #'PLURAL)
   #:with FIELD-LIST-STRUCT  (format-id #'STRUCT-TYPE "~a%"         #'PLURAL)
   #:with FIELD-STRUCT       (format-id #'STRUCT-TYPE "~a%"         #'STRUCT-TYPE)

   #:with STRUCT-INDEXES  (format-id #'STRUCT-TYPE "~a-index"       #'STRUCT-TYPE)

   #:with SEEK-BY-ID      (format-id #'STRUCT-TYPE "~a:"            #'STRUCT-TYPE)
   #:with REMOVE-BY-ID    (format-id #'STRUCT-TYPE "~a/~~!"          #'STRUCT-TYPE)
   #:with DESERIALIZE     (format-id #'STRUCT-TYPE "deserialize-~a" #'STRUCT-TYPE)
   #:with CTOR-KW-ID      (format-id #'STRUCT-TYPE "~a-~a"          #'PREFIX-MAKE #'STRUCT-TYPE)
   #:with CTOR-KW         (append (list #'CTOR-KW-ID #'#:id #'[id (buid)])
                                  (flatten (map (λ a a)
                                                (syntax->list #'(FIELD.key ...))
                                                (syntax->list #'(FIELD.default  ...)))))

   #:with CTOR            (format-id #'STRUCT-TYPE "~a-~a~a"    #'PREFIX-MAKE #'STRUCT-TYPE #'SUFIX-RAW)
   #:with ACCESOR         (format-id #'STRUCT-TYPE "~a-ref"     #'STRUCT-TYPE)
   #:with ACCESOR-ID      (format-id #'STRUCT-TYPE "~a-id"      #'STRUCT-TYPE)
   #:with MUTATOR         (format-id #'STRUCT-TYPE "~a-mutator" #'STRUCT-TYPE)
   #:with MUTATOR-ID!     (format-id #'STRUCT-TYPE "~a-id~a!"   #'STRUCT-TYPE #'SUFIX-RAW)
   #:with REINDEX-STRUCT! (format-id #'STRUCT-TYPE "reindex-~a!"#'STRUCT-TYPE)


   #:with (FIELD-ACCESOR ...)
   (map (λ (A-FIELD)
          (format-id #'STRUCT-TYPE "~a-~a" #'STRUCT-TYPE A-FIELD ))
        (syntax->list #'(FIELD.NAME ...)))

   #:with (FIELD-MUTATOR-RAW! ...)
   (map (λ (A-FIELD)
          (format-id A-FIELD "~a-~a~a!"  #'STRUCT-TYPE A-FIELD #'SUFIX-RAW ))
        (syntax->list #'(FIELD.NAME ...)))
   #:with (APPEND-INDEX ...)
   (map (λ (A-FIELD)
          (format-id A-FIELD "index-append-~a-~a" #'STRUCT-TYPE A-FIELD  ))
        (syntax->list #'(INDEX.NAME ...)))
   #:with (FIELD-MUTATOR! ...)
   (map (λ (A-FIELD)
          (format-id #'STRUCT-TYPE  "~a-~a!" #'STRUCT-TYPE A-FIELD ))
        (syntax->list #'(FIELD.NAME ...)))
   #:with (FIELD-MUTATOR!/ID ...)
   (map (λ (A-FIELD)
          (format-id #'STRUCT-TYPE  "~a-~a!/id" #'STRUCT-TYPE A-FIELD ))
        (syntax->list #'(FIELD.NAME ...)))
   #:with (SEEK-BY-FIELD ...)
   (map (λ (A-FIELD)
          (format-id #'STRUCT-TYPE  "~a-~a:"  #'PLURAL A-FIELD ))
        (syntax->list #'(INDEX.NAME ...)))
   #:with (INDEXES ...)
   (map (λ (STX-INDEX BUILD-INDEX)
          (make-stx-index-struct-persistent #'STRUCT-TYPE #'PLURAL #'STRUCT-DIRECTORY STX-INDEX BUILD-INDEX))
        (syntax->list #'(INDEX.NAME ...))
        (syntax->list #'(INDEX.build ...))
        )
   #:with (INDEX-FIELD ...)
   (map (λ (INDEX) (format-id #'STRUCT-TYPE "index-~a-~a"  #'STRUCT-TYPE  INDEX))
        (syntax->list #'(INDEX.NAME ...)))
   (with-syntax ([(POS ...)   (range 0  (length (syntax-e #'(FIELD.NAME ...))))])

     #`(begin

         (define STRUCT-DIRECTORY
           (build-path
            (let ([dir (syntax-source-directory #'STRUCT-TYPE)])
              (if (false? dir)
                  (make-temporary-file "rkttmp~a" 'directory)
                  dir))
            (~a "~" (symbol->string 'PLURAL)) ))
         (define STRUCT-CHANNEL   (make-async-channel))

         (begin INDEXES ...)
         (define (ACCESOR-ID s)      (struct-persistent-type-id s))

         (define (MUTATOR-ID! s id)  (set-struct-persistent-type-id! s id ))
         (provide ACCESOR-ID MUTATOR-ID!)
         ;
         ;
         ;
         ;                 ;;
         ;    ;           ;
         ;   ;;    ;;;   ;;;;   ;;
         ;    ;    ;  ;   ;    ;  ;
         ;    ;    ;  ;   ;    ;  ;
         ;    ;    ;  ;   ;    ;  ;
         ;   ;;;;  ;  ;   ;    ;;;
         ;
         ;
         ;

         (define (STRUCT-INFO)
           (struct-persistent-info
            CTOR
            PREDICATE
            (make-hash (list (cons 'FIELD.NAME FIELD.type/c )   ... ))
            (make-hash (list (cons 'id ACCESOR-ID)
                             (cons 'FIELD.NAME FIELD-ACCESOR ) ... ))
            (make-hash (list (cons 'id MUTATOR-ID!)
                             (cons 'FIELD.NAME FIELD-MUTATOR! ) ... ))
            (make-hash (list (cons '*  (hash 'seek SEEK-ALL))
                             (cons 'id (hash 'seek SEEK-BY-ID))
                             (cons 'INDEX.NAME (hash 'seek SEEK-BY-FIELD 'append APPEND-INDEX)) ... ))
            SEEK-BY-ID
            STORE
            STRUCT-DIRECTORY))
         (define DESERIALIZE
           (make-deserialize-info
            (λ (id FIELD.default ...)
              ;;;;;;
              ;;;;;;
              ;;;;;;
              ;;;;;;
              (CTOR id
                    (if (linked-struct? FIELD.NAME)
                        (linked-struct-recover FIELD.NAME)
                        FIELD.NAME)...
                    )
              )
            (λ ()
              (define dummy (CTOR 'transporter-error  POS ...))
              (values dummy
                      (λ (id  FIELD.default ...)
                        (MUTATOR-ID! dummy id )
                        ;(set-struct-persistent-type-version! dummy version)
                        (FIELD-MUTATOR-RAW! dummy (if (linked-struct? FIELD.NAME)
                                                      (linked-struct-recover FIELD.NAME)
                                                      FIELD.NAME) )... )))))
         (provide DESERIALIZE)
         (define-values (STRUCT-TYPE CTOR PREDICATE ACCESOR MUTATOR)
           (make-struct-type
            'STRUCT-TYPE ;; name
            struct:struct-persistent-type ;;super-type
            (length '( FIELD.NAME ...)) ;;init-field-cnt
            0 ;;auto-field-cnt
            #f ;;auto-v
            (list
             (cons prop:struct/persistent-info STRUCT-INFO)
             (cons prop:serializable
                   (make-serialize-info
                    (λ (this)
                      (vector (struct-persistent-type-id this)
                              (let ([value (FIELD-ACCESOR this)])
                                  (if (struct? value)
                                  (make-linked-struct value)
                                   value
                              )) ...
                              ))
                    #'DESERIALIZE
                    #t
                    (or (current-load-relative-directory) (current-directory)))
                   ));;props
            #f;;inspector
            #f;;proc-spec
            '();;immutables
            (struct-guard/c buid/c FIELD.type/c ... );;guard
            'CTOR;;constructor-name
            ))
         (provide STRUCT-TYPE CTOR PREDICATE ACCESOR MUTATOR)


         (define FIELD-ACCESOR      (make-struct-field-accessor ACCESOR POS 'FIELD.NAME)) ...
         (provide FIELD-ACCESOR) ...
         (define FIELD-MUTATOR-RAW! (make-struct-field-mutator  MUTATOR POS 'FIELD.NAME)) ...
         (begin
           (begin
             (provide FIELD-MUTATOR!) ...
             (define (FIELD-MUTATOR! s v)
               (FIELD-MUTATOR-RAW! s v)
               ;(async-channel-put STRUCT-CHANNEL (cons 'store s))
               (struct-persist-set! #:store STORE s)
               (async-channel-put STRUCT-CHANNEL (cons 'index-field (vector s 'FIELD.NAME v)))) ...)

           (define (FIELD-MUTATOR!/ID id v)
             (let ([s (SEEK-BY-ID id)])
               (if (PREDICATE s)
                   (FIELD-MUTATOR! s v)
                   (raise-argument-error 'id "valid id" id)))) ...)

         ;
         ;
         ;
         ;
         ;   ;;;; ;;;;;  ;;;   ;;;;        ; ;; ; ; ;
         ;  ;   ;   ;   ;   ;  ;  ;        ; ;  ; ; ;
         ;  ;       ;   ;   ;  ; ;;        ;;    ;;;;
         ;  ;       ;   ;   ;  ;;;         ; ;   ; ;;
         ;   ;  ;   ;    ;  ;  ; ;         ; ;   ; ;;
         ;   ;;;    ;    ;;;   ;  ;        ;  ;  ; ;
         ;
         ;
         ;

         (define CTOR-KW
           (let ([a-struct (CTOR id FIELD.NAME ...)])
             (struct-persist-set! #:store STORE a-struct)
             (index-idx-append! a-struct)
             (REINDEX-STRUCT! a-struct)
             a-struct))

         (provide CTOR-KW-ID)
         (define (SEEK-BY-ID id [fail-result undefined])
           (struct-persist-id->struct-persist  #:store STORE STRUCT-DIRECTORY id undefined))
         (provide SEEK-BY-ID)

         (define (REMOVE-BY-ID s)
           (struct-persist-off! s #:store STORE))

         (provide REMOVE-BY-ID)
         (define (ALL-IDS)
           (struct-persist-all-ids STRUCT-DIRECTORY))

         (provide ALL-IDS)
         (define (TOTAL)
           (struct-persist-count STRUCT-DIRECTORY))
         (provide TOTAL)

         (define (SEEK-ALL #:offset [Offset 0] #:limit [Limit (TOTAL)])
           (TAKE-DROP  (ALL-IDS) #:map SEEK-BY-ID #:limit Limit #:offset Offset ))
         (provide SEEK-ALL)

         (define (TAKE-DROP a-list #:map [a-map (λ (a) a)]
                                   #:offset [Offset 0]
                                   #:limit [Limit (TOTAL)])
           (let* ([~count           (struct-persist-count STRUCT-DIRECTORY)]
                  [Limit            (if (> Limit ~count ) ~count  Limit)]
                  [Offset           (if (> Offset ~count) ~count Offset )]
                  [drop-list        (drop  a-list Offset)]
                  [drop-list-length (length drop-list)]
                  [drop-limit       (if (> Limit drop-list-length) drop-list-length Limit)])
             (if (empty? drop-list)
                 '()
                 (map a-map (take drop-list drop-limit)))))

         (define (REINDEX-STRUCT! a-struct [field #f] [value null])
           (let ([indexes  (struct-persistent-info-indexes  (STRUCT-INFO))]
                 [accesors (struct-persistent-info-accesors (STRUCT-INFO))])
             (if (false? field)
                 (map (λ (field)
                        (when (hash-has-key? (hash-ref indexes field) 'append)
                          (REINDEX-STRUCT! a-struct field ((hash-ref accesors field) a-struct) )))
                      (hash-keys indexes))
                 (when (and (hash-has-key? indexes field)
                            (hash-has-key? (hash-ref indexes field) 'append))
                   ((hash-ref (hash-ref indexes field) 'append) a-struct value)))))

         (define (STRUCT-INIT!)
           (define (next) (async-channel-get STRUCT-CHANNEL))

           (define (process)
             (let loop ([data (next)])
               (let ([action (car data) ]
                     [data   (cdr data)])
                 (case action
                   [(store)       (struct-persist-set! #:store STORE data)
                                  (index-idx-append! data)]
                   [(index)       (REINDEX-STRUCT! data) ]
                   [(index-field)
                    (index-remove! (vector-ref data 0) (symbol->string (vector-ref data 1)))
                    (REINDEX-STRUCT! (vector-ref data 0)
                                     (vector-ref data 1)
                                     (vector-ref data 2))]))
               (loop (next))))
           (thread  process)
           (void))
         (STRUCT-INIT!)
         (define LIST-OF-STRUCT (listof PREDICATE))
         (provide LIST-OF-STRUCT)
         (define STORE (make-hash))
         (define-fields ([(FIELD-LIST-STRUCT)  [PLURAL LIST-OF-STRUCT]]
                         [(FIELD-STRUCT)       [STRUCT-TYPE PREDICATE]]))
         ;(collect-garbage 'incremental)
         ))])
