#lang racket
(require  "common.rkt"
          "persist.rkt"
          racket/undefined
          racket/serialize
          racket/date
          (for-syntax
           racket/syntax
           syntax/parse)
          net/base64
          )
(require racket/trace)
(provide (all-defined-out))
(date-display-format  'iso-8601)
#|
http://www.plantuml.com/plantuml/uml/VPA_ReCm4CRtUmhBKpiWXBj6KVgdGzkbOze56RWaLemTsKOAVVe6cs8WD4o--txVSUVpnZfkN5DBi4uRlaV2S-jCKxgCN3vk2RqBCJcXiLfniYhMZq_F6NjOfSNwde9G3isEbqXwOougRmmKlQhCRbJUVdt_NvdzLGuK1JZWcU-r8aTiCwzyO4lU6wxeM6A3z8a6BOLIIucb4rfP8gfphdHT6FJKsykMXF0QmveBIGRCXnHW9oGIATGncumzfDcqvvsM5HfoukwG2rNX6Qq7-bj8Wp_j9Rh4ZniEntIP2nrtc3zTA028UqYIzOI7SJ1LurzlG-6NqodxxSGHUpRjUmyBcpdOBKVOOnmbytt4zpURCJwVkILkVq48eqngBJhOt14PhtJpB-GO07HIl4CFTieanG2L4wubjgtZCtjLalyWiQ8kj1Ibfj3peU00m-3owN24XRr6LoI6NaAf8AsIP7pL6rHLKyjV
|#
(define (scalar->string v)
  (cond
    [(string? v)    (~a "s" v)]
    [(number? v)    (~a "n" (number->string v))]
    [(boolean? v)   (~a "b" (if v "true" "false"))]
    [(char? v)      (~a "c" (char->integer v))]
    [(symbol? v)    (~a "S" (symbol->string v))]
    [(keyword? v)   (~a "k" (keyword->string v))]
    [(empty?  v)    "e"]
    [(date*? v)     (date->string v #t)]
    [(list? v)      (call-with-output-string (λ (out) (write v out)))]
    [else            #f]))

(define (index-key->string v)
  (cond
    [(scalar->string  v)]
    [(struct-persistent-type? v ) (~a "%" (struct/persistent-id v))
                                   #;(string-replace
                                   (path->string
                                    (struct-persist->path:dir v)) "/" "%")]
    [else                         (raise-argument-error 'v "value" v)]))
(module+ test
  (require rackunit )
  (check-equal? "e" (index-key->string '()))
  (check-equal? "(1 2)" (index-key->string '(1 2)))
  (check-equal? "btrue" (index-key->string #t))
  (check-equal? "bfalse" (index-key->string #f))
  (check-equal? "Shola" (index-key->string 'hola))
  (check-equal? "khola" (index-key->string '#:hola))
  (check-equal? "c97" (index-key->string #\a))
  (check-equal? "shola" (index-key->string "hola"))
  (check-equal? "n12" (index-key->string 12))
  (check-equal? "n12.2" (index-key->string 12.2))
  )
(define (string->index-key v)
  (define check (string-ref v 0))
  (define value (substring v 1))
  (cond
    [(eq? check #\s)  value]
    [(eq? check #\e)  null]
    [(eq? check #\n)  (string->number value)]
    [(eq? check #\v)  (equal? value "true")]
    [(eq? check #\c)  (integer->char (string->number value))]
    [(eq? check #\S)  (string->symbol value)]
    [(eq? check #\k)  (string->keyword value)]
    [(eq? check #\()  (read (open-input-string v))]
    [(eq? check #\%)  (substring v 1)]
    [else (raise-argument-error 'v "value" v)]))

(define (index-key->path #:directory a-struct-directory index-name key)
  (build-path (struct-directory-index-name->path a-struct-directory index-name)
              (index-key->string key)))

(define (struct-directory-index-name->path a-struct-directory index-name)
  (build-path a-struct-directory "indexes" index-name))

(define (struct->path:indexes a-struct)
  (build-path (struct-type->path:dir a-struct) "indexes"))

(define (struct->path:index-dir a-struct index-name )
  (build-path (struct->path:indexes a-struct) index-name ))

(define (index-key-struct->path:dir a-struct index-name a-key )
  (build-path (struct->path:indexes a-struct)
              index-name
              (index-key->string a-key)))

(define (index-key-struct->path:link a-struct index-name a-key )
  (build-path (index-key-struct->path:dir a-struct index-name a-key )
              (struct/persistent-id a-struct)))

(define (index-idx-append! a-struct)
  (let* ([idx-dir   (build-path (struct-type->path:dir a-struct) "indexes")]
         [idx-file  (build-path idx-dir "ids")])
    (unless (directory-exists? idx-dir) (make-directory* idx-dir))
    (call-with-output-file idx-file #:exists 'append
      (lambda (out)
        (displayln (struct-persistent-type-id a-struct) out)))))

(define (index-append! a-struct index-name a-key)
  (let ([struct-type-idx-dir (struct->path:index-dir a-struct index-name)]
         [file-indexes        (index-key-struct->path:dir a-struct index-name a-key )])
    (unless (directory-exists? (struct->path:indexes a-struct)) (struct->path:indexes a-struct))
    (unless (directory-exists? struct-type-idx-dir)      (make-directory* struct-type-idx-dir))
    ;;(println (~a index-name " " (index-key->string a-key)))
    (call-with-output-file file-indexes #:exists 'append
      (lambda (out)
        (displayln (struct/persistent-id a-struct) out)))))

(define (index-append*! reference index-name . keys)
  (for/list ([k keys]) (index-append!  reference index-name k)))

(define (index-key? #:directory struct-persist-dir index-name a-key)
  (file-exists? (index-key->path #:directory struct-persist-dir index-name a-key)))

(define (index-key->struct-ids  #:directory struct-persist-dir index-name a-key)
  (port->lines (open-input-file (index-key->path #:directory struct-persist-dir index-name a-key))))

(define (index-key-ref #:directory struct-persist-dir
                       #:index     index-name
                       #:store store
                       key [fail-result undefined])
  (define (load id) (struct-persist-id->struct-persist #:store store struct-persist-dir id  fail-result))

  (if (index-key? #:directory struct-persist-dir index-name key)
      (map load (index-key->struct-ids  #:directory struct-persist-dir index-name key))
      fail-result))
;;deberia enviar los id para poder realizar offset.
(define (index-remove! a-struct index-name)
  (let* ([struct-idx-dir      (build-path (struct-persist->path:dir a-struct) index-name )]
         [struct-idx-file     (build-path struct-idx-dir "index")])

    (when (file-exists? struct-idx-file)
      (call-with-input-file struct-idx-file
        (lambda (in)
          (let loop ([file (read-line in)])
            (unless (eof-object? file)
              (when (file-exists? file) (delete-file file))
              (loop (read-line in)))))))))


#|
(define (index-search-term term index-name [fail-result undefined])
  (let* ([key   (index-key->string term)])
    (hash-ref index-name key fail-result)))
|#

(define (index-ref
         #:directory struct-directory
         #:index index-name
         #:store store
         term [fail-result undefined])

  (index-key-ref  #:directory struct-directory
                  #:index     index-name
                  #:store store
                  term fail-result))


(define (index-keys  #:directory struct-directory
                     #:index index-name)
  (map string->index-key
       (map
        path->string
        (directory-list (struct-directory-index-name->path struct-directory index-name)))))

(define (index-compare comp value
                       #:directory struct-directory
                       #:index index-name
                       #:store store
                       #:unique [unique #f])
  (let* ([filter-func     (λ (key)  (with-handlers ([exn:fail? (λ (e) #f)]) (comp key value))) ]
         [ref             (λ (term) (index-ref term #:store store #:directory struct-directory #:index index-name))]
         [filter-structs  (map ref (filter filter-func  (index-keys  #:directory struct-directory
                                                                     #:index index-name)) )]
         [result          (flatten filter-structs)])
    (if unique
        (remove-duplicates result)
        result)))

(define (index-filter filter?
                      #:directory struct-directory
                      #:index index-name
                      #:store store
                      #:unique [unique #f])
  (let* ([filter-func    (λ (key)  (with-handlers ([exn:fail? (λ (e) #f)]) (filter? key )))]
         [filter-structs (filter filter-func (index-keys))]
         [ref            (λ (term) (index-ref term #:store store #:directory struct-directory #:index index-name))]
         [result         (flatten (map ref filter-structs))])
    (if unique
        (remove-duplicates result)
        result)))

(module+ test
  (require rackunit base64)
  (check-equal? "e" (index-key->string '()))
  (check-equal? "(1 2)" (index-key->string '(1 2)))
  (check-equal? "btrue" (index-key->string #t))
  (check-equal? "bfalse" (index-key->string #f))
  )