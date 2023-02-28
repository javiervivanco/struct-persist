#lang racket/base
(require  "common.rkt"
          racket/list
          racket/port
          racket/file
          racket/serialize
          (only-in racket/format ~a)
          racket/undefined)

(provide (all-defined-out))

(define (struct-persist-all-ids a-struct-persist-dir)
  (let ([a-file (build-path a-struct-persist-dir "indexes/ids") ])
    (if (file-exists? a-file)
        (remove-duplicates (port->list read-line (open-input-file a-file)))
        '())))

(define (struct-persist-count a-struct-persist-dir)
  (length (struct-persist-all-ids a-struct-persist-dir)))

(define (struct-persist-values #:store store a-struct-persist-dir)
  (map  (λ (id) (struct-persist-id->struct-persist #:store store  a-struct-persist-dir id ))
        (struct-persist-all-ids a-struct-persist-dir)))

(define (struct-persist-set!  #:store store s)
  (let* ([struct-dir  (struct-persist->path:dir s)]
         [latest      (struct-persist->path:latest s)]
         [archive     (struct-persist->path:archive s)])

    (unless (directory-exists? struct-dir) (make-directory* struct-dir))
    ;(when   (link-exists? latest )         (delete-file latest))
    (define (save out)
      (writeln (serialize s ) out)
      (unless (hash-has-key? store (struct/persistent-id s)) (hash-set! store (struct/persistent-id s) s))
      )
    (call-with-file-lock/timeout
     latest 'exclusive #:max-delay 0.2
     (λ ()
       #;(call-with-file-lock/timeout archive 'exclusive #:max-delay 200
                                      (λ ()
                                        ;;@TODO aca va el historio de cambios
                                        ))
       (call-with-output-file #:exists 'replace latest save)
       (let* ([version   (number->string (add1 (length (directory-list struct-dir ))))]
              [new-file  (build-path struct-dir version)])
         (call-with-output-file #:exists 'replace new-file save)
         ))
     (λ () (printf "Failed to obtain lock for file\n")))))

(define (struct-persist-off! s #:store store)
  (rename-file-or-directory
   (struct-persist->path:dir s)
   (build-path (struct-type->path:dir s) (~a "~" (struct/persistent-id s))))
   (hash-remove! store (struct/persistent-id s))
  )
  
(define (struct-persist->path:latest s)
  (build-path (struct-persist->path:dir s) "latest.rktd"))

(define (struct-persist->path:archive s)
  (build-path (struct-persist->path:dir s) "archive.rktd"))

(define (struct-persist->path:dir s)
  (build-path (struct-type->path:dir s)  (struct/persistent-id s)))

(define (struct-type->path:dir s)
  (let* ([info       (struct/persistent-info-ref s)]
         [directory  (struct-persistent-info-directory (info))])
    (build-path directory )))

(define (file->struct-persist latest)
  (define (reader-data data)
    (let* ([data     (read (open-input-string data))]
           [a-struct (deserialize data)])
      a-struct))
    (if (file-exists? latest)
        (let* ([in (open-input-file latest)]
               [data (read-line in)])
          (close-input-port in)
          (if (eof-object? data)
              undefined
              (reader-data data)))
        undefined))

(define (struct-persist-id->struct-persist #:store store a-struct-persist-dir struct-id [fail-result undefined])
  (define (reader-data data)
    (let* ([data     (read (open-input-string data))]
           [a-struct (deserialize data)])
      ;;
      ;; Cache?
      ;;
      (hash-set! store struct-id a-struct)
      ;;
      ;;
      ;;
      a-struct))
  (let* ([struct-dir  (build-path a-struct-persist-dir  struct-id)]
         [latest      (build-path struct-dir "latest.rktd") ])
    (if (file-exists? latest)
        (let* ([in (open-input-file latest)]
               [data (read-line in)])
          (close-input-port in)
          (if (eof-object? data)
              fail-result
              (reader-data data)))
        fail-result)))

(define (linked-struct? value)
  (and (pair? value)
       (not (null? value))
       (pair? (car value))
       (equal? '~~linked (caar value))))

(define (make-linked-struct value)
  (cons (cons '~~linked (struct-persist->path:latest value)) (struct/persistent-id value) ))

(define (linked-struct-recover value)
  (when (linked-struct? value)
    (let* ([file   (cdar value)]
           [idx    (cdr value)]
           [s      (file->struct-persist file)]
           [info   ((struct/persistent-info-ref s))]
           [store  (struct-persistent-info-store info)]
           [seeker (struct-persistent-info-seeker info)]
           )
      (if (hash-has-key? store idx)
          (hash-ref store idx)
          (seeker idx)))))