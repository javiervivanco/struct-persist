struct-persist
==============
README text here.

 ```racket

 #lang racket/base

(require struct-persist )


(struct-persistent (user users register)
                     ([name non-empty-string?]
                      [email        non-empty-string?]
                      [phone        non-empty-string?]))

;; search

(user-email: "user@example.com")
;; set
(user-name! "Joe")
;; all
(users:)
 ```