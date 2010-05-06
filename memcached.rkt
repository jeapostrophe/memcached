#lang racket
(require "binary.rkt")

;; Pool Interface

(struct memcached-pool (servers))

(define (memcached . ss)
  (define servers
    (match-lambda
      [(list) empty]
      [(list-rest s (list-rest p ss))
       (list* (connect s p) (servers ss))]))
  (memcached-pool (servers ss)))

(struct conn (from to))

(define (connect s p)
  (define-values (from to) (tcp-connect s p))
  (conn from to))

(define (memcached-pool-comm! mp thnk)
  ; XXX use pool
  (define conn (first (memcached-pool-servers mp)))
  (parameterize ([current-input-port (conn-from conn)]
                 [current-output-port (conn-to conn)])
    (thnk)))

(define no-flags #"\x00\x00\x00\x00")
(define empty-cas #"\0\0\0\0\0\0\0\0")

;; Command interface
(define-syntax-rule
  (define-command (id . args) e ...)
  (define (id mp . args)
    (memcached-pool-comm! mp (λ () e ...))))

(define-command (memcached-get k)
  (write-get* 'Get k) (read-get*))
(define-command (memcached-set! k v #:expiration [exp 0] #:cas [cas empty-cas])
  (write-set* 'Set k v no-flags exp cas) (read-set*))
(define-command (memcached-add! k v #:expiration [exp 0])
  (write-set* 'Add k v no-flags exp empty-cas) (read-set*))
(define-command (memcached-replace! k v #:expiration [exp 0] #:cas [cas empty-cas])
  (write-set* 'Replace k v no-flags exp cas) (read-set*))
(define-command (memcached-delete! k #:cas [cas empty-cas])
  (write-delete* 'Delete k cas) (read-delete*))
(define-command (memcached-incr! k #:amount [amt 1] #:initial [init #f] #:expiration [exp 0] #:cas [cas empty-cas])
  (write-incr* 'Increment k amt (or init 0) (and init exp) empty-cas) (read-incr*))
(define-command (memcached-decr! k #:amount [amt 1] #:initial [init #f] #:expiration [exp 0] #:cas [cas empty-cas])
  (write-incr* 'Decrement k amt (or init 0) (and init exp) empty-cas) (read-incr*))
(define-command (memcached-append! k v #:cas [cas empty-cas])
  (write-append* 'Append k v cas) (read-append*))
(define-command (memcached-prepend! k v #:cas [cas empty-cas])
  (write-append* 'Prepend k v cas) (read-append*))

;;; Interface
(define key? bytes?)
(define value? bytes?)
(define (cas? x)
  (and (bytes? x) (= (bytes-length x) 8)))
(define uint4? exact-nonnegative-integer?)
(define uint8? exact-nonnegative-integer?)

(provide/contract
 [memcached-pool? (any/c . -> . boolean?)]
 ; XXX
 [memcached (() () #:rest list? . ->* . memcached-pool?)]
 [memcached-get (memcached-pool? key? . -> . (values (or/c false/c value?) cas?))]
 [memcached-set! ((memcached-pool? key? value?) (#:expiration uint4? #:cas cas?) . ->* . (or/c false/c cas?))]
 [memcached-add! ((memcached-pool? key? value?) (#:expiration uint4?) . ->* . (or/c false/c cas?))]
 [memcached-replace! ((memcached-pool? key? value?) (#:expiration uint4? #:cas cas?) . ->* . (or/c false/c cas?))]
 [memcached-delete! ((memcached-pool? key?) (#:cas cas?) . ->* . boolean?)]
 ; XXX I don't understand the initial argument value
 [memcached-incr! ((memcached-pool? key?) (#:amount uint8? #:initial (or/c false/c #;uint8?) #:expiration uint4? #:cas cas?) . ->* . (or/c false/c uint8?))]
 [memcached-decr! ((memcached-pool? key?) (#:amount uint8? #:initial (or/c false/c #;uint8?) #:expiration uint4? #:cas cas?) . ->* . (or/c false/c uint8?))]
 [memcached-append! ((memcached-pool? key? value?) (#:cas cas?) . ->* . (or/c false/c cas?))]
 [memcached-prepend! ((memcached-pool? key? value?) (#:cas cas?) . ->* . (or/c false/c cas?))])