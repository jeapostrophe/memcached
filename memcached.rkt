#lang racket
(require tests/eli-tester)

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

;; Protocol
(define request-magic #x80)
(define response-magic #x81)

(define (hasheqp id . a)
  (define ht (apply hasheq a))
  (λ (k)
    (hash-ref ht k (λ () (error id "Not found: ~s" k)))))

(define response-status
  (hasheqp 'response-status
   #x0000  "No error"
   #x0001  "Key not found"
   #x0002  "Key exists"
   #x0003  "Value too large"
   #x0004  "Invalid arguments"
   #x0005  "Item not stored"
   #x0006  "Incr/Decr on non-numeric value."
   #x0081  "Unknown command"
   #x0082  "Out of memory"))

(define (rhasheqp id . a)
  (define swap
    (match-lambda
      [(list) empty]
      [(list-rest o (list-rest t r))
       (list* t o (swap r))]))
  (apply hasheqp id (swap a)))

(define-syntax-rule (opcodes . codes)
  (values (hasheqp 'opcodes . codes) (rhasheqp 'opcodes . codes)))

(define-values
  (byte->opcode opcode->byte)
  (opcodes
   #x00    'Get
   #x01    'Set
   #x02    'Add
   #x03    'Replace
   #x04    'Delete
   #x05    'Increment
   #x06    'Decrement
   #x07    'Quit
   #x08    'Flush
   #x09    'GetQ
   #x0A    'No-op
   #x0B    'Version
   #x0C    'GetK
   #x0D    'GetKQ
   #x0E    'Append
   #x0F    'Prepend
   #x10    'Stat
   #x11    'SetQ
   #x12    'AddQ
   #x13    'ReplaceQ
   #x14    'DeleteQ
   #x15    'IncrementQ
   #x16    'DecrementQ
   #x17    'QuitQ
   #x18    'FlushQ
   #x19    'AppendQ
   #x1A    'PrependQ
   
   #x20    'SASL-List-Mechs
   #x21    'SASL-Auth
   #x22    'SASL-Step
   
   #x30    'RGet
   #x31    'RSet
   #x32    'RSetQ
   #x33    'RAppend
   #x34    'RAppendQ
   #x35    'RPrepend
   #x36    'RPrependQ
   #x37    'RDelete
   #x38    'RDeleteQ
   #x39    'RIncr
   #x3a    'RIncrQ
   #x3b    'RDecr
   #x3c    'RDecrQ))

(define raw-data-type #x00)

(define (write-request-header! opcode key-len extras-len total-body-len cas)
  (write-byte request-magic)
  (write-byte (opcode->byte opcode))
  (write-bytes (integer->integer-bytes key-len 2 #f #t))
  (write-byte extras-len)
  (write-byte 0) ; data type
  (write-bytes #"\0\0") ; reserved
  (write-bytes (integer->integer-bytes total-body-len 4 #f #t))
  (write-bytes #"\0\0\0\0") ; opaque (copied back)
  (write-bytes cas))

(define-syntax-rule (define* i e)
  (begin (define _1 (fprintf (current-error-port) "~S = ...~n" 'i))
         (define i e)
         (define _2 (fprintf (current-error-port) "~S = ~S~n" 'i i))))

(define (read-response)
  (define magic (read-byte))
  (define opcode (byte->opcode (read-byte)))
  (define key-len (integer-bytes->integer (read-bytes 2) #f #t))
  (define extras-len (read-byte))
  (define data-type (read-byte))
  (define status (integer-bytes->integer (read-bytes 2) #f #t))
  (define total-body-len (integer-bytes->integer (read-bytes 4) #f #t))
  (define opaque (read-bytes 4))
  (define cas (read-bytes 8))
  (define val-len (- total-body-len key-len extras-len))
  (define extras (read-bytes extras-len))
  (define key (read-bytes key-len))
  (define val (read-bytes val-len))
  (values opcode key extras status val cas))
  
(define (write-get* opcode key)
  (define key-len (bytes-length key))
  (write-request-header! opcode key-len 0 key-len #"\0\0\0\0\0\0\0\0")
  (write-bytes key)
  (flush-output))

(test
 (with-output-to-bytes
     (lambda ()
       (write-get* 'Get #"Hello")))
 =>
 #"\x80\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Hello")

(define (read-get*)
  (define-values (opcode key extras status val cas) (read-response))
  ; XXX check opcode and extras-len = 4 and status
  (define flags extras)
  (values key 
          (if (zero? status)
              val
              #f)
          cas))

(test
 (parameterize ([current-input-port
                (open-input-bytes
                 #"\x81\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Not found")])
   (read-get*))
 =>
 (values #""
         #f
         #"\0\0\0\0\0\0\0\0")
 
 (parameterize ([current-input-port
                (open-input-bytes
                 #"\x81\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xde\xad\xbe\xefWorld")])
   (read-get*))
 =>
 (values #""
         #"World"
         #"\0\0\0\0\0\0\0\1")
 
 (parameterize ([current-input-port
                (open-input-bytes
                 #"\x81\x00\x00\x05\x04\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xde\xad\xbe\xefHelloWorld")])
   (read-get*))
 =>
 (values #"Hello"
         #"World"
         #"\0\0\0\0\0\0\0\1"))

(define (write-set* opcode key value flags expiration cas)
  (define key-len (bytes-length key))
  (write-request-header! opcode key-len 8 (+ key-len 8 (bytes-length value)) cas)
  (write-bytes flags)
  (write-bytes (integer->integer-bytes expiration 4 #f #t))
  (write-bytes key)
  (write-bytes value)
  (flush-output))

(test
 (with-output-to-bytes
     (lambda ()
       (write-set* 'Add #"Hello" #"World" #"\xde\xad\xbe\xef" 3600 #"\0\0\0\0\0\0\0\0")))
 =>
 #"\x80\x02\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xde\xad\xbe\xef\x00\x00\x0e\x10HelloWorld")

(define (read-set*)
  (define-values (opcode key extras status val cas) (read-response))
  ; XXX check opcode and extras = #""
  (if (zero? status)
      cas
      #f))

(test
 (parameterize ([current-input-port
                (open-input-bytes
                 #"\x81\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01")])
   (read-set*))
 =>
 #"\0\0\0\0\0\0\0\1")

(define (write-delete* opcode key cas)
  (define key-len (bytes-length key))
  (write-request-header! opcode key-len 0 key-len cas)
  (write-bytes key)
  (flush-output))

(test
 (with-output-to-bytes
     (lambda ()
       (write-delete* 'Delete #"Hello" #"\0\0\0\0\0\0\0\0")))
 =>
 #"\x80\x04\x00\x05\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Hello")

(define (read-delete*)
  (define-values (opcode key extras status val cas) (read-response))
  ; XXX check opcode and extras = #"", val = #""
  (zero? status))

(define (write-incr* opcode key amt init expiration cas)
  (define key-len (bytes-length key))
  (write-request-header! opcode key-len 20 (+ key-len 20) cas)
  (write-bytes (integer->integer-bytes amt 8 #f #t))
  (write-bytes (integer->integer-bytes init 8 #f #t))
  (if expiration
      (write-bytes (integer->integer-bytes expiration 4 #f #t))
      (write-bytes #"\xff\xff\xff\xff"))
  (write-bytes key)
  (flush-output))

(test
 (with-output-to-bytes
     (lambda ()
       (write-incr* 'Increment #"counter" 1 0 3600 #"\0\0\0\0\0\0\0\0")))
 =>
 #"\x80\x05\x00\x07\x14\x00\x00\x00\x00\x00\x00\x1b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0e\x10counter")

(define (read-incr*)
  (define-values (opcode key extras status val cas) (read-response))
  ; XXX check opcode and val = #""
  (if (zero? status)
      val
      #f))

(test
 (parameterize ([current-input-port
                (open-input-bytes
                 #"\x81\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00")])
   (read-incr*))
 =>
 #"\x00\x00\x00\x00\x00\x00\x00\x00")

; XXX quit
; XXX flush
; XXX noop
; XXX version

(define (write-append* opcode key val cas)
  (define key-len (bytes-length key))
  (define val-len (bytes-length val))
  (write-request-header! opcode key-len 0 (+ key-len val-len) cas)
  (write-bytes key)
  (write-bytes val)
  (flush-output))

(test
 (with-output-to-bytes
     (lambda ()
       (write-append* 'Append #"Hello" #"!" #"\0\0\0\0\0\0\0\0")))
 =>
 #"\x80\x0e\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Hello!")

(define (read-append*)
  (define-values (opcode key extras status val cas) (read-response))
  ; XXX check opcode and extras, val = #""
  (and (zero? status) cas))

;; Command interface
(define (memcached-get mp k)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-get* 'Get k)
     (let-values ([(k v cas) (read-get*)])
       v))))
(define (memcached-gets mp k)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-get* 'Get k)
     (let-values ([(k v cas) (read-get*)])
       (values v cas)))))
(define (memcached-set! mp k v)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-set* 'Set k v #"\x00\x00\x00\x00" 0 #"\0\0\0\0\0\0\0\0")
     (read-set*))))
(define (memcached-add! mp k v)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-set* 'Add k v #"\x00\x00\x00\x00" 0 #"\0\0\0\0\0\0\0\0")
     (read-set*))))
(define (memcached-replace! mp k v)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-set* 'Replace k v #"\x00\x00\x00\x00" 0 #"\0\0\0\0\0\0\0\0")
     (read-set*))))
(define (memcached-cas! mp k cas v)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-set* 'Set k v #"\x00\x00\x00\x00" 0 cas)
     (read-set*))))
(define (memcached-delete! mp k)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-delete* 'Delete k #"\0\0\0\0\0\0\0\0")
     (read-delete*))))
(define (memcached-incr! mp k)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-incr* 'Increment k 1 0 #f #"\0\0\0\0\0\0\0\0")
     (read-incr*))))
(define (memcached-decr! mp k)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-incr* 'Decrement k 1 0 #f #"\0\0\0\0\0\0\0\0")
     (read-incr*))))
(define (memcached-append! mp k v)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-append* 'Append k v #"\0\0\0\0\0\0\0\0")
     (read-append*))))
(define (memcached-prepend! mp k v)
  (memcached-pool-comm!
   mp
   (λ () 
     (write-append* 'Prepend k v #"\0\0\0\0\0\0\0\0")
     (read-append*))))

;;; Interface
(define key? bytes?)
(define value? bytes?)
(define (cas? x)
  (and (bytes? x) (= 8 (bytes-length x))))

(provide/contract
 [memcached-pool? (any/c . -> . boolean?)]
 ; XXX
 [memcached (() () #:rest list? . ->* . memcached-pool?)]
 [memcached-get (memcached-pool? key? . -> . (or/c false/c value?))]
 [memcached-gets (memcached-pool? key? . -> . (values (or/c false/c value?) (or/c false/c cas?)))]
 [memcached-set! (memcached-pool? key? value? . -> . (or/c false/c cas?))]
 [memcached-cas! (memcached-pool? key? cas? value? . -> . (or/c false/c cas?))]
 [memcached-add! (memcached-pool? key? value? . -> . (or/c false/c cas?))]
 [memcached-replace! (memcached-pool? key? value? . -> . (or/c false/c cas?))]
 [memcached-append! (memcached-pool? key? value? . -> . (or/c false/c cas?))]
 [memcached-prepend! (memcached-pool? key? value? . -> . (or/c false/c cas?))]
 [memcached-delete! (memcached-pool? key? . -> . boolean?)]
 [memcached-incr! (memcached-pool? key? . -> . (or/c false/c value?))]
 [memcached-decr! (memcached-pool? key? . -> . (or/c false/c value?))])