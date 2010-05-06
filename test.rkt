#lang racket
(require "main.rkt"
         tests/eli-tester)

(define-syntax-rule
  (with-memcached p e ...)
  (local [(define sp #f)]
    (dynamic-wind
     (λ () 
       (define-values (the-sp stdout stdin stderr) (subprocess (current-output-port) #f (current-error-port) "/opt/local/bin/memcached" "-p" (number->string p)))
       (set! sp the-sp))
     (λ () e ...)
     (λ () (subprocess-kill sp #t)))))

(define-syntax with-memcacheds
  (syntax-rules ()
    [(_ () e ...) (let () e ...)]
    [(_ (p . ps) e ...)
     (with-memcached p (with-memcacheds ps e ...))]))

(define port 11211)
(define mc #f)
(define cas #f)
(define-syntax-rule (record-cas! e)
  (let-values ([(e-v e-cas) e])
    (set! cas e-cas)
    e-v))    
(with-memcacheds (#;(+ port 0) #;(+ port 1) #;(+ port 2))
  (test
   (set! mc 
         (memcached
          "localhost" (+ port 0)
          ;"localhost" (+ port 1)
          ;"localhost" (+ port 2)
          ))
   (memcached-set! mc #"foo" #"bar")
   (memcached-get mc #"foo") => #"bar"
   
   (memcached-add! mc #"foo" #"zog") => #f
   (memcached-get mc #"foo") => #"bar"
   
   (or (memcached-delete! mc #"zag") #t)
   (memcached-add! mc #"zag" #"zog")
   (memcached-get mc #"zag") => #"zog"
   
   (memcached-replace! mc #"zag" #"zig")
   (memcached-get mc #"zag") => #"zig"
   (memcached-replace! mc #"zig" #"zoo") => #f
   (memcached-get mc #"zig") => #f
   
   (memcached-set! mc #"list" #"2")
   (memcached-get mc #"list") => #"2"
   (memcached-append! mc #"list" #"3")
   (memcached-get mc #"list") => #"23"
   (memcached-prepend! mc #"list" #"1")
   (memcached-get mc #"list") => #"123"
   
   (record-cas! (memcached-gets mc #"foo")) => #"bar"
   (memcached-cas! mc #"foo" cas #"zog")
   (memcached-get mc #"foo") => #"zog"
   (memcached-set! mc #"foo" #"bleg")
   (memcached-get mc #"foo") => #"bleg"
   (memcached-cas! mc #"foo" cas #"zig") => #f
   (memcached-get mc #"foo") => #"bleg"
   
   (memcached-delete! mc #"foo")
   (memcached-get mc #"foo") => #f
   
   (or (memcached-delete! mc #"k") #t)
   (memcached-incr! mc #"k") => #f
   (memcached-decr! mc #"k") => #f
   (memcached-set! mc #"k" #"0")
   (memcached-get mc #"k") => #"0"
   (memcached-incr! mc #"k")
   (memcached-get mc #"k") => #"1"
   (memcached-incr! mc #"k")
   (memcached-get mc #"k") => #"2"
   (memcached-decr! mc #"k")
   (memcached-get mc #"k") => #"1"
   
   ; XXX statistics
   ))