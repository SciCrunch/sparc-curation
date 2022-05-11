#lang racket

(require racket/gui
         racket/generic
         racket/pretty
         racket/runtime-path
         framework
         compiler/compilation-path
         compiler/embed
         gui-widget-mixins
         json
         json-view
         net/url)

(define-runtime-path asdf "viewer.rkt")
(define this-file (path->string asdf))
(define this-file-compiled (get-compilation-bytecode-file this-file))
(define this-file-exe (embedding-executable-add-suffix (path-replace-extension this-file "") #f))
(define this-file-exe-tmp (path-add-extension this-file-exe "tmp"))

(when (file-exists? this-file-exe-tmp)
  ; windows can't remove a running exe ... but can rename it ... and then delete the old
  ; file on next start
  (delete-file this-file-exe-tmp))

(define running? #t) ; don't use parameter, this needs to be accessible across threads
(define update-running? #f)
(define selected-dataset #f) ; selected dataset is the global value across threads

;; parameters (yay dynamic variables)
(define path-cache-dir (make-parameter #f))
(define path-cache-datasets (make-parameter #f))
(define path-cleaned-dir (make-parameter #f))
(define path-export-dir (make-parameter #f))
(define path-export-datasets (make-parameter #f))
(define path-source-dir (make-parameter #f))
(define url-prod-datasets (make-parameter "https://cassava.ucsd.edu/sparc/datasets"))
(define current-blob (make-parameter #f))
(define current-dataset (make-parameter #f))
(define current-datasets (make-parameter #f))
(define current-datasets-view (make-parameter #f))
(define current-jview (make-parameter #f))
(define overmatch (make-parameter #f))
(define power-user? (make-parameter #f))
(define python-interpreter (make-parameter
                            (path->string
                             (find-executable-path
                              (case (system-type)
                                ((windows) "python.exe")
                                ; osx is still stuck on 2.7 by default so need brew
                                ; but for whatever reason find-executable-path is not brew aware
                                ((macosx) "/usr/local/bin/python3")
                                ((unix) "python") ; all of these should be >= 3.7 by this point
                                (else (error "uhhhhh? beos is this you?")))))))

;; other variables

(define include-keys
  ; have to filter down due to bad performance in the viewer
  ; this is true even after other performance improvements
  '(id meta rmeta status prov submission))

;; temporary orthauth extract

(define (config-paths [os #f])
  (case (or os (system-type))
    ;; ucp udp uchp ulp
    ((unix) (map
             string->path
             '("~/.config"
               "~/.local/share"
               "~/.cache"
               "~/.cache/log")))
    ((macosx) (map
               string->path
               '("~/Library/Application Support"
                 "~/Library/Application Support"
                 "~/Library/Caches"
                 "~/Library/Logs")))
    ((windows) (let ((ucp (build-path (find-system-path 'home-dir) "AppData" "Local")))
                 (list ucp ucp ucp (build-path ucp "Logs"))))
    (else (error (format "Unknown OS ~a" (or os (system-type)))))))

(define *config-paths* (config-paths))

(define (fcp position suffix-list)
  (let ([base-path (position *config-paths*)])
    (if suffix-list
        (apply build-path base-path suffix-list)
        base-path)))

(define (user-config-path . suffix) (fcp first  suffix))
(define (user-data-path   . suffix) (fcp second suffix))
(define (user-cache-path  . suffix) (fcp third  suffix))
(define (user-log-path    . suffix) (fcp fourth suffix))

;; python argvs

(define (python-mod-args module-name . args)
  (cons (python-interpreter) (cons "-m" (cons module-name args))))

(define argv-simple-for-racket (python-mod-args "sparcur.simple.utils" "for-racket"))
(define argv-simple-git-repos-update (python-mod-args "sparcur.simple.utils" "git-repos" "update"))
(define argv-spc-export (python-mod-args "sparcur.cli" "export"))
(define (argv-simple-retrieve ds) (python-mod-args "sparcur.simple.retrieve" "--sparse-limit" "-1" "--dataset-id" (dataset-id ds)))
(define argv-spc-find-meta
  (python-mod-args
   "sparcur.cli"
   "find"
   "--name" "*.xlsx"
   "--name" "*.xml"
   "--name" "submission*"
   "--name" "code_description*"
   "--name" "dataset_description*"
   "--name" "subjects*"
   "--name" "samples*"
   "--name" "manifest*"
   "--name" "resources*"
   "--name" "README*"
   "--limit" "-1"
   "--fetch"))
(define (argv-clean-metadata-files ds)
  (python-mod-args ; XXX note that this is separate from normalize metadata files
   "sparcur.simple.clean_metadata_files"
   "--dataset-id" (dataset-id ds)))

(define (argv-open-ipython ds)
  (let*-values ([(path) (dataset-export-latest-path ds)]
                [(parent name dir?) (split-path path)]
                [(path-meta-path) (build-path parent "path-metadata.json")])
    (cons "/usr/bin/urxvt" ; FIXME find the right terminal emulator
          (cons "-e" ; FIXME running without bash loses readline somehow
                (cons "rlwrap"
                      (python-mod-args
                       "IPython"
                       "-i" "-c"
                       ; LOL PYTHON can't use with in the special import line syntax SIGH
                       (format "import json;print('~a');f = open('~a', 'rt');blob = json.load(f);f.close();f = open('~a', 'rt');path_meta = json.load(f);f.close()"
                               parent path path-meta-path)))))))

;; utility functions

#; ; sigh
(define (in-cwd-thread argv cwd)
  ; FIXME not quite right because we need to be able to run multiple things in a row
  ; it would have to be a list of argv cwd pairs or we just use this for the oneoffs
  (thread
   (thunk
    (let ([status #f])
      (parameterize ([current-directory cwd])
        (values
         (with-output-to-string
           (thunk (set! status (apply system* argv #:set-pwd? #t))))
         status))))))

(define (path->json path)
  (let ([path (expand-user-path (if (path? path) path (string->path path)))])
    (with-input-from-file path
      (λ () (read-json)))))

(define (url->json url)
  (call/input-url
   (string->url url)
   get-pure-port
   read-json))

(define (resolve-relative-path path)
  "Resolve a symlink to a relative path from the parent folder of the symlink."
  ; simple-form-path calls path->complete-path internally, which we don't want
  ; also have to tell simplify-path not to use the file system
  (if (link-exists? path)
      (path->complete-path (resolve-path path) (simplify-path (build-path path 'up) #f))
      (error 'non-existent-path "cannot resolve a non-existent link ~a" path)))

(define (object->methods obj)
  (interface->method-names (object-interface obj)))

(define (populate-datasets)
  (let* ([pcd (path-cache-datasets)]
         [result (if (file-exists? pcd)
                     (with-input-from-file pcd
                       (λ () (read)))
                     (begin
                       (let ([pc-dir (path-cache-dir)])
                         (unless (directory-exists? pc-dir)
                           ; `make-directory*' will make parents but
                           ; only if pc-dir is not a relative path
                           (make-directory* pc-dir)))
                       (let ([result (get-dataset-list)])
                         (with-output-to-file pcd
                           #:exists 'replace ; yes if you run multiple of these you could have a data race
                           (λ () (pretty-write result)))
                         ; just to confuse everyone
                         result))
                     )]
         [datasets (result->dataset-list result)])
    (current-datasets datasets)
    (set-datasets-view! lview (current-datasets)) ; FIXME TODO have to store elsewhere for search so we
    result))

(define (ensure-directory! path-dir)
  (unless (directory-exists? path-dir)
    (make-directory* path-dir)))

(define (init-paths!)
  "initialize or reset the file system paths to cache, export, and source directories"
  ; FIXME 'cache-dir is NOT what we want for this as it is ~/.racket/
  ; FIXME more cryptic errors if sparcur.simple isn't tangled
  ; FIXME it should be possible for the user to configure path-source-dir
  (path-source-dir (build-path (find-system-path 'home-dir) "files" "sparc-datasets"))
  (path-cache-dir (expand-user-path (user-cache-path "sparcur" "racket")))
  (path-cache-datasets (build-path (path-cache-dir) "datasets-list.rktd"))
  (path-cleaned-dir (expand-user-path (user-data-path "sparcur" "cleaned")))
  (path-export-dir (expand-user-path (user-data-path "sparcur" "export")))
  (path-export-datasets (build-path (path-export-dir) "datasets")))

(define (manifest-report)
  ; FIXME this will fail if one of the keys isn't quite right
  ; TODO displayln this into a text% I think?
  (for-each (λ (m) (displayln (regexp-replace #rx"SPARC Consortium/[^/]+/" m "\\0\n")) (newline))
            ; FIXME use my hr function from elsewhere
            (let ([ihr (hash-ref
                        (hash-ref
                         (current-blob)
                         'status)
                        'path_error_report
                        #f)])
              (if ihr
                  (hash-ref (hash-ref ihr '|#/path_metadata/-1| #hash((messages . ()))) 'messages)
                  '()))))

;; update viewer

(define (update-viewer)
  "stash and pull all git repos, rebuild the viewer"
  ; find the git repos
  (if update-running?
      (println "Update is already running!")
      (begin
        (set! update-running? #t)
        (thread
         (thunk
          (dynamic-wind
            (λ () #t)
            (thunk
             (println "Update starting ...")
             (let ([exec-file (path->string (find-system-path 'exec-file))]
                   [raco-exe (path->string (find-executable-path "raco"))] ; XXX SIGH
                   [status
                    (parameterize ()
                      (apply system* argv-simple-git-repos-update))])
                  ; TODO pull changes for racket dependent repos as well
                  (println (format "running raco make -v ~a" this-file))
                  (let ([mtime-before (file-or-directory-modify-seconds
                                       this-file-compiled
                                       #f
                                       (λ () -1))])
                    (parameterize ()
                      (system* raco-exe "make" "-v" this-file))
                    #; ; raco exe issues ... i love it when abstractions break :/
                    (parameterize ([current-command-line-arguments
                                    (vector "--vv" this-file)])
                      (dynamic-require 'compiler/commands/make #f))
                    #;
                    (system* exec-file "-l-" "raco/main.rkt" "make" "--vv" "--" this-file)
                    (let ([mtime-after (file-or-directory-modify-seconds
                                        this-file-compiled
                                        #f
                                        (λ () -2))])
                      (when (not (= mtime-before mtime-after))
                        (println (format "running raco exe -v -o ~a ~a "
                                         this-file-exe this-file))
                        (parameterize ()
                          (rename-file-or-directory this-file-exe this-file-exe-tmp)
                          (system* raco-exe "exe" "-v" "-o" this-file-exe this-file)
                          (unless (file-exists? this-file-exe) ; restore the old version on failure
                            (rename-file-or-directory this-file-exe-tmp this-file-exe)))
                        #; ; this is super cool but an eternal pain for raco exe
                        (parameterize ([current-command-line-arguments
                                        (vector
                                         "++lib" "compiler/commands/make"
                                         "++lib" "compiler/commands/exe"
                                         "++lib" "racket/lang/reader"
                                         "--vv" this-file)])
                          (dynamic-require 'compiler/commands/exe #f))
                        #;
                        (system* exec-file "-l-" "raco/main.rkt" "exe" "--vv" "--" this-file)))))
             (println "Update complete!"))
            (λ () (set! update-running? #f))))))))

;; json view

(define-syntax when-not (make-rename-transformer #'unless))

(define (apply-items-rec function item)
  ;(pretty-print item)
  (when (function item)
    (let* ([sub-items (send item get-items)])
      (for [(sub-item sub-items)] (apply-items-rec function sub-item)))))

(define (do-open item)
  ; FIXME still slow
  (let*-values ([(ud) (send item user-data)]
                [(type name) (values (node-data-type ud) (node-data-name ud))])
    #;
    (pretty-print (list (node-data-type ud) (node-data-name ud) (node-data-value ud)))
    (and (or (eq? type 'hash)
             (and
              (eq? type 'list)
              (not (memq name ; filter cases that are v slow to open
                         '(; FIXME hardcoded
                           synonyms
                           curation_errors
                           unclassified_errors
                           submission_errors
                           unclassified_stages
                           )))))
     (send item open))))

(define (set-jview! jview json)
  "set the default state for jview"
  (define jhl (get-field json-hierlist jview))
  (define old-root (get-field root jhl))
  (when old-root
    ; this is safe because we force selection at startup
    ; strangely this code this makes the interface less jerky when scrolling quickly
    (send jhl delete-item old-root))
  (send jview set-json! json)
  (define root (get-field root jhl))
  (send jhl sort json-list-item< #t)
  #;
  (send root open)
  ; can't thread this because some part of it is not thread safe
  (apply-items-rec do-open root)
  (send jhl scroll-to 0 0 0 0 #t)

  #;
  (define (by-name name items)
    (let ([v (for/list ([i items]
                        #:when (let ([ud (send i user-data)])
                                 (and (eq? (node-data-type ud) 'hash)
                                      (eq? (node-data-name ud) name))))
               i)])
      (and (not (null? v)) (car v))))
  #;
  (let ([ritems (send root get-items)])
    (when (> (length ritems) 1) ; id alone is length 1
      (map (λ (x)
             (let ([ud (send x user-data)])
               (when (and (eq? (node-data-type ud) 'hash)
                          (memq (node-data-name ud) '(meta submission status))) ; FIXME hardcoded
                 (when-not (null? (send x get-items)) ; compound item
                           (send x open))
                 (when (eq? (node-data-name ud) 'status)
                   (let ([per (by-name 'path_error_report (send x get-items))])
                     (when per
                       (send per open)))))))
           ritems))))

(define jviews (make-hash))

(define (dataset-jview! dataset #:update [update #f] #:background [background #f])
  ; FIXME #:background is bad design forced by having (current-blob) decoupled
  (let* ([uuid (id-uuid dataset)]
         [hr-jview (hash-ref jviews uuid #f)]
         [jview
          (if (and hr-jview (not update))
              (begin
                (current-blob #f)
                hr-jview)
              (letrec ([hier-class json-hierlist%
                        #; ; too slow when doing recursive opens
                        (class json-hierlist% (super-new)
                          (rename-super [super-on-item-opened on-item-opened])
                          (define/override (on-item-opened item)
                            (define root (get-field json-hierlist jview-inner))
                            #;
                            (define-values (x y w h)
                              (values
                                 (send root get-x)
                                 (send root get-y)
                                 (send root get-width)
                                 (send root get-height)))
                            (super-on-item-opened item)
                            (send root sort json-list-item< #t)
                            #; ; this isn't working as a way to stabilize the position of the buffer
                            ; when we get to the end of its content, which is the classic and
                            ; monumentally stupid behavior of most gui toolkits, not clear what can
                            ; be done about this
                            ; XXX the behavior is worse on-item-close so maybe a solution there
                            (send root scroll-to x y w h #t)))]
                       [jview-inner
                        (or (and update hr-jview)
                            (new json-view%
                                 [hier-class% hier-class]
                                 ;;[font (make-object font% 10 'modern)]
                                 [parent frame-helper]))])
                (let* ([lp (dataset-export-latest-path dataset)]
                       [json (if lp (path->json lp)
                                 (hash
                                  'id (dataset-id dataset)
                                  'meta (hash 'folder_name (dataset-title dataset))))]
                       [jhash (for/hash ([(k v) (in-hash json)]
                                         ; FIXME I think we don't need include keys anymore
                                         ; XXX false, there are still performance issues
                                         #:when (member k include-keys))
                                (values k v))])
                  (when-not background
                    (current-blob json)) ; FIXME this will go stale
                  (set-jview! jview-inner jhash)
                  (hash-set! jviews uuid jview-inner)
                  jview-inner)))])
    jview))

(define (get-jview! dataset)
  (let ([jview (dataset-jview! dataset)])
    ; FIXME deal with parents and visibility
    ; FIXME unparent the current jview
    (let ([old-jview (current-jview)])
      (when old-jview
          (send old-jview reparent frame-helper)))
    (send jview reparent panel-right)
    (current-jview jview)
    jview))

;; dataset struct and generic functions

(define-generics ds
  (populate-list ds list-box)
  (lb-cols ds)
  (lb-data ds)
  (id-short ds)
  (id-uuid ds)
  (uri-human ds)
  (dataset-src-path ds)
  (dataset-export-latest-path ds)
  (dataset-cleaned-latest-path ds)
  (fetch-export-dataset ds)
  (fetch-dataset ds)
  (clean-metadata-files ds)
  (load-remote-json ds)
  (export-dataset ds))

(struct dataset (id title pi-last-name)
  #:methods gen:ds
  [(define (populate-list ds list-box)
     ; FIXME annoyingly it looks like these things need to be set in
     ; batch in order to get columns to work
     (send list-box append
           (list (dataset-pi-last-name ds)
                 (dataset-title ds)
                 (id-short ds))
           ds))
   (define (lb-cols ds)
     (list (dataset-pi-last-name ds)
           (dataset-title ds)
           (id-short ds)))
   (define (lb-data ds)
     ; TODO we may want to return more stuff here
     ds)
   (define (dataset-export-latest-path ds)
     (let* ([uuid (id-uuid ds)]
            [lp (build-path (path-export-datasets)
                            uuid "LATEST" "curation-export.json")]
            #;
            [asdf (println lp)]
            [qq (and (file-exists? lp) (resolve-path lp))])
       ; FIXME not quite right?
       qq))
   (define (dataset-cleaned-latest-path ds)
     (let* ([uuid (id-uuid ds)]
            [lp (build-path (path-cleaned-dir)
                            uuid "LATEST")]
            [qq (and (directory-exists? lp) ; sigh, paths are hard
                     (build-path (path-cleaned-dir) uuid (resolve-path lp)))])
       qq))
   (define (dataset-src-path ds)
     (let ([uuid (id-uuid ds)])
       (build-path (path-source-dir) uuid)))
   (define (fetch-dataset ds)
     (println (format "dataset fetch starting for ~a" (dataset-id ds))) ; TODO gui and/or log
     (let ([cwd-1 (path-source-dir)]
           [cwd-2 (build-path (dataset-src-path ds)
                              ; FIXME dataset here is hardcoded
                              "dataset")]
           [argv-1 (argv-simple-retrieve ds)]
           [argv-2 argv-spc-find-meta])
       (thread
        (thunk
         (let ([status-1 #f]
               [status-2 #f])
           (ensure-directory! cwd-1)
           (parameterize ([current-directory cwd-1])
             (with-output-to-string (thunk (set! status-1
                                                 (apply system* argv-1 #:set-pwd? #t)))))
           (parameterize ([current-directory (resolve-relative-path cwd-2)])
             (with-output-to-string (thunk (set! status-2
                                                 (apply system* argv-2 #:set-pwd? #t)))))
           (if (and status-1 status-2)
               (begin
                 (when (equal? ds selected-dataset)
                   (send button-export-dataset enable #t)
                   (send button-clean-metadata-files enable #t))
                 (println (format "dataset fetch completed for ~a" (dataset-id ds))))
               (println (format "dataset fetch FAILED for ~a" (dataset-id ds)))))))))
   (define (export-dataset ds)
     (println (format "dataset export starting for ~a" (dataset-id ds))) ; TODO gui and/or log
     (let ([cwd-2 (build-path (dataset-src-path ds)
                              ; FIXME dataset here is hardcoded
                              "dataset")]
           [argv-3 argv-spc-export])
       (thread
        (thunk
         (let ([status-3 #f]
               [cwd-2-resolved (resolve-relative-path cwd-2)])
           (parameterize ([current-directory cwd-2-resolved])
             (with-output-to-string (thunk (set! status-3
                                                 (apply system* argv-3 #:set-pwd? #t)))
               ))
           (if status-3
               (begin
                 (dataset-jview! ds #:update #t #:background #t)
                 (println (format "dataset export completed for ~a" (dataset-id ds))))
               (println (format "dataset export FAILED for ~a" (dataset-id ds)))))))))
   (define (fetch-export-dataset ds)
     (println (format "dataset fetch and export starting for ~a" (dataset-id ds))) ; TODO gui and/or log
     (let ([cwd-1 (path-source-dir)]
           ; we can't resolve-path on cwd-2 here because it
           ; may not exist or it may point to a previous release
           [cwd-2 (build-path (dataset-src-path ds)
                              ; FIXME dataset here is hardcoded
                              "dataset")]
           [argv-1 (argv-simple-retrieve ds)]
           [argv-2 argv-spc-find-meta]
           [argv-3 argv-spc-export])
       (thread
        (thunk
         (let ([status-1 #f]
               [status-2 #f]
               [status-3 #f])
           (ensure-directory! cwd-1)
           (parameterize ([current-directory cwd-1])
             #;
             (println (string-join argv-1 " "))
             (with-output-to-string (thunk (set! status-1
                                                 (apply system* argv-1 #:set-pwd? #t)))))
           (if status-1
               (let ([cwd-2-resolved (resolve-relative-path cwd-2)])
                 (parameterize ([current-directory cwd-2-resolved])
                   (with-output-to-string (thunk (set! status-2
                                                       (apply system* argv-2 #:set-pwd? #t))))
                   ; TODO figure out if we need to condition this run on status-2 #t
                   (with-output-to-string (thunk (set! status-3
                                                       (apply system* argv-3 #:set-pwd? #t)))))
                 ; TODO gui indication
                 (if (and status-2 status-3)
                     (begin
                       ; now that we have 1:1 jview:json we can automatically update the jview for
                       ; this dataset in the background thread and it shouldn't block the ui
                       (when (equal? ds selected-dataset)
                         (send button-export-dataset enable #t)
                         (send button-clean-metadata-files enable #t))
                       (dataset-jview! ds #:update #t #:background #t)
                       (println (format "dataset fetch and export completed for ~a" (dataset-id ds))))
                     (format "dataset ~a FAILED for ~a"
                             (cond ((nor status-2 status-3) "fetch and export")
                                   (status-2 "export")
                                   (status-3 "fetch"))
                             (dataset-id ds))))
               (println (format "dataset retrieve FAILED for ~a" (dataset-id ds))))
           #;
           (values status-1 status-2 status-3)
           #; ; this doesn't work because list items cannot be customized independently SIGH
           ; I can see why people use the web for stuff like this, if you want to be able to
           ; customize some particular entry why fight with the canned private opaque things
           ; that don't actually meet your use cases?
           (set-lview-item-color lview ds) ; FIXME lview free variable
           )))))
   (define (clean-metadata-files ds)
     (println (format "cleaning metadata files for ~a" (dataset-id ds))) ; TODO gui and/or log
     (let ([cwd (build-path (dataset-src-path ds)
                            ; FIXME dataset here is hardcoded
                            "dataset")]
           [argv-1 (argv-clean-metadata-files ds)])
       (thread
        (thunk
         (let ([status-1 #f]
               [cwd-resolved (resolve-relative-path cwd)])
           (parameterize ([current-directory (path-source-dir)])
             (with-output-to-string (thunk (set! status-1
                                                 (apply system* argv-1 #:set-pwd? #t)))
               ))
           (if status-1
               (begin
                 ; TODO open folder probably ?
                 (println (format "cleaning metadata files completed for ~a" (dataset-id ds))))
               (println (format "cleaning metadata files FAILED for ~a" (dataset-id ds)))))))))
   (define (dataset-latest-prod-url ds)
     (string-append (url-prod-datasets) "/" (id-uuid ds) "/LATEST/curation-export.json"))
   (define (load-remote-json ds)
     (let* ([uuid (id-uuid ds)]
            [hr-jview (hash-ref jviews uuid #f)]
            [jview-inner
             (or hr-jview
                 (new json-view%
                      [hier-class% json-hierlist%]
                      [parent frame-helper]))]
            [url (dataset-latest-prod-url ds)]
            [json (url->json url)]
            [jhash (for/hash ([(k v) (in-hash json)]
                              ; FIXME I think we don't need include keys anymore
                              ; XXX false, there are still performance issues
                              #:when (member k include-keys))
                     (values k v))])
       (current-blob json) ; FIXME this will go stale
       (set-jview! jview-inner jhash)
       (hash-set! jviews uuid jview-inner)
       jview-inner))
   (define (set-lview-item-color lview ds)
     ; find the current row based on data ??? and then change color ?
     (println "TODO set color") ; pretty sure this cannot be done with this gui widgit
     )
   (define (id-short ds)
     (let-values ([(N type uuid)
                   (apply values (string-split (dataset-id ds) ":"))])
       (string-append
        type
        ":"
        (substring uuid 0 4)
        " ... "
        (let ([slu (string-length uuid)])
          (substring uuid (- slu 4) slu)))))
   (define (id-uuid ds)
     (let-values ([(N type uuid)
                   (apply values (string-split (dataset-id ds) ":"))])
       ; XXX NOTE for some reason racket decided that multiple values
       ; must always be bound together so you can't just ignore later
       ; values if you want like in elisp or common lisp
       uuid))
   (define (id-project ds)
     ; FIXME derive from file system
     "N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0")
   (define (uri-human ds)
     (string-append "https://app.pennsieve.io/" (id-project ds) "/datasets/N:dataset:" (id-uuid ds)))])

(define (set-datasets-view! list-box datasets)
  (send/apply list-box set (apply map list (map lb-cols datasets)))
  (for ([ds datasets]
        [n (in-naturals)])
    (send list-box set-data n (lb-data ds)))
  (send button-refresh-datasets set-label
        (format lbt-refresh-datasets (length datasets))) ; XXX free variable on the button in question
  )

(define (set-datasets-view*! list-box . datasets)
  (set-datasets-view! list-box datasets))

(define (get-current-dataset)
  ; FIXME lview free variable
  (car (for/list ([index (send lview get-selections)])
         (let ([dataset (send lview get-data index)])
           dataset))))

(define (fetch-current-dataset)
  (fetch-dataset (current-dataset)))

(define (export-current-dataset)
  (export-dataset (current-dataset)))

(define (fetch-export-current-dataset)
  ; FIXME TODO check to make sure that the dataset has changed since the last time we retrieved it
  ; so that we can safely click the button over and over without firing off a download process
  ; FIXME one vs many export
  ; TODO highlight changed since last time
  (fetch-export-dataset (current-dataset)))

(define (json-list-item< a b)
  "sort json hierlist items"
  (let ([uda (send a user-data)]
        [udb (send b user-data)])
    #;
    (pretty-print (list uda udb))
    (let ([tda (node-data-type uda)]
          [tdb (node-data-type udb)]
          [nda (node-data-name uda)]
          [ndb (node-data-name udb)])
      #;
      (or (println (list tda tdb nda ndb)) #t)
      (or (not (or (symbol<? tda tdb) (symbol=? tda tdb)))
          (and (eq? tda tdb)
               (or (and (eq? tda 'value)
                        ; really not sure how these can possibly show up at the same level but whatever
                        (or (and (symbol? nda)
                                 (symbol? ndb)
                                 #;
                                 (apply string<? (map symbol->string (list nda ndb)))
                                 (symbol<? nda ndb))
                            (and (integer? nda)
                                 (integer? ndb)
                                 (< nda ndb)))))
               ; value hash list
               #; ; can't have duplicate keys so this shouldn't ever happen
               (let ([va (node-data-value uda)]
                     [vb (node-data-value udb)])
                 ; the lack of type-of in racket is really annoying :/
                 ; especially given that it is dynamically typed :/
                 #f))))))

;; callbacks

(define (cb-dataset-selection obj event)
  (define (set-button-status-for-dataset ds)
    (let* ([path (dataset-src-path ds)]
           [symlink (build-path path "dataset")])
      ; performance is a little bit better, but wow this seems like a bad way to
      ; say "wait and see if the current dataset is actually the current dataset"
      ; I think what needs to happen is that there is a parameter for the
      ; current-dataset which is static per thread, and then selected-dataset is
      ; the module variable that transcends threads, compare the two to see if we
      ; do anything, yep, this seems to work
      (thread
       (thunk
        (if (equal? selected-dataset (current-dataset))
            (let ([enable? (link-exists? symlink)])
              (send button-export-dataset enable enable?)
              (send button-clean-metadata-files enable enable?))
            (void)
            #; ; yep, this case does happen :)
            (println "TOO FAST! dataset no longer selected"))))))
  (for ([index (send obj get-selections)])
    (let ([dataset (send obj get-data index)]
          [cd (current-dataset)])
      ; https://docs.racket-lang.org/guide/define-struct.html#(part._struct-equal)
      (when (not (equal? dataset cd))
        (set! selected-dataset dataset)
        (current-dataset dataset) ; FIXME multiple selection case
        (get-jview! dataset)
        (set-button-status-for-dataset dataset)))))

(define (result->dataset-list result)
  (map (λ (ti)
         #;
         (pretty-print ti)
         (apply dataset (append ti (list "???"))))
       result))

(define (get-dataset-list)
  (let* ([argv argv-simple-for-racket]
         [status #f]
         [result-string
          (parameterize ()
            (with-output-to-string (λ () (set! status (apply system* argv)))))]
         [result (read (open-input-string result-string))])
    (unless status
      (error "Failed to get dataset list! ~a" (string-join argv-simple-for-racket " ")))
    result))

(define (cb-refresh-dataset-metadata obj event)
  ; XXX requries python sparcur to be installed
  (let* ([result (get-dataset-list)]
         [datasets (result->dataset-list result)])
    (current-datasets datasets)
    (set-datasets-view! lview (current-datasets)) ; FIXME lview is a free variable here
    (println "dataset metadata has been refreshed") ; TODO gui viz on this
    (with-output-to-file (path-cache-datasets)
      #:exists 'replace ; yes if you run multiple of these you could have a data race
      (λ () (pretty-write result)))))

(define (cb-update-viewer obj event)
  (update-viewer))

(define (cb-toggle-expand obj event)
  (displayln (list obj event (send event get-event-type)))
  ; TODO recursively open/close and possibly restore default view
  )

(define (cb-fetch-dataset obj event)
  (fetch-current-dataset))

(define (cb-export-dataset obj event)
  (export-current-dataset))

(define (cb-fetch-export-dataset obj event)
  (fetch-export-current-dataset))

(define (cb-load-remote-json obj event)
  (load-remote-json (current-dataset)))

(define (cb-open-export-folder obj event)
  (let*-values ([(path) (dataset-export-latest-path (current-dataset))]
                [(parent name dir?) (split-path path)])
    (xopen-path parent)))

(define (cb-open-export-json obj event)
  (let*-values ([(path) (dataset-export-latest-path (current-dataset))])
    (xopen-path path)))

(define (xopen-path path)
  (let ([command (find-executable-path
                  (case (system-type 'os*)
                    ((linux) "xdg-open")
                    ((macosx) "open")
                    ;((windows) "start") ; XXX only works from powerhsell it seems
                    (else (error "don't know xopen command for this os"))))])
    (subprocess #f #f #f command path)))

(define (xopen-folder path)
  (case (system-type)
    ((windows)
     (thread ; if this is not in a thread then for some reason it
      (thunk ; will crash the whole program ?? weird stuff going on here
       (parameterize ([current-directory path])
         ; it looks like implemeting this with subprocess #f #f #f
         ; somehow class the evil rktio function which was fixed, so
         ; for now we use system* while we wait for the fix
         (system* (find-executable-path "explorer.exe") "." #:set-pwd? #t)))))
    (else (xopen-path path))))

(define (cb-open-export-ipython obj event)
  (let ([argv (argv-open-ipython (current-dataset))])
    ; FIXME bad use of thread
    (thread (thunk (apply system* argv)))))

(define (cb-open-dataset-remote obj event)
  (xopen-path (uri-human (current-dataset))))

(define (cb-manifest-report obj event #:show [show #t])
  (let ([lp (dataset-export-latest-path (current-dataset))])
    ; this was moved from the fast branch of dataset-jview!
    ; to avoid calls to disk for current-blob
    (when lp ; FIXME performance is BAD when going rapidly through list
      ; maybe wait for a short time and if the current jview is this
      ; jview then do the set? pretty sure we don't want to add a cache
      ; to path->json at all, we would want a managed hash table
      (current-blob (path->json lp))
      #;
      (when (send frame-manifest-report is-shown?)
        (cb-manifest-report 'dataset-jview! 'called #:show #f))))
  ; TODO populate the editor
  ; TODO implement this as a method on edcanv-man-rep ?
  (let ((ed (send edcanv-man-rep get-editor)))
    (send ed select-all)
    (send ed clear)
    (send ed insert (with-output-to-string (λ () (manifest-report))))
    (send ed scroll-to-position 0))
  (when show
    (send frame-manifest-report show #t)))

(define (cb-clean-metadata-files obj event)
  ; WARNING THIS IS A DESTRUCTIVE OPERATION
  ; check if there are metadata files that need to be cleaned up
  ; clean them up and put them in a hierarchy that mirrors where they should be
  ; at some point provide flow to automatically do upload and replace
  ; TODO (probably on the python side) figure out how to track the source
  ; files and make sure that we don't run multiple times, and that we don't
  ; clean files that have already been cleaned
  (let ([ds (current-dataset)])
    (thread
     (thunk
      (let ([thread-clean (clean-metadata-files ds)])
        (thread-wait thread-clean)
        (let ([path (dataset-cleaned-latest-path ds)])
          (xopen-folder path)))))))

(define (cb-open-dataset-folder obj event)
  (let* ([ds (current-dataset)]
         [path (dataset-src-path ds)]
         [symlink (build-path path "dataset")])
    #;
    (println (list "should be opening something here ...." ds resolved))

    (if (directory-exists? symlink)
        (let ([path (resolve-relative-path symlink)])
          (xopen-folder path))
        ; TODO gui visible logging
        (println "Dataset has not been fetched yet!"))))

(define (match-datasets text datasets)
  "given text return datasets whose title or identifier matches"
  (if text
      (if (string-contains? text "dataset:")
          (let* ([m (last (string-split text "dataset:"))]
                 [uuid (if (string-contains? m "/")
                           (car (string-split m "/"))
                           m)]
                 [matches (for/list ([d datasets]
                                     ; use string-contains? instead of string=?
                                     ; so that incomplete copies still match
                                     #:when (string-contains? (id-uuid d) uuid))
                            d)])
            (if (null? matches)
                datasets
                matches))
          (let ([matches (for/list ([d datasets]
                                    #:when (or (string-contains? (id-uuid d) text)
                                               (string-contains? (string-downcase (dataset-title d))
                                                                 (string-downcase text))))
                           d)])
            (if (null? matches)
                datasets
                matches)))
      datasets))

(define (cb-search-dataset obj event)
  "callback for text field that should highlight filter sort matches in
the list to the top and if there is only a single match select and
switch to that"
  (let* ([text (send obj get-value)]
         [matching (match-datasets text (current-datasets))])
    (unless (or (null? matching) (eq? matching (or (current-datasets-view) (current-datasets))))
      (current-datasets-view matching)
      (set-datasets-view! lview matching) ; FIXME lview free variable
      (when (= (length matching) 1)
        (send lview set-selection 0)
        (cb-dataset-selection lview #f)))))

;; keymap keybind

(define keymap (new keymap%))

(define (k-test receiver event)
  (pretty-print (list "test" receiver event)))

(define (k-quit receiver event)
  (when (send frame-main can-close?)
    (send frame-main on-close)
    (send frame-main show #f)))

(define (k-fetch-export-dataset receiver event)
  (fetch-export-current-dataset))

(define (k-export-dataset receiver event)
  (export-current-dataset))

(define (backward-kill-word receiver event)
  (when (eq? receiver text-search-box)
    (let* ([ed (send receiver get-editor)]
           [spos (send ed get-start-position)]
           [epos (send ed get-end-position)]
           [sbox (box spos)]
           [s-delete (begin (send ed find-wordbreak sbox #f 'caret)
                            (unbox sbox))])
      (send ed delete s-delete epos)
      ; delete from epos to the backward word break
      )))

; add functions
(send* keymap
  (add-function "test" k-test)
  (add-function "fetch-export-dataset" k-fetch-export-dataset)
  (add-function "export-dataset" k-export-dataset)
  (add-function "quit" k-quit)
  (add-function "test-backspace" (λ (a b) (displayln (format "delete all the things! ~a ~a" a b))))
  (add-function "copy-value" (λ (obj event)
                               ; TODO proper chaining
                               (when (is-a? obj json-hierlist%)
                                 (let ([value (node-data-value (send (send obj get-selected) user-data))])
                                   (send the-clipboard set-clipboard-string value (current-milliseconds))))))
  (add-function "backward-kill-word" backward-kill-word)
  (add-function "focus-search-box" (λ (a b) (send text-search-box focus)))
  (add-function "open-dataset-folder" cb-open-dataset-folder)
  (add-function "open-export-folder" cb-open-export-folder)
  (add-function "open-export-json" cb-open-export-json)
  (add-function "open-export-ipython" cb-open-export-ipython))

; map functions
(send* keymap
  (map-function "m:backspace" "backward-kill-word")
  #;
  (map-function "c:r"     "refresh-datasets")
  (map-function "c:c"     "copy-value")
  (map-function "c:t"     "test")
  (map-function "f5"      "fetch-export-dataset")
  (map-function "c:t"     "fetch-export-dataset") ; XXX bad overlap with find
  (map-function "c:f"     "focus-search-box") ; XXX bad overlap with find
  (map-function "c:l"     "focus-search-box") ; this makes more sense
  #;
  (map-function "c:c;c:e" "fetch-export-dataset") ; XXX cua intersection
  (map-function "c:x"     "export-dataset")
  #; ; defined as the key for the menu item so avoid double calls
  (map-function "c:q"     "quit")
  (map-function "c:w"     "quit")
  (map-function "c:o" "open-dataset-folder")
  (map-function "c:p" "open-export-folder") ; FIXME these are bad bindings
  (map-function "c:j" "open-export-json")
  (map-function "c:i" "open-export-ipython"))

#;
(send keymap call-function "test" 1 (new event%))

;; gui setup

(define frame-main
  (new (class frame% (super-new)
         (rename-super [super-on-subwindow-char on-subwindow-char])
         (define/override (on-subwindow-char receiver event)
           (super-on-subwindow-char receiver event)
           (send keymap handle-key-event receiver event))
         (define/augment (on-close)
           (set! running? #f)
           (send frame-preferences show #f)
           (displayln "see ya later")))
       [label "sparcur control panel"]
       [width 1280]
       [height 1024]))

(define frame-helper
  (new frame%
       [label "invisible helper frame"]))

(define menu-bar-main (new menu-bar%
                           [parent frame-main]))
(define menu-file (new menu% [label "File"] [parent menu-bar-main]))
(define menu-item-quit (new menu-item%
                            [label "Quit"]
                            [shortcut #\Q]
                            [shortcut-prefix '(ctl)]
                            [callback k-quit]
                            [parent menu-file]))
(define menu-item-update-viewer (new menu-item%
                                     [label "Update Viewer"]
                                     [callback cb-update-viewer]
                                     [parent menu-file]))
(define menu-edit (new menu% [label "Edit"] [parent menu-bar-main]))
(define menu-item-preferences (new menu-item%
                                   [label "Preferences..."]
                                   [shortcut #\;]
                                   [shortcut-prefix '(ctl)]
                                   [callback (λ (a b) (send frame-preferences show #t))]
                                   [parent menu-edit]))

(define panel-holder (new panel:horizontal-dragable%
                          [parent frame-main]))

(define panel-left (new vertical-panel%
                        [parent panel-holder]))

(define panel-right (new vertical-panel%
                         [parent panel-holder]))

(define panel-org-actions (new horizontal-panel%
                               [stretchable-height #f]
                               [parent panel-left]))

(define text-search-box
  ; text box to make it easier to paste in identifiers or titles and
  ; find a match and view it
  (new text-field%
       [label ""]
       [callback cb-search-dataset]
       [parent panel-left]))

; lbt- label template
(define lbt-refresh-datasets "Refresh Datasets (~a)")
(define button-refresh-datasets (new button%
                                     [label lbt-refresh-datasets]
                                     [callback cb-refresh-dataset-metadata]
                                     [parent panel-org-actions]))

#; ; prefer file menu to avoid accidental clicks
(define button-update-viewer (new button%
                                  [label "Update Viewer"]
                                  [callback cb-update-viewer]
                                  [parent panel-org-actions]))

(define lview (new list-box%
                   [label ""]
                   [font (make-object font% 10 'modern)]
                   [choices '()]
                   [columns '("Lab" "Folder Name" "Identifier")]
                   ; really is single selection though we want to be
                   ; able to highlight and reorder multiple
                   [style '(single column-headers clickable-headers)]
                   [callback cb-dataset-selection]
                   [parent panel-left]))

; FIXME TODO scale these based on window size
; and remove the upper limit when if/when someone is dragging
(send* lview
  (set-column-width 0 50 50 100)
  (set-column-width 1 120 60 300)
  (set-column-width 2 120 60 9999))

(define panel-ds-actions (new horizontal-panel%
                              [stretchable-height #f]
                              [parent panel-right]))

(define button-fexport (new (tooltip-mixin button%)
                            [label "Fetch+Export"]
                            [tooltip "Shortcut C-f"] ; FIXME this should populate dynamically
                            [tooltip-delay 100]
                            [callback cb-fetch-export-dataset]
                            [parent panel-ds-actions]))

(define button-fetch (new (tooltip-mixin button%)
                          [label "Fetch"]
                          [tooltip "Shortcut <not-set>"]
                          [tooltip-delay 100]
                          [callback cb-fetch-dataset]
                          [parent panel-ds-actions]))

(define button-export-dataset (new (tooltip-mixin button%)
                                   [label "Export"]
                                   [tooltip "Shortcut C-x"]
                                   [tooltip-delay 100]
                                   [callback cb-export-dataset]
                                   [parent panel-ds-actions]))

; FIXME there is currently no way to go back to viewing the local
; export without running export or restarting
(define button-load-remote-json (new button%
                                     [label "View Prod Export"]
                                     [callback cb-load-remote-json]
                                     [parent panel-ds-actions]))

; FXIME probably goes in the bottom row of a vertical panel
(define button-toggle-expand (new button%
                                  [label "Expand All"]
                                  [callback cb-toggle-expand]
                                  [parent panel-ds-actions]))
(send button-toggle-expand enable #f) ; XXX remove when implementation complete

(define button-open-dataset-folder (new button%
                                        [label "Open Folder"]
                                        [callback cb-open-dataset-folder]
                                        [parent panel-ds-actions]))

(define button-open-dataset-remote (new button%
                                        [label "Open Remote"]
                                        [callback cb-open-dataset-remote]
                                        [parent panel-ds-actions]))

(define button-manifest-report (new button%
                                    [label "Manifest Report"]
                                    [callback cb-manifest-report]
                                    [parent panel-ds-actions]))

(define button-clean-metadata-files (new button%
                                         [label "Clean Metadata"]
                                         [callback cb-clean-metadata-files]
                                         [parent panel-ds-actions]))

#; ; too esoteric
(define button-open-export-folder (new button%
                                       [label "Open Export"]
                                       [callback cb-open-export-folder]
                                       [parent panel-ds-actions]))

(define button-open-export-json (new button%
                                         [label "Open JSON"]
                                         [callback cb-open-export-json]
                                         [parent panel-ds-actions]))

(define button-open-export-ipython (new button%
                                         [label "Open IPython"]
                                         [callback cb-open-export-ipython]
                                         [parent panel-ds-actions]))

(send button-open-export-json show (power-user?))
(send button-open-export-ipython show (power-user?))

;; manifest report

(define frame-manifest-report
  (new (class frame% (super-new)
         (rename-super [super-on-subwindow-char on-subwindow-char])
         (define/override (on-subwindow-char receiver event)
           (super-on-subwindow-char receiver event)
           (send keymap handle-key-event receiver event))
         #; ; show hide basically
         (define/augment (on-close)
           (displayln "prefs closed")))
       [label "sparcur manifest report"]
       [width 640]
       [height 480]))

(define edcanv-man-rep
  (new editor-canvas%
       [editor (new text%)]
       [parent frame-manifest-report]))

;; TODO preferences
(define frame-preferences
  (new (class frame% (super-new)
         (rename-super [super-on-subwindow-char on-subwindow-char])
         (define/override (on-subwindow-char receiver event)
           (super-on-subwindow-char receiver event)
           (send keymap handle-key-event receiver event))
         #; ; show hide basically
         (define/augment (on-close)
           (displayln "prefs closed")))
       [label "sparcur preferences"]
       [width 640]
       [height 480]))

; api keys
; paths
(define panel-prefs-holder (new vertical-panel%
                                [parent frame-preferences]))

#;
(define panel-prefs-left (new vertical-panel%
                        [parent panel-prefs-holder]))

#;
(define panel-prefs-right (new vertical-panel%
                         [parent panel-prefs-holder]))

(define text-prefs-api-key (new text-field%
                                [font (make-object font% 10 'modern)]
                                [label "API Key   "]
                                [init-value "fake-api-key-value"]
                                [parent panel-prefs-holder]))

(define text-prefs-api-sec (new text-field%
                                [font (make-object font% 10 'modern)]
                                [label "API Secret"]
                                [init-value "fake-api-sec-value"]
                                [parent panel-prefs-holder]))

(define panel-prefs-paths (new horizontal-panel%
                               [parent panel-prefs-holder]
                               ))
(define text-prefs-path-? (new text-field%
                                [font (make-object font% 10 'modern)]
                                [label "Path ???"]
                                [enabled #f]
                                [init-value "/fake/path/to/thing/for/reference"]
                                ; TODO need a way to click button to open
                                [parent panel-prefs-paths]))

(define (toggle-show obj)
  (send obj show (not (send obj is-shown?))))

(define check-box-power-user (new check-box%
                                  [label "Power user?"]
                                  [callback (λ (o e)
                                              (power-user? (not (power-user?)))
                                              ; XXX these can get out of sync
                                              (toggle-show button-open-export-json)
                                              (toggle-show button-open-export-ipython))]
                                  [parent panel-prefs-holder]))

(define button-prefs-path
  (new button%
       [label "Open Path"]
       ;[callback cb-toggle-expand]
       [parent panel-prefs-paths]))

(define (render-datasets)
  ; run hierlist open in the background since I can't figure out how to construct them open by default
  ; not perfect, but better than havin ui lag, don't try to spin up a thread per dataset
  (thread
   (thunk
    (println "starting to expand dataset views")
    (for [(dataset (current-datasets))]
      ; FIXME check on interactions with cb-refresh-dataset-metadata
      ; I think it is ok because everything runs in the dataset-id
      (when running?
        (dataset-jview! dataset #:background #t)
        #; ; debug
        (when running?
          (println (format "dataset view expanded for ~a" (dataset-id dataset))))))
    (when running?
      (println "finished expanding dataset views")))))

(module+ main
  (init-paths!)
  (define result (populate-datasets))
  (send frame-main show #t) ; show this first so that users know something is happening
  (send lview set-selection 0) ; first time to ensure current-dataset always has a value
  (send text-search-box focus)
  ; do this last so that if there is a 0th dataset the time to render the hierlist isn't obtrusive
  (cb-dataset-selection lview #f)
  (render-datasets))
