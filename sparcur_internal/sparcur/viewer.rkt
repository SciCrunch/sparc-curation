#lang racket

; TODO need to add a warning for files that are not accounted for in
; any manifest even though it is not technically an error in the same
; way that we do for folders

; TODO missing biophsyics as a general appraoch

; TODO remote-organization is currently read-only and is not easy to change/switch

; TODO make manifest report and paths report work from view prod export

; TODO protocols.io renew credientials workflow

; TODO reminder for download to avoid caching probably

; TODO need to refresh when fetch finishes

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
         net/url
         (except-in gregor date? date)
         (prefix-in oa- orthauth/base)
         orthauth/paths
         orthauth/python-interop
         orthauth/utils
         )

(define-runtime-path asdf "viewer.rkt")
(define this-file (path->string asdf))
(define this-file-compiled (with-handlers ([exn? (λ (e) this-file)]) (get-compilation-bytecode-file this-file)))
(define this-file-exe (embedding-executable-add-suffix (path-replace-extension this-file "") #f))
(define this-file-exe-tmp (path-add-extension this-file-exe "tmp"))
(define this-package-path (let-values ([(parent name dir?) (split-path this-file)]) parent))

(when (and this-file-exe-tmp (file-exists? this-file-exe-tmp))
  ; windows can't remove a running exe ... but can rename it ... and then delete the old
  ; file on next start
  (delete-file this-file-exe-tmp))

(define running? #t) ; don't use parameter, this needs to be accessible across threads
(define update-running? #f)
; true global variables that should not be thread local
(define *current-dataset* #f)
(define *current-datasets* #f)
(define *current-datasets-view* #f)

;; parameters (yay dynamic variables)
(define path-config (make-parameter #f))
(define path-log-dir (make-parameter #f))
(define path-cache-dir (make-parameter #f))
(define path-cache-push (make-parameter #f))
(define path-cache-datasets (make-parameter #f))
(define path-cleaned-dir (make-parameter #f))
(define path-export-dir (make-parameter #f))
(define path-export-datasets (make-parameter #f))
(define path-source-dir (make-parameter #f))
(define url-prod-datasets (make-parameter "https://cassava.ucsd.edu/sparc/datasets"))
(define *current-blob* #f)
(define current-blob
  (case-lambda
    [() *current-blob*]
    [(value) (set! *current-blob* value)]))
(define current-dataset
  (case-lambda
    [() *current-dataset*]
    [(value) (set! *current-dataset* value)]))
(define current-datasets
  (case-lambda
    [() *current-datasets*]
    [(value) (set! *current-datasets* value)]))
(define current-datasets-view
  (case-lambda
    [() *current-datasets-view*]
    [(value) (set! *current-datasets-view* value)]))
(define *current-jview* #f)
(define current-jview
  (case-lambda
    [() *current-jview*]
    [(value) (set! *current-jview* value)]))
(define *current-mode-panel* #f)
(define current-mode-panel ; TODO read from config/history
  (case-lambda
    [() *current-mode-panel*]
    [(value) (set! *current-mode-panel* value)]))
(define overmatch (make-parameter #f))
(define power-user? (make-parameter #f))

(define *allow-update* #f)
(define allow-update?
  ; "don't set this, it should only be used to keep things in sync with the config"
  (case-lambda
    [() *allow-update*]
    [(value) (set! *allow-update* value)]))

;; TODO add check to make sure that the python modules are accessible as well

(define terminal-emulator
  (begin
    #;
    (sort (environment-variables-names (current-environment-variables)) bytes<?)
    (for/or ([emu '("urxvt"
                    "xfce4-terminal"
                    "konsole"
                    "gnome-terminal"
                    #; ; here's a nickle kid, go buy yourself a real terminal emulator
                    "xterm")]
             [args (list "-cd" ; urxvt
                         "--default-working-directory" ; xfce4-terminal
                         "--workdir" ; konsole
                         "--working-directory" ; gnome-terminal
                         #; ; xterm
                         (lambda (ps) (format "-e 'cd \"~a\"; ~a'" ps (getenv "SHELL"))))])
      (let ([ep (find-executable-path emu)])
        (and ep (list ep args))))))

(define (unix-vt path-string)
  (append terminal-emulator (list path-string)))

(define (win-vt path-string)
  (list (find-executable-path "cmd") "/c" "start"
        "powershell" "-NoLogo" "-WindowStyle" "normal" "-WorkingDirectory" path-string))

(define (macos-vt path-string)
  ;; oh the horror ... racket -> osascript -> bash/zsh/whoevenknows
  ;; and yes, users put single quotes in their dataset names ALL THE TIME AAAAAAAAAAAAAAAAAAAAAAAAA
  (let* ([ps (string-replace (string-replace path-string "'" "\\'") "\"" "\\\"")]
         [horror (format "tell application \"Terminal\" to do script \"cd '~a'\"" ps)])
    (list (find-executable-path "osascript") "-e" horror)))

(define (vt-at-path path [os #f])
  (case (or os (system-type))
    ((unix) (unix-vt path))
    ((macosx) (macos-vt path))
    ((windows) (win-vt path))))

;; string constants

(define msg-dataset-not-fetched "Dataset has not been fetched yet!")
(define msg-dataset-not-exported "Dataset has not been exported yet!")
(define msg-no-logs "Dataset has no logs.")
(define missing-var "???")

;; other variables

(define include-keys
  ; have to filter down due to bad performance in the viewer
  ; this is true even after other performance improvements
  '(id meta rmeta status prov submission))

;; python argvs

(define (python-mod-args module-name . args)
  (cons (python-interpreter) (cons "-m" (cons module-name args))))

(define argv-simple-for-racket (python-mod-args "sparcur.simple.utils" "for-racket")) ; FIXME needs --project-id probably
(define (argv-simple-for-racket-meta path-string)
  ; this is peak LOL PYTHON for slowness on startup which makes this completely non-viable
  (parameterize ([python-interpreter
                  (if (string=? (python-interpreter) "pypy3") ; bad startup time ??? no just LOL PYTHON everywhere
                      "python"
                      (python-interpreter))])
    (python-mod-args "sparcur.simple.utils" "for-racket" "meta" path-string)))
(define (argv-simple-diff ds)
  (python-mod-args "sparcur.simple.utils" "for-racket" "diff"
                   (path->string (dataset-working-dir-path ds))))
(define (argv-simple-make-push-manifest ds updated-transitive push-id)
  (python-mod-args "sparcur.simple.utils" "for-racket" "make-push-manifest"
                   (dataset-id ds) updated-transitive push-id (path->string (dataset-working-dir-path ds))))
(define (argv-simple-push ds updated-transitive push-id)
  (python-mod-args "sparcur.simple.utils" "for-racket" "push" (dataset-id ds) updated-transitive push-id
                   (path->string (dataset-working-dir-path ds))))
(define argv-simple-git-repos-update (python-mod-args "sparcur.simple.utils" "git-repos" "update"))
(define argv-spc-export (python-mod-args "sparcur.cli" "export"))
(define (argv-simple-retrieve ds) (python-mod-args "sparcur.simple.retrieve" "--sparse-limit" "-1" "--dataset-id" (dataset-id ds)))
(define argv-spc-find-meta
  (python-mod-args
   "sparcur.cli"
   "find"
   "--name" "*.xml"
   "--name" "submission*"
   "--name" "code_description*"
   "--name" "dataset_description*"
   "--name" "subjects*"
   "--name" "samples*"
   "--name" "sites*"
   "--name" "performances*"
   "--name" "manifest*"
   "--name" "resources*"
   "--name" "README*"
   "--limit" "-1"
   "--fetch"))

(define (argv-clean-metadata-files ds)
  (let ([argv (python-mod-args ; XXX note that this is separate from normalize metadata files
               "sparcur.simple.clean_metadata_files"
               "--dataset-id" (dataset-id ds))])
    (if (power-user?) ; FIXME decouple
        (append argv '("--log-level" "DEBUG"))
        argv)))

(define (argv-open-dataset-shell ds)
  (let ([path (dataset-src-path ds)])
    (if (directory-exists? path)
        (vt-at-path (dataset-working-dir-path ds))
        (begin (println msg-dataset-not-fetched) #f))))

(define (xopen-dataset-latest-log ds)
  (let ([latest-log-path (dataset-latest-log-path ds)])
    (if latest-log-path
        (xopen-path latest-log-path)
        (begin (println msg-no-logs) #f))))

(define (argv-download-everything ds)
  (python-mod-args
   "sparcur.simple.fetch_files"
   "--extension" "*"
   (dataset-working-dir-path ds)))

(define (argv-open-export-ipython ds)
  (let*-values ([(path) (dataset-export-latest-path ds)]
                [(parent name dir?) (split-path path)]
                [(path-meta-path) (build-path parent "path-metadata.json")]
                [(python-code) ; LOL PYTHON can't use with in the special import line syntax SIGH
                 (format "import json;print('~a');f = open('~a', 'rt');\
                          blob = json.load(f);f.close();f = open('~a', 'rt');\
                          path_meta = json.load(f);f.close()"
                         parent path path-meta-path)])
    (if (directory-exists? parent)
        (append (unix-vt parent)
                (cons "-e" ; FIXME running without bash loses readline somehow
                      (cons "rlwrap"
                            (python-mod-args
                             "IPython"
                             "-i" "-c"
                             python-code))))
        (begin (println msg-dataset-not-exported) #f))))

(define path-getfattr (find-executable-path "getfattr"))
(define (argv-getfattr path-string) (list path-getfattr "-d" path-string))

;; utility functions

(define --sigh (gensym))
(define (py-system* exe #:set-pwd? [set-pwd? --sigh] . args)
  (call-with-environment
   (λ ()
     (if (eq? set-pwd? --sigh)
         (apply system* exe args)
         (apply system* exe args #:set-pwd? set-pwd?)))
   '(("PYTHONBREAKPOINT" . "0")
     ; silence error logs during pennsieve top level import issue
     ("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION" . "python"))))

(define (datetime-file-system-safe inexact-seconds)
  "Y-M-DTHMS,6Z" ; FIXME this is producing garbages results with weird and bad padding
  (let ([d (seconds->date inexact-seconds #f)])
    (string-join
     (map (λ (e) (format "~a" e))
          (list
           (date-year d)
           "-"
           (date-month d)
           "-"
           (date-day d)
           "T"
           (date-hour d)
           (date-minute d)
           (date-second d)
           ","
           (inexact->exact (truncate (/ (date*-nanosecond d) 1e3)))
           "Z"
           ))
     "")))

#; ; broken due to padding issues above
(define (datetime-for-path-now)
  (datetime-file-system-safe
   (/ (current-inexact-milliseconds) 1e3)))

(define time-format-friendly "YYYY-MM-dd'T'HHmmss,SSSSSSX")
(define (datetime-for-path-now)
  (~t (posix->moment (/ (current-inexact-milliseconds) 1e3) "Etc/UTC") time-format-friendly))

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
  (parameterize ([oa-current-auth-config-path (python-mod-auth-config-path "sparcur")])
    (define ac (oa-read-auth-config))
    (path-config (build-path (expand-user-path (user-config-path "sparcur")) "viewer.rktd"))
    ; FIXME sppsspps stupidity
    (path-source-dir (or (oa-get-path ac 'data-path #:exists? #f)
                         (build-path (find-system-path 'home-dir) "files" "sparc-datasets")))
    (path-log-dir (build-path
                   (or (oa-get-path ac 'log-path #:exists? #f)
                       (expand-user-path (user-log-path "sparcur")))
                   "datasets"))
    (path-cache-dir (build-path
                     (or (oa-get-path ac 'cache-path #:exists? #f)
                         (expand-user-path (user-cache-path "sparcur")))
                     "racket"))
    (path-cache-push
     (build-path ; must match python or sparcur.simple.utils won't be able to find {push-id}/paths.sxpr
      (or (oa-get-path ac 'cache-path #:exists? #f)
          (expand-user-path (user-cache-path "sparcur")))
      "push"))
    (path-cache-datasets (build-path (path-cache-dir) "datasets-list.rktd"))
    (path-cleaned-dir (or (oa-get-path ac 'cleaned-path #:exists? #f)
                          (expand-user-path (user-data-path "sparcur" "cleaned"))))
    (path-export-dir (or (oa-get-path ac 'export-path #:exists? #f)
                         (expand-user-path (user-data-path "sparcur" "export"))))
    (path-export-datasets (build-path (path-export-dir) "datasets"))))

(define (save-config!)
  (with-output-to-file (path-config)
    #:exists 'replace
    (λ () (pretty-write
           (list
            (cons 'viewer-mode viewer-mode-state)
            (cons 'power-user? (power-user?))
            )))))

(define (*->string maybe-string)
  (cond
    [(symbol? maybe-string) (symbol->string maybe-string)]
    [(number? maybe-string) (number->string maybe-string)]
    [(keyword? maybe-string) (keyword->string maybe-string)]
    [(path? maybe-string) (path->string maybe-string)]
    [(boolean? maybe-string) (format "~a" maybe-string)]
    [(string? maybe-string) maybe-string]
    [else (error '*->string "unknown object type ~s" maybe-string)]))

(define (load-config!)
  ; TODO various configuration options
  (parameterize ([oa-current-auth-config-path (python-mod-auth-config-path "sparcur")])
    (define config (oa-read-auth-config))
    (let ([org (oa-get config 'remote-organization)]
          [orgs (oa-get config 'remote-organizations)] ; TODO
          [never-update (oa-get config 'never-update)])
      (allow-update? (not never-update))
      (send menu-item-update-viewer enable (allow-update?))
      (let* ([pc (path-config)]
             [cfg (if (and pc (file-exists? pc))
                      (with-input-from-file pc
                        (λ ()
                          (read)))
                      '())])
        (define (obfus str [ends 4])
          (let ([ls (string-length str)])
            (string-append
             (substring str 0 ends)
             (make-string (- ls (* 2 ends)) #\*)
             (substring str (- ls ends) ls))))
        (send text-prefs-remote-organization set-value (*->string org))
        (send text-prefs-api-key set-value (obfus (*->string (oa-get-sath 'pennsieve org 'key))))
        (send text-prefs-api-sec set-value (obfus (*->string (oa-get-sath 'pennsieve org 'secret))))
        #;
        (send text-prefs-path-? set-value "egads")
        (send text-prefs-path-config set-value (path->string (path-config)))
        (send text-prefs-path-user-config set-value (oa-user-config-path))
        (send text-prefs-path-secrets set-value (oa-secrets-path))
        (send text-prefs-path-data set-value (path->string (path-source-dir)))
        (let* ([config-exists (assoc 'viewer-mode cfg)]
               [power-user-a (assoc 'power-user? cfg)]
               [power-user (and power-user-a (cdr power-user-a))])
          (if config-exists
              (begin
                ; set-selection does not trigger the callback
                (send radio-box-viewer-mode set-selection (cdr config-exists))
                (cb-viewer-mode radio-box-viewer-mode #f)
                (power-user? (not power-user))
                ; cb does the toggle interinally so we set the opposite of what we want first
                (cb-power-user check-box-power-user #f))
              (set-current-mode-panel! panel-validate-mode)))))))

(define refresh-dataset-metadata-semaphore (make-semaphore 1))
(define (refresh-dataset-metadata text-search-box)
  ; XXX requries python sparcur to be installed
  (call-with-semaphore
   refresh-dataset-metadata-semaphore
   (thunk
    (dynamic-wind
      (thunk (send button-refresh-datasets enable #f))
      (thunk
       (let* ([result (get-dataset-list)]
              [datasets (result->dataset-list result)]
              [unfiltered? (= 0 (string-length (string-trim (send text-search-box get-value))))])
         (current-datasets datasets)
         (if unfiltered?
             (set-datasets-view! (send text-search-box list-box) (current-datasets))
             (cb-search-dataset text-search-box #f))
         (println "dataset metadata has been refreshed") ; TODO gui viz on this (beyond updating the number)
         (with-output-to-file (path-cache-datasets)
           #:exists 'replace ; yes if you run multiple of these you could have a data race
           (λ () (pretty-write result)))))
      (thunk (send button-refresh-datasets enable #t))))))

(define (get-path-err)
  (hash-ref
   (hash-ref
    (current-blob)
    'status
    (hash))
   'path_error_report
   #f))

(define (paths-report)
  (for-each (λ (m) (displayln (regexp-replace #rx"SPARC( Consortium)?/[^/]+/" m "\\0\n")) (newline))
            ; FIXME use my hr function from elsewhere
            (let ([ihr (get-path-err)])
              (if ihr
                  (hash-ref (hash-ref ihr '|#/specimen_dirs| #hash((messages . ()))) 'messages)
                  '()))))

(define (manifest-report)
  ; FIXME this will fail if one of the keys isn't quite right
  ; TODO displayln this into a text% I think?
  (for-each (λ (m) (displayln (regexp-replace #rx"SPARC( Consortium)?/[^/]+/" m "\\0\n")) (newline))
            ; FIXME use my hr function from elsewhere
            (let ([ihr (get-path-err)])
              (if ihr
                  (hash-ref (hash-ref ihr '|#/path_metadata/-1| #hash((messages . ()))) 'messages)
                  '()))))

;; update viewer

(define (update-viewer)
  "stash and pull all git repos, rebuild the viewer"
  ;; FIXME the menu option should be disabled if on a system where this is broken/banned
  ; find the git repos
  ;; XXX for upgrading python/racket from setup.org we need PYROOTS and RKTROOTS
  ;; and then REPOS, see specifically clone-repos and python-setup
  ;; XXX also bug where git@github.com:org/repo.git was used but no key is available
  (if (or update-running? (not (allow-update?)))
      (println
       (if update-running?
           "Update is already running!"
           "User config is set to never allow update (e.g. because this is a development system)."))
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
                      (apply py-system* argv-simple-git-repos-update))])
               ; TODO pull changes for racket dependent repos as well
               ; TODO raco pkg install local git derived packages as well
               (println (format "running raco pkg update ~a" this-package-path))
               (let ([mtime-before (file-or-directory-modify-seconds
                                    this-file-compiled
                                    #f
                                    (λ () -1))])
                 (parameterize ()
                   (system* raco-exe "pkg" "update" "--batch" this-package-path)
                   #;
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
                       (when (file-exists? this-file-exe)
                         (rename-file-or-directory this-file-exe this-file-exe-tmp))
                       (system* raco-exe "exe" "-v" "-o" this-file-exe this-file)
                       (unless (file-exists? this-file-exe) ; restore the old version on failure
                         (when (file-exists? this-file-exe-tmp)
                           (rename-file-or-directory this-file-exe-tmp this-file-exe))))
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
             (println "Update complete!")
             #; ; TODO issue this only if the viewer itself was updated and there was a success
             (println "Restart at your convenience.")
             )
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

(define (set-jview-json! jview json)
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
                                  'meta (hash
                                         'id-project (id-project dataset)
                                         'folder_name (dataset-title dataset)
                                         'owner (dataset-pi-name-lf dataset)
                                         'updated (dataset-updated dataset))))]
                       [jhash (for/hash ([(k v) (in-hash json)]
                                         ; FIXME I think we don't need include keys anymore
                                         ; XXX false, there are still performance issues
                                         #:when (member k include-keys))
                                (values k v))])
                  (unless (or background (not (is-current? dataset)))
                    (current-blob json)) ; FIXME this will go stale
                  (set-jview-json! jview-inner jhash)
                  (hash-set! jviews uuid jview-inner)
                  jview-inner)))])
    jview))

(define (unset-current-jview)
  (let ([old-jview (current-jview)])
    (when old-jview
      (send old-jview reparent frame-helper)
      (current-jview #f))))

(define jviews-loading (set))
(define jview-semaphore (make-semaphore 1))
(define (set-jview! dataset)
  (let ([uuid (id-uuid dataset)])
    ; unset-current-jview is safe to call unconditionally because even
    ; if we can't draw the jview for the new dataset we definitely should
    ; stop drawing the jview for the most recently deselected dataset
    (unset-current-jview)
    ; when scrolling fast back and forth don't try to load the same jview multiple times
    (unless (set-member? jviews-loading uuid)
      (set! jviews-loading (set-add jviews-loading uuid))
      ; setting current dataset must be called in the main thread so that
      ; curent-dataset is always guranteed to be synchronized with the gui view ...
      ; setting current dataset is called with a semaphore becuase I have seen one freak case
      ; where a hang resulted in the double jview behavior, I have tested with this and not
      ; seen it happen again, however that doesn't mean much, in principle this should block
      ; the main thread from proceeding until the fast part of the thread below is done which
      ; in principle should ensure fully sequential behavior, but that's just the theory
      (call-with-semaphore
       jview-semaphore
       (thunk (current-dataset dataset))) ; XXX this is the only place current-dataset should ever be set
      (thread
       (thunk ; we thread because dataset-jview! can be quite slow for large json blobs
        (let ([jview (dataset-jview! dataset)])
          ; we're done loading so if you want to load again knock yourself out
          (set! jviews-loading (set-remove jviews-loading uuid))
          ; we must use a semaphore here because if you have two consecutive events
          ; that are very close together in time the exact orderin of the threads
          ; they spawn can result in a case where (is-current? a) will be called and
          ; return true before (current-dataset b) is called and (reparent a) is called
          ; after (b-unparent) is called, said another way, the call to unset-current-jview
          ; in in main thread for b can fall between the call (is-current? a) and (reparent a)
          ; in the a subthread, the semaphore ensures that even if this happens that the call
          ; to unset-current-jview in b will unparent a because b's reparent cannot start until
          ; after a's finishes in the case where b's unset has been called between ica and ra (confusing I know)
          (call-with-semaphore
           jview-semaphore
           (thunk
            (when (string=? uuid (id-uuid (current-dataset)))
              ; even with the semaphore there is a chance that another thread snuck in and set itself while
              ; this one was working so we unset the current jview again here otherwise we periodically get two jviews
              (unset-current-jview)
              (send jview reparent panel-right)
              (current-jview jview))))))))))

;; dataset struct and generic functions

(define-generics ds
  (populate-list ds list-box)
  (lb-cols ds)
  (lb-data ds)
  (updated-short ds)
  (id-short ds)
  (id-uuid ds)
  (id-project ds)
  (uri-human ds)
  (uri-sds-viewer ds)
  (dataset-src-path ds)
  (dataset-working-dir-path ds)
  (dataset-log-path ds)
  (dataset-latest-log-path ds)
  (dataset-export-latest-path ds)
  (dataset-cleaned-latest-path ds)
  (fetch-export-dataset ds)
  (fetch-dataset ds)
  (clean-metadata-files ds)
  (load-remote-json ds)
  (export-dataset ds)
  (is-current? ds))

(struct dataset (id title updated pi-name-lf id-project)
  #:methods gen:ds
  [(define (populate-list ds list-box)
     ; FIXME annoyingly it looks like these things need to be set in
     ; batch in order to get columns to work
     (send list-box append
           (list
            (dataset-pi-name-lf ds)
            (dataset-title ds)
            (id-short ds)
            (updated-short ds))
           ds))
   (define (lb-cols ds)
     (list
      (dataset-pi-name-lf ds)
      (dataset-title ds)
      (id-short ds)
      (updated-short ds)))
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
   (define (dataset-working-dir-path ds)
     (let ([path (dataset-src-path ds)])
       (if (directory-exists? path)
           (resolve-relative-path (build-path path "dataset"))
           (error msg-dataset-not-fetched))))
   (define (dataset-log-path ds)
     (let ([uuid (id-uuid ds)])
       (build-path (path-log-dir) uuid)))
   (define (dataset-latest-log-path ds)
     (define lp (build-path (dataset-log-path ds) "LATEST/stdout.log"))
     (and (file-exists? lp) (resolve-path lp)))
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
                                                 (apply py-system* argv-1 #:set-pwd? #t)))))
           (parameterize ([current-directory (resolve-relative-path cwd-2)])
             (with-output-to-string (thunk (set! status-2
                                                 (apply py-system* argv-2 #:set-pwd? #t)))))
           (when status-1 (set-button-status-for-dataset ds))
           (if (and status-1 status-2)
               (println (format "dataset fetch completed for ~a" (dataset-id ds)))
               (println (format "dataset fetch FAILED for ~a" (dataset-id ds)))))))))
   (define (export-dataset ds)
     (println (format "dataset export starting for ~a" (dataset-id ds))) ; TODO gui and/or log
     (let ([cwd-2 (build-path (dataset-src-path ds)
                              ; FIXME dataset here is hardcoded
                              "dataset")]
           [argv-3 (append argv-spc-export '("--fill-cache-metadata"))]) ; fill cache metadata in case a file was modified and xattrs were lost
       (thread
        (thunk
         (let ([status-3 #f]
               [cwd-2-resolved (resolve-relative-path cwd-2)])
           (parameterize ([current-directory cwd-2-resolved])
             (with-output-to-string (thunk (set! status-3
                                                 (apply py-system* argv-3 #:set-pwd? #t)))
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
           [dataset-log-path-now (build-path (dataset-log-path ds) (datetime-for-path-now) "stdout.log")]
           [argv-1 (argv-simple-retrieve ds)]
           [argv-2 argv-spc-find-meta]
           [argv-3 argv-spc-export])
       (let*-values ([(date-path _ __) (split-path dataset-log-path-now)]
                     [(uuid-path local-date-path __) (split-path date-path)]
                     [(latest-path) (build-path uuid-path "LATEST")])
         (ensure-directory! uuid-path)
         (ensure-directory! date-path)
         (when (link-exists? latest-path)
           (delete-file latest-path))
         (make-file-or-directory-link local-date-path latest-path))
       (thread
        (thunk
         (let ([status-1 #f]
               [status-2 #f]
               [status-3 #f])
           (ensure-directory! cwd-1)
           (parameterize ([current-directory cwd-1])
             #;
             (println (string-join argv-1 " "))
             (with-output-to-file
               dataset-log-path-now
               (thunk
                (parameterize (#;[current-error-port (current-output-port)]) ; TODO
                  (set! status-1
                        (apply py-system* argv-1 #:set-pwd? #t))))
               #:exists 'append))
           (if status-1
               (let ([cwd-2-resolved (resolve-relative-path cwd-2)])
                 (parameterize ([current-directory cwd-2-resolved])
                   (with-output-to-file
                     dataset-log-path-now
                     (thunk (set! status-2
                                  (apply py-system* argv-2 #:set-pwd? #t)))
                     #:exists 'append)
                   ; TODO figure out if we need to condition further steps to run on status-2 #t
                   (println (format "dataset fetch for export completed for ~a" (dataset-id ds)))
                   (set-button-status-for-dataset ds)
                   (with-output-to-file
                     dataset-log-path-now
                     (thunk (set! status-3
                                  (apply py-system* argv-3 #:set-pwd? #t)))
                     #:exists 'append))
                 ; TODO gui indication
                 (if (and status-2 status-3)
                     (begin
                       ; now that we have 1:1 jview:json we can automatically update the jview for
                       ; this dataset in the background thread and it shouldn't block the ui
                       (when (equal? ds (current-dataset))
                         (send button-export-dataset enable #t)
                         (send button-open-dataset-shell enable #t)
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
                                                 (apply py-system* argv-1 #:set-pwd? #t)))
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
       (set-jview-json! jview-inner jhash)
       (hash-set! jviews uuid jview-inner)
       jview-inner))
   (define (set-lview-item-color lview ds)
     ; find the current row based on data ??? and then change color ?
     (println "TODO set color") ; pretty sure this cannot be done with this gui widgit
     )
   (define (updated-short ds)
     (if (string=? (dataset-updated ds) missing-var)
         (dataset-updated ds)
         (let* ([momnt-z (iso8601->moment (dataset-updated ds))]
                [momnt-l (adjust-timezone momnt-z (current-timezone))])
           (~t momnt-l "YY-MM-dd  HH:mm"))))
   (define (id-short ds)
     (let-values ([(N type uuid)
                   (apply values (string-split (dataset-id ds) ":"))])
       (string-append
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
   (define (is-current? ds)
     ; https://docs.racket-lang.org/guide/define-struct.html#(part._struct-equal)
     (let ([cd (current-dataset)])
       (and cd (string=? (id-uuid ds) (id-uuid cd)))))
   (define (id-project ds)
     ; TODO cache these, and probably derive from org list, include in dataset struct
     ; etc. so that we don't have to read from the file system at all
     (dataset-id-project ds)
     #;
     (let* ([symlink (build-path (dataset-src-path ds) "dataset")]
            [resolved (and (link-exists? symlink)
                           (path->complete-path ; resolve-relative-path
                            (resolve-path symlink)
                            (simplify-path (build-path symlink 'up) #f)))]
            [project-path (simplify-path (build-path resolved 'up) #f)]
            #;; XXX this is hideously slow
            [meta (and resolved (get-path-meta project-path))])
       #;
       (cadr (member ':id meta))
       (let* ([status-1 #f]
              [sigh (with-output-to-string (thunk (set! status-1 (apply system* (argv-getfattr project-path)))))]
              [lines (string-split sigh)]
              [m (for/list ([l (in-list lines)] #:when (string-prefix? l "user.bf.id")) (last (string-split l "=")))]
              [i (read (open-input-string (car m)))])
         i)
       #;
       "N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0"))
   (define (uri-human ds)
     (string-append "https://app.pennsieve.io/" (id-project ds) "/datasets/N:dataset:" (id-uuid ds)))
   (define (uri-sds-viewer ds)
     ; FIXME hardcoded
     (string-append "https://metacell.github.io/sds-viewer/?id=" (id-uuid ds)))])

(define *current-lview-column* 3)
(define *current-lview-ascending* #f)
(define (col-lab-base list-box column-number)
  (car (string-split (list-ref (send list-box get-column-labels) column-number))))
(define (col-lab-updn list-box column-number asc)
  (send list-box set-column-label column-number
        (string-append
         (col-lab-base list-box column-number)
         " "
         ; arrow points toward bigger seems clearer?
         ; up and down arrows are too small and hard to read ↓↑
         (if asc "v" "^"))))
(define (set-lview-column-state! list-box column-number #:view-only? [view-only? #f])
  ; FIXME ick bad convention on #:view-only ugh
  (let ([old-column *current-lview-column*])
    ; FIXME should probably retain a hash table or something because if there was ever
    ; more than one list box in the ui only lview itself would have its state change ...
    (if (= *current-lview-column* column-number)
        (begin
          (unless view-only?
            (set! *current-lview-ascending* (not *current-lview-ascending*)))
          (col-lab-updn list-box column-number *current-lview-ascending*))
        (begin
          (when view-only? (error 'duh "should never be called this way"))
          (set! *current-lview-column* column-number)
          ; after experimenting with the ux, preserving the asc/desc when changing
          ; columns provides a much nicer experience than always resetting to something
          ; therefor we do not set *current-lview-ascending*
          (send list-box set-column-label old-column (col-lab-base list-box old-column))
          (col-lab-updn list-box column-number *current-lview-ascending*)))))

(define (make-sort-datasets column ascending)
  (define operator
    (if ascending string<? string>?))
  (define selector
    (case column
      [(0) (compose string-downcase dataset-pi-name-lf)]
      [(1) (compose string-downcase dataset-title)]
      [(2) id-uuid]
      [(3) dataset-updated]))
  (lambda (ds1 ds2)
    (operator (selector ds1) (selector ds2))))

(define (current-dataset-sort)
  (make-sort-datasets *current-lview-column* *current-lview-ascending*))

(define (set-datasets-view! list-box datasets)
  (define selected (current-dataset))
  (current-datasets-view datasets)
  (define sorted (sort datasets (current-dataset-sort)))
  (send/apply list-box set (apply map list (map lb-cols sorted)))
  (for ([ds datasets]
        [n (in-naturals)])
    (send list-box set-data n (lb-data ds)))
  (send button-refresh-datasets set-label
        (format lbt-refresh-datasets (length datasets))) ; XXX free variable on the button in question
  (let ([current-dataset-index (index-of sorted selected
                                         (lambda (element target) ; struct id changes so eq? by itself fails
                                           (and element target (string=? (id-uuid element) (id-uuid target)))))])
    (when current-dataset-index
      (send list-box select current-dataset-index))))

(define (set-datasets-view*! list-box . datasets)
  (set-datasets-view! list-box datasets))

(define (get-selected-dataset list-box)
  (let ([indexes (send list-box get-selections)])
    (if (null? indexes)
        #f
        (send list-box get-data (car indexes)))))

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

(define set-button-semaphore (make-semaphore 1))
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
      (call-with-semaphore
       set-button-semaphore
       (thunk
        (if (is-current? ds)
            (let ([enable? (link-exists? symlink)]
                  [logs-enable? (dataset-latest-log-path (current-dataset))]
                  [export-enable? (dataset-export-latest-path (current-dataset))])
              ; source data folder
              (for ([button (in-list all-button-open-dataset-folder)]) (send button enable enable?))
              (send button-export-dataset enable enable?)
              (send button-open-dataset-shell enable enable?)
              (send button-clean-metadata-files enable enable?)
              (send button-upload-changes enable enable?)
              ; export
              (send button-open-export-json enable export-enable?)
              (send button-open-export-ipython enable export-enable?)
              ; logs
              (send button-open-dataset-latest-log enable logs-enable?)
              )
            (void)
            #; ; yep, this case does happen :)
            (println "TOO FAST! dataset no longer selected")))))))
  (void))

;; callbacks

(define (make-cb-open-path text-field)
  (λ (o e)
    (xopen-path (send text-field get-value))))

(define (cb-dataset-selection obj event)
  (let ([event-type (if event (send event get-event-type) #f)])
    (case event-type
      [(list-box-column)
       (let ([column (send event get-column)])
         (set-lview-column-state! obj column)
         (set-datasets-view! obj (current-datasets-view)))]
      [else
       (let ([dataset (get-selected-dataset obj)])
         (unless (or (not dataset) (is-current? dataset))
           (set-jview! dataset)
           (set-button-status-for-dataset dataset)))])))

(define (result->dataset-list result)
  (let ([nargs (procedure-arity dataset)])
    (map (λ (ti)
           (let ([pad (for/list ([i (in-range (- nargs (length ti)))]) missing-var)])
             (apply dataset (append ti pad))))
         result)))

(define (get-dataset-list)
  (let* ([argv argv-simple-for-racket]
         [status #f]
         [result-string
          (parameterize ()
            (with-output-to-string (λ () (set! status (apply py-system* argv)))))]
         [result (read (open-input-string result-string))])
    (unless status
      (error "Failed to get dataset list! ~a" (string-join argv-simple-for-racket " ")))
    result))

(define (get-path-meta path)
  (let* ([path-string (path->string path)]
         [argv (argv-simple-for-racket-meta path-string)]
         [status #f]
         [result-string
          (parameterize ()
            (with-output-to-string (λ () (set! status (apply py-system* argv)))))]
         [result (read (open-input-string result-string))])
    (unless status
      (error "Failed to get dataset cache meta! ~a" (string-join argv " ")))
    result))

(define (cb-toggle-prefs o e)
  (let ([do-show? (not (send frame-preferences is-shown?))])
    (when do-show? ; resync with any external changes
      (load-config!))
    (send frame-preferences show do-show?)))

(define iconize-hack (eq? 'unix (system-type)))
(define (cb-toggle-upload o e #:show? [show? #f])
  ; TODO detect when an actual quite is run
  ; FIXME toggling this via button when the window is open is extremely confusing
  ; it should probably raise the window in that case?
  (let*-values
      ([(new? frame-upload) (get-frame-upload! (current-dataset))]
       [(do-show?) (or show? (not (send frame-upload is-shown?)))])
    #; ; not clear whether we actually ever want to close these even if the user hits the x on the window
    ; it is unlikely to leak large amounts of memory and there is no reason to lose the state
    (println (list 'cbtu: o e))
    (send frame-upload show do-show?)
    ; iconize hack to get the frame to come to the front when using gtk
    (when iconize-hack
      (send frame-upload iconize #t)
      (send frame-upload iconize #f))
    (when (and do-show? (or new? (not (send frame-upload updated-once?))))
      (send frame-upload update))))

(define (cb-upload-button-show-and-raise o e)
  (cb-toggle-upload o e #:show? #t))

(define (cb-refresh-dataset-metadata obj event)
  (thread
   (thunk ; FIXME text-search-box is a free variable
    (refresh-dataset-metadata text-search-box))))

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
  (let* ([is-win? #f]
         [command (find-executable-path
                  (case (system-type 'os*)
                    ((linux) "xdg-open") ; if firefox complains, make sure it matches firefox not firefox-bin xdg-settings get default-web-browser
                    ((macosx) "open")
                    ((windows) (set! is-win? #t) "explorer.exe") ; requires an associated file type
                    (else (error "don't know xopen command for this os"))))])
    #; ; don't use subprocess for this, there is WAY too much cleanup required
    (subprocess #f #f #f command path)
    (thread
     (thunk
      (let* ([is-dir? #f]
             [cwd
             (cond
               [(directory-exists? path) (set! is-dir? #t) path]
               [(file-exists? path) (simple-form-path (build-path path 'up))]
               [(regexp-match #rx"^https?" path) (current-directory)]
               [else (error 'xopen-path "path-does-not-exist: ~s" path)])])
        (parameterize ([current-directory cwd])
          (system* command (if (and is-win? is-dir?) "." path) #:set-pwd? #t)))))))

#; ; no longer needed
(define (xopen-folder path)
  (case (system-type)
    ((windows)
     (thread
      (thunk
       (parameterize ([current-directory path])
         (system* (find-executable-path "explorer.exe") "." #:set-pwd? #t)))))
    (else (xopen-path path))))

(define (cb-open-export-ipython obj event)
  (let ([argv (argv-open-export-ipython (current-dataset))])
    ; FIXME bad use of thread
    (when argv
      (thread (thunk (apply system* argv))))))

(define (cb-open-dataset-shell obj event)
  (let ([argv (argv-open-dataset-shell (current-dataset))])
    ; FIXME bad use of thread
    (when argv
      (thread (thunk (apply system* argv))))))

(define (cb-open-dataset-lastest-log obj event)
  (xopen-dataset-latest-log (current-dataset)))

(define (cb-download-all-files obj event)
  (let* ([ds (current-dataset)]
         [argv (argv-download-everything ds)])
    (when argv
      ; FIXME bad use of thread
      (thread
       (thunk
        (println (format "dataset download starting for ~a" (dataset-id ds)))
        (apply py-system* argv) ; TODO detect and report failure
        (println (format "dataset download completed for ~a" (dataset-id ds))))))))

(define (cb-open-dataset-remote obj event)
  (xopen-path (uri-human (current-dataset))))

(define (cb-open-dataset-sds-viewer obj event)
  (xopen-path (uri-sds-viewer (current-dataset))))

#; ; now cb-toggle-upload
(define (cb-upload-button o e #:show [show #t]) ; TODO switch show to #t when ready
  (when #t ; not ready
    (send frame-upload update))
  (when show
    (send frame-upload show #t)))

(define (cb-paths-report o e #:show [show #t])
  (cb-x-report o e 'paths #:show show))

(define (cb-manifest-report o e #:show [show #t])
  (cb-x-report o e 'manifest #:show show))

(define (cb-x-report obj event type #:show [show #t])
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
  (let-values ([(report-function
                 report-frame
                 report-edcanv)
                (case type
                  [(paths) (values paths-report frame-paths-report edcanv-path-rep)]
                  [(manifest) (values manifest-report frame-manifest-report edcanv-man-rep)]
                  [else (error 'cb-x-report "unknown report type ~s" type)])])
    (let ([ed (send report-edcanv get-editor)])
      ; select-all clear does not work if the cursor is moved manually, locking
      ; the canvas prevents the issue and avoids other issues at the same time
      (when (send ed is-locked?)
        (send ed lock #f))
      (send ed select-all)
      (send ed clear)
      (send ed insert (with-output-to-string (λ () (report-function))))
      (send ed scroll-to-position 0)
      (send ed lock #t))
    (when show
      (send report-frame show #t))))

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
          (when path ; only open if the cleaning succeeded
            ; the failure above will be logged
            (xopen-path path))))))))

(define (cb-open-dataset-folder obj event)
  (let* ([ds (current-dataset)]
         [path (dataset-src-path ds)]
         [symlink (build-path path "dataset")])
    #;
    (println (list "should be opening something here ...." ds resolved))

    (if (directory-exists? symlink)
        (let ([path (resolve-relative-path symlink)])
          (xopen-path path))
        ; TODO gui visible logging
        (println msg-dataset-not-fetched))))

(define (match-datasets text datasets)
  "given text return datasets whose title or identifier matches"
  (if text
      (if (string-contains? text "dataset:")
          (let* ([hrm (string-split text "dataset:")]
                 [m (if (null? hrm) "" (last hrm))]
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
          (let* ([downcased-text (string-downcase text)]
                 [matches (for/list ([d datasets]
                                     #:when (or (string-contains? (id-uuid d) text)
                                                (string-contains? (string-downcase (dataset-title d))
                                                                  downcased-text)
                                                (string-contains? (string-downcase (dataset-pi-name-lf d))
                                                                  downcased-text)))
                            d)])
            (if (null? matches)
                datasets
                matches)))
      datasets))

(define search-semaphore (make-semaphore 1))
(define (cb-search-dataset obj event)
  "callback for text field that should highlight filter sort matches in
the list to the top and if there is only a single match select and
switch to that"
  (define text (string-trim (send obj get-value)))
  (define list-box (send obj list-box))
  (thread
   (thunk
    ; adding this helps reduce the number of calls to set-datasets-view!
    ; and the semaphore allows the sleep time to be short enough that we
    ; don't have to worry about weird ordering issues
    (sleep 0.08)
    (call-with-semaphore
     search-semaphore
     (thunk
      (let ([wat (string-trim (send obj get-value))])
        ; don't run this if the text has changed in the other thread
        ; without this you get non deterministic behavior when typing
        ; and sometimes the shorter result will supplant the long
        (if (= 0 (string-length text))
            (when (not (equal? (current-datasets) (current-datasets-view)))
              (set-datasets-view! list-box (current-datasets)))
            (let ([matching (match-datasets text (current-datasets))])
              (when (string=? text wat)
                (unless (or (null? matching)
                            (eq? matching (or (current-datasets-view) (current-datasets))))
                  (set-datasets-view! list-box matching)
                  (when (= (length matching) 1)
                    (send list-box set-selection 0)
                    (cb-dataset-selection list-box #f))))))))))))

(define (cb-power-user o e)
  ; this can be triggered by keypress as well so cannot use o
  (power-user? (not (power-user?)))
  ; XXX these can get out of sync
  (send check-box-power-user set-value (power-user?))
  (send panel-power-user reparent
        (if (power-user?)
            panel-ds-actions
            frame-helper))
  (when e
    (save-config!)))

(define (cb-reload-config o e)
  (load-config!))

(define viewer-mode-state #f)
(define (cb-viewer-mode o e)
  ; XXX this callback is triggered twice on click so we have to use viewer-mode-state
  ; as a stable reference to know whether something actually changed, events will also
  ; trigger when the same button is clicked multiple times, so must ignore those as well
  (let* ([event-state (send o get-selection)]
         [change-event (or (not viewer-mode-state)
                           (not (= viewer-mode-state event-state)))])
    (when change-event
      (set! viewer-mode-state event-state)
      (let* ([mode (send o get-item-plain-label viewer-mode-state)]
             [panel-to-show (case mode
                              [("Validate") panel-validate-mode]
                              [("Convert") panel-convert-mode]
                              [else panel-validate-mode])])
        ; we rereparent power-user so that it is always on the right
        (when (power-user?) (send panel-power-user reparent frame-helper))
        (set-current-mode-panel! panel-to-show)
        (when (power-user?) (send panel-power-user reparent panel-ds-actions))
        (when e
          (save-config!))))))

;; keymap keybind

(define keymap (new (keymap:aug-keymap-mixin keymap%)))

(define (k-test receiver event)
  (pretty-print (list "test" receiver event)))

(define (k-quit receiver event)
  (when (send frame-main can-close?)
    (send frame-main on-close)
    (send frame-main show #f)))

(define (k-fetch-export-dataset receiver event)
  (fetch-export-current-dataset))

(define (k-fetch-dataset receiver event)
  (fetch-current-dataset))

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

(define (k-next-thing r e)
  "do nothing")

; add functions
(send* keymap
  (add-function "test" k-test)
  (add-function "fetch-export-dataset" k-fetch-export-dataset)
  (add-function "fetch-dataset" k-fetch-dataset)
  (add-function "export-dataset" k-export-dataset)
  (add-function "quit" k-quit)
  (add-function "test-backspace" (λ (a b) (displayln (format "delete all the things! ~a ~a" a b))))
  (add-function "copy-value" (λ (obj event)
                               ; TODO proper chaining
                               (when (is-a? obj json-hierlist%)
                                 (let* ([raw-value (node-data-value (send (send obj get-selected) user-data))]
                                        [value (*->string raw-value)])
                                   (send the-clipboard set-clipboard-string value (current-milliseconds))))))
  (add-function "backward-kill-word" backward-kill-word)
  (add-function "next-thing" k-next-thing)
  (add-function "focus-search-box" (λ (a b) (send text-search-box focus)))
  (add-function "open-dataset-folder" cb-open-dataset-folder)
  (add-function "open-export-folder" cb-open-export-folder)
  (add-function "toggle-power-user" cb-power-user)
  (add-function "open-export-json" cb-open-export-json)
  (add-function "open-export-ipython" cb-open-export-ipython)
  (add-function "open-dataset-shell" cb-open-dataset-shell)
  (add-function "open-dataset-latest-log" cb-open-dataset-lastest-log)
  (add-function "toggle-prefs" cb-toggle-prefs)
  (add-function "toggle-upload" cb-toggle-upload)
  )

(define (fox key-string)
  ; TODO if osx relace c: with cmd: or whatever
  key-string)

; map functions
(send* keymap
  (map-function "m:backspace" "backward-kill-word")
  (map-function "tab" "next-thing")
  (map-function "c:semicolon" "toggle-prefs")
  (map-function "c:u" "toggle-upload") ; u for Upload
  (map-function "c:y" "toggle-upload") ; y for sYnc ; FIXME pick one?
  #;
  (map-function "c:r"     "refresh-datasets")
  (map-function (fox "c:c")     "copy-value") ; FIXME osx cmd:c
  (map-function "c:t"     "test")
  (map-function "f5"      "fetch-export-dataset")
  (map-function "c:t"     "fetch-dataset")
  (map-function "c:f"     "focus-search-box") ; XXX bad overlap with find
  #; ; overlaps with open logs
  (map-function "c:l"     "focus-search-box")
  (map-function "c:k"     "focus-search-box") ; this makes more sense given browser binds
  #;
  (map-function "c:c;c:e" "fetch-export-dataset") ; XXX cua intersection
  (map-function "c:x"     "export-dataset")
  #; ; defined as the key for the menu item so avoid double calls
  (map-function "c:q"     "quit")
  (map-function "c:w"     "quit")
  (map-function "c:o" "open-dataset-folder")
  #;
  (map-function "c:p" "open-export-folder") ; FIXME these are bad bindings
  (map-function "c:p" "toggle-power-user")
  (map-function "c:j" "open-export-json")
  (map-function "c:i" "open-export-ipython")
  (map-function "c:b" "open-dataset-shell")
  (map-function "c:l" "open-dataset-latest-log")
  )

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
                                   [label "Preferences...        C-;"]
                                   ; XXX we cannot show the shortcut here because there is
                                   ; no sane way to have a menu based shortcut and a keymap
                                   ; shortcut share the same binding because it is impossible
                                   ; to tell whether a menu specified shortcut was actually
                                   ; triggered by a keypress event or by a mouse click on the
                                   ; menu ... SIGH
                                   #;
                                   [shortcut #\;] ; shortcut here for discoverability only
                                   #;
                                   [shortcut-prefix '(ctl)]
                                   [callback cb-toggle-prefs]
                                   [help-string "Shortcut is C-;"]
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
  (new (class text-field% (super-new)
         (define *list-box* #f)
         (define/public list-box
           (case-lambda
             [() *list-box*]
             [(value) (set! *list-box* value)])))
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
                   [columns '("Owner" "Folder Name" "Identifier" "Updated")]
                   ; really is single selection though we want to be
                   ; able to highlight and reorder multiple
                   [style '(single column-headers clickable-headers)]
                   [callback cb-dataset-selection]
                   [parent panel-left]))

(send text-search-box list-box lview)
; FIXME TODO scale these based on window size
; and remove the upper limit when if/when someone is dragging
(send* lview
  (set-column-width 0 70 70 300)
  (set-column-width 1 120 60 300)
  (set-column-width 2 120 60 300)
  (set-column-width 3 140 60 9999)
  )

(define panel-ds-actions (new horizontal-panel%
                              [stretchable-height #f]
                              [parent panel-right]))

(define panel-validate-mode (new horizontal-panel%
                              [stretchable-height #f]
                              [parent frame-helper]))

(define panel-convert-mode (new horizontal-panel%
                              [stretchable-height #f]
                              [parent frame-helper]))

;; validate mode panel

(define button-fexport (new (tooltip-mixin button%)
                            [label "Fetch+Export"]
                            [tooltip "Shortcut F5"] ; FIXME this should populate dynamically
                            [tooltip-delay 100]
                            [callback cb-fetch-export-dataset]
                            [parent panel-validate-mode]))

(define all-button-open-dataset-folder '()) ; we do it this way since these should enable/disable as a group
(define (make-button-open-dataset-folder parent)
  (define butt
    (new button%
         [label "Open Folder"]
         [callback cb-open-dataset-folder]
         [parent parent]))
  (set! all-button-open-dataset-folder (cons butt all-button-open-dataset-folder)))

(make-button-open-dataset-folder panel-validate-mode)

; FIXME there is currently no way to go back to viewing the local
; export without running export or restarting
(define button-load-remote-json (new button%
                                     [label "View Prod Export"]
                                     [callback cb-load-remote-json]
                                     [parent panel-validate-mode]))

(define button-open-dataset-sds-viewer (new button%
                                            [label "Viewer"]
                                            [callback cb-open-dataset-sds-viewer]
                                            [parent panel-validate-mode]))

(define button-manifest-report
  (new button%
       [label "Manifest Rep"] ; used sometimes
       [callback cb-manifest-report]
       [parent panel-validate-mode]))

(define button-paths-report
  (new button%
       [label "Paths Report"] ; used sometimes
       [callback cb-paths-report]
       [parent panel-validate-mode]))

(define button-clean-metadata-files (new button%
                                         [label "Clean Metadata"] ; 5 star
                                         [callback cb-clean-metadata-files]
                                         [parent panel-validate-mode]))

(define button-upload-changes
  (new (tooltip-mixin button%)
       [label "Upload"]
       [callback cb-upload-button-show-and-raise]
       [tooltip "Shortcut C-u"]
       ; TODO separate button for the convert use case?
       [parent panel-validate-mode]))

#; ; too esoteric
(define button-open-export-folder (new button%
                                       [label "Open Export"]
                                       [callback cb-open-export-folder]
                                       [parent panel-validate-mode]))

#; ; never used ; FXIME probably goes in the bottom row of a vertical panel
(define button-toggle-expand (new button%
                                  [label "Expand All"]
                                  [callback cb-toggle-expand]
                                  [parent panel-validate-mode]))
#;
(send button-toggle-expand enable #f) ; XXX remove when implementation complete
#;
(send button-toggle-expand show #f)

;; convert mode panel

(define all-button-fetch '())
(define (make-button-fetch parent)
  (define butt
    (new (tooltip-mixin button%)
         [label "Fetch"] ; curation does not use
         [tooltip "Shortcut <not-set>"]
         [tooltip-delay 100]
         [callback cb-fetch-dataset]
         [parent parent]))
  (set! all-button-fetch (cons butt all-button-fetch)))

(make-button-fetch panel-convert-mode)

(define all-button-download-all-files '())
(define (make-button-download-all-files parent)
  (define butt
    (new (tooltip-mixin button%)
         [label "Download"]
         [tooltip "Download all files"]
         [tooltip-delay 100]
         [callback cb-download-all-files]
         [parent parent]))
  ; TODO this would have to be a macro because we need the exact name for the list
  (set! all-button-download-all-files (cons butt all-button-download-all-files)))

(make-button-download-all-files panel-convert-mode)

(make-button-open-dataset-folder panel-convert-mode)

;; power user panel

(define panel-power-user (new horizontal-panel%
                              [stretchable-height #f]
                              [parent frame-helper]))

(make-button-fetch panel-power-user)

(define button-export-dataset (new (tooltip-mixin button%)
                                   [label "Export"] ; curation does not use
                                   [tooltip "Shortcut C-x"]
                                   [tooltip-delay 100]
                                   [callback cb-export-dataset]
                                   [parent panel-power-user]))

(make-button-download-all-files panel-power-user)

(define button-open-dataset-remote (new button%
                                        ; curation doesn't use this, flow is inverted
                                        [label "Remote"]
                                        [callback cb-open-dataset-remote]
                                        [parent panel-power-user]))

(define button-open-export-json (new (tooltip-mixin button%)
                                     [label "JSON"]
                                     [tooltip "Shortcut C-j"]
                                     [tooltip-delay 100]
                                     [callback cb-open-export-json]
                                     [parent panel-power-user]))

(define button-open-export-ipython (new (tooltip-mixin button%)
                                         [label "IPython"]
                                         [tooltip "Shortcut C-i"]
                                         [tooltip-delay 100]
                                         [callback cb-open-export-ipython]
                                         [parent panel-power-user]))

(define button-open-dataset-shell (new (tooltip-mixin button%)
                                       [label "Shell"]
                                       [tooltip "Shortcut C-b"]
                                       [tooltip-delay 100]
                                       [callback cb-open-dataset-shell]
                                       [parent panel-power-user]))

(define button-open-dataset-latest-log (new (tooltip-mixin button%)
                                            [label "Logs"]
                                            [tooltip "Shortcut C-l"]
                                            [tooltip-delay 100]
                                            [callback cb-open-dataset-lastest-log]
                                            [parent panel-power-user]
                                            ))

;; upload

(define frame-upload%
  ; upload frames are scoped per dataset which is a much better
  ; and clearer affordance for what is going on
  (class frame%
    (init dataset)
    ;(define dataset dataset)
    (define update-semaphore (make-semaphore 1)) ; must come before super new I think?
    (super-new)
    (rename-super [super-on-subwindow-char on-subwindow-char])
    (define/override (on-subwindow-char receiver event)
      (super-on-subwindow-char receiver event)
      (send keymap handle-key-event receiver event))
    (define previous-selection
      ; FIXME issues with reordering as a result of soring columns
      '())
    (define list-box
      (new
       list-box%
       [label ""]
       [font (make-object font% 10 'modern)]
       [choices
        (if #f ;'test
            '("path/to/test/1"
              "path/to/test/2"
              "path/to/test/3"
              "path/to/test/4"
              "path/to/test/5")
            '())]
       [columns '(" " "path" "previous id" "actions")]
       [style '(extended column-headers clickable-headers)]
       [callback (λ (o e)
                   (let ([s (send list-box get-selections)])
                     (unless (set=? previous-selection s)
                       (cb-confirm #f #f #:force-off? #t))
                     (set! previous-selection s))
                   (send check-box-confirm enable
                         (and (not locked-in?) ; duh
                              (not (null? (send list-box get-selections)))))
                   (println "TODO frame-upload list-box callback"))]
       [parent this]
       ))
    (send* list-box
      (set-column-width 0 20 20 40)
      (set-column-width 1 300 120 1200)
      (set-column-width 2 120 60 1200)
      (set-column-width 3 220 60 1200))

    (define hp
      (new horizontal-panel%
           [stretchable-height #f]
           [alignment '(right center)]
           [parent this]))

    #;
    (define dataset
      (case-lambda
        [() *dataset*]
        [(value) (error "cannot set dataset after construction")
                 #;(set! *dataset* value)]))

    (define *updated-transitive-moment* #f)
    (define updated-transitive-moment ; FIXME TODO
      (case-lambda
        [() *updated-transitive-moment*]
        [(value) (set! *updated-transitive-moment* value)]))
    (define (updated-transitive)
      "file system friendly"
      (~t (updated-transitive-moment) time-format-friendly))

    (define *push-id* #f)
    (define push-id
      (case-lambda
        [() *push-id*]
        [(value) (set! *push-id* value)]))
    (define (new-push-id)
      #; ; uuids have a nasty property that they don't sort well
      (push-id (uuid-string))
      (push-id (datetime-for-path-now)))

    (define *confirmed* #f)
    (define confirmed?
      (case-lambda
        [() *confirmed*]
        [(value) (set! *confirmed* value)]))

    (define (push-dir)
      ; XXX this must always match the python conventions
      (build-path (path-cache-push) (id-uuid dataset) (updated-transitive) (push-id)))
    (define (write-push-paths)
      (ensure-directory! (push-dir))
      (define paths (build-path (push-dir) "paths.sxpr"))
      (with-output-to-file paths
        #:exists 'error ; any rewrite should change
        (λ () (pretty-write
               ; TODO data from selected rows
               (for/list ([h (selected)])
                 ; FIXME ops is wrong
                 (list ':path (hash-ref h 'path) ':type (hash-ref h 'type) ':ops (hash-ref h 'ops)))))))
    (define (paths-to-manifest)
      (let ([argv (argv-simple-make-push-manifest dataset (updated-transitive) (push-id))]
            [status-1 #f])
        (parameterize () ; FIXME threading and probably semaphore?
          (set! status-1 (apply py-system* argv)))
        (unless status-1
          (error "make-push-manifest failed with ~a for ~a" status-1 (string-join argv " ")))))
    (define (unfinished-push?)
      (define path-dut-latest (build-path (path-cache-push) (id-uuid dataset) (updated-transitive) "LATEST"))
      (define cands '("done.sxpr" "completed.sxpr" "manifest.sxpr" "paths.sxpr"))
      (define first-present (for/or [(cand cands) #:when (file-exists? (build-path path-dut-latest cand))] cand))
      (and first-present
           (not (string=? first-present "done.sxpr"))
           ; FIXME pretty sure I need to inverte the order of build-path and resolve-relative-path
           (resolve-relative-path (build-path path-dut-latest first-present))))

    (define button-select-all "TODO") ; much easier than having to click scroll shift click etc. for thouands of files
    (define (cb-refresh o e)
      ; TODO make sure we restore the selection! otherwise lost work v annoying
      (update))
    (define button-diff
      (new button%
           [label "Refresh"]
           [callback cb-refresh]
           [parent hp])
      )
    (define locked-in? #f)
    (define (cb-confirm o e #:force-off? [force-off? #f])
      (confirmed? (if force-off? #f (not (confirmed?))))
      ; TODO save the list of paths to push to a file at this point
      ; in case something goes wrong, and because we need it to kick
      ; off the push, NOTE the actual change log (needed for undo functionality)
      ; and successful change accounting for resume will be all on the python side
      (when (confirmed?)
        (send check-box-confirm enable #f) ; lock in changes and avoid reconfirm unless selection changes
        (set! locked-in? #t)
        (new-push-id) ; TODO see if this is actually the right place to set this and whether we need to unset it at some point as well?
        (write-push-paths)
        (paths-to-manifest))
      (when force-off?
        ; reset lockin in the force-off case
        (set! locked-in? #f))
      (send check-box-confirm set-value (confirmed?))
      (send button-push enable (confirmed?)))
    (define check-box-confirm
      (new check-box%
           [label "Confirm selection?"]
           [callback cb-confirm]
           [parent hp]))
    (define (push-from-manifest)
      (unless (confirmed?)
        (error "not confirmed? how did you manage this?"))
      #f)
    (define (cb-push-to-remote o e)
      ; TODO probably disable clicking the button again until the process finishes or fails?
      ; 1. confirm pass the manifest of changes
      (println "Upload is not implemented yet.")
      (push-from-manifest))
    (define button-push
      (new button%
           [label "Push selected changes to remote"]
           [callback cb-push-to-remote]
           [enabled #f]
           [parent hp]))
    (define (get-diff)
      ; FIXME FIXME figure out how we are passing updated-transitive
      (let* ([argv (argv-simple-diff dataset)]
             [status-1 #f]
             [result-string (parameterize ()
                              (println "aaaieeeeeeeeeeeeeeeeeeeee")
                              (println (string-join argv " "))
                              (with-output-to-string
                                (thunk
                                 (set! status-1 (apply py-system* argv)))))])
        (unless status-1
          (error "diff failed with ~a for ~a" status-1 (string-join argv " ")))
        (let ([sport (open-input-string result-string)]
              [diff #f])
          (set! diff (read sport))
          diff
          )))
    (define (result->updated-transitive-moment+diff-list result)
      (pretty-write (list 'oops: result))
      (define h (plist->hash result))
      #; ; nothing to do with this one right now I think?
      (hash-ref h 'dataset-id)
      (values (iso8601->moment (hash-ref h 'updated-transitive))
              (map better-diff-hash (hash-ref h 'diff))))
    (define (better-diff-hash record)
      (define raw-ops (hash-ref record 'ops))
      (define-values (old-id ops)
        (if (member "change" raw-ops)
            (let* ((rrops (reverse raw-ops))
                   (old-meta (car rrops))
                   (old-id (let ([hr (hash-ref old-meta 'old-id)])
                             (let-values ([(N type uuid)
                                           (apply values (string-split (car hr) ":"))])
                               (string-append
                                (substring uuid 0 4)
                                " ... "
                                (let ([slu (string-length uuid)])
                                  (substring uuid (- slu 4) slu))))))
                   (ops (reverse (cdr rrops))))
              (values old-id ops))
            (values #f raw-ops)))
      (set! record (hash-set record 'ops ops))
      (set! record (hash-set record 'old-id old-id))
      record)
    (define (diff-list-to-columns record)
      (println (list 'diff-list-to-columns-wat? record))
      (define type
        (case (hash-ref record 'type)
          [("dir")  "d"]
          [("file") "f"]
          [("link") "l"]))
      #;
      (define raw-ops (hash-ref record 'ops))
      #;
      (define-values (old-id ops)
        (if (member "change" raw-ops)
            (let* ((rrops (reverse raw-ops))
                   (old-meta (car rrops))
                   (old-id (let ([hr (hash-ref old-meta 'old-id)])
                             (let-values ([(N type uuid)
                                           (apply values (string-split (car hr) ":"))])
                               (string-append
                                (substring uuid 0 4)
                                " ... "
                                (let ([slu (string-length uuid)])
                                  (substring uuid (- slu 4) slu))))))
                   (ops (reverse (cdr rrops))))
              (values old-id ops))
            (values "" raw-ops)))
      #; ; TODO determine whether do display this maybe for use in sorting?
      (hash-ref record 'updated)
      (list
       type
       (hash-ref record 'path)
       (or (hash-ref record 'old-id) "")
       #;
       old-id
       #;
       (string-join ops " ")
       (string-join (hash-ref record 'ops) " ")
       ))
    (define (set-rows diff-list)
      (unless (null? diff-list)
        (send/apply list-box set (apply map list (map diff-list-to-columns diff-list)))
        (for ([diff (in-list diff-list)]
              [n (in-naturals)])
          (send list-box set-data n diff))))
    (define updated-once #f)
    (define/public (updated-once?) updated-once)
    (define (selected)
      ; TODO will need a separate hash table or something to do rapid lookup via uuid
      ; to restore the state
      (for/list ([i (in-list (send list-box get-selections))])
        (send list-box get-data i)))
    (define (do-update)
      (define diff (get-diff))
      (println (list 'diff-hash? diff))
      (define-values (updated-trans-moment diff-list) (result->updated-transitive-moment+diff-list diff)) ; FIXME need updated-transitive from here
      (updated-transitive-moment updated-trans-moment)
      ; FIXME yeah ... here comes the synchronization issues
      (let ([ufp? (unfinished-push?)] ; FIXME not sure if this is actually the right place to handle this?
            )
        (if ufp?
            (begin
              (println "TODO TODO TODO handle unfinished push case")
              )
            (begin
              (set-rows diff-list)
              )))
        )
    (define/public (update)
      ; FIXME all sorts of issues with transitive updated if someone changes the most recently updated file !!!
      ; XXX I think this has to be handled in dataset xattrs during pull and we would still have to traverse
      ; the tree on the off chance that someone did something stupid such as pulling a subtree (yeah, these
      ; synchornization and consistency issues were always going to come back to haunt us :/)
      ; FIXME further issues given that this will be called in a thread
      ; FIXME lots of background activity here
      (thread
       (thunk
        (call-with-semaphore
         update-semaphore
         (thunk
          (dynamic-wind
            (thunk
             (send button-diff enable #f)
             (send check-box-confirm enable #f))
            (thunk
             (do-update)
             (unless updated-once
               (set! updated-once #t)))
            (thunk
             (send button-diff enable #t)
             (send check-box-confirm enable (and (not locked-in?) (not (null? (send list-box get-selections))))))))))))))

#; ; debug helper
(define fu (hash-ref frame-uploads (id-uuid (current-dataset))))
(define frame-uploads (make-hash))
(define (get-frame-upload! dataset)
  ; FIXME TODO the design of this does not correctly decouple the ui from the workflow state per dataset
  ; so if you change datasets for whatever reason you will lose the state, which seems wrong
  ; the correct factoring moves upload workflow state to its own class probably?
  ; for now just warn that this is a known issue or better maybe just check the state of the latest push?
  ; XXX a bit better now
  (let* ([uuid (id-uuid dataset)]
         [hr-frame-upload (hash-ref frame-uploads uuid #f)]
         [frame-upload
          (if hr-frame-upload
              hr-frame-upload
              (let ([fu
                     (new frame-upload%
                          [dataset dataset]
                          [width 640]
                          [height 480]
                          [label (format "upload for ~a" (id-uuid dataset))])])
                (hash-set! frame-uploads (id-uuid dataset) fu)
                fu))])
    ; TODO remove from hash on close probably? the workflow is a bit different
    (values (not hr-frame-upload) frame-upload)))

;; reports

(define (make-frame-report label)
  (new
   (class frame% (super-new)
     (rename-super [super-on-subwindow-char on-subwindow-char])
     (define/override (on-subwindow-char receiver event)
       (super-on-subwindow-char receiver event)
       (send keymap handle-key-event receiver event))
     #; ; show hide basically
     (define/augment (on-close)
       (displayln "prefs closed")))
   [label label]
   [width 640]
   [height 480]))

(define frame-manifest-report (make-frame-report "sparcur report manifests"))

(define frame-paths-report (make-frame-report "sparcur report paths"))

(define edcanv-man-rep
  (new editor-canvas%
       [editor (new text%)]
       [parent frame-manifest-report]))

(define edcanv-path-rep
  (new editor-canvas%
       [editor (new text%)]
       [parent frame-paths-report]))

;; preferences
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
; power user
; fetch mode
; curation mode
; persistance
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

(define text-prefs-remote-organization
  (new text-field%
       [font (make-object font% 10 'modern)]
       [label "Remote Org "]
       [enabled #f]
       [init-value ""]
       [parent panel-prefs-holder])
  )

(define text-prefs-api-key
  (new text-field%
       [font (make-object font% 10 'modern)]
       [label "API Key    "]
       [enabled #f]
       [init-value "no api key found"]
       [parent panel-prefs-holder]))

(define text-prefs-api-sec
  (new text-field%
       [font (make-object font% 10 'modern)]
       [label "API Secret "]
       [enabled #f]
       [init-value "no api secret found"]
       [parent panel-prefs-holder]))

(define (make-text-prefs-path label [init-value ""] #:enabled? [enabled #f])
  (define panel-prefs-path (new horizontal-panel%
                                [parent panel-prefs-holder]))
  (define text-field
    (new text-field%
         [font (make-object font% 10 'modern)]
         [label label]
         [enabled enabled]
         [init-value init-value]
         ; TODO need a way to click button to open
         [parent panel-prefs-path]))
  (new button%
       [label "Open Path"]
       [callback (make-cb-open-path text-field)]
       [parent panel-prefs-path])
  text-field)

#;
(define text-prefs-path-? (make-text-prefs-path           "Path ???   " "/fake/path/to/thing/for/reference"))
(define text-prefs-path-config (make-text-prefs-path      "config     "))
(define text-prefs-path-user-config (make-text-prefs-path "user-config"))
(define text-prefs-path-secrets (make-text-prefs-path     "secrets    "))
(define text-prefs-path-data (make-text-prefs-path        "data-path  "))

(define radio-box-viewer-mode
  (new radio-box%
       [label "Viewer Workflow"]
       [choices '("Validate" "Convert")]
       [callback cb-viewer-mode]
       [parent panel-prefs-holder]))

(define (toggle-show obj)
  (send obj show (not (send obj is-shown?))))

(define panel-prefs-bottom (new horizontal-panel%
                                [parent panel-prefs-holder]
                                [alignment '(center center)]))
(define check-box-power-user (new check-box%
                                  [label "Power user?"]
                                  [callback cb-power-user]
                                  [parent panel-prefs-bottom]))

; yes toggling validate and convert will reload the config as well, but that is a side effect
; so we have and explicit button for it here as well
(define button-reload-config (new button%
                                  [label "Reload Config"]
                                  [callback cb-reload-config]
                                  [parent panel-prefs-bottom]))

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

(define (set-current-mode-panel! panel)
  (let ([cmp (current-mode-panel)])
    (when cmp
      (send cmp reparent frame-helper)))
  (send panel reparent panel-ds-actions)
  (current-mode-panel panel))

(module+ main
  (init-paths!)
  (load-config!)
  (define result (populate-datasets)) ; slow if not cached, but thus only on the very first run
  (send frame-main show #t) ; show this first so that users know something is happening
  (send lview select 0) ; first time to ensure current-dataset always has a value
  (set-lview-column-state! lview *current-lview-column* #:view-only? #t)
  (send text-search-box focus)
  ; do this last so that if there is a 0th dataset the time to render the hierlist isn't obtrusive
  (cb-dataset-selection lview #f)
  (void (thread (thunk (refresh-dataset-metadata text-search-box)))) ; refresh datasets in a separate thread to avoid delaying startup
  (unless (eq? (system-type) 'unix)
    ; do NOT run this when gtk is the windowing toolkit it will eat up
    ; tens of gigs of memory, windows and macos don't have the issue
    ; it also takes multiple minutes to run due to all the allocations?
    (render-datasets)))
