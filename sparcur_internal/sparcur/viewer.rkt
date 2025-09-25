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
         net/base64
         framework
         compiler/compilation-path
         compiler/embed
         (rename-in (only-in gui-widget-mixins tooltip-mixin) [tooltip-mixin tooltip-mixin-real])
         json
         json-view
         net/url
         (except-in gregor date? date)
         (prefix-in oa- orthauth/base)
         orthauth/paths
         orthauth/python-interop
         orthauth/utils
         )

(define-runtime-path icon-16 "resources/sparcur-icon-16.png")
(define-runtime-path icon-32 "resources/sparcur-icon-32.png")
(define-runtime-path icon-48 "resources/sparcur-icon-48.png")
(define-runtime-path icon-ico "resources/sparcur-icon-48.ico")
(define-runtime-path icon-icns "resources/sparcur-icon-48.icns")
(define-runtime-path this-file-runtime-path "viewer.rkt")
(define this-file (path->string this-file-runtime-path))
(define this-file-compiled (with-handlers ([exn? (λ (e) this-file)]) (get-compilation-bytecode-file this-file)))
(define this-file-exe (embedding-executable-add-suffix (path-replace-extension this-file "") #f))
(define this-file-exe-tmp (path-add-extension this-file-exe "tmp"))
(define this-package-path (let-values ([(parent name dir?) (split-path this-file)]) parent))

(when (and this-file-exe-tmp (file-exists? this-file-exe-tmp))
  ; windows can't remove a running exe ... but can rename it ... and then delete the old
  ; file on next start
  (delete-file this-file-exe-tmp))

(define (git-commit-hash)
  (let ((git-exe (find-executable-path "git")))
    (if git-exe
        (string-trim
         (with-output-to-string
           (thunk
            (parameterize ([current-directory this-package-path])
              (killable-system* git-exe "rev-parse" "HEAD" #:set-pwd? #t)))))
        #f)))

(define (set-commit-hash!)
  (define commits
    (for/list ([flag (in-vector (current-command-line-arguments))]
               #:when (string-prefix? flag "--commit="))
      (substring flag 9)))
  (current-git-commit-hash
   (if (null? commits)
       (git-commit-hash)
       (car commits)))
  (when (current-git-commit-hash)
    (send text-prefs-viewer-commit set-value (current-git-commit-hash))
    (send (send text-prefs-viewer-commit get-editor) lock 'write)
    )
  (void))

(define (make-lexical-parameter [default #f])
  "sometimes you want the parameter interface but don't want the thread local behavior
note of course that you don't get dynamic binding with version since it is not thread local"
  (let ([v default])
    (case-lambda
      [() v]
      [(value) (set! v value)])))

(define running? #t) ; don't use parameter, this needs to be accessible across threads
(define update-running? #f)
(define python-first-time? #f)

(define debug-push (make-lexical-parameter #f))
(define current-git-commit-hash (make-lexical-parameter #f))

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

; true global variables that should not be thread local
(define current-projects (make-lexical-parameter)) ; FIXME check that default value ???
(define (current-blob) (hash-ref uuid-json-hash (id-uuid (current-dataset))))
(define (current-path-meta-blob) (hash-ref uuid-path-meta-json-hash (id-uuid (current-dataset))))
(define current-dataset (make-lexical-parameter))
(define current-datasets (make-lexical-parameter)) ; FIXME check that default value ???
(define current-datasets-view (make-lexical-parameter))
(define current-jview (make-lexical-parameter))
(define current-mode-panel ; TODO read from config/history
  (make-lexical-parameter))
(define remote-org-keys (make-lexical-parameter '()))
(define missing-remote-org-keys (make-lexical-parameter '()))

(define overmatch (make-parameter #f))
(define power-user? (make-parameter #f))

(define allow-update?
  ; "don't set this, it should only be used to keep things in sync with the config"
  (make-lexical-parameter))

(define (str-not-set) "<not-set>")
(define (str-path-does-not-exist) "<no-file-at-path>")

(define jview-sort-new #t)
(define key-rank-hash
  (for/hash ([k (in-list '(id meta status rmeta |#/path-metadata| prov))]
             [i (in-naturals)]) (values k i)))

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

(define/contract (macos-vt path-string)
  (-> string? any)
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
                   ; "--log-level=-1" ; for debug on cli
                   (path->string (dataset-working-dir-path ds))))
(define (argv-simple-make-push-manifest ds updated-transitive push-id)
  (python-mod-args "sparcur.simple.utils" "for-racket" "make-push-manifest"
                   (dataset-id ds) updated-transitive push-id (path->string (dataset-working-dir-path ds))))
(define (argv-simple-push ds updated-transitive push-id)
  (python-mod-args "sparcur.simple.utils" "for-racket" "push" (dataset-id ds) updated-transitive push-id
                   (path->string (dataset-working-dir-path ds))))
(define argv-simple-git-repos-update (python-mod-args "sparcur.simple.utils" "git-repos" "update"))
(define argv-spc-export (python-mod-args "sparcur.cli" "export"))
(define (argv-simple-retrieve ds)
  (python-mod-args "sparcur.simple.retrieve" "--sparse-limit" "-1"
                   "--dataset-id" (dataset-id ds)
                   "--project-id" (dataset-id-project ds)))
(define argv-spc-find-meta
  (python-mod-args
   "sparcur.cli"
   "find"
   "--name" "*.xml"
   "--name" "submission*"
   "--name" "curation*"
   "--name" "code_description*"
   "--name" "dataset_description*"
   "--name" "subjects*"
   "--name" "samples*"
   "--name" "sites*"
   "--name" "performances*"
   "--name" "manifest*"
   "--name" "resources*"
   "--name" "README*"
   "--name" ".dss*"
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
        (vt-at-path (path->string (dataset-working-dir-path ds)))
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
   (path->string (dataset-working-dir-path ds))))

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

(define uuid-dataset-hash (make-immutable-hash))

(define (rehash-current-datasets!)
  ; FIXME this should almost certainly be done automatically if current-datasets is called with an argument
  (set! uuid-dataset-hash
    (make-immutable-hash
     (for/list ([ds (current-datasets)])
       (cons (id-uuid ds) ds)))))

(define (ident->dataset ident)
  (let ((uuid (car (reverse (string-split ident ":")))))
    (hash-ref uuid-dataset-hash uuid)))

(define (uuid->bytes uuid-string)
  (let* ([s (string-replace uuid-string "-" "")]
         [ls (string-length s)])
    (list->bytes ; surely there is a better way but I'm not going to hunt for it right now
     (for/list ([sigh (in-range 0 ls 2)])
       (string->number
        (substring s sigh (+ sigh 2))
        16)))))

(define (base64-urlsafe-encode uuid-bytes)
  (string-replace
   (string-replace
    (substring (bytes->string/utf-8 (base64-encode uuid-bytes #"")) 0 22)
    "+" "-")
   "/" "_"))

(define (base64-urlsafe-decode b64-uuid)
  ; FIXME hilariously inefficient
  (string-join
   (for/list
  ([i (bytes->list
       (base64-decode
        (string->bytes/utf-8
         (string-replace
          (string-replace
           b64-uuid
           "-" "+")
          "_" "/"))))]
   [j (in-naturals)])
     (let ([by (if (< i 16)
                   (string-append "0" (number->string i 16))
                   (number->string i 16))])
       (if (member j '(4 6 8 10))
           (string-append "-" by)
           by
           )
       ))
   ""))

(define (uuid->b64-uuid uuid-string)
  (base64-urlsafe-encode (uuid->bytes uuid-string)))

(define (b64-uuid->uuid b64-uuid-string)
  (base64-urlsafe-decode b64-uuid-string))

(define --sigh (gensym))
(define hash-subprocess-control (make-hash))

(define exit-control-signal ; windows has pipe issues unless kill is sent
  (if (eq? (system-type 'os*) 'windows) 'kill 'interrupt))

(define existing-exit-handler (exit-handler))
(define (our-exit-handler v)
  (define bad (gensym))
  (for ([(pid control) (in-hash hash-subprocess-control bad)]
        #:unless (eq? pid bad))
    (when (eq? (control 'status) 'running)
      (control exit-control-signal)
      (when #f ; debug
        (displayln (list "killed" pid)))))
  (existing-exit-handler v))

(exit-handler our-exit-handler)

(define (killable-system* exe #:set-pwd? [set-pwd? --sigh] . args)
  (define-values (cout cin cerr) (values (current-output-port) (current-input-port) (current-error-port)))
  (define-values (out in pid err control)
    (if (eq? set-pwd? --sigh)
        (apply values (apply process*/ports cout cin cerr exe args))
        (apply values (apply process*/ports cout cin cerr exe args #:set-pwd? set-pwd?))))
  (hash-set! hash-subprocess-control pid control)
  (control 'wait)
  (hash-remove! hash-subprocess-control pid)
  (when err (close-input-port err))
  (when out (close-input-port out))
  (when in (close-output-port in))
  (eq? (control 'status) 'done-ok))

(define (py-system* exe #:set-pwd? [set-pwd? --sigh] . args)
  (call-with-environment
   (λ ()
     (if (eq? set-pwd? --sigh)
         (apply killable-system* exe args)
         (apply killable-system* exe args #:set-pwd? set-pwd?)))
   '(("PYTHONBREAKPOINT" . "0")
     ; silence error logs during pennsieve top level import issue
     ("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION" . "python"))))

(define (tooltip-mixin c)
  (if (eq? (system-type 'os*) 'macosx)
      (class c ; avoid issues on macos
        (init-field [tooltip #f]
                    [tooltip-delay 500])
        (super-new))
      (tooltip-mixin-real c)))

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
         [result-projects (if (file-exists? pcd)
                     (with-input-from-file pcd
                       (λ () (list (read) (read))))
                     (begin
                       (let ([pc-dir (path-cache-dir)])
                         (unless (directory-exists? pc-dir)
                           ; `make-directory*' will make parents but
                           ; only if pc-dir is not a relative path
                           (make-directory* pc-dir)))
                       (let* ([result-projects (get-dataset-project-list)]
                              [result (car result-projects)]
                              [projects (cadr result-projects)])
                         (with-output-to-file pcd
                           #:exists 'replace ; yes if you run multiple of these you could have a data race
                           (λ ()
                             (pretty-write result)
                             (pretty-write projects)))
                         ; just to confuse everyone
                         result-projects))
                     )]
         [result (car result-projects)]
         [projects (cadr result-projects)]
         [datasets-remote (result->dataset-list result)]
         [datasets-local (get-local-datasets datasets-remote #:fast? #t)]
         [datasets (append datasets-remote datasets-local)])
    (current-projects projects)
    (current-datasets datasets)
    (rehash-current-datasets!)
    (set-datasets-view! lview (current-datasets)) ; FIXME TODO have to store elsewhere for search so we
    result))

(define (get-local-datasets datasets-remote #:fast? [fast? #f])
  ;; FIXME TODO this slows down startup time should probably be
  ;; deferred and/or cached
  (define ru (apply set (map id-uuid datasets-remote)))
  (define ped (path-export-datasets))
  (if (and ped (directory-exists? ped))
      (for/list ([uuid (map path->string (directory-list ped))]
                 #:when (not (set-member? ru uuid)))
        (define jp (build-path (path-export-datasets) uuid "LATEST" "curation-export.json"))
        (define lej
          (if (and (not fast?) (file-exists? jp))
              (path->json jp)
              #hash()))
        (let* ([meta (hash-ref lej 'meta #hash())]
               [folder-name (hash-ref meta 'folder_name "?")]
               [timestamp-updated (hash-ref meta 'timestamp_updated "1970-01-01T00:00:00Z")]
               [id-organization (hash-ref meta 'id_organization "?")])
          (dataset
           (string-append "N:dataset:" uuid)
           folder-name
           timestamp-updated
           "?" ; the only thing I don't record is the dataset owner name
           (string-append "N:" id-organization)
           "no-access")))
      '()))

(define (ensure-directory! path-dir)
  (unless (directory-exists? path-dir)
    (make-directory* path-dir)))

(define (python-module-user-config-path pymod)
  (parameterize* ([oa-current-auth-config-path (python-mod-auth-config-path pymod)]
                  [oa-current-auth-config (oa-read-auth-config)])
    (oa-user-config-path)))

(define (init-paths!)
  (parameterize* ([oa-current-auth-config-path (python-mod-auth-config-path "sparcur")]
                  [oa-current-auth-config (oa-read-auth-config)]
                  [oa-current-user-config
                   (with-handlers ([exn? (λ (e) (set! python-first-time? #t) #hash())])
                     (oa-read-user-config))]
                  [oa-current-secrets
                   (with-handlers ([exn? (λ (e) #hash())])
                     (oa-read-secrets))])
    (init-paths-int!)
    ))

(define (init-paths-int!)
  "initialize or reset the file system paths to cache, export, and source directories"
  ; FIXME 'cache-dir is NOT what we want for this as it is ~/.racket/
  ; FIXME more cryptic errors if sparcur.simple isn't tangled
  ; FIXME it should be possible for the user to configure path-source-dir
  (define ac (oa-current-auth-config))
  (path-config (build-path (expand-user-path (user-config-path "sparcur")) "viewer.rktd"))
  ; FIXME sppsspps stupidity
  (path-source-dir (expand-user-path ; redundant but avoids #f contract violation
                    (or (oa-get-path ac 'data-path #:exists? #f)
                        (build-path (find-system-path 'home-dir) "files" "sparc-datasets"))))
  (path-log-dir (build-path
                 (expand-user-path
                  (or (oa-get-path ac 'log-path #:exists? #f)
                      (user-log-path "sparcur")))
                 "datasets"))
  (path-cache-dir (build-path
                   (expand-user-path
                    (or (oa-get-path ac 'cache-path #:exists? #f)
                        (user-cache-path "sparcur")))
                   "racket"))
  (path-cache-push
   (build-path ; must match python or sparcur.simple.utils won't be able to find {push-id}/paths.sxpr
    (expand-user-path
     (or (oa-get-path ac 'cache-path #:exists? #f)
         (user-cache-path "sparcur")))
    "push"))
  (path-cache-datasets (build-path (path-cache-dir) "datasets-list.rktd"))
  (path-cleaned-dir (expand-user-path
                     (or (oa-get-path ac 'cleaned-path #:exists? #f)
                         (user-data-path "sparcur" "cleaned"))))
  (path-export-dir (expand-user-path
                    (or (oa-get-path ac 'export-path #:exists? #f)
                        (user-data-path "sparcur" "export"))))
  (path-export-datasets (build-path (path-export-dir) "datasets")))

(define (save-config!)
  (define pc (path-config))
  (ensure-directory! (simplify-path (build-path pc 'up) #f))
  (with-output-to-file pc
    #:exists 'replace
    (λ () (pretty-write
           (list
            (cons 'viewer-mode (or viewer-mode-state 0)) ; avoid contract violation if saved when viewer-mode is #f
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
  (parameterize* ([oa-current-auth-config-path (python-mod-auth-config-path "sparcur")]
                  [oa-current-auth-config (oa-read-auth-config)]
                  [oa-current-user-config
                   (with-handlers ([exn? (λ (e) (set! python-first-time? #t) #hash())])
                     (oa-read-user-config))]
                  [oa-current-secrets
                   (with-handlers ([exn? (λ (e) #hash())])
                     (oa-read-secrets))])
    (load-config-int!)))

(define (set-value-or-alt obj value alt)
  (send obj set-value (or value alt)))

(define (load-config-int!)
  ; TODO various configuration options
  (let ([org (oa-get (oa-current-auth-config) 'remote-organization)] ; TODO make sure in orgs
        [orgs (oa-get (oa-current-auth-config) 'remote-organizations)]
        [never-update (oa-get (oa-current-auth-config) 'never-update)]
        [not-set (str-not-set)]
        [path-does-not-exist (str-path-does-not-exist)])
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
      ; FIXME very confusing error message if there is no value for org in sparcur config (i.e. it is #f)
      (send choice-prefs-remote-organization clear)
      (let ([rok '()] [missing-keys '()])
        (for ([-o (or orgs (list org))])
          ; FIXME TODO append org name
          (define backend 'pennsieve) ; default for backward compat
          (define o
            (if (hash? -o)
                (begin
                  (set! backend (car (hash-keys -o)))
                  (car (hash-values -o)))
                -o))
          (send choice-prefs-remote-organization append (if o (*->string o) not-set))
          ; TODO if backend needs auth
          (set! rok
            (cons
             (cons
              ; FIXME confusing error message from a make-string contract if the key or secret are too short
              (with-handlers ([exn? (lambda (e) (set! missing-keys (cons o missing-keys)) not-set)])
                (obfus (*->string (oa-get-sath backend o 'key))))
              (with-handlers ([exn? (lambda (e) (set! missing-keys (cons o missing-keys)) not-set)])
                (obfus (*->string (oa-get-sath backend o 'secret)))))
             rok)))
        (send choice-prefs-remote-organization set-selection 0) ; reset selection to avoid stale ?
        (remote-org-keys (reverse rok))
        (missing-remote-org-keys missing-keys)
        (cb-select-remote-org choice-prefs-remote-organization #f))
      #;
      (send text-prefs-path-? set-value "egads")
      (send text-prefs-path-config set-value (path->string (path-config)))
      (set-value-or-alt text-prefs-path-user-config (oa-user-config-path) path-does-not-exist)
      (set-value-or-alt text-prefs-path-idlib-cfg (python-module-user-config-path "idlib") path-does-not-exist)
      (send text-prefs-path-ontqu-cfg set-value
            (python-module-user-config-path "ontquery.plugins.services"))
      (set-value-or-alt
       text-prefs-path-ontqu-cfg (python-module-user-config-path "ontquery.plugins.services")
       path-does-not-exist)
      (set-value-or-alt text-prefs-path-pyont-cfg (python-module-user-config-path "pyontutils") path-does-not-exist)
      (set-value-or-alt
       text-prefs-path-secrets
       (with-handlers ([exn? (lambda (e) not-set)]) (oa-secrets-path)) ; hash-ref failure will produce an error
       path-does-not-exist)
      (send text-prefs-path-data set-value (path->string (path-source-dir)))
      (send text-prefs-path-logs set-value (path->string (path-log-dir)))
      (let* ([config-exists (assoc 'viewer-mode cfg)]
             [power-user-a (assoc 'power-user? cfg)]
             [power-user (and power-user-a (cdr power-user-a))])
        (if config-exists
            (begin
              ; set-selection does not trigger the callback
              (send radio-box-viewer-mode set-selection
                    ; prevent contract violation of viewer-mode was somehow #f when config was written
                    (or (cdr config-exists) 0))
              (cb-viewer-mode radio-box-viewer-mode #f)
              (power-user? (not power-user))
              ; cb does the toggle interinally so we set the opposite of what we want first
              (cb-power-user check-box-power-user #f))
            (set-current-mode-panel! panel-validate-mode)))
      (send frame-preferences refresh))))

(define (init-paths-load-config!)
  ; minimize disk reads
  (parameterize* ([oa-current-auth-config-path (python-mod-auth-config-path "sparcur")]
                  [oa-current-auth-config (oa-read-auth-config)]
                  [oa-current-user-config
                   (with-handlers ([exn? (λ (e) (set! python-first-time? #t) #hash())])
                     (oa-read-user-config))]
                  [oa-current-secrets
                   (with-handlers ([exn? (λ (e) #hash())])
                     (oa-read-secrets))])
    (init-paths-int!)
    (load-config-int!)))

(define refresh-dataset-metadata-semaphore (make-semaphore 1))
(define (refresh-dataset-metadata text-search-box)
  ; XXX requries python sparcur to be installed
  (call-with-semaphore
   refresh-dataset-metadata-semaphore
   (thunk
    (dynamic-wind
      (thunk (send button-refresh-datasets enable #f))
      (thunk
       (let* ([result-projects (get-dataset-project-list)]
              [result (car result-projects)]
              [projects (cadr result-projects)]
              [datasets-remote (result->dataset-list result)]
              [datasets-local (get-local-datasets datasets-remote)]
              [datasets (append datasets-remote datasets-local)]
              [unfiltered? (= 0 (string-length (string-trim (send text-search-box get-value))))])
         (current-projects projects)
         (current-datasets datasets)
         (rehash-current-datasets!)
         (if unfiltered?
             (call-with-semaphore
              jview-semaphore
              (thunk ; needed to prevent attemts to modify an empty lview from on-subwindow-char
               (set-datasets-view! (send text-search-box list-box) (current-datasets))))
             (cb-search-dataset text-search-box #f))
         (println "dataset metadata has been refreshed") ; TODO gui viz on this (beyond updating the number)
         (with-output-to-file (path-cache-datasets)
           #:exists 'replace ; yes if you run multiple of these you could have a data race
           (λ ()
             (pretty-write result)
             (pretty-write projects)))))
      (thunk (send button-refresh-datasets enable #t))))))

(define (get-path-err)
  (hash-ref
   (hash-ref
    (current-blob)
    'status
    (hash))
   'path_error_report
   #f))

(define ok-ws '(#\space #\newline))
(define (render-ws char)
  (cond
    [(char=? char #\tab) '(#\\ #\t)]
    ;[(char=? char #\Newline) '(#\\ #\n)]
    [else (cons #\\ (cons #\x (string->list (number->string (char->integer char) 16))))]))

(define (expand-abnormal-whitespace str)
  ; FIXME horribly inefficient
  ; TODO apparently text extents can have style<%> ? will have to review
  ; i know it can be done using a draw context directly ...
  (let ([l '()]
        [hit #f])
    (for ([char (in-string str)])
      (if (and (char-whitespace? char)
               (not (for/or ([wsc (in-list ok-ws)])
                      ; basically anything not a space or newline is evil
                      ; newline is evil too, but is present in the target text
                      (char=? wsc char))))
          (begin
            (set! hit #t)
            (for ([char-char (in-list (render-ws char))])
              (set! l (cons char-char l))))
          (set! l (cons char l))))
    (if hit
        (list->string (reverse l))
        str)))

(define (fs-report)
  (for-each (λ (m)
              (displayln
               (expand-abnormal-whitespace m))
              (newline))
            (let ([ihr (hash-ref
                        (current-path-meta-blob)
                        'errors
                        #f)]
                  [hah (make-hash)])
              (if ihr
                  (begin
                    (for ([e ihr]
                          #:when (hash-ref e 'file_path #f))
                      (let* ([msg (hash-ref e 'message)]
                             [l (hash-ref! hah msg '())])
                        (hash-set! hah msg (cons (hash-ref e 'file_path) l))))
                    (for/list
                      ([k (sort (hash-keys hah) string<?)])
                      (string-append k "\n    " (string-join (sort (hash-ref hah k) string<?) "\n    "))))
                  '()))))

(define (paths-report)
  (for-each (λ (m)
              (displayln
               (expand-abnormal-whitespace
                (regexp-replace #rx"SPARC( Consortium)?/[^/]+/" m "\\0\n")))
              (newline))
            ; FIXME use my hr function from elsewhere
            (let ([ihr (get-path-err)])
              (if ihr
                  (hash-ref
                   (hash-ref
                    ihr '|#/entity_dirs|
                    (hash-ref ; try the old path since there is a clean transition
                     ihr '|#/specimen_dirs|
                     #hash((messages . ()))))
                   'messages)
                  '()))))

(define (manifest-report)
  ; FIXME this will fail if one of the keys isn't quite right
  ; TODO displayln this into a text% I think?
  (for-each (λ (m)
              (displayln
               (expand-abnormal-whitespace
                (regexp-replace #rx"SPARC( Consortium)?/[^/]+/" m "\\0\n")))
              (newline))
            ; FIXME use my hr function from elsewhere
            (let ([ihr (get-path-err)])
              (if ihr
                  (hash-ref (hash-ref ihr '|#/path_metadata/-1| #hash((messages . ()))) 'messages)
                  '()))))

(define (current-json-view-text)
  ; TODO consider jsexpr->pretty-json
  (display (jsexpr->string (current-blob) #:indent 2)))

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
                   (killable-system* raco-exe "pkg" "update" "--batch" this-package-path)
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
                                     (λ () -2))]
                       [gch (format "--commit=~a" (git-commit-hash))])
                   (when (not (= mtime-before mtime-after))
                     (parameterize ()
                       (when (file-exists? this-file-exe)
                         (rename-file-or-directory this-file-exe this-file-exe-tmp))
                       (let ([argv
                              (append
                               (list raco-exe "exe")
                               (case (system-type 'os)
                                 ((windows) (list "--ico" icon-ico))
                                 ((macosx) (list "--icns" icon-icns))
                                 (else '()))
                               (list "-v" "++exf" gch "-o" this-file-exe this-file))])
                         (println (string-join (cons "running" (map (λ (v) (if (path? v) (path->string v) v)) argv))))
                         (apply killable-system* argv)))
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
            (λ () ; restore the old version on failure
              (when (and (not (file-exists? this-file-exe)) (file-exists? this-file-exe-tmp))
                (rename-file-or-directory this-file-exe-tmp this-file-exe))
              (set! update-running? #f))))))))

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
                           ; path metadata
                           errors
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

; reminder, use uuids as keys because the struct hash changes when the dataset list is refreshed
(define jviews (make-hash))
(define uuid-json-hash (make-hash))
(define uuid-path-meta-json-hash (make-hash))

(define (dataset-jview! dataset #:update [update #f])
  (let* ([uuid (id-uuid dataset)]
         [hr-jview (hash-ref jviews uuid #f)]
         [jview
          (if (and hr-jview (not update))
              (begin
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
                  (let* ([path-meta-path (dataset-export-path-metadata-latest-path dataset)]
                         [path-meta-json (and path-meta-path (path->json path-meta-path))])
                    (when (and path-meta-json (hash-ref path-meta-json 'path_error_report #f))
                      (set! jhash
                        (hash-set jhash '|#/path-metadata|
                                  (hash 'path_error_report (hash-ref path-meta-json 'path_error_report)
                                        'errors (hash-ref path-meta-json 'errors)))))
                    (when lp
                      (hash-set! uuid-path-meta-json-hash uuid path-meta-json)
                      (hash-set! uuid-json-hash uuid json)))
                  (hash-set! jviews uuid jview-inner)
                  (set-jview-json! jview-inner jhash)
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
      (unless (and (current-dataset) (string=? (id-uuid dataset) (id-uuid (current-dataset))))
        ; we also set current-dataset in the lview on-subwindow-char to keep lview state synced
        (call-with-semaphore
         jview-semaphore
         (thunk (current-dataset dataset)))) ; XXX this is one of two places current-dataset should ever be set
      (thread
       (thunk ; we thread because dataset-jview! can be quite slow for large json blobs
        (let ([jview (dataset-jview! dataset)])
          ; we're done loading so if you want to load again knock yourself out
          (set! jviews-loading (set-remove jviews-loading uuid))
          ; we must use a semaphore here because if you have two consecutive events
          ; that are very close together in time the exact ordering of the threads
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
              (current-jview jview)
              ; we call set-button-status-for-dataset in the jview thread after dataset-jview!
              ; returns because setting buttons correctly depends uuid-json-hash having been
              ; populated which happens in dataset-jview! thus if set-button-status-for-dataset
              ; is called in the main thread uuid-json-hash may not have been populated and we
              ; get unexpected results
              (set-button-status-for-dataset dataset)))))))
      )))

;; dataset struct and generic functions

(define-generics ds
  (populate-list ds list-box)
  (lb-cols ds)
  (lb-data ds)
  (updated-short ds)
  (id-short ds)
  (id-uuid ds)
  (id-uuid-b64 ds)
  (id-project ds)
  (uri-human ds)
  (uri-sds-viewer ds)
  (dataset-src-path ds)
  (dataset-working-dir-path ds)
  (dataset-log-path ds)
  (dataset-latest-log-path ds)
  (dataset-export-latest-path ds)
  (dataset-export-path-metadata-latest-path ds)
  (dataset-cleaned-latest-path ds)
  (dataset-project-name ds)
  (fetch-export-dataset ds)
  (fetch-dataset ds)
  (clean-metadata-files ds)
  (load-remote-json ds)
  (export-dataset ds)
  (is-current? ds))

(struct dataset (id title updated pi-name-lf id-project publication-status)
  #:methods gen:ds
  [(define (populate-list ds list-box)
     ; FIXME annoyingly it looks like these things need to be set in
     ; batch in order to get columns to work
     (send list-box append
           (list
            (dataset-pi-name-lf ds)
            (dataset-title ds)
            (id-short ds)
            (updated-short ds)
            (dataset-publication-status ds))
           ds))
   (define (lb-cols ds)
     (list
      (dataset-pi-name-lf ds)
      (let ([dt (dataset-title ds)]) ; must meet label-string? requirements
        (if (<= (string-length dt) 200)
            dt
            (string-append (substring dt 0 196) " ...")))
      (id-short ds)
      (updated-short ds)
      (dataset-publication-status ds)))
   (define (lb-data ds)
     ; TODO we may want to return more stuff here
     ds)
   (define (dataset-project-name ds)
     (define pair (assoc (dataset-id-project ds) (current-projects)))
     (if pair (cadr pair) "?"))
   (define (dataset-export-latest-path ds)
     (let* ([uuid (id-uuid ds)]
            [lp (build-path
                 (path-export-datasets)
                 uuid "LATEST" "curation-export.json")]
            #;
            [asdf (println lp)]
            [qq (and (file-exists? lp) (resolve-path lp))])
       ; FIXME not quite right?
       qq))
   (define (dataset-export-path-metadata-latest-path ds)
     (let* ([uuid (id-uuid ds)]
            [lp (build-path
                 (path-export-datasets)
                 uuid "LATEST" "path-metadata.json")]
            [qq (and (file-exists? lp) (resolve-path lp))])
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
           (when status-1
             (set-button-status-for-dataset ds)
             (post-fetch-for-dataset ds))
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
                 (dataset-jview! ds #:update #t)
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
                   (post-fetch-for-dataset ds)
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
                       (dataset-jview! ds #:update #t)
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
             (with-output-to-string
               (thunk
                (set! status-1 (apply py-system* argv-1 #:set-pwd? #t)))))
           (if status-1
               (println (format "cleaning metadata files completed for ~a" (dataset-id ds)))
               (println (format "cleaning metadata files FAILED for ~a" (dataset-id ds)))))))))
   (define (dataset-latest-prod-url ds)
     (string-append (url-prod-datasets) "/" (id-uuid ds) "/LATEST/curation-export.json"))
   (define (dataset-path-meta-latest-prod-url ds)
     (string-append (url-prod-datasets) "/" (id-uuid ds) "/LATEST/path-metadata.json"))
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
       (let* ([path-meta-url (dataset-path-meta-latest-prod-url ds)]
              [path-meta-json (url->json path-meta-url)])
         (hash-set! uuid-path-meta-json-hash uuid path-meta-json)
         (when (and path-meta-json (hash-ref path-meta-json 'path_error_report #f))
           (set! jhash
             (hash-set jhash '|#/path-metadata|
                       (hash 'path_error_report (hash-ref path-meta-json 'path_error_report)
                             'errors (hash-ref path-meta-json 'errors))))))
       (hash-set! uuid-json-hash uuid json)
       (hash-set! jviews uuid jview-inner)
       (set-jview-json! jview-inner jhash)
       (set-button-status-for-dataset ds)
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
   (define (id-uuid-b64 ds)
     (uuid->b64-uuid (id-uuid ds)))
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
      [(3) dataset-updated]
      [(4) dataset-publication-status]))
  (lambda (ds1 ds2)
    (operator (selector ds1) (selector ds2))))

(define (current-dataset-sort)
  (make-sort-datasets *current-lview-column* *current-lview-ascending*))

(define (set-datasets-view! list-box datasets)
  (define selected (current-dataset))
  (current-datasets-view datasets)
  (define sorted (sort datasets (current-dataset-sort)))
  (send/apply list-box set (apply map list (map lb-cols sorted)))
  (for ([ds sorted]
        [n (in-naturals)])
    (send list-box set-data n (lb-data ds)))
  (define ndt (length datasets))
  (define ndr (length (for/list ([d datasets] #:when (not (string=? (dataset-publication-status d) "no-access"))) d)))
  (define ndl (- ndt ndr))
  (send button-refresh-datasets set-label
        (format lbt-refresh-datasets ndt ndr ndl)) ; XXX free variable on the button in question
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
               (or (and (or (eq? tda 'value) (eq? tda 'hash))
                        ; really not sure how these can possibly show up at the same level but whatever
                        (or (and (symbol? nda)
                                 (symbol? ndb)
                                 #;
                                 (apply string<? (map symbol->string (list nda ndb)))
                                 (if (and (eq? tda 'hash) jview-sort-new)
                                     (let ([ka (hash-ref key-rank-hash nda #f)]
                                           [kb (hash-ref key-rank-hash ndb #f)])
                                       (if (and ka kb)
                                           (< ka kb)
                                           (symbol<? nda ndb)))
                                     (symbol<? nda ndb)))
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

(define (post-fetch-for-dataset ds)
  ; unconditional things that run regardless of whether the dataset is selected
  (let ([frame-upload (hash-ref frame-uploads (id-uuid ds) #f)])
    (when frame-upload
      ; update runs in a background thread so calling post-fetch-for-datasets won't lag the ui
      (send frame-upload update))))

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
                  [logs-enable? (dataset-latest-log-path ds)]
                  [export-enable? (dataset-export-latest-path ds)]
                  [cached-json? (hash-ref uuid-json-hash (id-uuid ds) #f)])
              ; source data folder
              (for ([button (in-list all-button-open-dataset-folder)]) (send button enable enable?))
              (send button-export-dataset enable enable?)
              (send button-open-dataset-shell enable enable?)
              (send button-clean-metadata-files enable enable?)
              (for ([button (in-list all-button-upload-changes)]) (send button enable enable?))
              ; export
              (send button-open-export-json-file enable export-enable?)
              (send button-open-export-ipython enable export-enable?)
              ; export dependent reports
              (send button-fs-report enable cached-json?)
              (send button-paths-report enable cached-json?)
              (send button-manifest-report enable cached-json?)
              (send button-open-export-json-view enable cached-json?)
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
    (let ([path (send text-field get-value)])
      (unless (or (string=? path (str-not-set)) (string=? path (str-path-does-not-exist)))
        (xopen-path path)))))

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
           (set-jview! dataset)))])))

(define (result->dataset-list result)
  (let ([nargs (procedure-arity dataset)])
    (map (λ (ti)
           (let ([pad (for/list ([i (in-range (- nargs (length ti)))]) missing-var)])
             (apply dataset (append ti pad))))
         result)))

(define (get-dataset-project-list)
  (let* ([argv argv-simple-for-racket]
         [status #f]
         [result-string
          (parameterize ()
            ; FIXME if the viewer is closed before this finishes then bad things happen
            ; with regard to printing error messages etc. pretty sure we need a way to
            ; send a signal to subprocesses on exit (annoyingly)
            (with-output-to-string (λ () (set! status (apply py-system* argv)))))]
         [string-port (open-input-string result-string)]
         [result (read string-port)]
         [-projects (read string-port)]
         [projects (if (eq? eof -projects)
                       (begin
                         (displayln "detected old output format from argv-simple-for-racket, please retangle sparcur.simple from dev notes")
                         '())
                       -projects)])
    (unless status
      (error "Failed to get dataset list! ~a" (string-join argv-simple-for-racket " ")))
    (list result projects)))

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
      (init-paths-load-config!))
    (send frame-preferences show do-show?)
    (for ([sigh (list text-prefs-api-key text-prefs-api-sec)])
      ; FIXME HACK workaround for text-field% value not updating if frame is not shown
      (send sigh set-value (send sigh get-value)))))

(define (cb-select-remote-org o e)
  (define index (send o get-selection))
  #;
  (println (list "before"
                 index
                 (send text-prefs-api-key get-value)
                 (send text-prefs-api-sec get-value)
                 ))
  (define key-sec (list-ref (remote-org-keys) index))
  ; FIXME gui bug this fails to actually update the field in some cases !?!??!
  ; for example if the prefs window is not visible ?!?!
  (send text-prefs-api-key set-value (car key-sec))
  (send text-prefs-api-sec set-value (cdr key-sec))
  #;
  (println (list "after"
                 key-sec
                 (send text-prefs-api-key get-value)
                 (send text-prefs-api-sec get-value)
                 )))


(define iconize-hack (eq? 'unix (system-type)))
(define (frame-to-front frame)
  (send frame show #t)
  ; iconize hack to get the frame to come to the front when using gtk
  (when iconize-hack
    (send frame iconize #t)
    (send frame iconize #f)))

(define (cb-toggle-upload o e #:show? [show? #f])
  ; TODO detect when an actual quite is run
  ; FIXME toggling this via button when the window is open is extremely confusing
  ; it should probably raise the window in that case?
  (let*-values
      ([(new? frame-upload) (get-frame-upload! (current-dataset))]
       [(do-show?) (or show? (not (send frame-upload is-shown?)))])
    (frame-to-front frame-upload)
    (when (and do-show? (or new? (not (send frame-upload updated-once?))))
      (send frame-upload update))))

(define (cb-upload-button-show-and-raise o e)
  (cb-toggle-upload o e #:show? #t))

(define (cb-toggle-download o e #:show? [show? #f])
  ; TODO detect when an actual quite is run
  ; FIXME toggling this via button when the window is open is extremely confusing
  ; it should probably raise the window in that case?
  (let*-values
      ([(new? frame-download) (get-frame-download! (current-dataset))]
       [(do-show?) (or show? (not (send frame-download is-shown?)))])
    (frame-to-front frame-download)
    (when (and do-show? (or new? (not (send frame-download updated-once?))))
      (send frame-download update))))

(define (cb-download-button-show-and-raise o e)
  (cb-toggle-download o e #:show? #t))

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

(define (cb-open-export-json-file obj event)
  (let*-values ([(path) (dataset-export-latest-path (current-dataset))])
    (xopen-path path)))

(define (cb-open-export-json-view obj event #:show [show? #t])
  (define frame (cb-x-report obj event 'json-view #:show show?))
  (when show?
    (frame-to-front frame)))

(define (path-to-program path)
  ; in the unlikely event that xdg-open is not on the system fail over
  ; to something to avoid an error
  (define text-program (for/or ([p '("emacs" "vim" "nano" "drracket")]) (find-executable-path p)))
  #; ; TODO if can't get xdg-open on some system maybe expand this a bit
  (define ext (path-get-extension path))
  text-program)

(define (xopen-path path)
  (let* ([is-win? #f]
         [command (find-executable-path
                  (case (system-type 'os*)
                    ((linux) (or (find-executable-path "xdg-open") ; if firefox complains, make sure it matches firefox not firefox-bin xdg-settings get default-web-browser
                                 (path-to-program path)))
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
          (killable-system* command (if (and is-win? is-dir?) "." path) #:set-pwd? #t)))))))

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

(define (cb-refresh-dataset-jview obj event)
  (dataset-jview! (current-dataset) #:update #t))

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

(define (cb-fs-report o e #:show [show #t])
  (cb-x-report o e 'fs #:show show))

(define (cb-paths-report o e #:show [show #t])
  (cb-x-report o e 'paths #:show show))

(define (cb-manifest-report o e #:show [show #t])
  (cb-x-report o e 'manifest #:show show))

(define (cb-x-report obj event type #:show [show #t])
  ; TODO populate the editor
  ; TODO implement this as a method on edcanv-man-rep ?
  (let-values ([(report-function
                 report-frame
                 report-edcanv)
                (case type
                  [(fs) (values fs-report frame-fs-report edcanv-fs-rep)]
                  [(paths) (values paths-report frame-paths-report edcanv-path-rep)]
                  [(manifest) (values manifest-report frame-manifest-report edcanv-man-rep)]
                  [(json-view) (values current-json-view-text frame-json-view edcanv-json-view)]
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
      (send report-frame show #t))
    report-frame))

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
                                                                  downcased-text)
                                                (string-contains? (dataset-id-project d) text)
                                                (string-contains? (string-downcase (dataset-project-name d))
                                                                  downcased-text)
                                                (string-contains? (dataset-publication-status d) downcased-text)))
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
  (when (current-datasets)
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
                      (cb-dataset-selection list-box #f)))))))))))))

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
  (init-paths-load-config!))

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
                              [("Convert" "Move") panel-convert-mode]
                              [else panel-validate-mode])])
        ; we rereparent power-user so that it is always on the right
        (for ([button all-button-download-all-files]) (send button enable (not (string=? mode "Move"))))
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

(define (k-next-thing r e)
  "do nothing")

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

(define (cb-select-all obj e)
  (when (or
         (is-a? obj editor-canvas%)
         (is-a? obj text-field%))
    (send (send obj get-editor) select-all)))

(define (cb-copy-value obj e)
  ; TODO proper chaining
  (when (is-a? obj json-hierlist%)
    (let* ([raw-value (node-data-value (send (send obj get-selected) user-data))]
           [value (*->string raw-value)])
      (send the-clipboard set-clipboard-string value (current-milliseconds))))
  (when (or
         (is-a? obj editor-canvas%)
         (is-a? obj text-field%))
    (send (send obj get-editor) copy)))

; add functions
(send* keymap
  (add-function "test" k-test)
  (add-function "fetch-export-dataset" k-fetch-export-dataset)
  (add-function "fetch-dataset" k-fetch-dataset)
  (add-function "export-dataset" k-export-dataset)
  (add-function "quit" k-quit)
  (add-function "test-backspace" (λ (a b) (displayln (format "delete all the things! ~a ~a" a b))))
  (add-function "copy-value" cb-copy-value)
  (add-function "backward-kill-word" backward-kill-word)
  (add-function "next-thing" k-next-thing)
  (add-function "focus-search-box" (λ (a b) (send text-search-box focus)))
  (add-function "open-dataset-folder" cb-open-dataset-folder)
  (add-function "open-export-folder" cb-open-export-folder)
  (add-function "toggle-power-user" cb-power-user)
  (add-function "open-export-json-file" cb-open-export-json-file)
  (add-function "open-export-ipython" cb-open-export-ipython)
  (add-function "open-dataset-shell" cb-open-dataset-shell)
  (add-function "open-dataset-latest-log" cb-open-dataset-lastest-log)
  (add-function "toggle-prefs" cb-toggle-prefs)
  (add-function "toggle-upload" cb-toggle-upload)
  (add-function "select-all" cb-select-all)
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
  (map-function "c:a"     "select-all")
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
  (map-function "c:j" "open-export-json-file")
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
(define lbt-refresh-datasets "Refresh Datasets (~a ~a ~a)")
(define button-refresh-datasets (new button%
                                     [label lbt-refresh-datasets]
                                     [callback cb-refresh-dataset-metadata]
                                     [parent panel-org-actions]))

#; ; prefer file menu to avoid accidental clicks
(define button-update-viewer (new button%
                                  [label "Update Viewer"]
                                  [callback cb-update-viewer]
                                  [parent panel-org-actions]))

(define lview
  (new (class list-box%
         (rename-super [super-on-subwindow-char on-subwindow-char])
         (inherit get-selection select get-number number-of-visible-items
                  get-first-visible-item
                  set-first-visible-item)
         (super-new)
         (define down-down #f)
         (define up-down #f)
         (define fvi-fix (if (eq? (system-type 'os*) 'macosx) add1 identity))
         (define (next-sel sel code)
           (unless (or ; do nothing cases
                    (and (= sel 0) (eq? code 'up))
                    (and (= (sub1 (get-number)) sel) (eq? code 'down)))
             (select sel #f)
             (let ([new-sel ((if (eq? code 'down) add1 sub1) sel)]
                   [fvi (get-first-visible-item)]
                   ; seems like nvi is off by 2 ???
                   [nvi (- (number-of-visible-items) 2)])
               (if (< (+ fvi nvi) new-sel)
                   (set-first-visible-item (+ 1 (- new-sel nvi)))
                   (when (<= new-sel (fvi-fix fvi)) ; issues where fvi thinks it is zero but actually isn't
                     (set-first-visible-item new-sel)))
               (select new-sel)
               (let ([dataset (get-selected-dataset this)])
                 (call-with-semaphore
                  jview-semaphore
                  (thunk (current-dataset dataset)))))))
         (define/override (on-subwindow-char receiver event)
           (let ([code (send event get-key-code)]
                 [release-code (send event get-key-release-code)]
                 [released-this-time #f]
                 )
             (if (eq? code 'release)
                 (cond
                   [(and (eq? release-code 'up) up-down) (set! up-down #f) (set! released-this-time #t)]
                   [(and (eq? release-code 'down) down-down) (set! down-down #f) (set! released-this-time #t)]
                   [else #f])
                 (cond
                   [(and (eq? code 'up) (not up-down)) (set! up-down #t)]
                   [(and (eq? code 'down) (not down-down)) (set! down-down #t)]
                   [else #f]))
             (if (or (eq? code 'down) (eq? code 'up))
                 (let ([sel (get-selection)])
                   (next-sel sel code))
                 (if released-this-time
                     (let ([dataset (current-dataset)]) ; we do not use cb-dataset-selection here because it assumes that
                       ; current-dataset will be set inside the call it itself instead of before
                       (set-jview! dataset))
                     (super-on-subwindow-char receiver event))))))
       [label ""]
       [font (make-object font% 10 'modern)]
       [choices '()]
       [columns '("Owner" "Folder Name" "Identifier" "Updated" "PubStat")]
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
  (set-column-width 1 120 60 1200)
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

(define all-button-open-dataset-folder '()) ; we do it this way since these should enable/disable as a group
(define (make-button-open-dataset-folder parent)
  (define butt
    (new button%
         [label "Open Folder"]
         [callback cb-open-dataset-folder]
         [parent parent]))
  (set! all-button-open-dataset-folder (cons butt all-button-open-dataset-folder)))

(make-button-open-dataset-folder panel-validate-mode)

(define button-fexport (new (tooltip-mixin button%)
                            [label "Fetch+Export"]
                            [tooltip "Shortcut F5"] ; FIXME this should populate dynamically
                            [tooltip-delay 100]
                            [callback cb-fetch-export-dataset]
                            [parent panel-validate-mode]))

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

(define (make-button-manifest-report parent)
  (new button%
       [label "Manifest Rep"] ; used sometimes
       [callback cb-manifest-report]
       [parent parent]))

(define button-manifest-report (make-button-manifest-report panel-validate-mode))

(define (make-button-paths-report parent)
  (new button%
       [label "Paths Report"] ; used sometimes
       [callback cb-paths-report]
       [parent parent]))

(define button-paths-report (make-button-paths-report panel-validate-mode))

(define (make-button-fs-report parent)
  (new button%
       [label "FS Report"]
       [callback cb-fs-report]
       [parent parent]))

(define button-fs-report (make-button-fs-report panel-validate-mode))

(define (make-button-clean-metadata-files parent)
  (new button%
       [label "Clean Metadata"] ; 5 star
       [callback cb-clean-metadata-files]
       [parent parent]))

(define button-clean-metadata-files (make-button-clean-metadata-files panel-validate-mode))

(define all-button-upload-changes '())
(define (make-button-upload-changes parent)
  (define butt
    (new (tooltip-mixin button%)
         [label "Upload"]
         [callback cb-upload-button-show-and-raise]
         [tooltip "Shortcut C-u"]
         ; TODO separate button for the convert use case?
         [parent parent]))
  (set! all-button-upload-changes (cons butt all-button-upload-changes)))

(make-button-upload-changes panel-validate-mode)

(define all-button-download-manager '())
(define (make-button-download-manager parent)
  (define butt
    (new (tooltip-mixin button%)
         [label "Download"]
         [callback cb-download-button-show-and-raise]
         ;[tooltip "Shortcut C-d"]
         ; TODO separate button for the convert use case?
         [parent parent]))
  (set! all-button-download-manager (cons butt all-button-download-manager)))

#; ; TODO not quite ready yet
(make-button-download-manager panel-validate-mode)

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
         [label "Download all"]
         [tooltip "Download all files"]
         [tooltip-delay 100]
         [callback cb-download-all-files]
         [parent parent]))
  ; TODO this would have to be a macro because we need the exact name for the list
  (set! all-button-download-all-files (cons butt all-button-download-all-files)))

(make-button-open-dataset-folder panel-convert-mode)

(make-button-download-all-files panel-convert-mode)

(make-button-upload-changes panel-convert-mode)

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

(define button-open-dataset-pipeline-status
  (new (tooltip-mixin button%)
       [label "PPS"]
       [tooltip "Production Pipeline Status"]
       [tooltip-delay 100]
       [callback
        (λ (o e)
          (xopen-path
           (string-append "https://cassava.ucsd.edu/sparc/pipelines/status/" (id-uuid (current-dataset)))))]
       [parent panel-power-user]
       ))

(define button-open-export-json-file
  (new (tooltip-mixin button%)
       [label "JSON"]
       [tooltip "Shortcut C-j"]
       [tooltip-delay 100]
       [callback cb-open-export-json-file]
       [parent panel-power-user]))

(define button-open-export-json-view
  (new button%
       [label "JVIEW"]
       [callback cb-open-export-json-view]
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
                                            [parent panel-power-user]))

(define button-refresh-dataset-jview
  ; use to sync view with cli export that can't call back could get
  ; fancy with inotify or a socket that the cli could check, but
  ; what's that point, if you have this open just click it
  (new button%
       [label "Refresh"]
       [callback cb-refresh-dataset-jview]
       [parent panel-power-user]))

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

    (define (dataset-push-cache-dir)
      ; XXX this must always match the python conventions
      (build-path (path-cache-push) (id-uuid dataset)))
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
          (error "make-push-manifest failed with ~a for ~a" status-1 (string-join argv " ")))
        (println (format "make-push-manifest completed for ~a" (dataset-id dataset)))))
    (define (unfinished-push?)
      (define path-dut-latest (build-path (path-cache-push) (id-uuid dataset) (updated-transitive) "LATEST"))
      (define cands '("done.sxpr" "completed.sxpr" "manifest.sxpr" "paths.sxpr"))
      (define first-present (for/or [(cand cands) #:when (file-exists? (build-path path-dut-latest cand))] cand))
      (and first-present
           (not (string=? first-present "done.sxpr"))
           ; FIXME pretty sure I need to inverte the order of build-path and resolve-relative-path
           (resolve-relative-path (build-path path-dut-latest first-present))))

    (define (get-latest-push-path base-path)
      ; XXX check that base-path exists outside this function
      ; TODO make sure to handle cases where the various parent paths do not exist beyond base-path
      (define updated-cache-path (last (directory-list base-path #:build? #t)))
      (last (directory-list updated-cache-path #:build? #t)))
    (define (cb-debug o e)
      (define dpcd (dataset-push-cache-dir))
      (when (directory-exists? dpcd)
        (let ([latest-path (get-latest-push-path dpcd)])
          (xopen-path latest-path))))
    (define button-debug-folder
      (new button%
           [label "Debug"]
           [callback cb-debug]
           [parent hp]))

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
      (println (format "Starting push from manifest for ~a" (id-uuid dataset)))
      (let* ([argv (argv-simple-push dataset (updated-transitive) (push-id))]
             [_ (when (debug-push) (println (list "debug push argv:" (string-join argv " "))))]
             [status-1 (debug-push)]
             [result-string
              (unless (debug-push)
                (parameterize ()
                  (println (string-join argv " "))
                  (with-output-to-string
                    (thunk
                     (set! status-1 (apply py-system* argv))))))])
        ; TODO need a visual cue that this has succeeded
        (unless status-1
          (error "push failed with ~a for ~a" status-1 (string-join argv " ")))
        (update) ; automatically run this now that we reindex
        (unless (debug-push)
          (println (format "push completed for ~a" (dataset-id dataset))))
        (when (debug-push)
          ; FIXME for some reason the workflow gets stuck and we can't walk through again
          ; unless these values are all reset
          (cb-confirm #f #f #:force-off? #t))))
    (define (cb-push-to-remote o e)
      ; TODO probably disable clicking the button again until the process finishes or fails?
      ; 1. confirm pass the manifest of changes
      #;
      (println "Upload is not implemented yet.")
      (send button-push enable #f)
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
             [result-string
              (parameterize ()
                #;
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
      #;
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
                   (old-id
                    (if (hash? old-meta)
                        (let ([hr (hash-ref old-meta 'old-id)])
                          (let-values ([(N type uuid)
                                        (apply values (string-split (car hr) ":"))])
                            (string-append
                             (substring uuid 0 4)
                             " ... "
                             (let ([slu (string-length uuid)])
                               (substring uuid (- slu 4) slu)))))
                        ; the alternative is that "change" itself is provided an meta id is still present
                        #f
                        ))
                   (ops (if old-id (reverse (cdr rrops)) raw-ops)))
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
      (define raw-ops (hash-ref record 'ops))
      (define-values (old-id ops)
        (if (member "change" raw-ops)
            (let* ((rrops (reverse raw-ops))
                   (old-meta (car rrops))
                   (old-id
                    (if (hash? old-meta)
                        (let ([hr (hash-ref old-meta 'old-id)])
                          (let-values ([(N type uuid)
                                        (apply values (string-split (car hr) ":"))])
                            (string-append
                             (substring uuid 0 4)
                             " ... "
                             (let ([slu (string-length uuid)])
                               (substring uuid (- slu 4) slu)))))
                        ; the alternative is that "change" itself is provided an meta id is still present
                        ""))
                   (ops (if (non-empty-string? old-id) (reverse (cdr rrops)) raw-ops)))
              (values old-id ops))
            (values (or (hash-ref record 'old-id) "") raw-ops)))
      #; ; TODO determine whether to display this maybe for use in sorting?
      (hash-ref record 'updated)
      (list
       type
       (hash-ref record 'path)
       old-id
       (string-join ops " ")
       ))
    #;
    (define/public (get-list-box) list-box) ; the problem with private stuff is that it is extremely annoying to debug relative to normal
    #; ; then you can use this in the repl ...
    (define lb (send (let-values ([(x f) (get-frame-upload! (current-dataset))]) f) get-list-box))
    (define (set-rows diff-list)
      ; TODO preserve existing selections I think?
      (if (null? diff-list) ; null? check protects againsts empty map
          (send list-box clear) ; oof non-homogenous
          (begin
            (send/apply list-box set (apply map list (map diff-list-to-columns diff-list)))
            (for ([diff (in-list diff-list)]
                  [n (in-naturals)])
              (send list-box set-data n diff)))))
    (define updated-once #f)
    (define/public (updated-once?) updated-once)
    (define (selected)
      ; TODO will need a separate hash table or something to do rapid lookup via uuid
      ; to restore the state
      (for/list ([i (in-list (send list-box get-selections))])
        (send list-box get-data i)))
    (define (do-update)
      (define currently-selected (selected))
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
      (when (for/or ([diff currently-selected]) (not (member diff diff-list)))
        ; we just fetched
        ; and the selected files changed or are longer in the list
        ; when selected is not in diff-list or it changed
        ; if all currently selected are still members and unchanged then ok to keep
        ; though we need to update the selection here I think? TODO
        (cb-confirm #f #f #:force-off? #t)))
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

;; download

(define (set-paths-view! list-box paths)
  (define -selected (send list-box get-selections)) ; FIXME selections that get filtered out are forgotten!
  (define selected (map (λ (i) (send list-box get-data i)) (if -selected -selected '())))
  (println selected)
  (send list-box current-paths-view paths)
  (define sorted paths)
  (send/apply list-box set (apply map list (map (λ (p) (list "???" p)) sorted)))
  (for ([p sorted]
        [n (in-naturals)])
    (send list-box set-data n p)) ; TODO more data
  (let ([current-paths-index
         (for/list ([one-selected selected])
           (index-of sorted one-selected
                     (lambda (element target) ; struct id changes so eq? by itself fails
                       (println (list element target))
                       (and element target (string=? element target)))))])
    (when current-paths-index
      (for ([index current-paths-index])
        (when index ; FIXME TODO need to stash previously selected and restore ?
            (send list-box select index))))))

(define (cb-paths-selection o e)
  (define button-do-download (send o download-button))
  (send button-do-download enable (not (null? (send o get-selections)))))

(define (match-paths text paths)
  "given text return paths that match"
  (if text
      (let* ([downcased-text (string-downcase text)]
             [matches (for/list ([p paths] #:when (or (string-contains?  p text))) p)])
        (if (null? matches)
            paths
            matches))
      paths))

(define (cb-search-paths obj e)
  (define text (string-trim (send obj get-value)))
  (define list-box (send obj list-box))
  (define (current-paths) (send list-box current-paths))
  (define (current-paths-view) (send list-box current-paths-view))
  (define search-semaphore (send obj search-semaphore))
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
            (when (not (equal? (current-paths) (current-paths-view)))
              (set-paths-view! list-box (current-paths)))
            (let ([matching (match-paths text (current-paths))])
              (when (string=? text wat)
                (unless (or (null? matching)
                            (eq? matching (or (current-paths-view) (current-paths))))
                  (set-paths-view! list-box matching)
                  (when (= (length matching) 1)
                    (send list-box set-selection 0)
                    (cb-paths-selection list-box #f))))))))))))

(define frame-download%
  (class frame%
    (init dataset)
    (define update-semaphore (make-semaphore 1))
    (super-new)
    (rename-super [super-on-subwindow-char on-subwindow-char])
    (define/override (on-subwindow-char receiver event)
      (super-on-subwindow-char receiver event)
      (send keymap handle-key-event receiver event))
    (define/public (update)
      (thread
       (thunk
        (call-with-semaphore
         update-semaphore
         (thunk
          (dynamic-wind
            (thunk
             (send button-do-download enable #f))
            (thunk ; nothing going on in here yet
             "TODO")
            (thunk
             (send button-do-download enable #t))))))))
    #|
    the layout we want for this is probably search box at the top or the bottom
    that will do substring filtering on the contents of the listbox

    then the listbox with all the possible paths and their status status to the left

    then to the right of the search box a button that says download but on
    |#

    (define list-box
      (new
       (class list-box% (super-new)
         (define *current-paths* #f)
         (define *current-paths-view* #f)
         (define/public current-paths
           (case-lambda
             [() *current-paths*]
             [(value) (set! *current-paths* value)]))
         (define/public current-paths-view
           (case-lambda
             [() *current-paths-view*]
             [(value) (set! *current-paths-view* value)]))
         (define/public (download-button) button-do-download)
         )
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
       [columns '("status" "path")]
       [style '(extended column-headers clickable-headers)]
       [callback cb-paths-selection]
       [parent this]))
    (send* list-box
      (set-column-width 0 60 20 120)
      (set-column-width 1 300 120 1200))
    ;(define (cb-selection o e))
    (let ([curps '("hello" "world" "how" "are" "you")])
      (send list-box current-paths curps)
      (send list-box current-paths-view curps))
    (define hp
      (new horizontal-panel%
           [stretchable-height #f]
           [alignment '(right center)]
           [parent this]))

    (define text-search-box
      ; text box to make it easier to paste in identifiers or titles and
      ; find a match and view it
      (new (class text-field% (super-new)
             (define *list-box* #f)
             (define/public (search-semaphore) (make-semaphore 1))
             (define/public list-box
               (case-lambda
                 [() *list-box*]
                 [(value) (set! *list-box* value)])))
           [label ""]
           [callback cb-search-paths]
           [parent hp]))

    (send text-search-box list-box list-box)

    (define (cb-do-download o e)
      ; TODO define this workflow and states so that we don't try to double download
      ; the full list of files to be downloaded needs to be here, and we will need
      ; a file watcher or something to know when they are actually done or we need
      ; the ability to check the status of just the downloading files, or other
      ; files in the event that e.g. i use spc fetch directly etc. there are more issues
      ; to work through here
      #f)
    (define button-do-download
      (new button%
           [label "Download"]
           [callback cb-do-download]
           [enabled #f]
           [parent hp]))
    (set-paths-view! list-box (send list-box current-paths-view))))

(define frame-downloads (make-hash))
(define (get-frame-download! dataset)
  (let* ([uuid (id-uuid dataset)]
         [hr-frame-download (hash-ref frame-downloads uuid #f)]
         [frame-download
          (if hr-frame-download
              hr-frame-download
              (let ([fu
                     (new frame-download%
                          [dataset dataset]
                          [width 640]
                          [height 480]
                          [label (format "download for ~a" (id-uuid dataset))])])
                (hash-set! frame-downloads (id-uuid dataset) fu)
                fu))])
    ; TODO remove from hash on close probably? the workflow is a bit different
    (values (not hr-frame-download) frame-download)))

;; reports

(define (make-frame-report label)
  (new
   (class
     (frame:searchable-text-mixin
      (frame:searchable-mixin
       (frame:text-mixin
        (frame:editor-mixin
         (frame:standard-menus-mixin
          (frame:basic-mixin frame%))))))
     (super-new)
     (rename-super [super-on-subwindow-char on-subwindow-char])
     (define/override (on-subwindow-char receiver event)
       (super-on-subwindow-char receiver event)
       (send keymap handle-key-event receiver event))
     #; ; show hide basically
     (define/augment (on-close)
       (displayln "prefs closed")))
   [filename label]
   [width 640]
   [height 480]))

(define frame-manifest-report (make-frame-report "sparcur report manifests"))

(define frame-paths-report (make-frame-report "sparcur report paths"))

(define frame-fs-report (make-frame-report "sparcur report fs"))

(define frame-json-view (make-frame-report "sparcur json view"))

(define edcanv-man-rep (send frame-manifest-report get-canvas))

(define edcanv-path-rep (send frame-paths-report get-canvas))

(define edcanv-fs-rep (send frame-fs-report get-canvas))

(define edcanv-json-view (send frame-json-view get-canvas))

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

(define text-prefs-viewer-commit
  (new text-field%
       [font (make-object font% 10 'modern)]
       [label "Commit     "]
       [enabled #t]
       [init-value ""]
       [parent panel-prefs-holder]))

(define choice-prefs-remote-organization
  (new choice%
       [font (make-object font% 10 'modern)]
       [label "Remote Org "]
       [enabled #t]
       [stretchable-width #t] ; needed for correct alignment
       [choices '()]
       [callback cb-select-remote-org]
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
(define text-prefs-path-idlib-cfg (make-text-prefs-path   "idlib      "))
(define text-prefs-path-ontqu-cfg (make-text-prefs-path   "ontquery   "))
(define text-prefs-path-pyont-cfg (make-text-prefs-path   "pyontutils "))
(define text-prefs-path-data (make-text-prefs-path        "data-path  "))
(define text-prefs-path-logs (make-text-prefs-path        "logs       "))

(define radio-box-viewer-mode
  (new radio-box%
       [label "Viewer Workflow"]
       [choices '("Validate" "Convert" "Move")]
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
        (dataset-jview! dataset)
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

(define (python-first-time)
  (when (let ([status #f])
          (with-output-to-string
            (thunk (set! status (apply py-system* (python-mod-args "sparcur.cli" "--help")))))
          status)
    (init-paths-load-config!)))

(define (set-icons frame)
  (if (eq? (system-type 'os) 'unix)
      (let ([sparcur-viewer-icon-16 (read-bitmap icon-16)]
            [sparcur-viewer-icon-32 (read-bitmap icon-32)])
        ; undocumented issue that icon sizes must be exact for gtk
        (send frame set-icon sparcur-viewer-icon-16 #f 'small)
        (send frame set-icon sparcur-viewer-icon-32 #f 'large))
      (let ([sparcur-viewer-icon-48 (read-bitmap icon-48)])
        (send frame set-icon sparcur-viewer-icon-48 #f 'both))))

(module+ main
  (init-paths-load-config!)
  (when python-first-time? (python-first-time))
  (define ok-to-fetch
    (and (null? (missing-remote-org-keys))
         (not (null? (remote-org-keys)))))
  (define result
    (when ok-to-fetch
      (populate-datasets))) ; slow if not cached, but thus only on the very first run
  (set-icons frame-main)
  (send frame-main show #t) ; show this first so that users know something is happening
  (set-lview-column-state! lview *current-lview-column* #:view-only? #t)
  (send text-search-box focus)
  ; do this last so that if there is a 0th dataset the time to render the hierlist isn't obtrusive
  (when ok-to-fetch
    (send lview select 0)) ; first time to ensure current-dataset always has a value XXX callback is NOT invoked using select so we call it manually here
  (void (cb-dataset-selection lview #f))
  (void (thread (thunk (set-commit-hash!))))
  (if ok-to-fetch
      (begin
        (thread (thunk (refresh-dataset-metadata text-search-box))) ; refresh datasets in a separate thread to avoid delaying startup
        (unless (eq? (system-type) 'unix)
          ; do NOT run this when gtk is the windowing toolkit it will eat up
          ; tens of gigs of memory, windows and macos don't have the issue
          ; it also takes multiple minutes to run due to all the allocations?
          (render-datasets)))
      (begin
        (when (not (null? (missing-remote-org-keys)))
          (displayln (cons "missing remote org keys:" (missing-remote-org-keys))))
        (displayln "Configuration incomplete please check Preferences"))))
