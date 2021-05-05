#lang racket

(require racket/gui
         racket/generic
         racket/pretty
         gui-widget-mixins
         json
         ; XXX json-view has lurking quadratic behavior
         json-view)

;; parameters (yay dynamic variables)
(define path-cache-dir (make-parameter #f))
(define path-cache-datasets (make-parameter #f))
(define path-export-dir (make-parameter #f))
(define path-export-datasets (make-parameter #f))
(define path-source-dir (make-parameter #f))
(define current-dataset (make-parameter #f))
(define current-datasets (make-parameter #f))
(define current-datasets-view (make-parameter #f))
(define current-jview (make-parameter #f))
(define overmatch (make-parameter #f))
(define python-interpreter (make-parameter
                            (find-executable-path
                             (case (system-type)
                               ((windows) "python.exe")
                               ; osx is still stuck on 2.7 by default so need brew
                               ; but for whatever reason find-executable-path is not brew aware
                               ((macosx) "/usr/local/bin/python3")
                               ((unix) "python") ; all of these should be >= 3.7 by this point
                               (else (error "uhhhhh? beos is this you?"))))))

;; other variables

(define include-keys
  ; have to filter down due to bad performance in the viewer
  ; this is true even after other performance improvements
  '(id meta rmeta status prov submission))

;; temporary orthauth extract

(define (config-paths [os #f])
  (case (or os (system-type))
    ;; ucp udp uchp ulp
    ((unix) '("~/.config"
              "~/.local/share"
              "~/.cache"
              "~/.cache/log"))
    ((macosx) '("~/Library/Application Support"
                "~/Library/Application Support"
                "~/Library/Caches"
                "~/Library/Logs"))
    ((windows) (let ((ucp (build-path (find-system-path 'home-dir) "AppData" "Local")))
                 (list ucp ucp ucp (build-path ucp "Logs"))))
    (else (error (format "Unknown OS ~a" (or os (system-type)))))))

(define *config-paths* (config-paths))

(define (fcp position [suffix #f])
  (let ([base-path (position *config-paths*)])
    (string->path
     (if suffix
         (format "~a/~a" base-path suffix)
         base-path))))

(define (user-config-path [suffix #f]) (fcp first  suffix))
(define (user-data-path   [suffix #f]) (fcp second suffix))
(define (user-cache-path  [suffix #f]) (fcp third  suffix))
(define (user-log-path    [suffix #f]) (fcp fourth suffix))

;; python argvs

(define (python-mod-args module-name . args)
  (cons (python-interpreter) (cons "-m" (cons module-name args))))

(define argv-simple-for-racket (python-mod-args "sparcur.simple.utils" "for-racket"))
(define argv-spc-export (python-mod-args "sparcur.cli" "export"))
(define (argv-simple-retrieve ds) (python-mod-args "sparcur.simple.retrieve" "--dataset-id" (dataset-id ds)))
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

(define (resolve-relative-path path)
  ; FIXME path->complete-path I think
  ; FIXME for some reason resolve-path returns only the relative
  ; portion of the path !? not helpful ...
  ; FIXME this can fail with a weird error message
  ; if the symlink does not exist
  (simple-form-path (build-path path ".." ".." (resolve-path path))))

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

(define (init-paths!)
  "initialize or reset the file system paths to cache, export, and source directories"
  ;; FIXME 'cache-dir is NOT what we want for this as it is ~/.racket/
  (path-source-dir (build-path (find-system-path 'home-dir) "files" "sparc-datasets"))
  (path-cache-dir (expand-user-path (user-cache-path "sparcur/racket")))
  (path-cache-datasets (build-path (path-cache-dir) "datasets-list.rktd"))
  (path-export-dir (expand-user-path (user-data-path "sparcur/export")))
  (path-export-datasets (build-path (path-export-dir) "datasets")))

;; json view

(define-syntax when-not (make-rename-transformer #'unless))

(define (set-jview! jview json)
  "set the default state for jview"
  (define jhl (get-field json-hierlist jview))
  (define old-root (get-field root jhl))
  (when old-root
    (send jhl delete-item old-root)) ; this is safe because we force selection at startup
  (send jview set-json! json) ; FIXME this is where the slowdown is
  ; even with reduced data size XXX consider having two jviews, one
  ; visible and one in the background, and render the invisible one in
  ; the background and then swap when visible, it won't always be
  ; fast, but at least it will be faster
  (define root (get-field root jhl))
  (send jhl sort json-list-item< #t) ; XXX this is also slow
  (send root open)
  (define (by-name name items)
    (let ([v (for/list ([i items]
                        #:when (let ([ud (send i user-data)])
                                 (and (eq? (node-data-type ud) 'hash)
                                      (eq? (node-data-name ud) name))))
               i)])
      (and (not (null? v)) (car v))))
  (let ([ritems (send root get-items)])
    (when (> (length ritems) 1) ; id alone is length 1
      (map (λ (x)
             (let ([ud (send x user-data)])
               (when (and (eq? (node-data-type ud) 'hash)
                          (memq (node-data-name ud) '(meta submission status)))
                 (when-not (null? (send x get-items)) ; compound item
                           (send x open))
                 (when (eq? (node-data-name ud) 'status)
                   (let ([per (by-name 'path_error_report (send x get-items))])
                     (when per
                       (send per open)))))))
           ritems))))

(define jviews (make-hash))

(define (get-jview! dataset)
  (let* ([uuid (id-uuid dataset)]
         [hr (hash-ref jviews uuid #f)]
         [jview (if hr
                    hr
                    (let ([jview (new json-view%
                                      ;;[font (make-object font% 10 'modern)]
                                      [parent frame-helper]
                                      #; ; hrm
                                      [parent panel-right])])
                      (let* ([lp (dataset-export-latest-path dataset)]
                             [json (if lp (path->json lp) (hash 'id (dataset-id dataset)))]
                             [jhash (for/hash ([(k v) (in-hash json)]
                                               ; FIXME I think we don't need include keys anymore
                                               #:when (member k include-keys))
                                      (values k v))])
                        (set-jview! jview jhash)
                        (hash-set! jviews uuid jview)
                        jview)))])
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
  (dataset-src-path ds)
  (dataset-export-latest-path ds)
  (fetch-export-dataset ds)
  (fetch-dataset ds)
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
           (parameterize ([current-directory cwd-1])
             (with-output-to-string (thunk (set! status-1 (apply system* argv-1 #:set-pwd? #t)))))
           (parameterize ([current-directory (resolve-relative-path cwd-2)])
             (with-output-to-string (thunk (set! status-2 (apply system* argv-2 #:set-pwd? #t)))))
           (println (format "dataset fetch completed for ~a" (dataset-id ds))))))))
   (define (export-dataset ds)
     (println (format "dataset export starting for ~a" (dataset-id ds))) ; TODO gui and/or log
     (let ([cwd-2 (build-path (dataset-src-path ds)
                              ; FIXME dataset here is hardcoded
                              "dataset")]
           [argv-3 argv-spc-export])
       (thread
        (thunk
         (let ([status-3 #f])
           (parameterize ([current-directory (resolve-relative-path cwd-2)])
             (with-output-to-string (thunk (set! status-3 (apply system* argv-3 #:set-pwd? #t)))))
           (println (format "dataset export completed for ~a" (dataset-id ds))))))))
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
           ; FIXME on the one hand this is amazing because I am getting
           ; logging output into the racket comint window in emacs on
           ; the other hand it is not amazing because system* is
           ; blocking, so probably switch to user subprocess?
           (parameterize ([current-directory cwd-1])
             #;
             (println (string-join argv-1 " "))
             (with-output-to-string (thunk (set! status-1 (apply system* argv-1 #:set-pwd? #t)))))
           (parameterize ([current-directory (resolve-relative-path cwd-2)])
             (with-output-to-string (thunk (set! status-2 (apply system* argv-2 #:set-pwd? #t))))
             (with-output-to-string (thunk (set! status-3 (apply system* argv-3 #:set-pwd? #t)))))
           #;
           (values status-1 status-2 status-3)
           #; ; this doesn't work because list items cannot be customized independently SIGH
           ; I can see why people use the web for stuff like this, if you want to be able to
           ; customize some particular entry why fight with the canned private opaque things
           ; that don't actually meet your use cases?
           (set-lview-item-color lview ds) ; FIXME lview free variable
           ; TODO gui indication
           (println (format "dataset fetch and export completed for ~a" (dataset-id ds))))))))
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
       uuid))])

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
  ; FIXME one vs many export
  ; TODO highlight changed since last time
  (fetch-export-dataset (current-dataset)))

(define (json-list-item< a b)
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
  (for ([index (send obj get-selections)])
    (let ([dataset (send obj get-data index)]
          [cd (current-dataset)])
      ; https://docs.racket-lang.org/guide/define-struct.html#(part._struct-equal)
      (when (not (equal? dataset cd))
        (current-dataset dataset) ; FIXME multiple selection case
        (get-jview! dataset)))))

(define (result->dataset-list result)
  (map (λ (ti)
         #;
         (pretty-print ti)
         (apply dataset (append ti (list "???"))))
       result))

(define (get-dataset-list)
  (let* ([argv argv-simple-for-racket]
         [status #f]
         [result-string (with-output-to-string (λ () (set! status (apply system* argv))))]
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

(define (xopen-path path)
  (let ([command (find-executable-path
                  (case (system-type 'os*)
                    ((linux) "xdg-open")
                    ((macosx) "open")
                    ;((windows) "start") ; XXX only works from powerhsell it seems
                    (else (error "don't know xopen command for this os"))))])
    (subprocess #f #f #f command path)))

(define (cb-open-dataset-folder obj event)
  (let* ([ds (current-dataset)]
         [path (dataset-src-path ds)]
         [symlink (build-path path "dataset")])
    #;
    (println (list "should be opening something here ...." ds resolved))

    (if (directory-exists? symlink)
        (let ([path (resolve-relative-path symlink)])
          (case (system-type)
            ((windows)
             ; FIXME this crashes/closes the gui (on windows obviously) !??!?
             (subprocess #f #f #f (find-executable-path "explorer.exe") path))
            (else (xopen-path path))))
        ; TODO gui visible logging
        (println "Dataset has not been fetched yet!"))))

(define (match-datasets text datasets)
  (if text
      (if (string-contains? text "dataset:")
          (let* ([m (last (string-split text "dataset:"))]
                 [uuid (if (string-contains? m "/")
                           (car (string-split m "/"))
                           m)]
                 [matches (for/list ([d datasets]
                                     #:when (string=? (id-uuid d) uuid))
                            d)])
            (if (null? matches)
                datasets
                matches))
          (let ([matches (for/list ([d datasets]
                                    #:when (or (string-contains? (id-uuid d) text)
                                               (string-contains? (string-downcase (dataset-title d)) text)))
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
      (set-datasets-view! lview matching))))

;; keymap

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
  (add-function "test-copy" (λ (a b) (displayln (format "copy thing ~a ~a" a b))))
  (add-function "backward-kill-word" backward-kill-word)
  (add-function "focus-search-box" (λ (a b) (send text-search-box focus)))
  )

; map functions
(send* keymap
  (map-function "m:backspace" "backward-kill-word")
  #;
  (map-function "c:r"     "refresh-datasets")
  (map-function "c:c"     "test-copy")
  (map-function "c:t"     "test")
  (map-function "f5"      "fetch-export-dataset")
  (map-function "c:t"     "fetch-export-dataset") ; XXX bad overlap with find
  (map-function "c:f"     "focus-search-box") ; XXX bad overlap with find
  #;
  (map-function "c:c;c:e" "fetch-export-dataset") ; XXX cua intersection
  (map-function "c:x"     "export-dataset")
  #; ; defined as the key for the menu item so avoid double calls
  (map-function "c:q"     "quit")
  (map-function "c:w"     "quit"))

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
(define menu-edit (new menu% [label "Edit"] [parent menu-bar-main]))
(define menu-item-preferences (new menu-item%
                                   [label "Preferences..."]
                                   [shortcut #\;]
                                   [shortcut-prefix '(ctl)]
                                   [callback (λ (a b) (send frame-preferences show #t))]
                                   [parent menu-edit]))

(define panel-holder (new horizontal-panel%
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

; FXIME probably goes in the bottom row of a vertical panel
(define button-toggle-expand (new button%
                                  [label "Expand All"]
                                  [callback cb-toggle-expand]
                                  [parent panel-ds-actions]))

(define button-open-dataset-folder (new button%
                                        [label "Open Folder"]
                                        [callback cb-open-dataset-folder]
                                        [parent panel-ds-actions]))

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
(define button-prefs-path
  (new button%
       [label "Open Path"]
       ;[callback cb-toggle-expand]
       [parent panel-prefs-paths]))


(module+ main
  (init-paths!)
  (define result (populate-datasets))
  (send frame-main show #t) ; show this first so that users know something is happening
  (send lview set-selection 0) ; first time to ensure current-dataset always has a value
  (send text-search-box focus)
  ; do this last so that if there is a 0th dataset the time to render the hierlist isn't obtrusive
  (cb-dataset-selection lview #f))
