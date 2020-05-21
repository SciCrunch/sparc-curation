#!/usr/bin/env python3.7
from sparcur.config import auth
__doc__ = f"""
SPARC curation cli for fetching, validating datasets, and reporting.
Usage:
    spc clone    [options] <project-id>
    spc pull     [options] [<directory>...]
    spc refresh  [options] [<path>...]
    spc fetch    [options] [<path>...]
    spc find     [options] --name=<PAT>...
    spc status   [options]
    spc meta     [options] [<path>...]
    spc rmeta    [options]
    spc export   [schemas] [options] [<path>...]
    spc report   size    [options] [<path>...]
    spc report   tofetch [options] [<directory>...]
    spc report   terms   [anatomy cells subcelluar] [options]
    spc report   [access filetypes pathids test]    [options]
    spc report   [completeness keywords subjects]   [options]
    spc report   [contributors samples errors mbf]  [options]
    spc report   [(anno-tags <tag>...) changes mis] [options]
    spc shell    [affil integration protocols exit] [options]
    spc server   [options]
    spc apinat   [options] <path-in> <path-out>
    spc tables   [options] [<directory>...]
    spc annos    [options] [export shell]
    spc feedback <feedback-file> <feedback>...
    spc missing  [options]
    spc xattrs   [options]
    spc demos    [options]
    spc goto     <remote-id>
    spc fix      [options] [duplicates mismatch] [<path>...]
    spc stash    [options --restore] <path>...
    spc make-url [options] [<id-or-path>...]
    spc show     [schemas rmeta (export [json ttl])] [options] [<project-id>]
    spc sheets   [update] [options] <sheet-name>

Commands:
    clone       clone a remote project (creates a new folder in the current directory)

    pull        retrieve remote file structure

                options: --empty

    refresh     retrieve remote file sizes and fild ids (can also fetch using the new data)

                options: --fetch
                       : --level
                       : --only-no-file-id

    fetch       fetch remote data based on local metadata (NOTE does NOT refresh first)

                options: --level
                       : --mbf      fetch mbf xml metadata and only for specific datasets

    find        list unfetched files with option to fetch

                options: --name=<PAT>...  glob options should be quoted to avoid expansion
                       : --existing       include existing files in search
                       : --refresh        refresh matching files
                       : --fetch          fetch matching files
                       : --level

    status      list existing files where local meta does not match cached

    meta        display the metadata the current folder or specified paths

                options: --diff     diff the local and cached metadata
                       : --uri      render the uri for the remote
                       : --browser  navigate to the human uri for this file
                       : --human
                       : --context  include context, e.g. dataset

    rmeta       retrieve metadata about files/folders from the remote

    export      export extracted data to json (and everything else)

                schemas         export schemas from python to json

                options: --latest   run derived pipelines from latest json
                       : --partial  run derived pipelines from the latest partial json export
                       : --open     open the output file using xopen
                       : --mbf      extract and export mbf embedded metadata

    report      print a report on all datasets

                size            dataset sizes and file counts
                completeness    submission and curation completeness
                filetypes       filetypes used across datasets
                pathids         mapping from local path to cached id
                keywords        keywords used per dataset
                terms           all ontology terms used in the export

                                anatomy
                                cells
                                subcelluar

                subjects        all headings from subjects files
                errors          list of all errors per dataset
                test            do as little as possible (use with --profile)
                mbf             mbf term report (can use with --unique)
                anno-tags       list anno exact for a curation tag
                mis             list summary predicates used per dataset

                options: --raw  run reports on live data without export
                       : --tab-table
                       : --sort-count-desc
                       : --unique
                       : --uri
                       : --uri-api
                       : --debug

    shell       drop into an ipython shell

                integration     integration subshell with different defaults

    server      reporting server

                options: --raw  run server on live data without export

    apinat      convert ApiNATOMY json to rdf and serialize to ttl

    missing     find and fix missing metadata
    xattrs      populate metastore / backup xattrs
    demos       long running example queries
    goto        given an id cd to the containing directory
                invoke as `pushd $(spc goto <id>)`
    dedupe      find and resolve cases with multiple ids
    fix         broke something? put the code to fix it here

                mismatch
                duplicates
    stash       stash a copy of the specific files and their parents
    make-url    return urls for blackfynn dataset ids, or paths

Options:
    -f --fetch              fetch matching files
    -R --refresh            refresh matching files
    -r --rate=HZ            sometimes we can go too fast when fetching [default: 5]
    -l --limit=SIZE_MB      the maximum size to download in megabytes [default: 2]
                            use negative numbers to indicate no limit
    -L --level=LEVEL        how deep to go in a refresh
                            used by any command that acceps <path>...
    -p --pretend            if the defult is to act, dont, opposite of fetch

    -h --human              print human readable values
    -b --browser            open the uri in default browser
    -u --uri                print the human uri for the path in question
    -a --uri-api            print the api uri for the path in question
    -c --context            include context for a file e.g. dataset
    -n --name=<PAT>         filename pattern to match (like find -name)
    -e --empty              only pull empty directories
    -x --exists             when searching include files that have already been pulled
    -m --only-meta          only pull known dataset metadata files
    -z --only-no-file-id    only pull files missing file_id
    -o --overwrite          fetch even if the file exists
    --project-path=<PTH>    set the project path manually

    -t --tab-table          print simple table using tabs for copying
    -A --latest             run derived pipelines from latest json
    -P --partial            run derived pipelines from the latest partial json export
    -W --raw                run reporting on live data without export

    -S --sort-size-desc     sort by file size, largest first
    -C --sort-count-desc    sort by count, largest first

    -O --open               open the output file
    -U --upload             update remote target (e.g. a google sheet) if one exists
    -N --no-google          hack for ipv6 issues
    -D --diff               diff local vs cache

    --port=PORT             server port [default: 7250]

    -j --jobs=N             number of jobs to run             [default: 12]
    -d --debug              drop into a shell after running a step
    -v --verbose            print extra information
    --profile               profile startup performance
    --local                 ignore network issues
    --mbf                   fetch/export mbf related metadata
    --unique                return a unique set of values without additional info

    --log-path=PATH         folder where logs are saved       [default: {auth.get_path('log-path')}]
    --cache-path=PATH       folder where remote data is saved [default: {auth.get_path('cache-path')}]
    --export-path=PATH      base folder for exports           [default: {auth.get_path('export-path')}]
"""

from time import time
start = time()

import os
import re
import sys
import json
import errno
import types
import pprint
from itertools import chain
from collections import Counter, defaultdict
import idlib
import htmlfn as hfn
import ontquery as oq
start_middle = time()
import augpathlib as aug
from hyputils import hypothesis as hyp
from augpathlib import FileSize
from augpathlib import RemotePath, AugmentedPath  # for debug
from pyontutils import clifun as clif
from pyontutils.core import OntResGit, OntGraph, OntResIri
from pyontutils.utils import UTCNOWISO, subclasses, anyMembers
from pyontutils.config import auth as pauth
from terminaltables import AsciiTable

from sparcur import config
from sparcur import datasets as dat
from sparcur import exceptions as exc
from sparcur.core import JT, JPointer, lj, DictTransformer as DT
from sparcur.core import OntId, OntTerm, get_all_errors, adops
from sparcur.utils import GetTimeNow
from sparcur.utils import log, logd, SimpleFileHandler, bind_file_handler
from sparcur.utils import python_identifier, want_prefixes, symlink_latest
from sparcur.paths import Path, BlackfynnCache, PathMeta, StashPath
from sparcur.state import State
from sparcur.derives import Derives as De
from sparcur.backends import BlackfynnRemote
from sparcur.curation import Summary, Integrator
from sparcur.curation import DatasetObject
from sparcur.protocols import ProtocolData
from sparcur.blackfynn_api import FakeBFLocal

try:
    breakpoint
except NameError:
    from IPython import embed as breakpoint


slow = False
if slow:
    # this sucker takes .4 seconds to start up and isn't used
    from pysercomb.pyr import units as pyru

stop = time()


class Options(clif.Options):

    @property
    def export_schemas_path(self):
        return Path(self.export_path) / 'schemas'  # FIXME not sure if correct

    @property
    def jobs(self):
        return int(self._args['--jobs'])

    @property
    def limit(self):
        l = int(self._args['--limit'])
        if l >= 0:
            return l

    @property
    def level(self):
        return int(self._args['--level']) if self._args['--level'] else None

    @property
    def rate(self):
        return int(self._args['--rate']) if self._args['--rate'] else None

    @property
    def mbf(self):
        # deal with the fact that both mbf and --mbf are present
        return self._args['--mbf'] or self._default_mbf


class Dispatcher(clif.Dispatcher):
    spcignore = ('.git',
                 '.~lock',)

    def _export(self, export_class, export_source_path=None, org_id=None):

        if export_source_path is None:
            export_source_path = self.cwd

        export_source_path = self.cwd
        export = export_class(self.options.export_path,
                              export_source_path,
                              self._folder_timestamp,
                              self._timestamp,
                              self.options.latest,
                              self.options.partial,
                              self.options.open,
                              org_id)
        return export

    def _print_table(self, rows, title=None, align=None, ext=None):
        """ ext is only used when self.options.server -> True """
        def simple_tsv(rows):
            return '\n'.join('\t'.join((str(c) for c in r)) for r in rows) + '\n'

        if self.options.tab_table:
            if title:
                print(title)

            print(simple_tsv(rows))

        elif self.options.server:
            if ext is not None:
                if ext == '.tsv':
                    nowish = UTCNOWISO('seconds')
                    fn = json.dumps(f'{title} {nowish}')
                    return (
                        simple_tsv(rows), 200,
                        {'Content-Type': 'text/tsv; charset=utf-8',
                         'Content-Disposition': f'attachment; filename={fn}'})
                if isinstance(ext, types.FunctionType):
                    return ext(hfn.render_table(rows[1:], *rows[0]), title=title)
                else:
                    return 'Not found', 404

            return hfn.render_table(rows[1:], *rows[0]), title

        else:
            table = AsciiTable(rows, title=title)
            if align:
                assert len(align) == len(rows[0])
                table.justify_columns = {i:('left' if v == 'l'
                                            else ('center' if v == 'c'
                                                  else ('right'
                                                        if v == 'r' else
                                                        'left')))
                                         for i, v in enumerate(align)}
            print(table.table)

    def _print_paths(self, paths, title=None):
        if self.options.sort_size_desc:
            key = lambda ps: -ps[-1]
        else:
            key = lambda ps: ps

        def derp(p):
            if p.cache is None:
                raise exc.NoCachedMetadataError(p)

        rows = [['Path', 'size', '?'],
                *((p, s.hr
                   if isinstance(s, FileSize) else
                   s, 'x' if p.exists() else '')
                  for p, s in
                  sorted(([p, ('/' if p.is_dir() else
                               (p.cache.meta.size
                                if p.cache.meta.size else
                                '??')
                               if p.cache.meta else '_')]
                          for p in paths if not derp(p)), key=key))]
        self._print_table(rows, title)


class Main(Dispatcher):
    child_port_attrs = ('anchor',
                        'project_path',
                        'project_id',
                        'bfl',
                        'BlackfynnRemote',
                        'summary',
                        'cwd',
                        'cwdintr',
                        '_datasets_with_extension',
                        '_timestamp',
                        '_folder_timestamp',)

    # any attr forced on children must be set before super().__init__ is called
    # set timestamp early so that the loggers can use it
    # I don't think this will cause too much trouble ...
    #_timestamp = None
    #_folder_timestamp = None

    # things all children should have
    # kind of like a non optional provides you WILL have these in your namespace
    def __init__(self, options, time_now=GetTimeNow()):
        self._time_now = time_now
        self._timestamp = self._time_now.START_TIMESTAMP
        self._folder_timestamp = self._time_now.START_TIMESTAMP_LOCAL
        super().__init__(options)
        if not self.options.verbose:
            log.setLevel('INFO')
            logd.setLevel('INFO')

        if self.options.project_path:
            self.cwd = Path(self.options.project_path).resolve()
        else:
            self.cwd = Path.cwd()

        Integrator.rate = self.options.rate
        Integrator.no_google = self.options.no_google

        self.cwdintr = Integrator(self.cwd)

        # pass debug along (sigh)
        AugmentedPath._debug = self.options.debug
        RemotePath._debug = self.options.debug

        # FIXME populate this via decorator
        if (self.options.clone or
            self.options.meta or
            self.options.show or
            self.options.sheets or
            self.options.goto or
            self.options.apinat or
            self.options.tofetch or  # size does need a remote but could do it lazily
            self.options.filetypes or
            self.options.anno_tags or
            self.options.status or  # eventually this should be able to query whether there is new data since the last check
            self.options.pretend or
            (self.options.export and self.options.schemas) or
            (self.options.find and not (self.options.fetch or self.options.refresh))):
            # short circuit since we don't know where we are yet
            Integrator.no_google = True
            return

        elif (self.options.pull or
              self.options.mismatch or
              self.options.stash or
              self.options.contributors or
              self.options.make_url or
              self.options.missing):
            Integrator.no_google = True

        self._setup_local()  # if this isn't run up here the internal state of the program get's wonky

        if (self.options.report and
            not self.options.raw and
            not self.options.access):
            Integrator.setup(local_only=True)  # FIXME sigh
        else:
            try:
                self._setup_bfl()
            except BaseException as e:
                if self.options.local:
                    log.exception(e)
                    self.BlackfynnRemote._api = FakeBFLocal(self.anchor.id, self.anchor)
                    self.BlackfynnRemote.anchorTo(self.anchor)
                else:
                    raise e

        if self.options.export or self.options.shell:
            self._setup_export()
            self._setup_ontquery()

    def _setup_local(self):
        self.BlackfynnRemote = BlackfynnCache._remote_class
        self.BlackfynnRemote._async_rate = self.options.rate

        local = self.cwd

        # we have to start from the cache class so that
        # we can configure
        try:
            _cpath = local.cache  # FIXME project vs subfolder
            if _cpath is None:
                raise exc.NoCachedMetadataError  # FIXME somehow we decided not to raise this!??!

            self.anchor = _cpath.anchor
        except exc.NoCachedMetadataError as e:
            root = local.find_cache_root()
            if root is not None:
                self.anchor = root.cache
                if local.skip_cache:
                    print(f'{local} is ignored!')
                    sys.exit(112)

                raise NotImplementedError('TODO recover meta?')
            else:
                print(f'{local} is not in a project!')
                sys.exit(111)

        # replace a bottom up anchoring rule with top down
        self.anchor.anchorClassHere(remote_init=False)

        self.project_path = self.anchor.local
        self.summary = Summary(self.project_path)
        Summary._n_jobs = self.options.jobs
        if self.options.debug:
            Summary._debug = True

    def _setup_bfl(self):
        self.BlackfynnRemote.init(self.anchor.id)

        self.bfl = self.BlackfynnRemote._api
        State.bind_blackfynn(self.bfl)

    def _setup_export(self):
        Integrator.setup()

    def _setup_ontquery(self):
        # FIXME this should be in its own setup method
        # pull in additional graphs for query that aren't loaded properly
        RDFL = oq.plugin.get('rdflib')
        olr = Path(pauth.get_path('ontology-local-repo'))
        branch = 'methods'
        for fn in ('methods', 'methods-helper', 'methods-core'):
            org = OntResGit(olr / f'ttl/{fn}.ttl', ref=branch)
            OntTerm.query.ladd(RDFL(org.graph, OntId))

    @property
    def project_name(self):
        return self.anchor.name
        #return self.bfl.organization.name

    @property
    def project_id(self):
        #self.bfl.organization.id
        return self.anchor.id

    @property
    def datasets(self):
        yield from self.anchor.children  # ok to yield from cache now that it is the bridge

    @property
    def datasets_remote(self):
        for d in self.anchor.remote.children:
            # FIXME lo the crossover (good for testing assumptions ...)
            #yield d.local
            yield d

    @property
    def datasets_local(self):
        for d in self.anchor.local.children: #self.datasets:
            if d.exists():
                yield d

    ###
    ## vars
    ###

    @property
    def directories(self):
        return [Path(string_dir).absolute()
                for string_dir in self.options.directory]

    @property
    def paths(self):
        return [Path(string_path).absolute()
                for string_path in self.options.path]

    @property
    def _paths(self):
        """ all relevant paths determined by the flags that have been set """
        # but if you use the generator version of _paths
        # then if you add a folder to the previous path
        # then it will yeild that folder! which is SUPER COOL
        # but breaks lots of asusmptions elsehwere
        paths = self.paths
        if not paths:
            paths = self.cwd,  # don't call Path.cwd() because it may have been set from --project-path

        if self.options.only_meta:
            paths = (mp.absolute()
                     for p in paths
                     for mp in dat.DatasetStructureLax(p).meta_paths)
            yield from paths
            return

        yield from self._build_paths(paths)

    def _build_paths(self, paths):
        def inner(paths, level=0, stop=self.options.level):
            """ depth first traversal of children """
            for path in paths:
                if self.options.only_no_file_id:
                    if (path.is_broken_symlink() and
                        (path.cache.meta.file_id is None)):
                        yield path
                        continue

                elif self.options.empty:
                    if path.is_dir():
                        try:
                            next(path.children)
                            # if a path has children we still want to
                            # for empties in them to the level specified
                        except StopIteration:
                            yield path
                    else:
                        continue
                else:
                    yield path

                if stop is None:
                    if self.options.only_no_file_id:
                        for rc in path.rchildren:
                            if (rc.is_broken_symlink() and
                                rc.cache.meta.file_id is None):
                                yield rc
                    else:
                        yield from path.rchildren

                elif level <= stop:
                    yield from inner(path.children, level + 1)

        yield from inner(paths)

    @property
    def _dirs(self):
        for p in self._paths:
            if p.is_dir():
                yield p

    @property
    def _not_dirs(self):
        for p in self._paths:
            if not p.is_dir():
                yield p

    def clone(self):
        project_id = self.options.project_id
        if project_id is None:
            print('no remote project id listed')
            sys.exit(4)
        # given that we are cloning it makes sense to _not_ catch a connection error here
        self.BlackfynnRemote = BlackfynnRemote._new(Path, BlackfynnCache)
        try:
            self.BlackfynnRemote.init(project_id)
        except exc.MissingSecretError:
            print(f'missing api secret entry for {project_id}')
            sys.exit(11)

        # make sure that we aren't in a project already
        existing_root = self.cwd.find_cache_root()
        if existing_root is not None and existing_root != self.cwd:
            message = f'fatal: already in project located at {existing_root.resolve()!r}'
            print(message)
            sys.exit(3)

        try:
            if self.options.project_path:
                anchor = self.BlackfynnRemote.anchorTo(self.cwd, create=True)
            else:
                anchor = self.BlackfynnRemote.dropAnchor(self.cwd)
        except exc.DirectoryNotEmptyError:
            message = (f'fatal: destination path already '
                       'exists and is not an empty directory.')
            print(message)
            sys.exit(2)
        except BaseException as e:
            log.exception(e)
            sys.exit(11111)

        anchor.local_data_dir_init()

        self.anchor = anchor
        self.project_path = self.anchor.local
        with anchor:
            self.cwd = Path.cwd()  # have to update self.cwd so pull sees the right thing
            self.pull()

    def pull(self):
        # TODO folder meta -> org
        from pyontutils.utils import Async, deferred
        only = tuple()
        recursive = self.options.level is None  # FIXME we offer levels zero and infinite!
        dirs = self.directories
        cwd = self.cwd
        skip = auth.get_list('datasets-no')
        if self.project_path.parent.name == 'big':
            only = auth.get_list('datasets-sparse')
        else:
            sparse = auth.get_list('datasets-sparse')

        if not dirs:
            dirs = cwd,

        dirs = sorted(dirs, key=lambda d: d.name)
        if self.project_path in dirs:
            check_dirs = [c for d in dirs for c in
                          (d.children if d == self.project_path else (d,))
                          if c.is_dir() and True or  # XXX not skipping sparse
                          (c.cache.id not in sparse
                           or only and c.cache.id in only)]
        else:
            check_dirs = dirs

        existing_locals = set(rc for d in check_dirs for rc in d.rchildren)  # this is fairly quick

        if self.options.profile:
            from desc.prof import profile_me
        else:
            profile_me = lambda f: f

        @profile_me
        def fast():
            # 2 seconds on sparse, 8.3 seconds for 223k ids
            # faster than find getfattr by a long shot
            existing_d = {e.cache_id:e for e in existing_locals}
            return existing_d

        @profile_me
        def slow():
            # some numbers when excluding the sparse datasets
            # 33 seconds !!! WAT WAT WAT WAT
            # now down to 22 by not parsing dates unless we need them
            # but still a 10x overhead compared to the fast version above
            # down to ~9 seconds just checking c.cach is not None
            # so roughly 4x overhead just to instantiate the caches ... ugh

            # FIXME TODO wow calling cache.meta is expensive
            # is is the overhead for parsing the paths again
            # probably worth creating a hack to init from local parts directly
            existing_d = {c:c for c in existing_locals
                          if c.cache is not None}  # yay null cache
            return existing_d

        existing_d1 = fast()
        #existing_d2 = slow()
        existing_d = existing_d1
        existing_ids = set(existing_d)

        log.debug(dirs)
        for d in dirs:
            if self.options.empty:
                if list(d.children):
                    continue

            if not d.is_dir():
                raise TypeError(f'dir is not a dir?!? {d}')

            if not (d.remote.is_dataset() or d.remote.is_organization()):
                log.warning('You are pulling recursively from below dataset level.')

            #r = d.remote
            # FIXME for some reason this does not seem to be working as expected
            # because new datasets are being added when there is an existing dataset
            #r.refresh(update_cache=True)  # if the parent folder has moved make sure to move it first
            c = d.cache
            newc = c.refresh()  # this does the move for us now
            if newc is None:
                continue  # directory was deleted

            def hrm(child):
                cd = child
                if cd.is_dir():
                    oc = cd.cache  # FIXME getting the cache slow
                    if oc is None and cd.skip_cache:
                        return

                    nc = oc.refresh()  # FIXME can't we just build an index off datasets here??
                    if nc != oc:
                        log.info(f'Dataset moved!\n{oc} -> {nc}')
                        # FIXME FIXME FIXME
                        rnl = self.anchor.local_data_dir / 'renames.log'
                        with open(rnl, 'at') as f:
                            f.write(f'{oc} -> {nc} -> {nc.id}\n')

            if d.cache.is_organization():  # FIXME FIXME FIXME hack to mask broken bootstrap handling of existing dirs :/
                if self.options.debug or self.options.jobs == 1:
                    _blank = [hrm(cd) for cd in d.children]
                else:
                    _blank = Async()(deferred(hrm)(cd) for cd in d.children)

            # FIXME something after this point is retaining stale filepaths on dataset rename ...
            #d = r.local  # in case a folder moved
            caches = newc.remote.bootstrap(recursive=recursive,
                                           only=only,
                                           skip=skip,
                                           sparse=sparse,)

        new_locals = set(c.local for c in caches if c is not None)  # FIXME 
        new_ids = {c.id:c for c in caches if c is not None}
        maybe_removed_ids = set(existing_ids) - set(new_ids)
        maybe_new_ids = set(new_ids) - set(existing_ids)
        if maybe_removed_ids:
            # FIXME pull sometimes has fake file extensions
            from pyontutils.utils import Async, deferred
            from pathlib import PurePath
            maybe_removed = [existing_d[id] for id in maybe_removed_ids]
            maybe_removed_stems = {PurePath(p.parent) / p.stem:p
                                   for p in maybe_removed}  # FIXME still a risk of collisions?
            maybe_new = [new_ids[id] for id in maybe_new_ids]
            maybe_new_stems = {PurePath(p.parent) / p.stem:p for p in maybe_new}
            for pstem, p in maybe_new_stems.items():
                if pstem in maybe_removed_stems:
                    mr_path = maybe_removed_stems[pstem]
                    #assert p != mr_path, f'wat\n{mr_path}\n{p}'
                    if p != mr_path:
                        new_new_path = p.refresh()
                    else:
                        new_new_path = p
                        # TODO check if file_id needs to be updated in some cases ...
                        # csv files match
                        log.info(f'wat\n{mr_path}\n{p}')

                    if new_new_path == mr_path:
                        maybe_removed.remove(mr_path)

            Async(rate=self.options.rate)(deferred(l.cache.refresh)()
                                          for l in maybe_removed
                                          # FIXME deal with untracked files
                                          if l.cache)

    def refresh(self):
        paths = self.paths
        cwd = self.cwd
        if not paths:
            paths = cwd,

        to_root = sorted(set(parent
                             for path in paths
                             for parent in path.parents
                             if parent.cache is not None),
                         key=lambda p: len(p.parts))

        if self.options.pretend:
            ap = list(chain(to_root, self._paths))
            self._print_paths(ap)
            print(f'total = {len(ap):<10}rate = {self.options.rate}')
            return

        self._print_paths(chain(to_root, self._paths))

        from pyontutils.utils import Async, deferred
        hz = self.options.rate
        fetch = self.options.fetch
        limit = self.options.limit

        drs = [d.remote for d in chain(to_root, self._dirs)]

        if not self.options.debug:
            refreshed = Async(rate=hz)(deferred(r.refresh)(
                update_data_on_cache=r.cache.is_file() and
                r.cache.exists())
                                       for r in drs)
        else:
            refreshed = [r.refresh(update_data_on_cache=r.cache.is_file() and
                                   r.cache.exists()) for r in drs]

        moved = []
        parent_moved = []
        for new, r in zip(refreshed, drs):
            if new is None:
                log.critical('utoh')

            oldl = r.local
            try:
                r.update_cache()  # calling this directly is ok for directories
            except FileNotFoundError as e:
                parent_moved.append(oldl)
                continue
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    log.error(f'{e}')
                    continue
                else:
                    raise e

            newl = r.local
            if oldl != newl:
                moved.append([oldl, newl])

        if moved:
            self._print_table(moved, title='Folders moved')
            for old, new in moved:
                if old == cwd:
                    log.info(f'Changing directory to {new}')
                    new.chdir()

        if parent_moved:
            self._print_paths(parent_moved, title='Parent moved')

        if not self.options.debug:
            refreshed = Async(rate=hz)(deferred(path.cache.refresh)(
                update_data=fetch, size_limit_mb=limit)
                                       for path in self._not_dirs)

        else:
            for path in self._not_dirs:
                path.cache.refresh(update_data=fetch, size_limit_mb=limit)

    def _datasets_with_extension(self, extension):
        """ Hack around the absurd slowness of python's rglob """

        command = fr"""for d in */; do
    find "$d" \( -type l -o -type f \) -name '*.{extension}' \
    -exec getfattr -n user.bf.id --only-values "$d" \; -printf '\n' -quit ;
done"""
        with os.popen(command) as p:
            string = p.read()

        has_extension = string.split('\n')
        datasets = [d for d in self.datasets if d.id in has_extension]
        return datasets

    def _fetch_mbf(self):
        from pyontutils.utils import Async, deferred
        def fetch_mbf_metadata(dataset):
            xmls = dataset.rglob('*.xml')
            Async(rate=5)(deferred(x.fetch)(size_limit_mb=None)
                          for x in xmls if not x.exists())

        for dataset in self._datasets_with_extension('xml'):
            fetch_mbf_metadata(dataset)

    def fetch(self):
        if self.options.mbf:  # FIXME hack -> fetch '*.xml' or something
            self._fetch_mbf()
            return

        paths = [p for p in self._paths if not p.is_dir()]
        self._print_paths(paths)
        if self.options.pretend:
            return

        from pyontutils.utils import Async, deferred
        hz = self.options.rate
        Async(rate=hz)(deferred(path.cache.fetch)(
            size_limit_mb=self.options.limit)
                       for path in paths
                       if not path.exists()
                       # FIXME need a staging area ...
                       # FIXME also, the fact that we sometimes need content_different
                       # means that there may be silent fetch failures
                       or path.content_different())

    def _check_duplicates(self, datasets):
        # NOTE this is an ok sanity check
        # but cannot catch the duplicate dataset issue
        # which actually comes from calling refresh
        # while iterating through the list of dataset paths
        # which can and does cause datasets move due to
        # changes in their folder name
        if not datasets:
            datasets = list(self.datasets_local)

        counts = Counter([d['id'] if isinstance(d, dict) else d.id
                          for d in datasets])
        bads = [(id, c) for id, c in counts.most_common() if c > 1]
        if bads:
            # FIXME sys.exit ?
            raise BaseException(f'duplicate datasets {bads}')

    def _check_exists(self, dataset_paths):
        bads = [p for p in dataset_paths if not p.exists()]
        if bads:
            raise FileNotFoundError(f'The following do not exist!\n{bads}')

    def export(self):
        from sparcur import export as ex  # FIXME very slow to import
        from sparcur import schemas as sc

        # FIXME export should be able to run without needing any external
        # data the fetch step should happen before the export so that
        # network connections don't creep into the process

        # TODO export source path without having to be in that folder

        if self.options.schemas:
            ex.core.export_schemas(self.options.export_schemas_path)

        elif self.options.mbf:
            export = self._export(ex.ExportXml)
            dataset_paths = self._datasets_with_extension('xml')
            self._check_duplicates(dataset_paths)
            self._check_exists(dataset_paths)
            blob_ir, *rest = export.export(dataset_paths=dataset_paths,
                                           jobs=self.options.jobs,
                                           debug=self.options.debug)

            return blob_ir

        else:
            export = self._export(ex.Export)
            dataset_paths = tuple(self.paths)
            self._check_duplicates(dataset_paths)  # NOTE can be empty
            self._check_exists(dataset_paths)
            noexport = (auth.get_list('datasets-noexport') +
                        auth.get_list('datasets-no'))
            blob_ir, *rest = export.export(dataset_paths=dataset_paths,
                                           exclude=noexport)

            sc.SummarySchema().validate_strict(export.latest_export)

        if self.options.debug:
            breakpoint()

    def annos(self):
        if self.options.raw:
            data = self.summary.data()
        else:
            from sparcur import export as ex
            data = self._export(ex.Export).latest_ir

        dataset_blobs = data['datasets']

        from protcur.analysis import protc, Hybrid
        from sparcur.protocols import ProtcurData
        ProtcurData.populate_annos()

        class ProtocolActual(ProtocolData):  # FIXME so ... bad ...
            @property
            def protocol_uris(self, outer_self=self):  # FIXME this needs to be pipelined
                for d in dataset_blobs:
                    try:
                        yield from adops.get(d, ['meta', 'protocol_url_or_doi'])
                    except exc.NoSourcePathError:
                        pass

        if self.options.export:
            with open('/tmp/sparc-protcur.rkt', 'wt') as f:
                f.write(protc.parsed())

        pa = ProtocolActual()
        all_blackfynn_uris = set(pa.protocol_uris_resolved)
        all_hypothesis_uris = set(a.uri for a in protc)
        if self.options.shell or self.options.debug:
            p, *rest = self._paths
            f = Integrator(p)
            all_annos = [list(protc.byIri(uri))
                         for uri in f.protocol_uris_resolved]
            breakpoint()

    def demos(self):
        # get the first dataset
        dataset = next(iter(summary))

        # another way to get the first dataset
        dataset_alt = next(org.children)

        # view all dataset descriptions call repr(tabular_view_demo)
        tabular_view_demo = [next(d.dataset_description).t
                                for d in ds[:1]
                                if 'dataset_description' in d.data]  # FIXME

        # get package testing
        bigskip = auth.get_list('datasets-sparse') + auth.get_list('datasets-no')
        bfds = self.bfl.bf.datasets()
        packages = [list(d.packages) for d in bfds[:3]
                    if d.id not in bigskip]
        n_packages = [len(ps) for ps in packages]

        # bootstrap a new local mirror
        # FIXME at the moment we can only have of these at a time
        # sigh more factories incoming
        #anchor = BlackfynnCache('/tmp/demo-local-storage')
        #anchor.bootstrap()

        if False:
            ### this is the equivalent of export, quite slow to run
            # export everything
            dowe = summary.data()

            # show all the errors from export everything
            error_id_messages = [(d['id'], e['message'])
                                 for d in dowe['datasets']
                                 for e in d['errors']]
            error_messages = [e['message']
                              for d in dowe['datasets']
                              for e in d['errors']]

        #rchilds = list(datasets[0].rchildren)
        #package, file = [a for a in rchilds if a.id == 'N:package:8303b979-290d-4e31-abe5-26a4d30734b4']

        return self.shell()

    def tables(self):
        """ print summary view of raw metadata tables, possibly per dataset """

        dat.DatasetStructure._refresh_on_missing = False
        dat.SubmissionFile._refresh_on_missing = False
        dat.SubjectsFile._refresh_on_missing = False
        dat.SamplesFile._refresh_on_missing = False
        droot = dat.DatasetStructure(self.cwd)
        if droot.cache.is_dataset():
            datasetdatas = droot,
        elif droot.cache.is_organization():
            datasetdatas = droot.children
        else:
            raise TypeError(f'whats a {type(droot)}? {droot}')

        def do_file(d, ft):
            if hasattr(d, ft):
                f = getattr(d, ft)
                if hasattr(f, 'object'):
                    return (f.object._t())
                else:
                    message = f'object missing on {ft} for {d}'
                    log.warning(message)
                    return message

            else:
                return f'{ft} missing on {d}'

        tables = [t for d in datasetdatas for t in
                  (do_file(d, 'dataset_description'),
                   do_file(d, 'submission'),
                   do_file(d, 'subjects'),
                   do_file(d, 'samples'),
                   ('\n--------------------------------------------------\n'
                      '=================================================='
                    '\n--------------------------------------------------\n'))]

        [print(t if isinstance(t, str) else repr(t)) for t in tables]
        return
        tabular_view_demo = [next(d.dataset_description).t
                                for d in summary
                                if 'dataset_description' in d.data]
        print(repr(tabular_view_demo))

    def find(self):
        paths = []
        if self.options.name:  # has to always be true now
            patterns = self.options.name
            path = self.cwd
            for pattern in patterns:
                # TODO filesize mismatches on non-fake
                # no longer needed due to switching to symlinks
                #if '.fake' not in pattern and not self.options.overwrite:
                    #pattern = pattern + '.fake*'

                for file in path.rglob(pattern):
                    paths.append(file)

        if paths:
            paths = [p for p in paths if not p.is_dir()]
            search_exists = self.options.exists
            if self.options.limit:
                old_paths = paths
                paths = [p for p in paths
                         if p.cache.meta.size is None or  # if we have no known size don't limit it
                         search_exists or
                         not p.exists() and
                         p.cache.meta.size.mb < self.options.limit or
                         p.exists() and p.size != p.cache.meta.size and
                         (not log.info(f'Truncated transfer detected for {p}\n'
                                       f'{p.size} != {p.cache.meta.size}'))
                         and p.cache.meta.size.mb < self.options.limit]

                n_skipped = len(set(p for p in old_paths
                                    if p.is_broken_symlink()) - set(paths))

            if self.options.pretend:
                self._print_paths(paths)
                print(f'skipped = {n_skipped:<10}rate = {self.options.rate}')
                return

            if self.options.verbose:
                for p in paths:
                    print(p.cache.meta.as_pretty(pathobject=p))

            if self.options.fetch or self.options.refresh:
                from pyontutils.utils import Async, deferred
                hz = self.options.rate  # was 30
                limit = self.options.limit
                fetch = self.options.fetch
                if self.options.refresh:
                    Async(rate=hz)(deferred(path.remote.refresh)(
                        update_cache=True, update_data=fetch, size_limit_mb=limit)
                                   for path in paths)
                elif fetch:
                    Async(rate=hz)(deferred(path.cache.fetch)(
                        size_limit_mb=limit)
                                   for path in paths)

            else:
                self._print_paths(paths)
                print(f'skipped = {n_skipped:<10}rate = {self.options.rate}')

    def feedback(self):
        file = self.options.feedback_file
        feedback = ' '.join(self.options.feedback)
        path = Path(file).resolve()
        eff = Integrator(path)
        # TODO pagenote and/or database
        print(eff, feedback)

    def missing(self):
        for path in self._paths:
            for rc in path.rchildren:
                if rc.is_broken_symlink():
                    m = rc.cache.meta
                    if m.file_id is None:
                        #print(rc)
                        print(m.as_pretty(pathobject=rc))
        #self.bfl.find_missing_meta()

    def xattrs(self):
        raise NotImplementedError('This used to populate the metastore, '
                                  'but currently does nothing.')

    def meta(self):
        if self.options.browser:
            import webbrowser

        BlackfynnCache._local_class = Path  # since we skipped _setup
        Path._cache_class = BlackfynnCache
        paths = self.paths
        if not paths:
            paths = self.cwd,

        old_level = log.level
        if not self.options.verbose:
            log.setLevel('ERROR')
        def inner(path):
            if self.options.uri or self.options.browser:
                uri = path.cache.uri_human
                print('+' + '-' * (len(uri) + 2) + '+')
                print(f'| {uri} |')
                if self.options.browser:
                    webbrowser.open(uri)

            try:
                cmeta = path.cache.meta
                if cmeta is not None:
                    if self.options.diff:
                        if cmeta.checksum is None:
                            if not path.cache.local_object_cache_path.exists():
                                # we are going to have to go to network
                                self._setup()

                            cmeta.checksum = path.cache.checksum()  # FIXME :/
                        lmeta = path.meta
                        # id and file_id are fake in this instance
                        setattr(lmeta, 'id', None)
                        setattr(lmeta, 'file_id', None)
                        print(lmeta.as_pretty_diff(cmeta, pathobject=path,
                                                   human=self.options.human))
                    else:
                        print(cmeta.as_pretty(pathobject=path,
                                              human=self.options.human))

                    if self.options.context:
                        pc = path.cache
                        if not pc.is_organization():
                            org = pc.organization.local
                            if not pc.is_dataset():
                                ds = pc.dataset.local
                                inner(ds)
                            elif self.options.verbose:
                                inner(org)

                        print()

            except exc.NoCachedMetadataError:
                print(f'No metadata for {path}. Run `spc refresh {path}`')

        for path in paths:
            inner(path)

        log.setLevel(old_level)

    def goto(self):
        # TODO this needs an inverted index
        if self.options.remote_id.startswith('N:dataset:'):
            gen = self.cwd.children
        else:
            gen = self.cwd.rchildren

        for rc in gen:
            try:
                if rc.cache.id == self.options.remote_id:
                    if rc.is_broken_symlink() or rc.is_file():
                        rc = rc.parent

                    print(rc.relative_to(self.cwd).as_posix())
                    return
            except AttributeError as e:
                if not rc.skip_cache:
                    log.critical(rc)
                    log.error(e)

    def status(self):
        project_path = self.cwd.find_cache_root()
        if project_path is None:
            print(f'{self.cwd} is not in a project!')
            sys.exit(111)

        existing_files = [f for f in project_path.rchildren if f.is_file()]
        different = []
        for f in existing_files:
            try:
                cmeta = f.cache.meta
            except AttributeError:
                if f.skip_cache:
                    continue

            lmeta = f.meta if cmeta.checksum else f.meta_no_checksum
            # id and file_id are fake in this instance
            setattr(lmeta, 'id', None)
            setattr(lmeta, 'file_id', None)
            if lmeta.content_different(cmeta):
                if self.options.status:
                    print(lmeta.as_pretty_diff(cmeta, pathobject=f,
                                               human=self.options.human))
                else:
                    yield f, lmeta, cmeta

    def server(self):
        from sparcur.server import make_app
        if self.options.raw:
            # FIXME ...
            self.dataset_index = {d.meta.id:Integrator(d)
                                  for d in self.datasets}
        else:
            from sparcur import export as ex
            data = self._export(ex.Export).latest_ir
            self.dataset_index = {d['id']:d for d in data['datasets']}

        report = Report(self)
        app, *_ = make_app(report, self.project_path)
        self.app = app  # debug only
        app.debug = False
        app.run(host='localhost', port=self.options.port, threaded=True)

    def stash(self, paths=None, stashmetafunc=lambda v:v):
        if paths is None:
            paths = self.paths

        stash_base = self.anchor.local.parent / 'stash'  # TODO move to operations
        to_stash = sorted(set(parent for p in paths for parent in
                               chain((p,), p.relative_to(self.anchor).parents)),
                           key=lambda p: len(p.as_posix()))

        if self.options.restore:
            # horribly inefficient, maybe build on a default dict?
            rcs = sorted((c for c in stash_base.rchildren if not c.is_dir()),
                         key=lambda p:p.as_posix(), reverse=True)
            for path in paths:
                relpath = path.relative_to(self.anchor)
                for p in rcs:
                    if p.parts[-len(relpath.parts):] == relpath.parts:
                        p.copy_to(path, force=True, copy_cache_meta=True)  # FIXME old remote may have been deleted, worth a check?
                        # TODO checksum? sync?
                        break

            breakpoint()

        else:
            stash = StashPath(stash_base, self._folder_timestamp)
            new_anchor = stash / self.anchor.name
            new_anchor.mkdir(parents=True)
            new_anchor.cache_init(self.anchor.meta, anchor=True)
            for p in to_stash[1:]:
                p = p.relative_to(self.anchor) if p.root == '/' else p
                new_path = new_anchor / p
                log.debug(p)
                log.debug(new_path)
                p = self.anchor.local / p
                if p.is_dir():
                    new_path.mkdir()
                    new_path.cache_init(p.cache.meta)
                else:
                    # TODO search existing stashes to see if
                    # we already have a stash of the file
                    log.debug(f'{p!r} {new_path!r}')
                    new_path.copy_from(p)
                    pc, npc = p.checksum(), new_path.checksum()
                    # TODO a better way to do this might be to
                    # treat the stash as another local for which
                    # the current local is the remote
                    # however this might require layering
                    # remote and remote remote metadata ...
                    assert pc == npc, f'\n{pc}\n{npc}'
                    cmeta = p.cache.meta
                    nmeta = stashmetafunc(cmeta)
                    log.debug(nmeta)
                    new_path.cache_init(nmeta)

            nall = list(new_anchor.rchildren)
            return nall

    def apinat(self):
        from sparcur import apinat
        path_in = Path(self.options.path_in)
        path_out = Path(self.options.path_out)
        with open(path_in) as f:
            resource_map = json.load(f)

        agraph = apinat.Graph(resource_map)
        graph = agraph.graph()
        graph.write(path=path_out)

    def rmeta(self):
        from pyontutils.utils import Async, deferred
        from sparcur.datasources import BlackfynnDatasetData
        dsr = self.datasets_remote
        prepared = [BlackfynnDatasetData(r) for r in dsr]
        hz = self.options.rate
        if not self.options.debug:
            blobs = Async(rate=hz)(deferred(d)() for d in prepared)
        else:
            blobs = [d() for d in  prepared]

    def make_url(self):
        lu = {d.cache.id:d.cache for d in self.datasets_local}
        for iop in self.options.id_or_path:
            if iop.startswith('N:dataset:'):
                cache = lu[iop]
            else:
                cache = Path(iop).cache

            print(hfn.atag(cache.uri_human, cache.id))

    def show(self):
        if self.options.export:
            from sparcur import export as ex
            org_id = self.options.project_id if self.options.project_id else auth.get('blackfynn-organization')
            export = self._export(ex.Export, org_id=org_id)
            if self.options.json:
                export.latest_export_path.xopen()
            elif self.options.ttl:
                export.latest_ttl_path.xopen()

        elif self.options.schemas:
            print(self.options.export_schemas_path)
            self.options.export_schemas_path.xopen()  # NOTE it is a folder

        elif self.options.rmeta:
            from sparcur import datasources as ds
            Path(ds.BlackfynnDatasetData.cache_base).xopen()

    def sheets(self):
        from pyontutils import sheets as ps
        from sparcur import sheets

        # get ir
        if self.options.raw:
            data = self.summary.data()
        else:
            from sparcur import export as ex
            org_id = auth.get('blackfynn-organization')
            export = self._export(ex.Export, org_id=org_id)
            data = export.latest_ir

        # check that the ir is sane
        self._check_duplicates(data['datasets'])

        # get sheets
        sheets = {(s.name, s.sheet_name):s for s in subclasses(ps.Sheet)
                  if not s.__name__.startswith('_')}
        by_sn = {sn:s for (n, sn), s in sheets.items()}
        sn = self.options.sheet_name
        Sheet = by_sn[sn]
        Sheet.fetch_grid = False

        if self.options.update:
            sheet = Sheet(readonly=False)
            sheet.update_from_ir(data)
        else:
            sheet = Sheet()

        if sheet.sheet_name == 'Organs':
            h = sheet.row_object(0)
            tech_cells = [
                cell
                for hcell in h.cells if 'technique' in hcell.value
                for cell in hcell.column.cells[1:] if cell.value]
            sheet.fetch_grid = True
            sheet.fetch()

        if self.options.debug:
            breakpoint()

        return sheet

    ### sub dispatchers

    def report(self):
        report = Report(self)
        return report('report')

    def shell(self):
        """ drop into an shell with classes loaded """
        shell = Shell(self)
        return shell('shell')

    def fix(self):
        fix = Fix(self)
        return fix('fix')


class Report(Dispatcher):

    paths = Main.paths
    _paths = Main._paths

    _print_table = Main._print_table

    @property
    def _sort_key(self):
        if self.options.sort_count_desc:
            return lambda kv: -kv[-1]
        else:
            return lambda kv: kv

    def access(self, ext=None):
        """ Report on datasets that are in the master sheet but which the
            automated pipelines do not have access to. """
        if self.anchor.id == 'N:organization:fake-organization-id':
            # FIXME need a way to generalize/fix this so that organization
            # can vary ...
            print('Not in correct organization for running access report.')
            sys.exit(9999)

        from sparcur.sheets import Overview, Master
        if self.options.server:
            def fmt(d): return hfn.atag(d.uri_human, d.id, new_tab=True)
        else:
            def fmt(d): return d.uri_human

        if self.options.server:
            uri_human = Master._uri_human()
            def fmtmc(s): return hfn.atag(uri_human, s, new_tab=True)
        else:
            def fmtmc(s): return s

        o = Overview()
        remote = self.anchor.remote
        remote_datasets = remote.children

        master_sheet_ids = o.dataset_ids()
        bf_api_ids = set(d.id for d in remote_datasets)

        missing_from_api = sorted(master_sheet_ids - bf_api_ids)
        missing_datasets = [self.BlackfynnRemote(i, local_only=True) for i in missing_from_api]
        missing_uris = [fmt(d) for d in missing_datasets]
        rows = [['', ''],
                [fmtmc('Master Count'), len(master_sheet_ids)],
                ['BF API Count', len(bf_api_ids)],
                ['No API Count', len(missing_from_api)],
                *[[i, u] for i, u in zip(missing_from_api, missing_uris)]
        ]
        return self._print_table(rows, title='Access Report', ext=ext)

    def contributors(self, ext=None):
        if self.options.raw:
            data = self.summary.data_for_export(UTCNOWISO())
        else:
            from sparcur import export as ex
            data = self._export(ex.Export).latest_ir

        datasets = data['datasets']
        unique = {c['id']:c for d in datasets
                  if 'contributors' in d
                  for c in d['contributors']}
        contribs = sorted(unique.values(),
                          key=lambda c: c['last_name'] if 'last_name' in c else (c['contributor_name'] if 'contributor_name' in c else 'zzzzzzzzzzzzzzzzzzzzzzzzzzzz'))
        #contribs = sorted((dict(c) for c in
                           #set(frozenset({k:tuple(v) if isinstance(v, list) else
                                          #(frozenset(v.items()) if isinstance(v, dict) else v)
                                          #for k, v in c.items()}.items())
                               #for d in datasets
                               #if 'contributors' in d
                               #for c in d['contributors']
                               #if not log.info(lj(c)))),
                          #key=lambda c: c['last_name'] if 'last_name' in c else c['name'])

        if self.options.debug:
            breakpoint()

        rows = [['id', 'last', 'first', 'PI', 'No Orcid']] + [[
            c['id'],
            c['last_name'] if 'last_name' in c else '',
            c['first_name'] if 'first_name' in c else '',
            'x' if 'contributor_role' in c and 'PrincipalInvestigator' in c['contributor_role'] else '',
            'x' if 'orcid' not in c['id'] else '']
            for c in contribs]

        return self._print_table(rows, title='Contributors Report', ext=ext)

    def tofetch(self, dirs=None):
        if dirs is None:
            dirs = self.options.directory
            if not dirs:
                dirs.append(self.cwd.as_posix())

        data = []

        def dead(p):
            raise ValueError(p)

        for d in dirs:
            if not Path(d).is_dir():
                continue  # helper files at the top level, and the symlinks that destory python
            path = Path(d).resolve()
            paths = path.rchildren #list(path.rglob('*'))
            path_meta = {p:p.cache.meta if p.cache else dead(p) for p in paths
                         if p.suffix not in ('.swp',)}
            outstanding = 0
            total = 0
            tf = 0
            ff = 0
            td = 0
            uncertain = False  # TODO
            for p, m in path_meta.items():
                #if p.is_file() and not any(p.stem.startswith(pf) for pf in self.spcignore):
                if p.is_file() or p.is_broken_symlink():
                    s = m.size
                    if s is None:
                        uncertain = True
                        continue

                    tf += 1
                    if s:
                        total += s

                    #if '.fake' in p.suffixes:
                    if p.is_broken_symlink():
                        ff += 1
                        if s:
                            outstanding += s

                elif p.is_dir():
                    td += 1

            data.append([path.name,
                         FileSize(total - outstanding),
                         FileSize(outstanding),
                         FileSize(total),
                         uncertain,
                         (tf - ff),
                         ff,
                         tf,
                         td])

        formatted = [[n, l.hr, o.hr, t.hr if not u else '??', lf, ff, tf, td]
                     for n, l, o, t, u, lf, ff, tf, td in
                     sorted(data, key=lambda r: (r[4], -r[3]))]
        rows = [['Folder', 'Local', 'To Retrieve', 'Total', 'L', 'R', 'T', 'TD'],
                *formatted]

        return self._print_table(rows, title='File size counts', ext=ext)

    def filetypes(self, ext=None):
        key = self._sort_key
        paths = self.paths if self.paths else (self.cwd,)
        paths = [c for p in paths for c in p.rchildren if not c.is_dir()]
        rex = re.compile('^\.[0-9][0-9][0-9A-Z]$')
        rex_paths = [p for p in paths if re.match(rex, p.suffix)]
        paths = [p for p in paths if not re.match(rex, p.suffix)]

        def count(thing):
            return sorted([(k if k else '', v) for k, v in
                            Counter([getattr(f, thing)
                                     for f in paths]).items()], key=key)

        each = {t:count(t) for t in ('suffix', 'mimetype', '_magic_mimetype')}
        each['suffix'].append((rex.pattern, len(rex_paths)))

        for title, rows in each.items():
            yield self._print_table(((title, 'count'), *rows),
                                    title=title.replace('_', ' ').strip())

        all_counts = sorted([(*[m if m else '' for m in k], v) for k, v in
                                Counter([(f.suffix, f.mimetype, f._magic_mimetype)
                                        for f in paths]).items()], key=key)

        header = ['suffix', 'mimetype', 'magic mimetype', 'count']
        return self._print_table((header, *all_counts),
                                 title='All types aligned (has duplicates)',
                                 ext=ext)

    def samples(self, ext=None):
        if self.options.raw:
            data = self.summary.data()
        else:
            from sparcur import export as ex
            data = self._export(ex.Export).latest_ir

        datasets = data['datasets']
        key = self._sort_key
        # FIXME we need the blob wrapper in addition to the blob generator
        # FIXME these are the normalized ones ...
        samples_headers = tuple(k for dataset_blob in datasets
                                 if 'samples' in dataset_blob  # FIXME inputs?
                                 for samples_blob in dataset_blob['samples']
                                 for k in samples_blob)
        counts = tuple(kv for kv in sorted(Counter(samples_headers).items(),
                                            key=key))

        rows = ((f'Column Name unique = {len(counts)}', '#'), *counts)
        return self._print_table(rows, title='Samples Report', ext=ext)

    def subjects(self, ext=None):
        if self.options.raw:
            data = self.summary.data()
        else:
            from sparcur import export as ex
            data = self._export(ex.Export).latest_ir

        datasets = data['datasets']
        key = self._sort_key
        # FIXME we need the blob wrapper in addition to the blob generator
        # FIXME these are the normalized ones ...
        subjects_headers = tuple(k for dataset_blob in datasets
                                 if 'subjects' in dataset_blob  # FIXME inputs?
                                 for subject_blob in dataset_blob['subjects']
                                 for k in subject_blob)
        counts = tuple(kv for kv in sorted(Counter(subjects_headers).items(),
                                            key=key))

        rows = ((f'Column Name unique = {len(counts)}', '#'), *counts)
        return self._print_table(rows, title='Subjects Report', ext=ext)

    def completeness(self, ext=None):
        if self.options.raw:
            raw = self.summary.completeness
        else:
            from sparcur import export as ex
            datasets = self._export(ex.Export).latest_ir['datasets']
            raw = [self.summary._completeness(data) for data in datasets]

        def rformat(i, si, ci, ei, name, id, award, organ):
            if self.options.server and isinstance(ext, types.FunctionType):
                rsurl = 'https://projectreporter.nih.gov/reporter_searchresults.cfm'
                dataset_dash_url = self.url_for('route_datasets_id', id=id)
                errors_url = self.url_for('route_reports_errors_id', id=id)
                si = hfn.atag(errors_url + '#submission', si)
                ci = hfn.atag(errors_url + '#curation', ci)
                ei = hfn.atag(errors_url + '#total', ei)
                name = hfn.atag(dataset_dash_url, name)
                id = hfn.atag(dataset_dash_url, id[:10] + '...')
                award = (
                    hfn.atag(('https://scicrunch.org/scicrunch/data/source/'
                              f'nif-0000-10319-1/search?q={award}'), award)
                    if award else 'MISSING')
                organ = organ if organ else ''
                if isinstance(organ, list) or isinstance(organ, tuple):
                    organ = ' '.join([o.atag() for o in organ])
                if isinstance(organ, OntTerm):
                    organ = organ.atag()
            else:
                award = award if award else ''
                organ = ((repr(organ) if isinstance(organ, OntTerm) else organ)
                         if organ else '')
                if isinstance(organ, list):
                    organ = ' '.join([repr(o) for o in organ])
                    

            return (i + 1, si, ci, ei, name, id, award, organ)

        rows = [('', 'SI', 'CI', 'EI', 'name', 'id', 'award', 'organ')]
        rows += [rformat(i, *cols) for i, cols in enumerate(sorted(
            raw, key=lambda t: (t[0], t[1], t[5] if t[5] else 'z' * 999, t[3])))]

        return self._print_table(rows, title='Completeness Report', ext=ext)

    def keywords(self, ext=None):
        from sparcur import export as ex
        data = (self.summary.data()
                if self.options.raw else
                self._export(ex.Export).latest_ir)
        datasets = data['datasets']
        _rows = [sorted(set(dataset_blob.get('meta', {}).get('keywords', [])),
                        key=lambda v: -len(v))
                    for dataset_blob in datasets]
        rows = [list(r) for r in sorted(
            set(tuple(r) for r in _rows if r),
            key = lambda r: (len(r), tuple(len(c) for c in r if c), r))]
        header = [[f'{i + 1}' for i, _ in enumerate(rows[-1])]]
        rows = header + rows
        return self._print_table(rows, title='Keywords Report')

    def size(self, dirs=None, ext=None):
        if dirs is None:
            dirs = self.paths
        intrs = [Integrator(p) for p in dirs]
        if not intrs:
            intrs = self.cwdintr,

        rows = [['path', 'id', 'dirs', 'files', 'size', 'hr'],
                *sorted([[d.name, d.id, c['dirs'], c['files'], c['size'], c['size'].hr]
                         for d in intrs
                         for c in (d.datasetdata.counts,)], key=lambda r: -r[-2])]

        return self._print_table(rows, title='Size Report',
                                 align=['l', 'l', 'r', 'r', 'r', 'r'], ext=ext)

    def test(self, ext=None):
        rows = [['hello', 'world'], [1, 2]]
        return self._print_table(rows, title='Report Test', ext=ext)

    def errors(self, *, id=None, ext=None):
        if self.options.raw:
            self.summary.data()['datasets']
        else:
            from sparcur import export as ex
            datasets = self._export(ex.Export).latest_ir['datasets']

        if self.cwd != self.anchor:
            id = self.cwd.cache.dataset.id
            
        if id is not None:
            if not id.startswith('N:dataset:'):
                return []

            def pt(rendered_table, title=None):
                """ passthrough """
                return rendered_table

            import htmlfn as hfn
            for dataset_blob in datasets:
                if dataset_blob['id'] == id:
                    dso = DatasetObject.from_json(dataset_blob)
                    title = f'Errors for {id}'
                    urih = dataset_blob['meta']['uri_human']
                    formatted_title = (hfn.h2tag(f'Errors for {hfn.atag(urih, id)}<br>\n') +
                                       (hfn.h3tag(dataset_blob['meta']['title']
                                        if 'title' in dataset_blob['meta'] else
                                        dataset_blob['meta']['folder_name'])))
                    log.info(list(dataset_blob.keys()))
                    errors = list(dso.errors)
                    return [(self._print_table(e.as_table(), ext=pt)) for e in errors], formatted_title, title
        else:
            pprint.pprint(sorted([(d['meta']['folder_name'], [e['message']
                                                              for e in get_all_errors(d)])
                                  for d in datasets], key=lambda ab: -len(ab[-1])))

    def pathids(self, ext=None):
        base = self.project_path.parent
        rows = [['path', 'id']] + sorted([c.relative_to(base), c.cache.id]#, c.cache.uri_api, c.cache.uri_human]
                                         # slower to include the uris
                                         for c in chain((self.cwd,), self.cwd.rchildren)
        )
        return self._print_table(rows, title='Path identifiers', ext=ext)

    def mbf(self, ext=None):
        et = tuple()
        from sparcur.extract import xml as exml

        def settype(mimetype):
            return {exml.ExtractMBF.mimetype: 'MBF Metadata',
                    exml.ExtractNeurolucida.mimetype: 'Neurolucida',}[mimetype]

        if self.options.raw:
            blob_ir = self.parent.export()
        else:
            from sparcur import export as ex
            blob_ir = self._export(ex.ExportXml).latest_ir  # FIXME need to load?

        mbf_types = tuple(c.mimetype for c in (exml.ExtractMBF, exml.ExtractNeurolucida))
        if self.options.unique:
            key = lambda p: (p[2], not p[0], p[1].lower(), p[1])
        else:
            key = lambda p: (p[2], p[3], not p[0], p[1].lower(), p[1])

        all_conts = sorted(set(((OntId(c['id_ontology'])
                                 if 'id_ontology' in c else
                                 ''),
                                c['name'],
                                settype(metadata_blob['type']),
                                *((dataset_xml['dataset_id'],) if not self.options.unique else et))
                               for dataset_xml in blob_ir.values()
                               if dataset_xml['type'] == 'all-xml-files'  # FIXME
                               for metadata_blob in dataset_xml['xml']
                               if metadata_blob['type'] in mbf_types and
                               'contours' in metadata_blob['extracted']
                               for c in metadata_blob['extracted']['contours']),
                           key=key)
        header = [['id', 'name', 'metadata source', 'dataset']]
        if self.options.unique:
            header[0] = header[0][:-1]

        return self._print_table(header + all_conts,
                                 title='Unique MBF contours')

    def terms(self, ext=None):
        # anatomy
        # cells
        # subcelluar
        import rdflib
        # FIXME cache these results and only recompute if latest changes?
        if self.options.raw:
            graph = self.summary.triples_exporter.graph
        else:
            from sparcur import export as ex  # FIXME very slow to import
            graph = OntGraph()
            self._export(ex.Export).latest_export_ttl_populate(graph)

        objects = set()
        skipped_prefixes = set()
        for t in graph:
            for e in t:
                if (isinstance(e, rdflib.URIRef) and
                    not e.startswith('info:') and
                    not e.startswith('doi:')):
                    oid = OntId(e)
                    if oid.prefix in want_prefixes:
                        objects.add(oid)
                    else:
                        skipped_prefixes.add(oid.prefix)

        if self.options.server and isinstance(ext, types.FunctionType):
            def reformat(ot):
                return [ot.label
                        if hasattr(ot, 'label') and ot.label else
                        '',
                        ot.atag(curie=True)]

        else:
            def reformat(ot):
                return [ot.label
                        if hasattr(ot, 'label') and ot.label else
                        '',
                        ot.curie]

        log.info(' '.join(sorted(skipped_prefixes)))
        objs = [OntTerm(o) if o.prefix not in ('TEMP', 'sparc') or
                o.prefix == 'TEMP' and o.suffix.isdigit() else
                o for o in objects]
        term_sets = {title:[o for o in objs if o.prefix == prefix]
                     for prefix, title in
                     (('NCBITaxon', 'Species'),
                      ('UBERON', 'Anatomy and age category'),  # FIXME
                      ('FMA', 'Anatomy (FMA)'),
                      ('PATO', 'Qualities'),
                      ('tech', 'Techniques'),
                      ('unit', 'Units'),
                      ('sparc', 'MIS terms'),
                      ('TEMP', 'Suggested terms'),
                     )}

        term_sets['Other'] = set(objs) - set(ot for v in term_sets.values()
                                             for ot in v)

        for title, terms in term_sets.items():
            header = [['Label', 'CURIE']]
            rows = header + [reformat(ot) for ot in
                            sorted(terms,
                                   key=lambda ot: (ot.prefix, ot.label.lower()
                                                   if hasattr(ot, 'label') and ot.label else ''))]

            yield self._print_table(rows, title=title, ext=ext)

    def _annos(self):
        if hasattr(self, '_c_annos'):
            return self._c_annos

        group_id = auth.get('hypothesis-group')
        cache_file = Path(hyp.group_to_memfile(group_id + 'sparcur'))
        get_annos = hyp.Memoizer(memoization_file=cache_file, group=group_id)
        get_annos.api_token = auth.get('hypothesis-api-key')
        self._c_annos = get_annos()
        return self._c_annos

    def _by_tags(self, *tags, links=True):
        annos = self._annos()
        [hyp.HypothesisHelper(a, annos) for a in annos]
        process_protc = anyMembers(tags,
                                   'protc:input',
                                   'protc:aspect',
                                   'protc:input-instance',
                                   'protc:implied-aspect',
                                   'protc:implied-input')
        if process_protc:
            from pysercomb.parsers import racket
            from pysercomb.pyr import units as pyru
            from protcur.analysis import protc
            [protc(a, annos) for a in annos]

        def normalize_exact(e):
            if e is not None:
                e = e.replace('\n', ' ')  # sigh
                e = e.replace('\t', ' ')
                e = e.replace('\xA0', ' ')  # nbsp
                e = e.strip().rstrip()
                return e
        def normtext(t):
            if 'https://' in t:
                return ''
            else:
                return t

        # note that HypothesisHelper.byTags is an AND search not and OR search
        matches = [(normalize_exact(a.exact), a) for tag in tags for a in hyp.HypothesisHelper.byTags(tag)]
        if process_protc:
            pm = [protc.byId(a.id) for _, a in matches]

            # testing
            if 'protc:input' in tags:
                input = pm[13]
                invar = next(next(input.children).children)
                paramparser = pyru.ParamParser()
                tv = racket.sexp(invar.parameter())[1]
                tp = invar._parameter[1]
                assert tv == tp
                hrm = paramparser(tv)

            hrm = defaultdict(list)
            def denone(t): return tuple('' if e is None else e for e in t)
            for p in pm:
                pt = [t for t in p.tags if t in tags]
                if pt:
                    hrm[denone((pt[0], p.value, normtext(p.text), normalize_exact(p.exact)))].append(p)
                else:
                    log.warning(f'WAT {p.tags}')

            header = [['tag', 'value', 'text', 'exact', 'count']]
            #rows = [[(*k, ' '.join([a.shareLink for a in v]))] for k, v in sorted(hrm.items())]
            rows = [[*k, len(v)] for k, v in sorted(hrm.items())]
            return self._print_table(header + rows,
                                     title=f'Annos for {" ".join(tags)}')

        md = defaultdict(list)
        for e, a in matches:
            md[e].append(a)

        def gtag(e):  # FIXME memoize/cache?
            return ' '.join(sorted(set(t for a in md[e] for t in a.tags if t in tags)))

        rows = sorted([[gtag(e), e, c, (' '.join([a.htmlLink if self.options.uri_api else a.shareLink
                                                 for a in md[e]] if links else ''))]
                       for e, c in Counter([normalize_exact(e) for e, a in matches]).most_common()],
                      key=lambda ab: (ab[0], -ab[2], ab[1].lower()))

        header = [['tag', 'exact', 'count', 'links']]
        return self._print_table(header + rows,
                                 title=f'Annos for {" ".join(tags)}')

    def anno_tags(self, *tags, links=False):
        if not tags:
            tags = self.options.tag

        links = links or self.options.uri or self.options.uri_api

        return self._by_tags(*tags, links=links)

    def changes(self):
        ori1 = OntResIri('https://cassava.ucsd.edu/sparc/exports/curation-export.ttl')
        ori2 = OntResIri('https://cassava.ucsd.edu/sparc/archive/exports/2020-03-23T12:16:56,102146-07:00/curation-export.ttl')
        g1 = ori1.graph
        g2 = ori2.graph

    def mis(self):
        ori1 = OntResIri('https://cassava.ucsd.edu/sparc/exports/curation-export.ttl')
        ori2 = OntResIri('https://cassava.ucsd.edu/sparc/archive/exports/2020-03-23T12:16:56,102146-07:00/curation-export.ttl')


class Shell(Dispatcher):
    # property ports
    paths = Main.paths
    _paths = Main._paths
    _build_paths = Main._build_paths
    datasets = Main.datasets
    datasets_local = Main.datasets_local
    stash = Main.stash

    def exit(self):
        """ useful for profiling startup time issues """
        print('Peace.')
        return

    def default(self):
        datasets = list(self.datasets)
        datas = [Integrator(d).datasetdata for d in datasets]
        datasets_local = list(self.datasets_local)
        dsd = {d.meta.id:d for d in datasets}
        ds = datasets
        summary = self.summary
        org = Integrator(self.project_path)
        breakpoint()

    def more(self):
        p, *rest = self._paths
        if p.cache.is_dataset():
            intr = Integrator(p)
            j = JT(intr.data())
            iddf = intr.datasetdata.dataset_description.object  # finally
            #triples = list(f.triples)

        try:
            latest_datasets = self._export(None).latest_export['datasets']
        except:
            pass

        rcs = list(datasets[-1].rchildren)
        asdf = rcs[-1]
        urg = list(asdf.data)
        resp = asdf.data_headers

        did = self.cwd.cache.id
        r = self.cwd.remote
        if did in dsd:
            dsi = datas.index(dsd[did])
        else:
            dsi = 2

        d = datas[dsi]
        try:
            smf = d.submission.object
            smd = smf.data

            ddf = d.dataset_description.object
            ddd = ddf.data

            suf = d.subjects.object
            sud = suf.data

            saf = d.samples.object
            sad = saf.data

        except AttributeError as e:
            logd.error(f'{d} is missing some file')

        breakpoint()

    def affil(self):
        from pyontutils.utils import Async, deferred
        from sparcur.sheets import Affiliations
        a = Affiliations()
        m = a.mapping
        rors = sorted(set(_ for _ in m.values() if _))
        #dat = Async(rate=5)(deferred(lambda r:r.data)(i) for i in rors)
        dat = [r.data for r in rors]  # once the cache has been populated
        breakpoint()

    def protocols(self):
        """ test protocol identifier functionality """
        org = Integrator(self.project_path)
        from pyontutils.utils import Async, deferred
        skip = '"none"', 'NA', 'no protocols', 'take protocol from other spreadsheet, ', 'na'
        asdf = [us for us in sorted(org.organs_sheet.byCol.protocol_url_1)
                if us not in skip and us and ',' not in us]
        wat = [idlib.Auto(_) for _ in asdf]
        inst = [i for i in wat]
        res = Async(rate=5)(deferred(i.dereference)(idlib.Auto) for i in inst)
        pis = [i for i in res if isinstance(i, idlib.Pio)]
        #dat = Async(rate=5)(deferred(lambda p: p.data)(i) for i in pis)
        #dois = [d['protocol']['doi'] for d in dat if d]
        dois = [p.doi for p in pis]
        breakpoint()

    def integration(self):
        from protcur.analysis import protc, Hybrid
        from sparcur import sheets
        #from sparcur.sheets import Organs, Progress, Grants, ISAN, Participants, Protocols as ProtocolsSheet
        from sparcur.protocols import ProtcurData
        p, *rest = self._paths
        intr = Integrator(p)
        j = JT(intr.data)
        pj = list(intr.protocol_jsons)
        pc = list(intr.triples_exporter.protcur)
        #apj = [pj for c in intr.anchor.children for pj in c.protocol_jsons]
        breakpoint()


class Fix(Shell):

    def default(self):
        pass

    def mismatch(self):
        """ once upon a time it was (still at the time of writing) possible
            to update meta on an existing file without preserving the old data
            AND without downloading the new data (insanity) this can be used to fix
            those cases, preserving the old version """
        oops = list(self.parent.status())
        [print(lmeta.as_pretty_diff(cmeta, pathobject=path, human=self.options.human))
         for path, lmeta, cmeta in oops]
        paths = [p for p, *_ in oops]

        def sf(cmeta):
            nmeta = PathMeta(id=cmeta.old_id)
            assert nmeta.id, f'No old_id for {pathmeta}'
            return nmeta

        nall = self.stash(paths, stashmetafunc=sf)
        [print(n.cache.meta.as_pretty(n)) for n in nall]
        breakpoint()
        # once everything is in order and backed up 
        # [p.cache.fetch() for p in paths]

    def duplicates(self):
        all_ = defaultdict(list)
        if not self.options.path:
            paths = self.anchor.local.rchildren
        else:
            paths = self.paths

        for rc in paths:
            if rc.cache is None:
                if not rc.skip_cache:
                    log.critical(f'WHAT THE WHAT {rc}')

                continue

            all_[rc.cache.id].append(rc)

        def mkey(p):
            mns = p.cache.meta
            return (not bool(mns),
                    not bool(mns.updated),
                    -mns.updated.timestamp())

        dupes = {i:sorted(l, key=mkey)#, reverse=True)
                 for i, l in all_.items() if len(l) > 1}
        dv = list(dupes.values())
        deduped = [a.dedupe(b, pretend=True) for a, b, *c in dv
                   if (not log.warning(c) if c else not c)
        ]  # FIXME assumes a single dupe...
        to_remove = [d for paths, new in zip(dv, deduped)
                     for d in paths if d != new]
        to_remove_size = [p for p in to_remove if p.cache.meta.size is not None]
        #[p.unlink() for p in to_remove if p.cache.meta.size is None] 
        breakpoint()


def main():
    time_now = GetTimeNow()
    from docopt import docopt, parse_defaults
    args = docopt(__doc__, version='spc 0.0.0')
    defaults = {o.name:o.value if o.argcount else None for o in parse_defaults(__doc__)}

    logpath = Path(args['--log-path'])
    if not logpath.exists():
        logpath.mkdir(parents=True)

    try:
        logfile = logpath / time_now.START_TIMESTAMP_SAFE  # FIXME configure and switch
        bind_file_handler(logfile)

        options = Options(args, defaults)
        main = Main(options, time_now)
        if main.options.debug:
            print(main.options)

        main()
    except BaseException as e:
        log.exception(e)
        print()
        raise e
    finally:
        if logfile.size == 0:
            logfile.unlink()

    if options.profile:
        exit = time()
        a = start_middle - start
        b = (stop - start_middle)
        c = (stop - start)
        d = (exit - stop)
        e = (exit - start)
        s = f'{a} + {b} = {c}\n{c} + {d} = {e}'
        print(s)


if __name__ == '__main__':
    main()
