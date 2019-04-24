"""augpathlib

Do you like pathlib? Have you ever wanted to see just how far you can
push the path abstraction? Do you like using the division operator
in ways that could potentially cause reading from the network or writing to disk?
Then augpathlib is for you!


sparcur makes extensive use of the pathlib Path object (and friends)
by augmenting the base PosixPath object with additional functionality
such as getting and setting xattrs, syncing with other mapped paths etc.

In essence there are 3 ways that a Path object can be used: Local, Cache, and Remote.
Local paths return data and metadata that are local the the current computer.
Cache paths return local metadata about remote objects (such as their remote id).
Remote objects provide an interface to remote data that is associated with a path.

Remote paths should be back by another object which is the representation of the
remote according to the remote's APIs.

Remote paths are only intended to provide a 1:1 mapping, so list(local.data) == list(remote.data)
should always be true if everything is in sync.

If there is additional metadata that is associated with a local path then that is
represented in the layer above this one (currently FThing, in the future a validation Stage).
That said, it does seem like we need a more formal place that can map between all these
things rather than always trying to derive the mappings from data embedded (bound) to
the derefereced path object. """

import os
import sys
import base64
import hashlib
import pathlib
import subprocess
from time import sleep
from errno import ELOOP, ENOENT, ENOTDIR, EBADF
from pathlib import PosixPath, PurePosixPath
from datetime import datetime
import xattr
import psutil
from dateutil import parser
from Xlib.display import Display
from Xlib import Xatom
from sparcur import exceptions as exc
from sparcur.core import log
from sparcur.pathmeta import PathMeta
from pyontutils.utils import sysidpath

# FIXME 3.7 fix only ...
if sys.version_info >= (3, 7):
    pathlib._IGNORED_ERROS += (ELOOP,)
    def _ignore_error(exception):
        pass
else:
    _IGNORED_ERROS = (ENOENT, ENOTDIR, EBADF, ELOOP)
    def _ignore_error(exception):
        return (getattr(exception, 'errno', None) in _IGNORED_ERROS)

    def _is_dir(entry):
        try:
            return entry.is_dir()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def _iterate_directories(self, parent_path, is_dir, scandir):
        """ patched to fix is_dir() erron """
        yield parent_path
        try:
            entries = list(scandir(parent_path))
            for entry in entries:
                if _is_dir(entry) and not entry.is_symlink():
                    path = parent_path._make_child_relpath(entry.name)
                    for p in self._iterate_directories(path, is_dir, scandir):
                        yield p
        except PermissionError:
            return

    pathlib._RecursiveWildcardSelector._iterate_directories = _iterate_directories




# remote data about remote objects -> remote_meta
# local data about remote objects -> cache_meta
# local data about local objects -> meta
# remote data about local objects <- not relevant yet? or is this actually rr?

# TODO by convetion we could store 'children' of files in a .filename.ext folder ...
# obviously this doesn't always work
# and we would only want to do this for files that actually had annotations that
# needed a place to live ...

class RemotePath:
    """ Remote data about a remote object. """

    _cache_class = None

    # we use a PurePath becuase we still want to key off this being local path
    # but we don't want any of the local file system operations to work by accident
    # so for example self.stat should return the remote value not the local value
    # which is what would happen if we used a PosixPath as the base class

    # need a way to pass the session information in independent of the actual path
    # abstractly having remote.data(global_id_for_local, self)
    # should be more than enough, the path object shouldn't need
    # to know that it has a remote id, the remote manager should
    # know that

    @classmethod
    def setup(cls, local_class, cache_class):
        """ call this once to bind everything together """
        cache_class.setup(local_class, cls)

    def bootstrap(self, recursive=False, only=tuple(), skip=tuple()):
        self.cache.remote = self  # duh
        # if you forget to tell the cache you exist of course it will go to
        # the internet to look for you, it isn't quite smart enough and
        # we're trying not to throw dicts around willy nilly here ...
        self.cache.bootstrap(self.meta, recursive=recursive, only=only, skip=skip)

    def __init__(self, id, cache=None):
        self.id = id
        if cache is not None:
            self.cache = cache
            self.cache._remote = self

    @property
    def cache(self):
        return self._cache

    @property
    def _cache(self):
        """ To catch a bad call to set ... """
        if hasattr(self, f'_c_cache'):
            return self._c_cache

    @_cache.setter
    def _cache(self, cache):
        if not isinstance(cache, CachePath):
            raise TypeError(f'cache is a {type(cache)} not a CachePath!')

        self._c_cache = cache

    @cache.setter
    def cache(self, cache):
        if cache.meta is None:  # FIXME this can't really happen since cache needs the id beforehand
            cache.meta = self.meta  # also easier than bootstrapping ...

        if cache.id is not None and cache.id != self.id:
            # yay! we are finally to the point of needing to filter files vs packages :D !?!? maybe?
            #breakpoint()
            if self.meta.created < cache.meta.created:  # same filename different blackfynn id
                # when they 'delete' files they still show up in the root
                # but then you have to know which came after which ...  ???

                msg = ('Cache id does not match remote id! '
                       'And cache file is newer!\n'
                       f'{cache.id} !=\n{self.id}'
                       f'\n{cache}\n{self.name}')
                log.critical(msg)
                #raise exc.MetadataIdMismatchError

        if not hasattr(self, '_cache') or self._cache is None:
            self._cache = cache

        else:
            raise BaseException('FIXME make a proper error type, cache exists')


    @property
    def local(self):
        return self.cache.local

    @property
    def anchor(self):
        """ the semantics of anchor for remote paths are a bit different
            RemotePath code expects this function to return a RemotePath
            NOT a string as is the case for core pathlib. """
        raise NotImplementedError

    @property
    def id(self):
        raise NotImplementedError
        self.remote_thing.id_from_machine_id_and_path(self)

    def refresh(self):
        """ Refresh the local in memory metadata for this remote.
            Implement actual functionality in your subclass. """

        # could be fetch or pull, but there are really multiple pulls as we know

        # clear the cached value for _meta
        if hasattr(self, '_meta'):
            delattr(self, '_meta')

    @property
    def data(self):
        self.cache.id
        for chunk in chunks:
            yield chunk

    @property
    def meta(self):
        # on blackfynn this is the package id or object id
        # this will error if there is no implementaiton if self.id
        return PathMeta(id=self.id)

    @meta.setter
    def meta(self, value):
        raise NotImplementedError
        return 'yes?'

    @property
    def annotations(self):
        # these are models etc in blackfynn
        yield from []
        raise NotImplementedError

    def as_path(self):
        """ returns the relative path construction for the child so that local can make use of it """
        return PurePosixPath(*self.parts)

    def _parts_relative_to(self, remote):
        parent_names = []  # FIXME massive inefficient due to retreading subpaths :/
        # have a look at how pathlib implements parents
        for parent in self.parents:
            if parent == remote:
                break
            elif parent is None:
                continue  # value error incoming
            else:
                parent_names.append(parent.name)

        else:
            raise ValueError(f'{remote} is not one of {self}\'s parents')

        args = (*reversed(parent_names), self.name)
        return args

    @property
    def parts(self):
        if not hasattr(self, '_parts'):
            self._parts = tuple(self.relative_to(self.anchor))

        return self._parts

    @property
    def parent(self):
        """ The atomic parent operation as understood by the remote. """
        raise NotImplementedError

    @property
    def parents(self):
        parent = self.parent
        while parent:
            yield parent
            parent = parent.parent

    @property
    def children(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        raise NotImplementedError

    @property
    def rchildren(self):
        # uniform interface for retrieving remote hierarchies decoupled from meta
        raise NotImplementedError

    def iterdir(self):
        # I'm guessing most remotes don't support this
        raise NotImplementedError

    def glob(self, pattern):
        raise NotImplementedError

    def rglob(self, pattern):
        raise NotImplementedError


class AugmentedPath(PosixPath):
    """ extra conveniences, mostly things that are fixed in 3.7 using IGNORE_ERROS """

    def exists(self):
        """ Turns out that python doesn't know how to stat symlinks that point
            to their own children, which is fine because that is what we do
            so a reasonable way to short circuit the issue """
        try:
            return super().exists()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def is_file(self):
        try:
            return super().is_file()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def is_dir(self):
        try:
            return super().is_dir()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def is_symlink(self):
        try:
            return super().is_symlink()
        except OSError as e:
            if not _ignore_error(e):
                raise

            return False

    def resolve(self):
        try:
            return super().resolve()
        except RuntimeError as e:
            msg = ('Unless this call to resolve was a mistake you should switch '
                   'to using readlink instead. Uncomment raise e to get a trace.\n'
                   'Alternately you might want to use absolute() in this situation instead?')
            raise RuntimeError(msg) from e

    def readlink(self):
        """ this returns the string of the link only due to cycle issues """
        link = os.readlink(self)
        #log.debug(link)
        return PurePosixPath(link)

    def access(self, mode='read', follow_symlinks=True):
        """ types are 'read', 'write', and 'execute' """
        if mode == 'read':
            mode = os.R_OK
        elif mode == 'write':
            mode = os.W_OK
        elif mode == 'execute':
            mode = os.X_OK
        else:
            raise TypeError(f'Unknown mode {mode}')

        return os.access(self, mode, follow_symlinks=follow_symlinks)


class CachePath(AugmentedPath):
    # CachePaths this needs to be a real path so that it can navigate the local path sturcture
    """ Local data about remote objects.
        This is where the mapping between the local id (aka path)
        and the remote id lives. In a git-like world this is the
        cache/index/whatever we call it these days 

        This is the bridge class that holds the mappings.
        Always start bootstrapping from one of these classes
        since it has both the local and remote identifiers,
        and therefore can be called and used before specifying
        the exact implementations for the local and remote objects.
    """

    _local_class = None
    _remote_class_factory = None

    _backup_cache = None
    _not_exists_cache = None

    def __new__(cls, *args, meta=None, remote=None, **kwargs):
        # TODO do we need a version of this for the locals
        # and remotes? I don't think we create 'alternate' remotes or locals ...

        self = super().__new__(cls, *args, **kwargs)

        # clone any existing locals and remotes
        if args:
            path = args[0]
            if isinstance(path, CachePath):
                self._cache_parent = path
                if hasattr(self._cache_parent, '_in_bootstrap'):
                    # it is ok to do this and not clean up because
                    # child caches are ephemoral
                    self._in_bootstrap = self._cache_parent._in_bootstrap

                if path.local is not None:  # this might be the very fist time local is called
                    #log.debug('setting local')
                    self._local = path.local

                #if hasattr(path, '_remote'):
                    # have to check for private to avoid infinite recursion
                    # when searching for meta (which is what we are doing right now if we get here)
                    # FIXME pretty sure the way we have it now there _always_ has to be a remote
                    # which is not what we want >_<
                    #log.debug('setting remote')
                    #if meta is not None:
                        #self.meta = meta
                    #else:
                        #breakpoint()
                        #raise TypeError('you have managed to have a remote but pass no meta ?!')

                    #if not hasattr(path, '_in_bootstrap'):  # FIXME this seems wrong?
                    #self._remote = path.remote  # no, use cache parent

            elif isinstance(path, LocalPath):
                self._local = path
                path._cache = self

            elif isinstance(path, RemotePath):
                #self._remote = path
                #self.meta = path.meta
                # in order for this to work the remote has to already
                # know where the cache should live, which it doesn't
                # use move instead for cases where the semantics are well defined
                raise TypeError('Not entirely sure what to do in this case ...')

        #if isinstance(self, SymlinkCache):
            #breakpoint()

        return self

    def __init__(self, *args, meta=None, remote=None, **kwargs):
        # FIXME not really the init for CachePath ... more BlackfynnCache
        if self.id is None:
            if meta is None and remote is None:
                msg = f'No cached meta exists and no meta and no remote provided for {self}'
                raise exc.NoCachedMetadataError(msg)
            elif remote and meta:
                raise TypeError(f'can only have one remote or one meta')
            elif remote is not None:
                if remote.name != self.name:
                    raise ValueError('remote has a different name {self.name} != {remote.name}')
                self.remote = remote
                return

            # we received meta
            if self.exists() and (self.meta and self.meta.id == meta.id
                                  or not self.meta):
                # file or folder that exists
                self.meta = meta
            elif not self.exists() and self.is_symlink():
                # symlink that exists so overwrite
                self.meta = meta
            elif not self.is_helper_cache:
                # there is nothing we should bootstrap
                self.bootstrap(meta)
            else:
                log.critical('how did we get here!?')

            if not self.is_helper_cache:
                if self.id.startswith('N:organization:'):  # FIXME
                    self._organization = self

            #elif hasattr(self, '_remote'):
            #else:
                #raise exc.NoRemoteMappingError(f'gonna need a remote here ... {self.local}')

        super().__init__()

    @property
    def is_helper_cache(self):
        return hasattr(self, '_cache_parent')

    def __truediv__(self, key):
        # basically RemotePaths are like relative CachePaths ... HRM
        # they are just a name and an id ... the id of their parent
        # root needs to match the id of the cache ... which it usually
        # does by construction
        if isinstance(key, RemotePath):
            # FIXME not just names but relative paths???
            try:
                child = self._make_child(key._parts_relative_to(self.remote), key)
            except AttributeError as e:
                breakpoint()
                raise exc.SparCurError('aaaaaaaaaaaaaaaaaaaaaa') from e

            return child
        else:
            raise TypeError('Cannot construct a new CacheClass from an object '
                            f'without an id and a name! {key}')

    def __rtruediv__(self, key):
        """ key is a subclass of self.__class__ """
        out = self._from_parts([key.name] + self._parts, init=False)
        out._init()
        out.meta = key.meta
        return out

    def _make_child(self, args, remote):
        drv, root, parts = self._parse_args(args)
        drv, root, parts = self._flavour.join_parsed_parts(
            self._drv, self._root, self._parts, drv, root, parts)
        child = self._from_parsed_parts(drv, root, parts, init=False)
        child._init()
        if isinstance(remote, RemotePath):
            #log.debug('remoooote')
            child._remote = remote  # have to use _remote since this is construction
            child.bootstrap(remote.meta)  # FIXME indicates that maybe we want bootstrap to be meta setter?
            child.meta = remote.meta
            child.remote
            #child.meta = child.meta
            if not hasattr(remote, '_cache') or remote._cache is None:
                remote.cache = child
            else:
                log.warning('Trying to set cache when it already exists!')
        else:
            breakpoint()

        return child

    @classmethod
    def setup(cls, local_class, remote_class_factory):
        """ call this once to bind everything together """
        cls._local_class = local_class
        cls._remote_class_factory = remote_class_factory
        local_class._cache_class = cls
        remote_class_factory._cache_class = cls

    def bootstrap(self, meta, *,
                  parents=False,
                  recursive=False,
                  fetch_data=False,
                  size_limit_mb=2,
                  only=tuple(),
                  skip=tuple()):
        try:
            self._in_bootstrap = True
            self._bootstrap(meta, parents=parents,
                            recursive=recursive,
                            fetch_data=fetch_data,
                            size_limit_mb=size_limit_mb,
                            only=only, skip=skip)
        finally:
            delattr(self, '_in_bootstrap')
            if hasattr(self, '_meta'):
                delattr(self, '_meta')

    def _bootstrap(self, meta, *,
                   parents=False,
                   fetch_data=False,
                   size_limit_mb=2,
                   recursive=False,
                   only=tuple(),
                   skip=tuple()):
        """ The actual bootstrap implementation """

        # figure out if we are actually bootstrapping this class or skipping it
        if not meta or meta.id is None:
            raise exc.BootstrappingError(f'PathMeta to bootstrap from has no id! {meta}')

        if only or skip:
            if meta.id.startswith('N:organization:'):  # FIXME :/
                # since we only go one organization at a time right now
                # we never want to skip the top level id
                log.info(f'Bootstrapping {meta.id} -> {self.local!r}')
            elif meta.id in skip:
                log.info(f'Skipped       {meta.id} since it is in skip')
                return
            elif only and meta.id not in only:
                log.info(f'Skipped       {meta.id} since it is not in only')
                return
            else:
                # if you pass the only mask so do your children
                log.info(f'Bootstrapping {id} -> {self.local!r}')
                only = tuple()


        if self.exists() and self.meta and self.meta.id == meta.id:
            self.meta = meta

        else:
            # set memory only meta
            self._bootstrap_meta_memory(meta)

            # directory, file, or fake file as symlink?
            is_file_and_fetch_data = self._bootstrap_prepare_filesystem(parents,
                                                                        fetch_data,
                                                                        size_limit_mb)
            self.meta = self.remote.meta
            self._bootstrap_data(is_file_and_fetch_data)

        # bootstrap the rest if we told it to
        if recursive:  # ah the irony of using loops to do this
            if self.id.startswith('N:organization:'):  # FIXME :/
                for child in self.remote.children:
                    child.bootstrap(recursive=True, only=only, skip=skip)
            else:
                for child in self.remote.rchildren:
                    # use the remote's recursive implementation
                    # not the local implementation, since the
                    # remote may have additional requirements
                    child.bootstrap(only=only, skip=skip)


        return

    def _bootstrap_meta_memory(self, meta):
        if meta is None:
            raise TypeError('what are you doin')

        if not self.meta or self.meta.id is None:
            #self._meta_setter(meta, memory_only=True)  # FIXME _meta_setter broken for memonly ...
            self._meta = meta
            if self.meta.id is None:
                log.warning(f'Existing meta for {self!r} no id so overwriting\n{self.meta}')

            if self.remote is None:
                breakpoint()
                raise AssertionError

        elif self.id != meta.id:
            msg = ('Existing cache id does not match new id! '
                   f'{self.meta.id} != {meta.id}\n{self.meta}')
            # TODO overwrite?
            # also newest wins, how does this happen on bf end?
            #if self.meta.created < meta.created:  # pretty sure doesn't work because which we got was rando
            log.critical(msg)
            #else:
                #raise exc.MetadataIdMismatchError(msg)

        elif self.exists():
            # we don't need to set meta since it is already on disk
            # but if the id's don't match we are in trouble
            if self.meta.id != meta.id:
                raise exc.SparCurError('something has gone wrong')
        elif self.is_symlink():
            pass
        else:
            raise BaseException('should not get here')


    def _bootstrap_prepare_filesystem(self, parents, fetch_data, size_limit_mb):
        if self.remote.is_dir():
            if not self.exists():
                # the bug where this if statement put in as an and is a really
                # good example of how case/cond etc help you reasona about what
                # a block of branches is really doing -- this one was implementing
                # a covering set which is not obvious if implemented this way
                # you could do this with a dict or something else in pythong
                # bit it is awkward (see also my crazy case implementation in interlex)
                self.mkdir(parents=parents)
        elif self.remote.is_file():
            if not self.parent.exists():
                self.parent.mkdir(parents=parents)

            toucha_da_filey = (fetch_data and
                               self.meta.size is not None and
                               self.meta.size.mb < size_limit_mb)

            if toucha_da_filey:
                self.touch()
                # running this first means that we will use xattrs instead of symlinks
                # this is a bit opaque, but since meta uses a setter we can't pass a
                # param to make it clear (oh look, python being dumb again!)
            else:
                pass  # we are using symlinks

        else:
            raise BaseException(f'Remote is not a file or directory {self}')

    def _bootstrap_data(self, is_file_and_fetch_data):
        if is_file_and_fetch_data:
            if self.remote.meta.size is None:
                self.remote.refresh(update_cache=True)

            self.local.data = self.remote.data
            # with open -> write should not cause the inode to change

            self.validate_file()

    def validate_file(self):
        if self.meta.checksum:
            lc = self.local.meta.checksum 
            cc = self.meta.checksum
            if lc != cc:
                raise exc.ChecksumError(f'Checksums do not match!\n(!=\n{lc}\n{cc}\n)')
        else:
            log.warning(f'No checksum! Your data is at risk! {self.remote!r} -> {self.local!r}! ')
            ls = self.local.meta.size
            cs = self.meta.size
            if ls != cs:
                raise exc.SizeError(f'Sizes do not match!\n(!=\n{ls}\n{cs}\n)')

    def __bootstrap_old(self):

        if not self.is_symlink() and self.exists():
            if self.meta and self.meta.id:
                # we have our files, update the metdata while we're at it
                self.meta = self.remote.meta

        elif not self.is_symlink() and (not self.exists() or
                                        self.exists() and
                                        not self.meta and
                                        not list(self.local.children)):
            if not self.meta:
                log.debug('problem')
                self.meta = meta  # memory only bootstrap

            remote_meta = self.remote.meta
            if self.remote.is_dir():
                self.mkdir(parents=parents)
                if not self.meta:
                    self.meta = meta  # save bootstrap meta to disk
                self.meta = remote_meta

            # FIXME this is really select remote cache type?
            elif self.remote.is_file():
                if not self.parent.exists():
                    self.parent.mkdir(parents=parents)

                if fetch_data and self.meta.size < size_limit_mb * 1024 ** 2:
                    # running this first means that we will use xattrs instead of symlinks
                    # this is a bit opaque, but since meta uses a setter we can't pass a
                    # param to make it clear (oh look, python being dumb again!)
                    self.touch()

                if not self.meta:
                    self.meta = meta  # save bootstrap meta to disk as xattr or symlink

                self.meta = remote_meta
                if self.meta != remote_meta:
                    # read back from storage to be sure ... annnd we failed
                    msg = '\n'.join([f'{k!r} {v!r} {getattr(remote_meta, k)!r}'
                                     for k, v in self.meta.items()])
                    raise exc.MetadataCorruptionError(msg)

                if fetch_data and self.meta.size < size_limit_mb * 1024 ** 2:
                    self.local.data = self.remote.data

                if self.meta.checksum:
                    lc = self.local.meta.checksum 
                    cc = self.meta.checksum
                    if lc != cc:
                        raise exc.ChecksumError('Checksums to not match! {lc} != {cc}')
                else:
                    log.warning(f'No checksum! Your data is at risk! {self.remote!r} -> {self.local!r}! ')

            else:
                raise BaseException(f'Remote is not a file or directory {self}')

    @property
    def remote(self):
        if hasattr(self, '_remote'):
            return self._remote

        if hasattr(self, '_cache_parent'):
            return self._cache_parent.remote

        if not self.id:
            return

        if self._remote_class_factory is not None:
            # we don't have to have a remote configured to check the cache
            if not hasattr(self, '_remote_class'):
                #log.debug('rc')
                # NOTE there are many possible ways to set the anchor
                # we need to pick _one_ of them
                self._remote_class = self._remote_class_factory(self.anchor,
                                                                self._local_class,
                                                                self.__class__)


            if not hasattr(self, '_remote'):
                self.remote = self._remote_class(self.id, cache=self)

            return self._remote

    @remote.setter
    def remote(self, remote):
        if self.meta is not None and self.id is not None:
            if hasattr(self, '_remote'):
                if remote is self._remote:
                    log.error('Not setting remote. Why are you trying to set the same remote again?')
                    return
                elif remote.meta == self.remote.meta:
                    log.error('Not setting remote. Remotes are different but meta is the same?')
                    return
                else:
                    diff = 'TODO'
                    log.info('Updating since remotes are different. {diff}')

            if self.id != remote.id:
                raise exc.MetadataIdMismatchError('Existing cache id does not match new id! '
                                                  f'{self.id} != {remote.id}\n{self.meta}')

            elif isinstance(remote, RemotePath):
                self._remote = remote
                remote._cache = self

            else:
                raise TypeError(f'{remote} is not a RemotePath! It is a {type(remote)}')

        #elif self.is_anchor() and not hasattr(self, '_remote'):
            #self.meta = remote.meta
            #self._remote = remote
        else:
            # make sure no monkey business is going on at least in the local graph
            #if self.parent and self.local.parent.cache.meta.id == self.parent.id:  # cache parents are from the file system so dont need
            if remote.parent is None:
                breakpoint()
            if self.parent and remote.parent.id == self.parent.id:  # we can avoid a net call since we just need the id
                self._remote = remote
                self.meta = remote.meta
                # meta checks to see whethere there is a remote
                # if there is a remote it assumes that 
                remote._cache = self

    @property
    def local(self):
        if self._local_class is not None or hasattr(self, '_local'):
            if not hasattr(self, '_local'):
                self._local = self._local_class(self)
                self._local._cache = self

            return self._local

    @property
    def id(self):
        if self.meta:
            return self.meta.id

    # TODO how to toggle fetch from remote to heal?
    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta  # for bootstrap

    @meta.setter
    def meta(self, pathmeta):
        self._meta_setter(pathmeta)  # will automatically error

    def _meta_setter(self, pathmeta, memory_only=False):
        """ so much for the pythonic way when the language won't even let you """
        if not memory_only:
            raise TypeError('You must explicitly set memory_only=True to use this '
                            'otherwise you risk dataloss.')

        if self.meta and self.id != pathmeta.id:
            raise exc.MetadataIdMismatchError('Cache id does not match meta id! '
                                              f'{self.id} != {pathmeta.id}\n{pathmeta}')

        self._meta = pathmeta

    @property
    def data(self):
        # we don't keep two copies of the local data
        # unless we are doing a git-like thing
        raise NotImplementedError

    def move(self, *, remote=None, target=None, meta=None):
        """ instantiate a new cache and cleanup self because we are moving """
        # FIXME what to do if we have data
        if remote is None and (target is None or meta is None):
            raise TypeError('either remote or meta and target are required arguments')

        if remote:
            target = self.local.parent / remote.name
            kwargs = dict(remote=remote)
        else:
            kwargs = dict(meta=meta)

        if target.absolute() == self.absolute():
            log.warning('trying to move a file onto itself {self.absolute()}')
            if remote:  # preserve remote updating semantics even if we do not move
                # probably a bad design choice here ...
                self.remote = remote

            return 

        if target.exists() or target.is_symlink():
            raise exc.PathExistsError(f'{target} already exists!')

        if self.exists():
            os.rename(self, target)
        elif self.is_symlink():
            self.unlink()  # don't move the meta since it will break the naming insurance measure

        for other in (self.local, self.remote):
            if hasattr(self.local, '_cache'):
                delattr(self.local, '_cache')

        return self.__class__(target, **kwargs)

    def __repr__(self):
        local = repr(self.local) if self.local else 'No local??' + str(self)
        remote = (f'{self.remote.__class__.__name__}({self.remote.meta.id!r})'
                  if self.remote else str(self.id))
        return self.__class__.__name__ + ' <' + local + ' -> ' + remote + '>'


class XattrPath(AugmentedPath):
    """ pathlib Path augmented with xattr support """

    def setxattr(self, key, value, namespace=xattr.NS_USER):
        if not isinstance(value, bytes):  # checksums
            bytes_value = str(value).encode()  # too smart? force pre encoded?
        else:
            bytes_value = value

        xattr.set(self.as_posix(), key, bytes_value, namespace=namespace)

    def setxattrs(self, xattr_dict, namespace=xattr.NS_USER):
        for k, v in xattr_dict.items():
            self.setxattr(k, v, namespace=namespace)

    def getxattr(self, key, namespace=xattr.NS_USER):
        # we don't deal with types here, we just act as a dumb store
        return xattr.get(self.as_posix(), key, namespace=namespace)

    def xattrs(self, namespace=xattr.NS_USER):
        # decode keys later
        try:
            return {k:v for k, v in xattr.get_all(self.as_posix(), namespace=namespace)}
        except FileNotFoundError as e:
            raise FileNotFoundError(self) from e


class XattrCache(CachePath, XattrPath):
    xattr_prefix = None

    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta

        if self.exists():
            xattrs = self.xattrs()
            pathmeta = PathMeta.from_xattrs(self.xattrs(), self.xattr_prefix, self)
            return pathmeta
        else:
            return super().meta

    @meta.setter
    def meta(self, pathmeta):
        # sigh
        self._meta_setter(pathmeta)

    def _meta_setter(self, pathmeta, memory_only=False):
        #log.warning(f'!!!!!!!!!!!!!!!!!!!!!!!!2 {self}')
        # TODO cooperatively setting multiple different cache types?
        # do we need to use super() or something?

        if self.exists():
            if self.is_symlink():
                raise TypeError('will not write meta on symlinks! {self}')
            self.setxattrs(pathmeta.as_xattrs(self.xattr_prefix))
            if hasattr(self, '_meta'):  # prevent reading from in-memory store
                delattr(self, '_meta')

        else:
            # the glories of the inconsistencies and irreglarities of python
            # you can't setattr using super() so yes you _do_ actually have to
            # implement a setter sometimes >_<
            super()._meta_setter(pathmeta, memory_only=memory_only)


class SqliteCache(CachePath):
    """ a persistent store to back up the xattrs if they get wiped """

    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta

        #log.error('SqliteCache getter not implemented yet.')

    @meta.setter
    def meta(self, value):
        """ set meta """
        #log.error('SqliteCache setter not implemented yet. Should probably be done in bulk anyway ...')


class SymlinkCache(CachePath):

    def __init__(self, *args, **kwargs):
        if 'meta' in kwargs:
            self.meta = kwargs.pop('meta')

    @property
    def meta(self):
        if hasattr(self, '_meta'):
            return self._meta

        if self.is_symlink():
            if not self.exists():  # if a symlink exists it is something other than what we want
                #assert PurePosixPath(self.name) == self.readlink().parent.parent
                return PathMeta.from_symlink(self)
            else:
                raise exc.PathExistsError(f'Target of symlink exists!\n{self} -> {self.resolve()}')

        else:
            return super().meta

    @meta.setter
    def meta(self, pathmeta):
        if not self.exists():
            # if the path does not exist write even temporary to disk
            if self.is_symlink():
                if self.meta.id != pathmeta.id:
                    msg = ('Existing cache id does not match new id! Keeping old id.'
                           f'{self.meta.id} != {pathmeta.id}\n{self.meta}')
                    log.critical(msg)
                    return
                    raise exc.MetadataIdMismatchError(msg)

                if self.meta.size is not None and pathmeta.size is None:
                    log.error('existing metadata found, but new meta has no size so will not')
                    return

                log.debug('existing metadata found, but ids match so will update')
                self.unlink()

            symlink = PosixPath(self.local.name) / pathmeta.as_symlink()
            self.local.symlink_to(symlink)

        else:
            raise exc.PathExistsError(f'Path exists {self}')


class BlackfynnCache(XattrCache):
    xattr_prefix = 'bf'
    _backup_cache = SqliteCache
    _not_exists_cache = SymlinkCache

    @property
    def meta(self):
        #if hasattr(self, '_in_bootstrap'):
        if hasattr(self, '_meta'):  # if we have in memory we are bootstrapping so don't fiddle about
            return self._meta

        if self.exists():
            meta = super().meta
            if meta:
                return meta

        elif not self.exists() and self._not_exists_cache and self.is_symlink():
            try:
                cache = self._not_exists_cache(self)
                return cache.meta
            except exc.NoCachedMetadataError as e:
                log.warning(e)

        if self._backup_cache:
            try:
                cache = self._backup_cache(self)
                meta = cache.meta
                self.meta = meta  # repopulate primary cache from backup
                return meta

            except exc.NoCachedMetadataError as e:
                log.warning(e)

    @meta.setter
    def meta(self, pathmeta):
        self._meta_setter(pathmeta)

    def _meta_setter(self, pathmeta, memory_only=False):
        """ we need memory_only for bootstrap I think """
        if not pathmeta:
            log.warning(f'Trying to setting empty pathmeta on {self}')
            return

        if not hasattr(self, '_remote') or self._remote is None:
            self._bootstrap_meta_memory(pathmeta)

        self._bootstrap_prepare_filesystem(parents=False, fetch_data=False, size_limit_mb=0)

        if not self.exists() and self._not_exists_cache:
            cache = self._not_exists_cache(self, meta=pathmeta)
        else:
            super()._meta_setter(pathmeta)

        if self._backup_cache:
            cache = self._backup_cache(self, meta=pathmeta)

        if hasattr(self, '_meta'):
            delattr(self, '_meta')

    @classmethod
    def decode_value(cls, field, value):
        if field in ('created', 'updated'):
            # if you get unicode decode error here it is because
            # struct packing of timestamp vs isoformat are fighting
            # in xattrs pathmeta helper
            return parser.parse(value.decode())  # FIXME with timezone vs without ...

    @property
    def anchor(self):
        return self.organization

    @property
    def organization(self):
        """ organization represents a permissioning boundary
            for blackfynn, so above this we would have to know
            in advance the id and have api keys for it and the
            containing folder would have some non-blackfynn notes
            also it seems likely that the same data could appear in
            multiple different orgs, so that would mean linking locally
        
            NOTE: unlike RemotePath, CachePath should use the local file
            structure to search for the local anchor.
        """

        # FIXME in reality files can have more than one org ...
        if self.meta is not None:
            id = self.id
            if id is None:
                parent = self.parent
                if parent == self:  # we have hit a root
                    return None

                organization = parent.organization

                if organization is not None:
                    # TODO repair
                    pass
                else:
                    raise exc.NotInProjectError()

            elif id.startswith('N:organization:'):
                return self

        if self.parent:
            # we have a case of missing metadata here as well
            return self.parent.organization

    @property
    def dataset(self):
        if self.meta.id.startswith('N:dataset:'):
            return self
        elif self.parent:
            return self.parent.dataset

    @property
    def human_uri(self):
        # org /datasets/ N:dataset /files/ N:collection
        # org /datasets/ N:dataset /files/ wat? /N:package  # opaque but consistent id??
        # org /datasets/ N:dataset /viewer/ N:package
        id = self.id
        N, type, suffix = id.split(':')
        if id.startswith('N:package:'):
            prefix = '/viewer/'
        elif id.startswith('N:collection:'):
            prefix = '/files/'
        elif id.startswith('N:dataset:'):
            prefix = '/datasets/'
            return self.parent.human_uri + prefix + id
        elif id.startswith('N:organization:'):
            return 'https://app.blackfynn.io/' + id
        else:
            raise exc.UnhandledTypeError(type)

        return self.dataset.human_uri + prefix + id

    @property
    def children(self):
        """ direct children """
        # if you want the local children go on local
        # this will give us the remote children in the local context
        # going in the reverse direction with parents
        # we don't do because the parents here are already defined
        # if a file has moved on the remote we can detect that and error for now
        for child_remote in self.remote.children:
            child_cache = self / child_remote
            yield child_cache

    @property
    def rchildren(self):
        # have to express the generator to build the index
        # otherwise child.parents will not work correctly (annoying)
        for child_remote in self.remote.rchildren:
            args = child_remote._parts_relative_to(self.remote)  # usually this would just be one level
            child_cache = self._make_child(args, child_remote)
            #child_cache = self
            #child_path = self.__class__(self, *args)
            #child_path.remote = child

            yield child_cache

        # if organization
        # if dataset (usually going to be the fastest in most cases)
        # if collection (can end up very slow)
        # if package/file


class LocalPath(AugmentedPath):
    # local data about remote objects

    _cache_class = None  # must be defined by child classes
    sysid = None  # set below

    @classmethod
    def setup(cls, cache_class, remote_class_factory):
        """ call this once to bind everything together """
        cache_class.setup(cls, remote_class_factory )

    @property
    def remote(self):
        return self.cache.remote

    @remote.setter
    def remote(self, remote):
        self.cache.remote = remote

    @property
    def cache(self):
        # local can't make a cache because id doesn't know the remote id
        # but if there is an existing cache (duh) the it can try to get it
        # otherwise it will error (correctly)
        if not hasattr(self, '_cache'):
            self._cache_class(self)  # we don't have to assign here because cache does it

        return self._cache

    def find_cache_root(self):
        """ find the root of the cache tree, even if we start with skips """
        found_cache = None
        # try all the variants in case some symlinking weirdness is going on
        # TODO may want to detect and warn on that?
        for variant in set((self, self.absolute(), self.resolve())):
            for parent in variant.parents:
                try:
                    parent.cache
                    found_cache = parent
                except exc.NoCachedMetadataError as e:
                    # if we had a cache, went to the parent and lost it
                    # then we are at the root, assuming of course that
                    # there aren't sparse caches on the way up (down?) the tree
                    if found_cache is not None:
                        return found_cache

            else:
                if found_cache:
                    return found_cache

    #@property
    #def id(self):  # FIXME reuse of the name here could be confusing, though it is technically correct
        #""" THERE CAN BE ONLY ONE """
        #return self.resolve().as_posix()

    @property
    def created(self):
        self.meta.created

    @property
    def meta(self):
        st = self.stat()
        # FIXME nanos vs millis ??
        change_tuple = (fs_metadata_changed_time,
                        fs_data_modified_time) = (st.st_ctime,
                                                  st.st_mtime)

        if hasattr(self, '_meta'):
            if self.__change_tuple == change_tuple:
                return self._meta

            old_meta = self._meta  # TODO log changes?


        self.__change_tuple = change_tuple  # TODO log or no?

        updated = datetime.fromtimestamp(fs_data_modified_time)
        # these use our internal representation of timestamps
        # the choice of how to store them in xattrs, sqlite, json, etc is handled at those interfaces
        # replace with comma since it is conformant to the standard _and_
        # because it simplifies PathMeta as_path
        mode = oct(st.st_mode)
        self._meta = PathMeta(size=st.st_size,
                              created=None,
                              updated=updated,
                              checksum=self.checksum(),
                              id=self.sysid + ':' + self.as_posix(),
                              user_id=st.st_uid,
                              # keep in mind that a @meta.setter
                              # will require a coverter for non-unix uids :/
                              # man use auth is all bad :/
                              gid=st.st_gid,
                              mode=mode)

        return self._meta

    @meta.setter
    def meta(self):
        raise TypeError('Cannot set meta on LocalPath, it is a source of metadata.')

    @property
    def data(self):
        with open(self, 'rb') as f:
            while True:
                data = f.read(4096)  # TODO hinting
                if not data:
                    break

                yield data

    @data.setter
    def data(self, generator):
        meta = self.meta
        log.debug(f'writing to {self}')
        with open(self, 'wb') as f:
            for chunk in generator:
                log.debug(chunk)
                f.write(chunk)

        self.meta = meta  # glories of persisting xattrs :/

    @property
    def children(self):
        yield from self.iterdir()

    @property
    def rchildren(self):
        yield from self.rglob('*')

    def diff(self):
        pass

    def meta_to_remote(self):
        # pretty sure that we don't wan't this independent of data_to_remote
        # sort of cp vs cp -a and commit date vs author date
        raise NotImplementedError
        meta = self.meta
        # FIXME how do we invalidate cache?
        self.remote.meta = meta  # this can super duper fail

    def data_to_remote(self):
        raise NotImplementedError
        self.remote.data = self.data

    def annotations_to_remote(self):
        raise NotImplementedError
        self.remote.data = self.data

    def to_remote(self):  # push could work ...
        # FIXME in theory we could have an endpoint for each of these
        # The remote will handle that?
        # this can definitely fail
        raise NotImplementedError('need the think about how to do this without causing disasters')
        self.remote.meta = self.meta
        self.remote.data = self.data
        self.remote.annotations = self.annotations

    def checksum(self, cypher=hashlib.sha256):
        if self.is_file():
            m = cypher()
            chunk_size = 4096
            with open(self, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if chunk:
                        m.update(chunk)
                    else:
                        break

            return m.digest()


LocalPath.sysid = base64.urlsafe_b64encode(LocalPath(sysidpath()).checksum()[:16])[:-2].decode()


class Path(LocalPath):  # NOTE this is a hack to keep everything consisten
    """ An augmented path for all the various local needs of the curation process. """
    _cache_class = BlackfynnCache

    def xopen(self):
        """ open file using xdg-open """
        process = subprocess.Popen(['xdg-open', self.as_posix()],
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.STDOUT)

        return  # FIXME this doesn't seem to update anything beyond python??
        pid = process.pid
        proc = psutil.Process(pid)
        process_window = None
        while not process_window:  # FIXME ick
            sprocs = [proc] + [p for p in proc.children(recursive=True)]
            if len(sprocs) < 2:  # xdg-open needs to call at least one more thing
                sleep(.01)  # spin a bit more slowly
                continue

            wpids = [s.pid for s in sprocs][::-1]  # start deepest work up
            # FIXME expensive to create this every time ...
            disp = Display()
            root = disp.screen().root
            children = root.query_tree().children
            #names = [c.get_wm_name() for c in children if hasattr(c, 'get_wm_name')]
            try:
                by_pid = {c.get_full_property(disp.intern_atom('_NET_WM_PID'), 0):c for c in children}
            except Xlib.error.BadWindow:
                sleep(.01)  # spin a bit more slowly
                continue

            process_to_window = {p.value[0]:c for p, c in by_pid.items() if p}
            for wp in wpids:
                if wp in process_to_window:
                    process_window = process_to_window[wp]
                    break

            if process_window:
                name = process_window.get_wm_name()
                new_name = name + ' ' + self.resolve().as_posix()[-30:]
                break  # TODO search by pid is broken, but if you can find it it will work ...
                # https://github.com/jordansissel/xdotool/issues/14 some crazy bugs there
                command = ['xdotool', 'search','--pid', str(wp), 'set_window', '--name', f'"{new_name}"']
                subprocess.Popen(command,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.STDOUT)
                print(' '.join(command))
                break
                process_window.set_wm_name(new_name)
                breakpoint()
                break
            else:
                sleep(.01)  # spin a bit more slowly
