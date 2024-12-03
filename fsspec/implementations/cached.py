from __future__ import annotations
import inspect
import logging
import os
import tempfile
import time
import weakref
from shutil import rmtree
from typing import TYPE_CHECKING, Any, Callable, ClassVar
from fsspec import AbstractFileSystem, filesystem
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.compression import compr
from fsspec.core import BaseCache, MMapCache
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cache_mapper import create_cache_mapper
from fsspec.implementations.cache_metadata import CacheMetadata
from fsspec.spec import AbstractBufferedFile
from fsspec.transaction import Transaction
from fsspec.utils import infer_compression
if TYPE_CHECKING:
    from fsspec.implementations.cache_mapper import AbstractCacheMapper
logger = logging.getLogger('fsspec.cached')

class WriteCachedTransaction(Transaction):
    pass

class CachingFileSystem(AbstractFileSystem):
    """Locally caching filesystem, layer over any other FS

    This class implements chunk-wise local storage of remote files, for quick
    access after the initial download. The files are stored in a given
    directory with hashes of URLs for the filenames. If no directory is given,
    a temporary one is used, which should be cleaned up by the OS after the
    process ends. The files themselves are sparse (as implemented in
    :class:`~fsspec.caching.MMapCache`), so only the data which is accessed
    takes up space.

    Restrictions:

    - the block-size must be the same for each access of a given file, unless
      all blocks of the file have already been read
    - caching can only be applied to file-systems which produce files
      derived from fsspec.spec.AbstractBufferedFile ; LocalFileSystem is also
      allowed, for testing
    """
    protocol: ClassVar[str | tuple[str, ...]] = ('blockcache', 'cached')

    def __init__(self, target_protocol=None, cache_storage='TMP', cache_check=10, check_files=False, expiry_time=604800, target_options=None, fs=None, same_names: bool | None=None, compression=None, cache_mapper: AbstractCacheMapper | None=None, **kwargs):
        """

        Parameters
        ----------
        target_protocol: str (optional)
            Target filesystem protocol. Provide either this or ``fs``.
        cache_storage: str or list(str)
            Location to store files. If "TMP", this is a temporary directory,
            and will be cleaned up by the OS when this process ends (or later).
            If a list, each location will be tried in the order given, but
            only the last will be considered writable.
        cache_check: int
            Number of seconds between reload of cache metadata
        check_files: bool
            Whether to explicitly see if the UID of the remote file matches
            the stored one before using. Warning: some file systems such as
            HTTP cannot reliably give a unique hash of the contents of some
            path, so be sure to set this option to False.
        expiry_time: int
            The time in seconds after which a local copy is considered useless.
            Set to falsy to prevent expiry. The default is equivalent to one
            week.
        target_options: dict or None
            Passed to the instantiation of the FS, if fs is None.
        fs: filesystem instance
            The target filesystem to run against. Provide this or ``protocol``.
        same_names: bool (optional)
            By default, target URLs are hashed using a ``HashCacheMapper`` so
            that files from different backends with the same basename do not
            conflict. If this argument is ``true``, a ``BasenameCacheMapper``
            is used instead. Other cache mapper options are available by using
            the ``cache_mapper`` keyword argument. Only one of this and
            ``cache_mapper`` should be specified.
        compression: str (optional)
            To decompress on download. Can be 'infer' (guess from the URL name),
            one of the entries in ``fsspec.compression.compr``, or None for no
            decompression.
        cache_mapper: AbstractCacheMapper (optional)
            The object use to map from original filenames to cached filenames.
            Only one of this and ``same_names`` should be specified.
        """
        super().__init__(**kwargs)
        if fs is None and target_protocol is None:
            raise ValueError('Please provide filesystem instance(fs) or target_protocol')
        if not (fs is None) ^ (target_protocol is None):
            raise ValueError('Both filesystems (fs) and target_protocol may not be both given.')
        if cache_storage == 'TMP':
            tempdir = tempfile.mkdtemp()
            storage = [tempdir]
            weakref.finalize(self, self._remove_tempdir, tempdir)
        elif isinstance(cache_storage, str):
            storage = [cache_storage]
        else:
            storage = cache_storage
        os.makedirs(storage[-1], exist_ok=True)
        self.storage = storage
        self.kwargs = target_options or {}
        self.cache_check = cache_check
        self.check_files = check_files
        self.expiry = expiry_time
        self.compression = compression
        self._cache_size = None
        if same_names is not None and cache_mapper is not None:
            raise ValueError('Cannot specify both same_names and cache_mapper in CachingFileSystem.__init__')
        if cache_mapper is not None:
            self._mapper = cache_mapper
        else:
            self._mapper = create_cache_mapper(same_names if same_names is not None else False)
        self.target_protocol = target_protocol if isinstance(target_protocol, str) else fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
        self._metadata = CacheMetadata(self.storage)
        self.load_cache()
        self.fs = fs if fs is not None else filesystem(target_protocol, **self.kwargs)

        def _strip_protocol(path):
            return self.fs._strip_protocol(type(self)._strip_protocol(path))
        self._strip_protocol: Callable = _strip_protocol

    def cache_size(self):
        """Return size of cache in bytes.

        If more than one cache directory is in use, only the size of the last
        one (the writable cache directory) is returned.
        """
        if self._cache_size is None:
            self._cache_size = sum(os.path.getsize(os.path.join(self.storage[-1], f))
                                   for f in os.listdir(self.storage[-1])
                                   if os.path.isfile(os.path.join(self.storage[-1], f)))
        return self._cache_size

    def load_cache(self):
        """Read set of stored blocks from file"""
        self._metadata.load()
        self._check_cache()

    def save_cache(self):
        """Save set of stored blocks from file"""
        self._metadata.save()

    def _check_cache(self):
        """Reload caches if time elapsed or any disappeared"""
        self._metadata.check_cache(self.cache_check)

    def _check_file(self, path):
        """Is path in cache and still valid"""
        path = self._strip_protocol(path)
        details = self._metadata.get_metadata(path)
        if details:
            return self.fs.isfile(path) and not self.fs.modified(path) > details["time"]
        return False

    def clear_cache(self):
        """Remove all files and metadata from the cache

        In the case of multiple cache locations, this clears only the last one,
        which is assumed to be the read/write one.
        """
        rmtree(self.storage[-1])
        os.makedirs(self.storage[-1], exist_ok=True)
        self._metadata.clear()
        self._cache_size = None

    def clear_expired_cache(self, expiry_time=None):
        """Remove all expired files and metadata from the cache

        In the case of multiple cache locations, this clears only the last one,
        which is assumed to be the read/write one.

        Parameters
        ----------
        expiry_time: int
            The time in seconds after which a local copy is considered useless.
            If not defined the default is equivalent to the attribute from the
            file caching instantiation.
        """
        expiry_time = expiry_time or self.expiry
        now = time.time()
        for path, detail in self._metadata.items():
            if now - detail["time"] > expiry_time:
                self.pop_from_cache(path)
        self._cache_size = None

    def pop_from_cache(self, path):
        """Remove cached version of given file

        Deletes local copy of the given (remote) path. If it is found in a cache
        location which is not the last, it is assumed to be read-only, and
        raises PermissionError
        """
        path = self._strip_protocol(path)
        cached_path = self._mapper(path)
        for storage in self.storage[:-1]:
            fn = os.path.join(storage, cached_path)
            if os.path.exists(fn):
                raise PermissionError(f"Cannot delete cached file {fn}")
        fn = os.path.join(self.storage[-1], cached_path)
        if os.path.exists(fn):
            os.remove(fn)
            self._metadata.pop(path)
            self._cache_size = None

    def _open(self, path, mode='rb', block_size=None, autocommit=True, cache_options=None, **kwargs):
        """Wrap the target _open

        If the whole file exists in the cache, just open it locally and
        return that.

        Otherwise, open the file on the target FS, and make it have a mmap
        cache pointing to the location which we determine, in our cache.
        The ``blocks`` instance is shared, so as the mmap cache instance
        updates, so does the entry in our ``cached_files`` attribute.
        We monkey-patch this file, so that when it closes, we call
        ``close_and_update`` to save the state of the blocks.
        """
        path = self._strip_protocol(path)
        cache_options = cache_options or {}
        cache_path = self._mapper(path)

        if 'r' in mode:
            detail = self._check_file(path)
            if detail:
                cache_path = detail['fn']
                cache_location = detail['location']
                fn = os.path.join(cache_location, cache_path)
                return open(fn, mode)

        # File not in cache or write mode, open normally
        f = self.fs._open(path, mode=mode, block_size=block_size, **kwargs)
        if 'r' in mode:
            f.cache = BaseCache(f.size, f.blocksize, f._fetch_range, f.cache.cache_location or self.storage[-1])
            closer = f.close
            f.close = lambda: self.close_and_update(f, closer)
        return f

    def close_and_update(self, f, close):
        """Called when a file is closing, so store the set of blocks"""
        if f.closed:
            return
        close()
        path = self._strip_protocol(f.path)
        fn = self._mapper(path)
        blocks = f.cache.blocks
        location = f.cache.cache_location
        if not blocks:
            return
        self._metadata.update({path: {"fn": fn, "blocks": blocks, "time": time.time(), "location": location}})
        self.save_cache()

    def __getattribute__(self, item):
        if item in {'load_cache', '_open', 'save_cache', 'close_and_update', '__init__', '__getattribute__', '__reduce__', '_make_local_details', 'open', 'cat', 'cat_file', 'cat_ranges', 'get', 'read_block', 'tail', 'head', 'info', 'ls', 'exists', 'isfile', 'isdir', '_check_file', '_check_cache', '_mkcache', 'clear_cache', 'clear_expired_cache', 'pop_from_cache', 'local_file', '_paths_from_path', 'get_mapper', 'open_many', 'commit_many', 'hash_name', '__hash__', '__eq__', 'to_json', 'to_dict', 'cache_size', 'pipe_file', 'pipe', 'start_transaction', 'end_transaction'}:
            return lambda *args, **kw: getattr(type(self), item).__get__(self)(*args, **kw)
        if item in ['__reduce_ex__']:
            raise AttributeError
        if item in ['transaction']:
            return type(self).transaction.__get__(self)
        if item in ['_cache', 'transaction_type']:
            return getattr(type(self), item)
        if item == '__class__':
            return type(self)
        d = object.__getattribute__(self, '__dict__')
        fs = d.get('fs', None)
        if item in d:
            return d[item]
        elif fs is not None:
            if item in fs.__dict__:
                return fs.__dict__[item]
            cls = type(fs)
            m = getattr(cls, item)
            if (inspect.isfunction(m) or inspect.isdatadescriptor(m)) and (not hasattr(m, '__self__') or m.__self__ is None):
                return m.__get__(fs, cls)
            return m
        else:
            return super().__getattribute__(item)

    def __eq__(self, other):
        """Test for equality."""
        if self is other:
            return True
        if not isinstance(other, type(self)):
            return False
        return self.storage == other.storage and self.kwargs == other.kwargs and (self.cache_check == other.cache_check) and (self.check_files == other.check_files) and (self.expiry == other.expiry) and (self.compression == other.compression) and (self._mapper == other._mapper) and (self.target_protocol == other.target_protocol)

    def __hash__(self):
        """Calculate hash."""
        return hash(tuple(self.storage)) ^ hash(str(self.kwargs)) ^ hash(self.cache_check) ^ hash(self.check_files) ^ hash(self.expiry) ^ hash(self.compression) ^ hash(self._mapper) ^ hash(self.target_protocol)

class WholeFileCacheFileSystem(CachingFileSystem):
    """Caches whole remote files on first access

    This class is intended as a layer over any other file system, and
    will make a local copy of each file accessed, so that all subsequent
    reads are local. This is similar to ``CachingFileSystem``, but without
    the block-wise functionality and so can work even when sparse files
    are not allowed. See its docstring for definition of the init
    arguments.

    The class still needs access to the remote store for listing files,
    and may refresh cached files.
    """
    protocol = 'filecache'
    local_file = True

class SimpleCacheFileSystem(WholeFileCacheFileSystem):
    """Caches whole remote files on first access

    This class is intended as a layer over any other file system, and
    will make a local copy of each file accessed, so that all subsequent
    reads are local. This implementation only copies whole files, and
    does not keep any metadata about the download time or file details.
    It is therefore safer to use in multi-threaded/concurrent situations.

    This is the only of the caching filesystems that supports write: you will
    be given a real local open file, and upon close and commit, it will be
    uploaded to the target filesystem; the writability or the target URL is
    not checked until that time.

    """
    protocol = 'simplecache'
    local_file = True
    transaction_type = WriteCachedTransaction

    def __init__(self, **kwargs):
        kw = kwargs.copy()
        for key in ['cache_check', 'expiry_time', 'check_files']:
            kw[key] = False
        super().__init__(**kw)
        for storage in self.storage:
            if not os.path.exists(storage):
                os.makedirs(storage, exist_ok=True)

class LocalTempFile:
    """A temporary local file, which will be uploaded on commit"""

    def __init__(self, fs, path, fn, mode='wb', autocommit=True, seek=0, **kwargs):
        self.fn = fn
        self.fh = open(fn, mode)
        self.mode = mode
        if seek:
            self.fh.seek(seek)
        self.path = path
        self.size = None
        self.fs = fs
        self.closed = False
        self.autocommit = autocommit
        self.kwargs = kwargs

    def __reduce__(self):
        return (LocalTempFile, (self.fs, self.path, self.fn, 'r+b', self.autocommit, self.tell()))

    def __enter__(self):
        return self.fh

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __repr__(self) -> str:
        return f'LocalTempFile: {self.path}'

    def __getattr__(self, item):
        return getattr(self.fh, item)
