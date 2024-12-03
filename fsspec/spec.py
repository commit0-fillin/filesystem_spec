from __future__ import annotations
import io
import json
import logging
import os
import threading
import warnings
import weakref
from errno import ESPIPE
from glob import has_magic
from hashlib import sha256
from typing import Any, ClassVar, Dict, Tuple
from .callbacks import DEFAULT_CALLBACK
from .config import apply_config, conf
from .dircache import DirCache
from .transaction import Transaction
from .utils import _unstrip_protocol, glob_translate, isfilelike, other_paths, read_block, stringify_path, tokenize
logger = logging.getLogger('fsspec')

class _Cached(type):
    """
    Metaclass for caching file system instances.

    Notes
    -----
    Instances are cached according to

    * The values of the class attributes listed in `_extra_tokenize_attributes`
    * The arguments passed to ``__init__``.

    This creates an additional reference to the filesystem, which prevents the
    filesystem from being garbage collected when all *user* references go away.
    A call to the :meth:`AbstractFileSystem.clear_instance_cache` must *also*
    be made for a filesystem instance to be garbage collected.
    """

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if conf.get('weakref_instance_cache'):
            cls._cache = weakref.WeakValueDictionary()
        else:
            cls._cache = {}
        cls._pid = os.getpid()

    def __call__(cls, *args, **kwargs):
        kwargs = apply_config(cls, kwargs)
        extra_tokens = tuple((getattr(cls, attr, None) for attr in cls._extra_tokenize_attributes))
        token = tokenize(cls, cls._pid, threading.get_ident(), *args, *extra_tokens, **kwargs)
        skip = kwargs.pop('skip_instance_cache', False)
        if os.getpid() != cls._pid:
            cls._cache.clear()
            cls._pid = os.getpid()
        if not skip and cls.cachable and (token in cls._cache):
            cls._latest = token
            return cls._cache[token]
        else:
            obj = super().__call__(*args, **kwargs)
            obj._fs_token_ = token
            obj.storage_args = args
            obj.storage_options = kwargs
            if obj.async_impl and obj.mirror_sync_methods:
                from .asyn import mirror_sync_methods
                mirror_sync_methods(obj)
            if cls.cachable and (not skip):
                cls._latest = token
                cls._cache[token] = obj
            return obj

class AbstractFileSystem(metaclass=_Cached):
    """
    An abstract super-class for pythonic file-systems

    Implementations are expected to be compatible with or, better, subclass
    from here.
    """
    cachable = True
    _cached = False
    blocksize = 2 ** 22
    sep = '/'
    protocol: ClassVar[str | tuple[str, ...]] = 'abstract'
    _latest = None
    async_impl = False
    mirror_sync_methods = False
    root_marker = ''
    transaction_type = Transaction
    _extra_tokenize_attributes = ()
    storage_args: Tuple[Any, ...]
    storage_options: Dict[str, Any]

    def __init__(self, *args, **storage_options):
        """Create and configure file-system instance

        Instances may be cachable, so if similar enough arguments are seen
        a new instance is not required. The token attribute exists to allow
        implementations to cache instances if they wish.

        A reasonable default should be provided if there are no arguments.

        Subclasses should call this method.

        Parameters
        ----------
        use_listings_cache, listings_expiry_time, max_paths:
            passed to ``DirCache``, if the implementation supports
            directory listing caching. Pass use_listings_cache=False
            to disable such caching.
        skip_instance_cache: bool
            If this is a cachable implementation, pass True here to force
            creating a new instance even if a matching instance exists, and prevent
            storing this instance.
        asynchronous: bool
        loop: asyncio-compatible IOLoop or None
        """
        if self._cached:
            return
        self._cached = True
        self._intrans = False
        self._transaction = None
        self._invalidated_caches_in_transaction = []
        self.dircache = DirCache(**storage_options)
        if storage_options.pop('add_docs', None):
            warnings.warn('add_docs is no longer supported.', FutureWarning)
        if storage_options.pop('add_aliases', None):
            warnings.warn('add_aliases has been removed.', FutureWarning)
        self._fs_token_ = None

    @property
    def fsid(self):
        """Persistent filesystem id that can be used to compare filesystems
        across sessions.
        """
        return hash(self)

    def __dask_tokenize__(self):
        return self._fs_token

    def __hash__(self):
        return int(self._fs_token, 16)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self._fs_token == other._fs_token

    def __reduce__(self):
        return (make_instance, (type(self), self.storage_args, self.storage_options))

    @classmethod
    def _strip_protocol(cls, path):
        """Turn path from fully-qualified to file-system-specific

        May require FS-specific handling, e.g., for relative paths or links.
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        if path.startswith(cls.protocol + '://'):
            return path[len(cls.protocol) + 3:]
        return path

    def unstrip_protocol(self, name: str) -> str:
        """Format FS-specific path to generic, including protocol"""
        if isinstance(self.protocol, tuple):
            protocol = self.protocol[0]
        else:
            protocol = self.protocol
        return f"{protocol}://{name}"

    @staticmethod
    def _get_kwargs_from_urls(path):
        """If kwargs can be encoded in the paths, extract them here

        This should happen before instantiation of the class; incoming paths
        then should be amended to strip the options in methods.

        Examples may look like an sftp path "sftp://user@host:/my/path", where
        the user and host should become kwargs and later get stripped.
        """
        return {}

    @classmethod
    def current(cls):
        """Return the most recently instantiated FileSystem

        If no instance has been created, then create one with defaults
        """
        if cls._latest is None:
            return cls()
        return cls._cache[cls._latest]

    @property
    def transaction(self):
        """A context within which files are committed together upon exit

        Requires the file class to implement `.commit()` and `.discard()`
        for the normal and exception cases.
        """
        pass

    def start_transaction(self):
        """Begin write transaction for deferring files, non-context version"""
        pass

    def end_transaction(self):
        """Finish write transaction, non-context version"""
        pass

    def invalidate_cache(self, path=None):
        """
        Discard any cached directory information

        Parameters
        ----------
        path: string or None
            If None, clear all listings cached else listings at or under given
            path.
        """
        pass

    def mkdir(self, path, create_parents=True, **kwargs):
        """
        Create directory entry at path

        For systems that don't have true directories, may create an for
        this instance only and not touch the real filesystem

        Parameters
        ----------
        path: str
            location
        create_parents: bool
            if True, this is equivalent to ``makedirs``
        kwargs:
            may be permissions, etc.
        """
        pass

    def makedirs(self, path, exist_ok=False):
        """Recursively make directories

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If False, will error if the target already exists
        """
        pass

    def rmdir(self, path):
        """Remove a directory, if empty"""
        pass

    def ls(self, path, detail=True, **kwargs):
        """List objects at path.

        This should include subdirectories and files at that location. The
        difference between a file and a directory must be clear when details
        are requested.

        The specific keys, or perhaps a FileInfo class, or similar, is TBD,
        but must be consistent across implementations.
        Must include:

        - full path to the entry (without protocol)
        - size of the entry, in bytes. If the value cannot be determined, will
          be ``None``.
        - type of entry, "file", "directory" or other

        Additional information
        may be present, appropriate to the file-system, e.g., generation,
        checksum, etc.

        May use refresh=True|False to allow use of self._ls_from_cache to
        check for a saved listing and avoid calling the backend. This would be
        common where listing may be expensive.

        Parameters
        ----------
        path: str
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        kwargs: may have additional backend-specific options, such as version
            information

        Returns
        -------
        List of strings if detail is False, or list of directory information
        dicts if detail is True.
        """
        pass

    def _ls_from_cache(self, path):
        """Check cache for listing

        Returns listing, if found (may be empty list for a directly that exists
        but contains nothing), None if not in cache.
        """
        pass

    def walk(self, path, maxdepth=None, topdown=True, on_error='omit', **kwargs):
        """Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        When topdown is True, the caller can modify the dirnames list in-place (perhaps
        using del or slice assignment), and walk() will
        only recurse into the subdirectories whose names remain in dirnames;
        this can be used to prune the search, impose a specific order of visiting,
        or even to inform walk() about directories the caller creates or renames before
        it resumes walk() again.
        Modifying dirnames when topdown is False has no effect. (see os.walk)

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.
        topdown: bool (True)
            Whether to walk the directory tree from the top downwards or from
            the bottom upwards.
        on_error: "omit", "raise", a collable
            if omit (default), path with exception will simply be empty;
            If raise, an underlying exception will be raised;
            if callable, it will be called with a single OSError instance as argument
        kwargs: passed to ``ls``
        """
        pass

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        """List all files below path.

        Like posix ``find`` command without conditions

        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.
        """
        pass

    def du(self, path, total=True, maxdepth=None, withdirs=False, **kwargs):
        """Space used by files and optionally directories within a path

        Directory size does not include the size of its contents.

        Parameters
        ----------
        path: str
        total: bool
            Whether to sum all the file sizes
        maxdepth: int or None
            Maximum number of directory levels to descend, None for unlimited.
        withdirs: bool
            Whether to include directory paths in the output.
        kwargs: passed to ``find``

        Returns
        -------
        Dict of {path: size} if total=False, or int otherwise, where numbers
        refer to bytes used.
        """
        pass

    def glob(self, path, maxdepth=None, **kwargs):
        """
        Find files by glob-matching.

        If the path ends with '/', only folders are returned.

        We support ``"**"``,
        ``"?"`` and ``"[..]"``. We do not support ^ for pattern negation.

        The `maxdepth` option is applied on the first `**` found in the path.

        kwargs are passed to ``ls``.
        """
        pass

    def exists(self, path, **kwargs):
        """Is there a file at the given path"""
        pass

    def lexists(self, path, **kwargs):
        """If there is a file at the given path (including
        broken links)"""
        pass

    def info(self, path, **kwargs):
        """Give details of entry at path

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.

        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        pass

    def checksum(self, path):
        """Unique value for current version of file

        If the checksum is the same from one moment to another, the contents
        are guaranteed to be the same. If the checksum changes, the contents
        *might* have changed.

        This should normally be overridden; default will probably capture
        creation/modification timestamp (which would be good) or maybe
        access timestamp (which would be bad)
        """
        pass

    def size(self, path):
        """Size in bytes of file"""
        pass

    def sizes(self, paths):
        """Size in bytes of each file in a list of paths"""
        pass

    def isdir(self, path):
        """Is this entry directory-like?"""
        pass

    def isfile(self, path):
        """Is this entry file-like?"""
        pass

    def read_text(self, path, encoding=None, errors=None, newline=None, **kwargs):
        """Get the contents of the file as a string.

        Parameters
        ----------
        path: str
            URL of file on this filesystems
        encoding, errors, newline: same as `open`.
        """
        pass

    def write_text(self, path, value, encoding=None, errors=None, newline=None, **kwargs):
        """Write the text to the given file.

        An existing file will be overwritten.

        Parameters
        ----------
        path: str
            URL of file on this filesystems
        value: str
            Text to write.
        encoding, errors, newline: same as `open`.
        """
        pass

    def cat_file(self, path, start=None, end=None, **kwargs):
        """Get the content of a file

        Parameters
        ----------
        path: URL of file on this filesystems
        start, end: int
            Bytes limits of the read. If negative, backwards from end,
            like usual python slices. Either can be None for start or
            end of file, respectively
        kwargs: passed to ``open()``.
        """
        pass

    def pipe_file(self, path, value, **kwargs):
        """Set the bytes of given file"""
        pass

    def pipe(self, path, value=None, **kwargs):
        """Put value into path

        (counterpart to ``cat``)

        Parameters
        ----------
        path: string or dict(str, bytes)
            If a string, a single remote location to put ``value`` bytes; if a dict,
            a mapping of {path: bytesvalue}.
        value: bytes, optional
            If using a single path, these are the bytes to put there. Ignored if
            ``path`` is a dict
        """
        pass

    def cat_ranges(self, paths, starts, ends, max_gap=None, on_error='return', **kwargs):
        """Get the contents of byte ranges from one or more files

        Parameters
        ----------
        paths: list
            A list of of filepaths on this filesystems
        starts, ends: int or list
            Bytes limits of the read. If using a single int, the same value will be
            used to read all the specified files.
        """
        pass

    def cat(self, path, recursive=False, on_error='raise', **kwargs):
        """Fetch (potentially multiple) paths' contents

        Parameters
        ----------
        recursive: bool
            If True, assume the path(s) are directories, and get all the
            contained files
        on_error : "raise", "omit", "return"
            If raise, an underlying exception will be raised (converted to KeyError
            if the type is in self.missing_exceptions); if omit, keys with exception
            will simply not be included in the output; if "return", all keys are
            included in the output, but the value will be bytes or an exception
            instance.
        kwargs: passed to cat_file

        Returns
        -------
        dict of {path: contents} if there are multiple paths
        or the path has been otherwise expanded
        """
        pass

    def get_file(self, rpath, lpath, callback=DEFAULT_CALLBACK, outfile=None, **kwargs):
        """Copy single remote file to local"""
        pass

    def get(self, rpath, lpath, recursive=False, callback=DEFAULT_CALLBACK, maxdepth=None, **kwargs):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        Calls get_file for each source.
        """
        pass

    def put_file(self, lpath, rpath, callback=DEFAULT_CALLBACK, **kwargs):
        """Copy single file to remote"""
        pass

    def put(self, lpath, rpath, recursive=False, callback=DEFAULT_CALLBACK, maxdepth=None, **kwargs):
        """Copy file(s) from local.

        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        Calls put_file for each source.
        """
        pass

    def head(self, path, size=1024):
        """Get the first ``size`` bytes from file"""
        pass

    def tail(self, path, size=1024):
        """Get the last ``size`` bytes from file"""
        pass

    def copy(self, path1, path2, recursive=False, maxdepth=None, on_error=None, **kwargs):
        """Copy within two locations in the filesystem

        on_error : "raise", "ignore"
            If raise, any not-found exceptions will be raised; if ignore any
            not-found exceptions will cause the path to be skipped; defaults to
            raise unless recursive is true, where the default is ignore
        """
        pass

    def expand_path(self, path, recursive=False, maxdepth=None, **kwargs):
        """Turn one or more globs or directories into a list of all matching paths
        to files or directories.

        kwargs are passed to ``glob`` or ``find``, which may in turn call ``ls``
        """
        pass

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        """Move file(s) from one location to another"""
        pass

    def rm_file(self, path):
        """Delete a file"""
        pass

    def _rm(self, path):
        """Delete one file"""
        pass

    def rm(self, path, recursive=False, maxdepth=None):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """
        pass

    def _open(self, path, mode='rb', block_size=None, autocommit=True, cache_options=None, **kwargs):
        """Return raw bytes-mode file-like from the file-system"""
        pass

    def open(self, path, mode='rb', block_size=None, cache_options=None, compression=None, **kwargs):
        """
        Return a file-like object from the filesystem

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        path: str
            Target file
        mode: str like 'rb', 'w'
            See builtin ``open()``
        block_size: int
            Some indication of buffering - this is a value in bytes
        cache_options : dict, optional
            Extra arguments to pass through to the cache.
        compression: string or None
            If given, open file using compression codec. Can either be a compression
            name (a key in ``fsspec.compression.compr``) or "infer" to guess the
            compression from the filename suffix.
        encoding, errors, newline: passed on to TextIOWrapper for text mode
        """
        pass

    def touch(self, path, truncate=True, **kwargs):
        """Create empty file, or update timestamp

        Parameters
        ----------
        path: str
            file location
        truncate: bool
            If True, always set file size to 0; if False, update timestamp and
            leave file unchanged, if backend allows this
        """
        pass

    def ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        pass

    def read_block(self, fn, offset, length, delimiter=None):
        """Read a block of bytes from

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read. If None, read to end.
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> fs.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> fs.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> fs.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        :func:`fsspec.utils.read_block`
        """
        pass

    def to_json(self, *, include_password: bool=True) -> str:
        """
        JSON representation of this filesystem instance.

        Parameters
        ----------
        include_password: bool, default True
            Whether to include the password (if any) in the output.

        Returns
        -------
        JSON string with keys ``cls`` (the python location of this class),
        protocol (text name of this class's protocol, first one in case of
        multiple), ``args`` (positional args, usually empty), and all other
        keyword arguments as their own keys.

        Warnings
        --------
        Serialized filesystems may contain sensitive information which have been
        passed to the constructor, such as passwords and tokens. Make sure you
        store and send them in a secure environment!
        """
        pass

    @staticmethod
    def from_json(blob: str) -> AbstractFileSystem:
        """
        Recreate a filesystem instance from JSON representation.

        See ``.to_json()`` for the expected structure of the input.

        Parameters
        ----------
        blob: str

        Returns
        -------
        file system instance, not necessarily of this particular class.

        Warnings
        --------
        This can import arbitrary modules (as determined by the ``cls`` key).
        Make sure you haven't installed any modules that may execute malicious code
        at import time.
        """
        pass

    def to_dict(self, *, include_password: bool=True) -> Dict[str, Any]:
        """
        JSON-serializable dictionary representation of this filesystem instance.

        Parameters
        ----------
        include_password: bool, default True
            Whether to include the password (if any) in the output.

        Returns
        -------
        Dictionary with keys ``cls`` (the python location of this class),
        protocol (text name of this class's protocol, first one in case of
        multiple), ``args`` (positional args, usually empty), and all other
        keyword arguments as their own keys.

        Warnings
        --------
        Serialized filesystems may contain sensitive information which have been
        passed to the constructor, such as passwords and tokens. Make sure you
        store and send them in a secure environment!
        """
        pass

    @staticmethod
    def from_dict(dct: Dict[str, Any]) -> AbstractFileSystem:
        """
        Recreate a filesystem instance from dictionary representation.

        See ``.to_dict()`` for the expected structure of the input.

        Parameters
        ----------
        dct: Dict[str, Any]

        Returns
        -------
        file system instance, not necessarily of this particular class.

        Warnings
        --------
        This can import arbitrary modules (as determined by the ``cls`` key).
        Make sure you haven't installed any modules that may execute malicious code
        at import time.
        """
        pass

    def _get_pyarrow_filesystem(self):
        """
        Make a version of the FS instance which will be acceptable to pyarrow
        """
        pass

    def get_mapper(self, root='', check=False, create=False, missing_exceptions=None):
        """Create key/value store based on this file-system

        Makes a MutableMapping interface to the FS at the given root path.
        See ``fsspec.mapping.FSMap`` for further details.
        """
        pass

    @classmethod
    def clear_instance_cache(cls):
        """
        Clear the cache of filesystem instances.

        Notes
        -----
        Unless overridden by setting the ``cachable`` class attribute to False,
        the filesystem class stores a reference to newly created instances. This
        prevents Python's normal rules around garbage collection from working,
        since the instances refcount will not drop to zero until
        ``clear_instance_cache`` is called.
        """
        pass

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        pass

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        pass

    def read_bytes(self, path, start=None, end=None, **kwargs):
        """Alias of `AbstractFileSystem.cat_file`."""
        pass

    def write_bytes(self, path, value, **kwargs):
        """Alias of `AbstractFileSystem.pipe_file`."""
        pass

    def makedir(self, path, create_parents=True, **kwargs):
        """Alias of `AbstractFileSystem.mkdir`."""
        pass

    def mkdirs(self, path, exist_ok=False):
        """Alias of `AbstractFileSystem.makedirs`."""
        pass

    def listdir(self, path, detail=True, **kwargs):
        """Alias of `AbstractFileSystem.ls`."""
        pass

    def cp(self, path1, path2, **kwargs):
        """Alias of `AbstractFileSystem.copy`."""
        pass

    def move(self, path1, path2, **kwargs):
        """Alias of `AbstractFileSystem.mv`."""
        pass

    def stat(self, path, **kwargs):
        """Alias of `AbstractFileSystem.info`."""
        pass

    def disk_usage(self, path, total=True, maxdepth=None, **kwargs):
        """Alias of `AbstractFileSystem.du`."""
        pass

    def rename(self, path1, path2, **kwargs):
        """Alias of `AbstractFileSystem.mv`."""
        pass

    def delete(self, path, recursive=False, maxdepth=None):
        """Alias of `AbstractFileSystem.rm`."""
        pass

    def upload(self, lpath, rpath, recursive=False, **kwargs):
        """Alias of `AbstractFileSystem.put`."""
        pass

    def download(self, rpath, lpath, recursive=False, **kwargs):
        """Alias of `AbstractFileSystem.get`."""
        pass

    def sign(self, path, expiration=100, **kwargs):
        """Create a signed URL representing the given path

        Some implementations allow temporary URLs to be generated, as a
        way of delegating credentials.

        Parameters
        ----------
        path : str
             The path on the filesystem
        expiration : int
            Number of seconds to enable the URL for (if supported)

        Returns
        -------
        URL : str
            The signed URL

        Raises
        ------
        NotImplementedError : if method is not implemented for a filesystem
        """
        pass

class AbstractBufferedFile(io.IOBase):
    """Convenient class to derive from to provide buffering

    In the case that the backend does not provide a pythonic file-like object
    already, this class contains much of the logic to build one. The only
    methods that need to be overridden are ``_upload_chunk``,
    ``_initiate_upload`` and ``_fetch_range``.
    """
    DEFAULT_BLOCK_SIZE = 5 * 2 ** 20
    _details = None

    def __init__(self, fs, path, mode='rb', block_size='default', autocommit=True, cache_type='readahead', cache_options=None, size=None, **kwargs):
        """
        Template for files with buffered reading and writing

        Parameters
        ----------
        fs: instance of FileSystem
        path: str
            location in file-system
        mode: str
            Normal file modes. Currently only 'wb', 'ab' or 'rb'. Some file
            systems may be read-only, and some may not support append.
        block_size: int
            Buffer size for reading or writing, 'default' for class default
        autocommit: bool
            Whether to write to final destination; may only impact what
            happens when file is being closed.
        cache_type: {"readahead", "none", "mmap", "bytes"}, default "readahead"
            Caching policy in read mode. See the definitions in ``core``.
        cache_options : dict
            Additional options passed to the constructor for the cache specified
            by `cache_type`.
        size: int
            If given and in read mode, suppressed having to look up the file size
        kwargs:
            Gets stored as self.kwargs
        """
        from .core import caches
        self.path = path
        self.fs = fs
        self.mode = mode
        self.blocksize = self.DEFAULT_BLOCK_SIZE if block_size in ['default', None] else block_size
        self.loc = 0
        self.autocommit = autocommit
        self.end = None
        self.start = None
        self.closed = False
        if cache_options is None:
            cache_options = {}
        if 'trim' in kwargs:
            warnings.warn("Passing 'trim' to control the cache behavior has been deprecated. Specify it within the 'cache_options' argument instead.", FutureWarning)
            cache_options['trim'] = kwargs.pop('trim')
        self.kwargs = kwargs
        if mode not in {'ab', 'rb', 'wb'}:
            raise NotImplementedError('File mode not supported')
        if mode == 'rb':
            if size is not None:
                self.size = size
            else:
                self.size = self.details['size']
            self.cache = caches[cache_type](self.blocksize, self._fetch_range, self.size, **cache_options)
        else:
            self.buffer = io.BytesIO()
            self.offset = None
            self.forced = False
            self.location = None

    def __hash__(self):
        if 'w' in self.mode:
            return id(self)
        else:
            return int(tokenize(self.details), 16)

    def __eq__(self, other):
        """Files are equal if they have the same checksum, only in read mode"""
        if self is other:
            return True
        return isinstance(other, type(self)) and self.mode == 'rb' and (other.mode == 'rb') and (hash(self) == hash(other))

    def commit(self):
        """Move from temp to final destination"""
        pass

    def discard(self):
        """Throw away temporary file"""
        pass

    def info(self):
        """File information about this path"""
        pass

    def tell(self):
        """Current file location"""
        pass

    def seek(self, loc, whence=0):
        """Set current file location

        Parameters
        ----------
        loc: int
            byte location
        whence: {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        pass

    def write(self, data):
        """
        Write data to buffer.

        Buffer only sent on flush() or if buffer is greater than
        or equal to blocksize.

        Parameters
        ----------
        data: bytes
            Set of bytes to be written.
        """
        pass

    def flush(self, force=False):
        """
        Write buffered data to backend store.

        Writes the current buffer, if it is larger than the block-size, or if
        the file is being closed.

        Parameters
        ----------
        force: bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be. Disallows further writing to this file.
        """
        pass

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        pass

    def _initiate_upload(self):
        """Create remote file/upload"""
        pass

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        pass

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary

        Parameters
        ----------
        length: int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        pass

    def readinto(self, b):
        """mirrors builtin file's readinto method

        https://docs.python.org/3/library/io.html#io.RawIOBase.readinto
        """
        pass

    def readuntil(self, char=b'\n', blocks=None):
        """Return data between current position and first occurrence of char

        char is included in the output, except if the end of the tile is
        encountered first.

        Parameters
        ----------
        char: bytes
            Thing to find
        blocks: None or int
            How much to read in each go. Defaults to file blocksize - which may
            mean a new read on every call.
        """
        pass

    def readline(self):
        """Read until first occurrence of newline character

        Note that, because of character encoding, this is not necessarily a
        true line ending.
        """
        pass

    def __next__(self):
        out = self.readline()
        if out:
            return out
        raise StopIteration

    def __iter__(self):
        return self

    def readlines(self):
        """Return all data, split by the newline character"""
        pass

    def close(self):
        """Close file

        Finalizes writes, discards cache
        """
        pass

    def readable(self):
        """Whether opened for reading"""
        pass

    def seekable(self):
        """Whether is seekable (only in read mode)"""
        pass

    def writable(self):
        """Whether opened for writing"""
        pass

    def __del__(self):
        if not self.closed:
            self.close()

    def __str__(self):
        return f'<File-like object {type(self.fs).__name__}, {self.path}>'
    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
