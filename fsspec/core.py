from __future__ import annotations
import io
import logging
import os
import re
from glob import has_magic
from pathlib import Path
from fsspec.caching import BaseCache, BlockCache, BytesCache, MMapCache, ReadAheadCache, caches
from fsspec.compression import compr
from fsspec.config import conf
from fsspec.registry import filesystem, get_filesystem_class
from fsspec.utils import _unstrip_protocol, build_name_function, infer_compression, stringify_path
logger = logging.getLogger('fsspec')

class OpenFile:
    """
    File-like object to be used in a context

    Can layer (buffered) text-mode and compression over any file-system, which
    are typically binary-only.

    These instances are safe to serialize, as the low-level file object
    is not created until invoked using ``with``.

    Parameters
    ----------
    fs: FileSystem
        The file system to use for opening the file. Should be a subclass or duck-type
        with ``fsspec.spec.AbstractFileSystem``
    path: str
        Location to open
    mode: str like 'rb', optional
        Mode of the opened file
    compression: str or None, optional
        Compression to apply
    encoding: str or None, optional
        The encoding to use if opened in text mode.
    errors: str or None, optional
        How to handle encoding errors if opened in text mode.
    newline: None or str
        Passed to TextIOWrapper in text mode, how to handle line endings.
    autoopen: bool
        If True, calls open() immediately. Mostly used by pickle
    pos: int
        If given and autoopen is True, seek to this location immediately
    """

    def __init__(self, fs, path, mode='rb', compression=None, encoding=None, errors=None, newline=None):
        self.fs = fs
        self.path = path
        self.mode = mode
        self.compression = get_compression(path, compression)
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.fobjects = []

    def __reduce__(self):
        return (OpenFile, (self.fs, self.path, self.mode, self.compression, self.encoding, self.errors, self.newline))

    def __repr__(self):
        return f"<OpenFile '{self.path}'>"

    def __enter__(self):
        mode = self.mode.replace('t', '').replace('b', '') + 'b'
        try:
            f = self.fs.open(self.path, mode=mode)
        except FileNotFoundError as e:
            if has_magic(self.path):
                raise FileNotFoundError("%s not found. The URL contains glob characters: you maybe needed\nto pass expand=True in fsspec.open() or the storage_options of \nyour library. You can also set the config value 'open_expand'\nbefore import, or fsspec.core.DEFAULT_EXPAND at runtime, to True.", self.path) from e
            raise
        self.fobjects = [f]
        if self.compression is not None:
            compress = compr[self.compression]
            f = compress(f, mode=mode[0])
            self.fobjects.append(f)
        if 'b' not in self.mode:
            f = PickleableTextIOWrapper(f, encoding=self.encoding, errors=self.errors, newline=self.newline)
            self.fobjects.append(f)
        return self.fobjects[-1]

    def __exit__(self, *args):
        self.close()

    def open(self):
        """Materialise this as a real open file without context

        The OpenFile object should be explicitly closed to avoid enclosed file
        instances persisting. You must, therefore, keep a reference to the OpenFile
        during the life of the file-like it generates.
        """
        return self.__enter__()

    def close(self):
        """Close all encapsulated file objects"""
        for f in reversed(self.fobjects):
            f.close()
        self.fobjects.clear()

class OpenFiles(list):
    """List of OpenFile instances

    Can be used in a single context, which opens and closes all of the
    contained files. Normal list access to get the elements works as
    normal.

    A special case is made for caching filesystems - the files will
    be down/uploaded together at the start or end of the context, and
    this may happen concurrently, if the target filesystem supports it.
    """

    def __init__(self, *args, mode='rb', fs=None):
        self.mode = mode
        self.fs = fs
        self.files = []
        super().__init__(*args)

    def __enter__(self):
        if self.fs is None:
            raise ValueError('Context has already been used')
        fs = self.fs
        while True:
            if hasattr(fs, 'open_many'):
                self.files = fs.open_many(self)
                return self.files
            if hasattr(fs, 'fs') and fs.fs is not None:
                fs = fs.fs
            else:
                break
        return [s.__enter__() for s in self]

    def __exit__(self, *args):
        fs = self.fs
        [s.__exit__(*args) for s in self]
        if 'r' not in self.mode:
            while True:
                if hasattr(fs, 'open_many'):
                    fs.commit_many(self.files)
                    return
                if hasattr(fs, 'fs') and fs.fs is not None:
                    fs = fs.fs
                else:
                    break

    def __getitem__(self, item):
        out = super().__getitem__(item)
        if isinstance(item, slice):
            return OpenFiles(out, mode=self.mode, fs=self.fs)
        return out

    def __repr__(self):
        return f'<List of {len(self)} OpenFile instances>'

def open_files(urlpath, mode='rb', compression=None, encoding='utf8', errors=None, name_function=None, num=1, protocol=None, newline=None, auto_mkdir=True, expand=True, **kwargs):
    """Given a path or paths, return a list of ``OpenFile`` objects.

    For writing, a str path must contain the "*" character, which will be filled
    in by increasing numbers, e.g., "part*" ->  "part1", "part2" if num=2.

    For either reading or writing, can instead provide explicit list of paths.

    Parameters
    ----------
    urlpath: string or list
        Absolute or relative filepath(s). Prefix with a protocol like ``s3://``
        to read from alternative filesystems. To read from multiple files you
        can pass a globstring or a list of paths, with the caveat that they
        must all have the same protocol.
    mode: 'rb', 'wt', etc.
    compression: string or None
        If given, open file using compression codec. Can either be a compression
        name (a key in ``fsspec.compression.compr``) or "infer" to guess the
        compression from the filename suffix.
    encoding: str
        For text mode only
    errors: None or str
        Passed to TextIOWrapper in text mode
    name_function: function or None
        if opening a set of files for writing, those files do not yet exist,
        so we need to generate their names by formatting the urlpath for
        each sequence number
    num: int [1]
        if writing mode, number of files we expect to create (passed to
        name+function)
    protocol: str or None
        If given, overrides the protocol found in the URL.
    newline: bytes or None
        Used for line terminator in text mode. If None, uses system default;
        if blank, uses no translation.
    auto_mkdir: bool (True)
        If in write mode, this will ensure the target directory exists before
        writing, by calling ``fs.mkdirs(exist_ok=True)``.
    expand: bool
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> files = open_files('2015-*-*.csv')  # doctest: +SKIP
    >>> files = open_files(
    ...     's3://bucket/2015-*-*.csv.gz', compression='gzip'
    ... )  # doctest: +SKIP

    Returns
    -------
    An ``OpenFiles`` instance, which is a list of ``OpenFile`` objects that can
    be used as a single context

    Notes
    -----
    For a full list of the available protocols and the implementations that
    they map across to see the latest online documentation:

    - For implementations built into ``fsspec`` see
      https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations
    - For implementations in separate packages see
      https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations
    """
    fs, fs_token, paths = get_fs_token_paths(urlpath, mode, num, protocol, expand=expand, name_function=name_function, storage_options=kwargs)
    
    if compression == "infer":
        compression = infer_compression(paths[0])
    
    openfiles = [OpenFile(fs, path, mode=mode, compression=compression, encoding=encoding, errors=errors, newline=newline) for path in paths]
    
    if auto_mkdir and 'w' in mode:
        parents = {fs._parent(path) for path in paths}
        [fs.makedirs(parent, exist_ok=True) for parent in parents]
    
    return OpenFiles(openfiles, mode=mode, fs=fs)

def url_to_fs(url, **kwargs):
    """
    Turn fully-qualified and potentially chained URL into filesystem instance

    Parameters
    ----------
    url : str
        The fsspec-compatible URL
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Returns
    -------
    filesystem : FileSystem
        The new filesystem discovered from ``url`` and created with
        ``**kwargs``.
    urlpath : str
        The file-systems-specific URL for ``url``.
    """
    chain = _unstrip_protocol(url)
    
    if len(chain) == 1:
        protocol, urlpath = chain[0]
        fs = filesystem(protocol, **kwargs)
        return fs, urlpath
    
    else:
        protocol, urlpath = chain.pop(0)
        fs = filesystem(protocol, **kwargs)
        
        while chain:
            protocol, urlpath = chain.pop(0)
            fs = fs.open(urlpath, protocol=protocol)
        
        return fs, urlpath
DEFAULT_EXPAND = conf.get('open_expand', False)

def open(urlpath, mode='rb', compression=None, encoding='utf8', errors=None, protocol=None, newline=None, expand=None, **kwargs):
    """Given a path or paths, return one ``OpenFile`` object.

    Parameters
    ----------
    urlpath: string or list
        Absolute or relative filepath. Prefix with a protocol like ``s3://``
        to read from alternative filesystems. Should not include glob
        character(s).
    mode: 'rb', 'wt', etc.
    compression: string or None
        If given, open file using compression codec. Can either be a compression
        name (a key in ``fsspec.compression.compr``) or "infer" to guess the
        compression from the filename suffix.
    encoding: str
        For text mode only
    errors: None or str
        Passed to TextIOWrapper in text mode
    protocol: str or None
        If given, overrides the protocol found in the URL.
    newline: bytes or None
        Used for line terminator in text mode. If None, uses system default;
        if blank, uses no translation.
    expand: bool or None
        Whether to regard file paths containing special glob characters as needing
        expansion (finding the first match) or absolute. Setting False allows using
        paths which do embed such characters. If None (default), this argument
        takes its value from the DEFAULT_EXPAND module variable, which takes
        its initial value from the "open_expand" config value at startup, which will
        be False if not set.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> openfile = open('2015-01-01.csv')  # doctest: +SKIP
    >>> openfile = open(
    ...     's3://bucket/2015-01-01.csv.gz', compression='gzip'
    ... )  # doctest: +SKIP
    >>> with openfile as f:
    ...     df = pd.read_csv(f)  # doctest: +SKIP
    ...

    Returns
    -------
    ``OpenFile`` object.

    Notes
    -----
    For a full list of the available protocols and the implementations that
    they map across to see the latest online documentation:

    - For implementations built into ``fsspec`` see
      https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations
    - For implementations in separate packages see
      https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations
    """
    if expand is None:
        expand = DEFAULT_EXPAND
    
    fs, path = url_to_fs(urlpath, **(kwargs or {}))
    
    if compression == "infer":
        compression = infer_compression(path)
    
    return OpenFile(fs, path, mode=mode, compression=compression, encoding=encoding, errors=errors, newline=newline)

def open_local(url: str | list[str] | Path | list[Path], mode: str='rb', **storage_options: dict) -> str | list[str]:
    """Open file(s) which can be resolved to local

    For files which either are local, or get downloaded upon open
    (e.g., by file caching)

    Parameters
    ----------
    url: str or list(str)
    mode: str
        Must be read mode
    storage_options:
        passed on to FS for or used by open_files (e.g., compression)
    """
    if 'r' not in mode:
        raise ValueError("Only read mode is supported")
    
    if isinstance(url, (str, Path)):
        urls = [url]
    else:
        urls = url
    
    fs, _, paths = get_fs_token_paths(urls, mode=mode, storage_options=storage_options)
    
    if not hasattr(fs, 'open_many'):
        raise ValueError("Cannot open multiple files")
    
    with fs.open_many(paths) as openfiles:
        return [f.name for f in openfiles]

def split_protocol(urlpath):
    """Return protocol, path pair"""
    if '://' in urlpath:
        protocol, path = urlpath.split('://', 1)
        return protocol, path
    return None, urlpath

def strip_protocol(urlpath):
    """Return only path part of full URL, according to appropriate backend"""
    protocol, path = split_protocol(urlpath)
    return path

def expand_paths_if_needed(paths, mode, num, fs, name_function):
    """Expand paths if they have a ``*`` in them (write mode) or any of ``*?[]``
    in them (read mode).

    :param paths: list of paths
    mode: str
        Mode in which to open files.
    num: int
        If opening in writing mode, number of files we expect to create.
    fs: filesystem object
    name_function: callable
        If opening in writing mode, this callable is used to generate path
        names. Names are generated for each partition by
        ``urlpath.replace('*', name_function(partition_index))``.
    :return: list of paths
    """
    if 'w' in mode:
        if isinstance(paths, str):
            if '*' in paths:
                paths = [paths.replace('*', name_function(i)) for i in range(num)]
            else:
                paths = [paths]
    elif isinstance(paths, str):
        if has_magic(paths):
            paths = fs.glob(paths)
        else:
            paths = [paths]
    
    return paths

def get_fs_token_paths(urlpath, mode='rb', num=1, name_function=None, storage_options=None, protocol=None, expand=True):
    """Filesystem, deterministic token, and paths from a urlpath and options.

    Parameters
    ----------
    urlpath: string or iterable
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    mode: str, optional
        Mode in which to open files.
    num: int, optional
        If opening in writing mode, number of files we expect to create.
    name_function: callable, optional
        If opening in writing mode, this callable is used to generate path
        names. Names are generated for each partition by
        ``urlpath.replace('*', name_function(partition_index))``.
    storage_options: dict, optional
        Additional keywords to pass to the filesystem class.
    protocol: str or None
        To override the protocol specifier in the URL
    expand: bool
        Expand string paths for writing, assuming the path is a directory
    """
    if isinstance(urlpath, (list, tuple)):
        if not urlpath:
            raise ValueError("empty urlpath sequence")
        protocols, paths = zip(*[split_protocol(u) for u in urlpath])
        protocol = protocol or protocols[0]
        if not all(p == protocol for p in protocols):
            raise ValueError("When specifying a list of paths, all paths must "
                             "share the same protocol")
        cls = get_filesystem_class(protocol)
        options = storage_options or {}
        fs = cls(**options)
        paths = expand_paths_if_needed(paths, mode, num, fs, name_function)

    elif isinstance(urlpath, str) or hasattr(urlpath, '__fspath__'):
        urlpath = stringify_path(urlpath)
        protocol, path = split_protocol(urlpath)
        protocol = protocol or 'file'

        cls = get_filesystem_class(protocol)

        options = storage_options or {}
        fs = cls(**options)

        if 'w' in mode:
            paths = expand_paths_if_needed([path], mode, num, fs, name_function)
        elif "*" in path:
            paths = sorted(fs.glob(path))
        else:
            paths = [path]

    else:
        raise TypeError('url type not understood: %s' % urlpath)

    fs_token = tokenize(urlpath, protocol, storage_options)
    
    return fs, fs_token, paths

class PickleableTextIOWrapper(io.TextIOWrapper):
    """TextIOWrapper cannot be pickled. This solves it.

    Requires that ``buffer`` be pickleable, which all instances of
    AbstractBufferedFile are.
    """

    def __init__(self, buffer, encoding=None, errors=None, newline=None, line_buffering=False, write_through=False):
        self.args = (buffer, encoding, errors, newline, line_buffering, write_through)
        super().__init__(*self.args)

    def __reduce__(self):
        return (PickleableTextIOWrapper, self.args)
