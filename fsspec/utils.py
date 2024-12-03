from __future__ import annotations
import contextlib
import logging
import math
import os
import pathlib
import re
import sys
import tempfile
from functools import partial
from hashlib import md5
from importlib.metadata import version
from typing import IO, TYPE_CHECKING, Any, Callable, Iterable, Iterator, Sequence, TypeVar
from urllib.parse import urlsplit
if TYPE_CHECKING:
    from typing_extensions import TypeGuard
    from fsspec.spec import AbstractFileSystem
DEFAULT_BLOCK_SIZE = 5 * 2 ** 20
T = TypeVar('T')

def infer_storage_options(urlpath: str, inherit_storage_options: dict[str, Any] | None=None) -> dict[str, Any]:
    """Infer storage options from URL path and merge it with existing storage
    options.

    Parameters
    ----------
    urlpath: str or unicode
        Either local absolute file path or URL (hdfs://namenode:8020/file.csv)
    inherit_storage_options: dict (optional)
        Its contents will get merged with the inferred information from the
        given path

    Returns
    -------
    Storage options dict.

    Examples
    --------
    >>> infer_storage_options('/mnt/datasets/test.csv')  # doctest: +SKIP
    {"protocol": "file", "path", "/mnt/datasets/test.csv"}
    >>> infer_storage_options(
    ...     'hdfs://username:pwd@node:123/mnt/datasets/test.csv?q=1',
    ...     inherit_storage_options={'extra': 'value'},
    ... )  # doctest: +SKIP
    {"protocol": "hdfs", "username": "username", "password": "pwd",
    "host": "node", "port": 123, "path": "/mnt/datasets/test.csv",
    "url_query": "q=1", "extra": "value"}
    """
    from urllib.parse import urlparse, parse_qs

    result = {}
    if inherit_storage_options:
        result.update(inherit_storage_options)

    parsed_path = urlparse(urlpath)
    
    protocol = parsed_path.scheme or 'file'
    result['protocol'] = protocol

    if protocol == 'file':
        result['path'] = parsed_path.path
    else:
        result.update({
            'path': parsed_path.path,
            'host': parsed_path.hostname,
            'username': parsed_path.username,
            'password': parsed_path.password,
        })

        if parsed_path.port:
            result['port'] = parsed_path.port

        if parsed_path.query:
            result['url_query'] = parsed_path.query
            result.update(parse_qs(parsed_path.query))

    return result
compressions: dict[str, str] = {}

def infer_compression(filename: str) -> str | None:
    """Infer compression, if available, from filename.

    Infer a named compression type, if registered and available, from filename
    extension. This includes builtin (gz, bz2, zip) compressions, as well as
    optional compressions. See fsspec.compression.register_compression.
    """
    import os
    from fsspec.compression import compr
    extension = os.path.splitext(filename)[-1].strip('.')
    return compr.get(extension)

def build_name_function(max_int: float) -> Callable[[int], str]:
    """Returns a function that receives a single integer
    and returns it as a string padded by enough zero characters
    to align with maximum possible integer

    >>> name_f = build_name_function(57)

    >>> name_f(7)
    '07'
    >>> name_f(31)
    '31'
    >>> build_name_function(1000)(42)
    '0042'
    >>> build_name_function(999)(42)
    '042'
    >>> build_name_function(0)(0)
    '0'
    """
    pad = len(str(int(max_int)))
    def name_function(i: int) -> str:
        return f"{i:0{pad}d}"
    return name_function

def seek_delimiter(file: IO[bytes], delimiter: bytes, blocksize: int) -> bool:
    """Seek current file to file start, file end, or byte after delimiter seq.

    Seeks file to next chunk delimiter, where chunks are defined on file start,
    a delimiting sequence, and file end. Use file.tell() to see location afterwards.
    Note that file start is a valid split, so must be at offset > 0 to seek for
    delimiter.

    Parameters
    ----------
    file: a file
    delimiter: bytes
        a delimiter like ``b'\\n'`` or message sentinel, matching file .read() type
    blocksize: int
        Number of bytes to read from the file at once.


    Returns
    -------
    Returns True if a delimiter was found, False if at file start or end.

    """
    if file.tell() == 0:
        return False
    
    last = b''
    while True:
        current = file.read(blocksize)
        if not current:
            return False
        full = last + current
        try:
            i = full.index(delimiter)
            file.seek(file.tell() - (len(full) - i) + len(delimiter))
            return True
        except ValueError:
            pass
        last = full[-len(delimiter):]

def read_block(f: IO[bytes], offset: int, length: int | None, delimiter: bytes | None=None, split_before: bool=False) -> bytes:
    """Read a block of bytes from a file

    Parameters
    ----------
    f: File
        Open file
    offset: int
        Byte offset to start read
    length: int
        Number of bytes to read, read through end of file if None
    delimiter: bytes (optional)
        Ensure reading starts and stops at delimiter bytestring
    split_before: bool (optional)
        Start/stop read *before* delimiter bytestring.


    If using the ``delimiter=`` keyword argument we ensure that the read
    starts and stops at delimiter boundaries that follow the locations
    ``offset`` and ``offset + length``.  If ``offset`` is zero then we
    start at zero, regardless of delimiter.  The bytestring returned WILL
    include the terminating delimiter string.

    Examples
    --------

    >>> from io import BytesIO  # doctest: +SKIP
    >>> f = BytesIO(b'Alice, 100\\nBob, 200\\nCharlie, 300')  # doctest: +SKIP
    >>> read_block(f, 0, 13)  # doctest: +SKIP
    b'Alice, 100\\nBo'

    >>> read_block(f, 0, 13, delimiter=b'\\n')  # doctest: +SKIP
    b'Alice, 100\\nBob, 200\\n'

    >>> read_block(f, 10, 10, delimiter=b'\\n')  # doctest: +SKIP
    b'Bob, 200\\nCharlie, 300'
    """
    if delimiter:
        f.seek(offset)
        if offset > 0:
            seek_delimiter(f, delimiter, 2**16)
        start = f.tell()
        if length is None:
            return f.read()
        else:
            f.seek(start + length)
            seek_delimiter(f, delimiter, 2**16)
            end = f.tell()
            f.seek(start)
            return f.read(end - start)
    else:
        f.seek(offset)
        if length is None:
            return f.read()
        else:
            return f.read(length)

def tokenize(*args: Any, **kwargs: Any) -> str:
    """Deterministic token

    (modified from dask.base)

    >>> tokenize([1, 2, '3'])
    '9d71491b50023b06fc76928e6eddb952'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    from hashlib import md5
    from .spec import AbstractFileSystem

    def _tokenize(obj):
        if isinstance(obj, (str, bytes)):
            return md5(str(obj).encode()).hexdigest()
        elif isinstance(obj, (int, float, bool, type(None))):
            return md5(str(obj).encode()).hexdigest()
        elif isinstance(obj, dict):
            return md5(str(sorted(map(_tokenize, obj.items()))).encode()).hexdigest()
        elif isinstance(obj, (list, tuple)):
            return md5(str(list(map(_tokenize, obj))).encode()).hexdigest()
        elif isinstance(obj, AbstractFileSystem):
            return _tokenize(obj.storage_options)
        else:
            return md5(str(obj).encode()).hexdigest()

    return _tokenize((args, kwargs))

def stringify_path(filepath: str | os.PathLike[str] | pathlib.Path) -> str:
    """Attempt to convert a path-like object to a string.

    Parameters
    ----------
    filepath: object to be converted

    Returns
    -------
    filepath_str: maybe a string version of the object

    Notes
    -----
    Objects supporting the fspath protocol are coerced according to its
    __fspath__ method.

    For backwards compatibility with older Python version, pathlib.Path
    objects are specially coerced.

    Any other object is passed through unchanged, which includes bytes,
    strings, buffers, or anything else that's not even path-like.
    """
    if isinstance(filepath, str):
        return filepath
    elif isinstance(filepath, pathlib.Path):
        return str(filepath)
    elif hasattr(filepath, '__fspath__'):
        return filepath.__fspath__()
    elif hasattr(os, 'fspath'):  # Python 3.6+
        return os.fspath(filepath)
    else:
        return str(filepath)

def common_prefix(paths: Iterable[str]) -> str:
    """For a list of paths, find the shortest prefix common to all"""
    paths = list(paths)
    if not paths:
        return ''
    s1 = min(paths)
    s2 = max(paths)
    for i, c in enumerate(s1):
        if c != s2[i]:
            return s1[:i]
    return s1

def other_paths(paths: list[str], path2: str | list[str], exists: bool=False, flatten: bool=False) -> list[str]:
    """In bulk file operations, construct a new file tree from a list of files

    Parameters
    ----------
    paths: list of str
        The input file tree
    path2: str or list of str
        Root to construct the new list in. If this is already a list of str, we just
        assert it has the right number of elements.
    exists: bool (optional)
        For a str destination, it is already exists (and is a dir), files should
        end up inside.
    flatten: bool (optional)
        Whether to flatten the input directory tree structure so that the output files
        are in the same directory.

    Returns
    -------
    list of str
    """
    if isinstance(path2, str):
        if exists:
            path2 = [os.path.join(path2, os.path.basename(p)) for p in paths]
        elif flatten:
            path2 = [os.path.join(path2, os.path.basename(p)) for p in paths]
        else:
            common = common_prefix(paths)
            path2 = [os.path.join(path2, p[len(common):]) for p in paths]
    else:
        assert len(paths) == len(path2)
    return path2

def can_be_local(path: str) -> bool:
    """Can the given URL be used with open_local?"""
    from urllib.parse import urlparse
    
    parsed = urlparse(path)
    return parsed.scheme in ['', 'file'] or parsed.scheme == 'local'

def get_package_version_without_import(name: str) -> str | None:
    """For given package name, try to find the version without importing it

    Import and package.__version__ is still the backup here, so an import
    *might* happen.

    Returns either the version string, or None if the package
    or the version was not readily  found.
    """
    from importlib.metadata import version, PackageNotFoundError
    
    try:
        return version(name)
    except PackageNotFoundError:
        try:
            import importlib
            module = importlib.import_module(name)
            return getattr(module, '__version__', None)
        except ImportError:
            return None

def mirror_from(origin_name: str, methods: Iterable[str]) -> Callable[[type[T]], type[T]]:
    """Mirror attributes and methods from the given
    origin_name attribute of the instance to the
    decorated class"""
    def wrapper(cls: type[T]) -> type[T]:
        def make_method(name):
            def method(self, *args, **kwargs):
                origin = getattr(self, origin_name)
                return getattr(origin, name)(*args, **kwargs)
            return method

        for name in methods:
            setattr(cls, name, make_method(name))
        return cls
    return wrapper

def merge_offset_ranges(paths: list[str], starts: list[int] | int, ends: list[int] | int, max_gap: int=0, max_block: int | None=None, sort: bool=True) -> tuple[list[str], list[int], list[int]]:
    """Merge adjacent byte-offset ranges when the inter-range
    gap is <= `max_gap`, and when the merged byte range does not
    exceed `max_block` (if specified). By default, this function
    will re-order the input paths and byte ranges to ensure sorted
    order. If the user can guarantee that the inputs are already
    sorted, passing `sort=False` will skip the re-ordering.
    """
    if isinstance(starts, int):
        starts = [starts] * len(paths)
    if isinstance(ends, int):
        ends = [ends] * len(paths)
    
    if sort:
        paths, starts, ends = zip(*sorted(zip(paths, starts, ends), key=lambda x: (x[0], x[1])))
        paths, starts, ends = list(paths), list(starts), list(ends)
    
    merged_paths, merged_starts, merged_ends = [], [], []
    
    for path, start, end in zip(paths, starts, ends):
        if not merged_paths or path != merged_paths[-1] or start > merged_ends[-1] + max_gap or (max_block and start - merged_starts[-1] > max_block):
            merged_paths.append(path)
            merged_starts.append(start)
            merged_ends.append(end)
        else:
            merged_ends[-1] = max(merged_ends[-1], end)
    
    return merged_paths, merged_starts, merged_ends

def file_size(filelike: IO[bytes]) -> int:
    """Find length of any open read-mode file-like"""
    offset = filelike.tell()
    try:
        return filelike.seek(0, io.SEEK_END)
    finally:
        filelike.seek(offset)

@contextlib.contextmanager
def atomic_write(path: str, mode: str='wb'):
    """
    A context manager that opens a temporary file next to `path` and, on exit,
    replaces `path` with the temporary file, thereby updating `path`
    atomically.
    """
    import tempfile
    import os

    dir_path, file_name = os.path.split(path)
    with tempfile.NamedTemporaryFile(mode=mode, dir=dir_path, delete=False) as tmp_file:
        try:
            yield tmp_file
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        except:
            os.unlink(tmp_file.name)
            raise
        else:
            tmp_file.close()
            os.rename(tmp_file.name, path)

def glob_translate(pat):
    """Translate a pathname with shell wildcards to a regular expression."""
    import re
    
    i, n = 0, len(pat)
    res = []
    while i < n:
        c = pat[i]
        i = i+1
        if c == '*':
            if i < n and pat[i] == '*':
                res.append('.*')
                i = i+1
            else:
                res.append('[^/]*')
        elif c == '?':
            res.append('[^/]')
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j+1
            if j < n and pat[j] == ']':
                j = j+1
            while j < n and pat[j] != ']':
                j = j+1
            if j >= n:
                res.append('\\[')
            else:
                stuff = pat[i:j].replace('\\', '\\\\')
                i = j+1
                if stuff[0] == '!':
                    stuff = '^' + stuff[1:]
                elif stuff[0] == '^':
                    stuff = '\\' + stuff
                res.append('[' + stuff + ']')
        else:
            res.append(re.escape(c))
    res.append('\Z(?ms)')
    return ''.join(res)
