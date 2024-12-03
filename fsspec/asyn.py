import asyncio
import asyncio.events
import functools
import inspect
import io
import numbers
import os
import re
import threading
from contextlib import contextmanager
from glob import has_magic
from typing import TYPE_CHECKING, Iterable
from .callbacks import DEFAULT_CALLBACK
from .exceptions import FSTimeoutError
from .implementations.local import LocalFileSystem, make_path_posix, trailing_sep
from .spec import AbstractBufferedFile, AbstractFileSystem
from .utils import glob_translate, is_exception, other_paths
private = re.compile('_[^_]')
iothread = [None]
loop = [None]
_lock = None
get_running_loop = asyncio.get_running_loop

def get_lock():
    """Allocate or return a threading lock.

    The lock is allocated on first use to allow setting one lock per forked process.
    """
    global _lock
    if _lock is None:
        _lock = threading.Lock()
    return _lock

def reset_lock():
    """Reset the global lock.

    This should be called only on the init of a forked process to reset the lock to
    None, enabling the new forked process to get a new lock.
    """
    global _lock
    _lock = None

def sync(loop, func, *args, timeout=None, **kwargs):
    """
    Make loop run coroutine until it returns. Runs in other thread

    Examples
    --------
    >>> fsspec.asyn.sync(fsspec.asyn.get_loop(), func, *args,
                         timeout=timeout, **kwargs)
    """
    e = threading.Event()
    result = [None]
    error = [False]

    async def f():
        try:
            result[0] = await func(*args, **kwargs)
        except Exception as ex:
            result[0] = ex
            error[0] = True
        finally:
            e.set()

    asyncio.run_coroutine_threadsafe(f(), loop)
    if timeout is not None:
        if not e.wait(timeout):
            raise FSTimeoutError

    if error[0]:
        raise result[0]
    return result[0]

def sync_wrapper(func, obj=None):
    """Given a function, make so can be called in blocking contexts

    Leave obj=None if defining within a class. Pass the instance if attaching
    as an attribute of the instance.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or args[0]
        return sync(self.loop, func, *args, **kwargs)
    return wrapper

def get_loop():
    """Create or return the default fsspec IO loop

    The loop will be running on a separate thread.
    """
    if loop[0] is None:
        with get_lock():
            if loop[0] is None:
                loop[0] = asyncio.new_event_loop()
                th = threading.Thread(target=loop[0].run_forever, daemon=True)
                th.start()
                iothread[0] = th
    return loop[0]
if TYPE_CHECKING:
    import resource
    ResourceError = resource.error
else:
    try:
        import resource
    except ImportError:
        resource = None
        ResourceError = OSError
    else:
        ResourceError = getattr(resource, 'error', OSError)
_DEFAULT_BATCH_SIZE = 128
_NOFILES_DEFAULT_BATCH_SIZE = 1280

def running_async() -> bool:
    """Being executed by an event loop?"""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False

async def _run_coros_in_chunks(coros, batch_size=None, callback=DEFAULT_CALLBACK, timeout=None, return_exceptions=False, nofiles=False):
    """Run the given coroutines in  chunks.

    Parameters
    ----------
    coros: list of coroutines to run
    batch_size: int or None
        Number of coroutines to submit/wait on simultaneously.
        If -1, then it will not be any throttling. If
        None, it will be inferred from _get_batch_size()
    callback: fsspec.callbacks.Callback instance
        Gets a relative_update when each coroutine completes
    timeout: number or None
        If given, each coroutine times out after this time. Note that, since
        there are multiple batches, the total run time of this function will in
        general be longer
    return_exceptions: bool
        Same meaning as in asyncio.gather
    nofiles: bool
        If inferring the batch_size, does this operation involve local files?
        If yes, you normally expect smaller batches.
    """
    if batch_size is None:
        batch_size = _get_batch_size(nofiles)
    elif batch_size == -1:
        batch_size = len(coros)

    results = []
    for i in range(0, len(coros), batch_size):
        batch = coros[i:i + batch_size]
        if timeout is not None:
            batch = [asyncio.wait_for(coro, timeout) for coro in batch]
        chunk_results = await asyncio.gather(*batch, return_exceptions=return_exceptions)
        results.extend(chunk_results)
        callback.relative_update(len(chunk_results))

    return results
async_methods = ['_ls', '_cat_file', '_get_file', '_put_file', '_rm_file', '_cp_file', '_pipe_file', '_expand_path', '_info', '_isfile', '_isdir', '_exists', '_walk', '_glob', '_find', '_du', '_size', '_mkdir', '_makedirs']

class AsyncFileSystem(AbstractFileSystem):
    """Async file operations, default implementations

    Passes bulk operations to asyncio.gather for concurrent operation.

    Implementations that have concurrent batch operations and/or async methods
    should inherit from this class instead of AbstractFileSystem. Docstrings are
    copied from the un-underscored method in AbstractFileSystem, if not given.
    """
    async_impl = True
    mirror_sync_methods = True
    disable_throttling = False

    def __init__(self, *args, asynchronous=False, loop=None, batch_size=None, **kwargs):
        self.asynchronous = asynchronous
        self._pid = os.getpid()
        if not asynchronous:
            self._loop = loop or get_loop()
        else:
            self._loop = None
        self.batch_size = batch_size
        super().__init__(*args, **kwargs)

    async def _process_limits(self, url, start, end):
        """Helper for "Range"-based _cat_file"""
        if start is not None or end is not None:
            start = start or 0
            end = end if end is not None else ""
            headers = {"Range": f"bytes={start}-{end}"}
        else:
            headers = {}
        return headers

    async def _cat_ranges(self, paths, starts, ends, max_gap=None, batch_size=None, on_error='return', **kwargs):
        """Get the contents of byte ranges from one or more files

        Parameters
        ----------
        paths: list
            A list of of filepaths on this filesystems
        starts, ends: int or list
            Bytes limits of the read. If using a single int, the same value will be
            used to read all the specified files.
        """
        if isinstance(starts, numbers.Integral):
            starts = [starts] * len(paths)
        if isinstance(ends, numbers.Integral):
            ends = [ends] * len(paths)
        if len(starts) != len(paths) or len(ends) != len(paths):
            raise ValueError("starts, ends, paths must be same length")

        chunks = []
        for path, start, end in zip(paths, starts, ends):
            if end is not None and start is not None:
                if end < start:
                    raise ValueError(f"End {end} is before start {start} for {path}")
                if start < 0:
                    raise ValueError(f"Start {start} is negative for {path}")
            chunks.append(self._cat_file(path, start=start, end=end, **kwargs))

        if batch_size is None:
            batch_size = self.batch_size
        return await _run_coros_in_chunks(chunks, batch_size=batch_size, on_error=on_error)

    async def _put(self, lpath, rpath, recursive=False, callback=DEFAULT_CALLBACK, batch_size=None, maxdepth=None, **kwargs):
        """Copy file(s) from local.

        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        The put_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit .
        """
        from .implementations.local import LocalFileSystem
        
        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        if recursive:
            rpaths = other_paths(
                [os.path.join(rpath, os.path.relpath(f, lpath)) for f in fs.expand_path(lpath, recursive=True, maxdepth=maxdepth)]
            )
            lpaths = other_paths(fs.expand_path(lpath, recursive=True, maxdepth=maxdepth))
        else:
            rpaths = [rpath]
            lpaths = [lpath]

        if batch_size is None:
            batch_size = self.batch_size
        
        coros = [self._put_file(lpath, rpath, **kwargs) for lpath, rpath in zip(lpaths, rpaths)]
        return await _run_coros_in_chunks(coros, batch_size=batch_size, callback=callback)

    async def _get(self, rpath, lpath, recursive=False, callback=DEFAULT_CALLBACK, maxdepth=None, **kwargs):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        The get_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit .
        """
        from .implementations.local import make_path_posix

        batch_size = kwargs.pop('batch_size', self.batch_size)
        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        
        if isinstance(rpath, str):
            rpaths = self.expand_path(rpath, recursive=recursive, maxdepth=maxdepth)
        else:
            rpaths = sum((self.expand_path(p, recursive=recursive, maxdepth=maxdepth) for p in rpath), [])

        lpaths = other_paths([lpath] if isinstance(lpath, str) else lpath, len(rpaths))

        coros = [self._get_file(rpath, lpath, **kwargs) for rpath, lpath in zip(rpaths, lpaths)]
        return await _run_coros_in_chunks(coros, batch_size=batch_size, callback=callback)

def mirror_sync_methods(obj):
    """Populate sync and async methods for obj

    For each method will create a sync version if the name refers to an async method
    (coroutine) and there is no override in the child class; will create an async
    method for the corresponding sync method if there is no implementation.

    Uses the methods specified in
    - async_methods: the set that an implementation is expected to provide
    - default_async_methods: that can be derived from their sync version in
      AbstractFileSystem
    - AsyncFileSystem: async-specific default coroutines
    """
    from .spec import AbstractFileSystem

    for method in async_methods + dir(AsyncFileSystem):
        if not method.startswith('_'):
            continue
        smethod = method[1:]
        if private.match(method):
            isco = inspect.iscoroutinefunction(getattr(obj, method, None))
            unsync = getattr(AbstractFileSystem, smethod, None)
            is_coro = inspect.iscoroutinefunction(getattr(obj, smethod, None))
            if isco and not is_coro:
                setattr(obj, smethod, sync_wrapper(getattr(obj, method), obj=obj))
            elif not isco and unsync is not None:
                setattr(obj, method, unsync)
    return obj

class FSSpecCoroutineCancel(Exception):
    pass

class AbstractAsyncStreamedFile(AbstractBufferedFile):

    async def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary

        Parameters
        ----------
        length: int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        if length < 0:
            length = self.size - self.loc
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if length == 0:
            return b""
        if self.loc >= self.size:
            return b""
        if self.cache is None:
            self.cache = await self._fetch_range(self.loc, self.loc + length)
        if length > len(self.cache) - self.loc:
            if self.loc + len(self.cache) != self.size:
                self.cache = await self._fetch_range(self.loc, self.loc + length)
        out = self.cache[self.loc : self.loc + length]
        self.loc += len(out)
        return out

    async def write(self, data):
        """
        Write data to buffer.

        Buffer only sent on flush() or if buffer is greater than
        or equal to blocksize.

        Parameters
        ----------
        data: bytes
            Set of bytes to be written.
        """
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.mode not in {"wb", "ab"}:
            raise ValueError("File not in write mode")
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() >= self.blocksize:
            await self.flush()
        return out

    async def close(self):
        """Close file

        Finalizes writes, discards cache
        """
        if self.closed:
            return
        if self.mode == "wb":
            await self.flush()
            await self._upload_chunk(self.buffer.getvalue())
        self.cache = None
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
