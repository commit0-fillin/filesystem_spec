from __future__ import annotations
import collections
import functools
import logging
import math
import os
import threading
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Generic, NamedTuple, Optional, OrderedDict, TypeVar
if TYPE_CHECKING:
    import mmap
    from typing_extensions import ParamSpec
    P = ParamSpec('P')
else:
    P = TypeVar('P')
T = TypeVar('T')
logger = logging.getLogger('fsspec')
Fetcher = Callable[[int, int], bytes]

class BaseCache:
    """Pass-though cache: doesn't keep anything, calls every time

    Acts as base class for other cachers

    Parameters
    ----------
    blocksize: int
        How far to read ahead in numbers of bytes
    fetcher: func
        Function of the form f(start, end) which gets bytes from remote as
        specified
    size: int
        How big this file is
    """
    name: ClassVar[str] = 'none'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int) -> None:
        self.blocksize = blocksize
        self.nblocks = 0
        self.fetcher = fetcher
        self.size = size
        self.hit_count = 0
        self.miss_count = 0
        self.total_requested_bytes = 0

    def _reset_stats(self) -> None:
        """Reset hit and miss counts for a more ganular report e.g. by file."""
        self.hit_count = 0
        self.miss_count = 0
        self.total_requested_bytes = 0

    def _log_stats(self) -> str:
        """Return a formatted string of the cache statistics."""
        return (f"Cache statistics:\n"
                f"  Hits: {self.hit_count}\n"
                f"  Misses: {self.miss_count}\n"
                f"  Total requested bytes: {self.total_requested_bytes}")

    def __repr__(self) -> str:
        return f'\n        <{self.__class__.__name__}:\n            block size  :   {self.blocksize}\n            block count :   {self.nblocks}\n            file size   :   {self.size}\n            cache hits  :   {self.hit_count}\n            cache misses:   {self.miss_count}\n            total requested bytes: {self.total_requested_bytes}>\n        '

class MMapCache(BaseCache):
    """memory-mapped sparse file cache

    Opens temporary file, which is filled blocks-wise when data is requested.
    Ensure there is enough disc space in the temporary location.

    This cache method might only work on posix
    """
    name = 'mmap'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int, location: str | None=None, blocks: set[int] | None=None) -> None:
        super().__init__(blocksize, fetcher, size)
        self.blocks = set() if blocks is None else blocks
        self.location = location
        self.cache = self._makefile()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        del state['cache']
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self.cache = self._makefile()

class ReadAheadCache(BaseCache):
    """Cache which reads only when we get beyond a block of data

    This is a much simpler version of BytesCache, and does not attempt to
    fill holes in the cache or keep fragments alive. It is best suited to
    many small reads in a sequential order (e.g., reading lines from a file).
    """
    name = 'readahead'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int) -> None:
        super().__init__(blocksize, fetcher, size)
        self.cache = b''
        self.start = 0
        self.end = 0

class FirstChunkCache(BaseCache):
    """Caches the first block of a file only

    This may be useful for file types where the metadata is stored in the header,
    but is randomly accessed.
    """
    name = 'first'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int) -> None:
        if blocksize > size:
            blocksize = size
        super().__init__(blocksize, fetcher, size)
        self.cache: bytes | None = None

class BlockCache(BaseCache):
    """
    Cache holding memory as a set of blocks.

    Requests are only ever made ``blocksize`` at a time, and are
    stored in an LRU cache. The least recently accessed block is
    discarded when more than ``maxblocks`` are stored.

    Parameters
    ----------
    blocksize : int
        The number of bytes to store in each block.
        Requests are only ever made for ``blocksize``, so this
        should balance the overhead of making a request against
        the granularity of the blocks.
    fetcher : Callable
    size : int
        The total size of the file being cached.
    maxblocks : int
        The maximum number of blocks to cache for. The maximum memory
        use for this cache is then ``blocksize * maxblocks``.
    """
    name = 'blockcache'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int, maxblocks: int=32) -> None:
        super().__init__(blocksize, fetcher, size)
        self.nblocks = math.ceil(size / blocksize)
        self.maxblocks = maxblocks
        self._fetch_block_cached = functools.lru_cache(maxblocks)(self._fetch_block)

    def cache_info(self):
        """
        The statistics on the block cache.

        Returns
        -------
        NamedTuple
            Returned directly from the LRU Cache used internally.
        """
        return self._fetch_block_cached.cache_info()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__
        del state['_fetch_block_cached']
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._fetch_block_cached = functools.lru_cache(state['maxblocks'])(self._fetch_block)

    def _fetch_block(self, block_number: int) -> bytes:
        """
        Fetch the block of data for `block_number`.
        """
        start = block_number * self.blocksize
        end = min(start + self.blocksize, self.size)
        self.miss_count += 1
        self.total_requested_bytes += end - start
        return self.fetcher(start, end)

    def _read_cache(self, start: int, end: int, start_block_number: int, end_block_number: int) -> bytes:
        """
        Read from our block cache.

        Parameters
        ----------
        start, end : int
            The start and end byte positions.
        start_block_number, end_block_number : int
            The start and end block numbers.
        """
        if start_block_number == end_block_number:
            block = self._fetch_block_cached(start_block_number)
            return block[start % self.blocksize : end % self.blocksize or None]
        
        result = []
        for block_number in range(start_block_number, end_block_number + 1):
            block = self._fetch_block_cached(block_number)
            if block_number == start_block_number:
                result.append(block[start % self.blocksize:])
            elif block_number == end_block_number:
                result.append(block[:end % self.blocksize or None])
            else:
                result.append(block)
        
        return b''.join(result)

class BytesCache(BaseCache):
    """Cache which holds data in a in-memory bytes object

    Implements read-ahead by the block size, for semi-random reads progressing
    through the file.

    Parameters
    ----------
    trim: bool
        As we read more data, whether to discard the start of the buffer when
        we are more than a blocksize ahead of it.
    """
    name: ClassVar[str] = 'bytes'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int, trim: bool=True) -> None:
        super().__init__(blocksize, fetcher, size)
        self.cache = b''
        self.start: int | None = None
        self.end: int | None = None
        self.trim = trim

    def __len__(self) -> int:
        return len(self.cache)

class AllBytes(BaseCache):
    """Cache entire contents of the file"""
    name: ClassVar[str] = 'all'

    def __init__(self, blocksize: int | None=None, fetcher: Fetcher | None=None, size: int | None=None, data: bytes | None=None) -> None:
        super().__init__(blocksize, fetcher, size)
        if data is None:
            self.miss_count += 1
            self.total_requested_bytes += self.size
            data = self.fetcher(0, self.size)
        self.data = data

class KnownPartsOfAFile(BaseCache):
    """
    Cache holding known file parts.

    Parameters
    ----------
    blocksize: int
        How far to read ahead in numbers of bytes
    fetcher: func
        Function of the form f(start, end) which gets bytes from remote as
        specified
    size: int
        How big this file is
    data: dict
        A dictionary mapping explicit `(start, stop)` file-offset tuples
        with known bytes.
    strict: bool, default True
        Whether to fetch reads that go beyond a known byte-range boundary.
        If `False`, any read that ends outside a known part will be zero
        padded. Note that zero padding will not be used for reads that
        begin outside a known byte-range.
    """
    name: ClassVar[str] = 'parts'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int, data: Optional[dict[tuple[int, int], bytes]]=None, strict: bool=True, **_: Any):
        super().__init__(blocksize, fetcher, size)
        self.strict = strict
        if data:
            old_offsets = sorted(data.keys())
            offsets = [old_offsets[0]]
            blocks = [data.pop(old_offsets[0])]
            for start, stop in old_offsets[1:]:
                start0, stop0 = offsets[-1]
                if start == stop0:
                    offsets[-1] = (start0, stop)
                    blocks[-1] += data.pop((start, stop))
                else:
                    offsets.append((start, stop))
                    blocks.append(data.pop((start, stop)))
            self.data = dict(zip(offsets, blocks))
        else:
            self.data = {}

class UpdatableLRU(Generic[P, T]):
    """
    Custom implementation of LRU cache that allows updating keys

    Used by BackgroudBlockCache
    """

    class CacheInfo(NamedTuple):
        hits: int
        misses: int
        maxsize: int
        currsize: int

    def __init__(self, func: Callable[P, T], max_size: int=128) -> None:
        self._cache: OrderedDict[Any, T] = collections.OrderedDict()
        self._func = func
        self._max_size = max_size
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        if kwargs:
            raise TypeError(f'Got unexpected keyword argument {kwargs.keys()}')
        with self._lock:
            if args in self._cache:
                self._cache.move_to_end(args)
                self._hits += 1
                return self._cache[args]
        result = self._func(*args, **kwargs)
        with self._lock:
            self._cache[args] = result
            self._misses += 1
            if len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
        return result

class BackgroundBlockCache(BaseCache):
    """
    Cache holding memory as a set of blocks with pre-loading of
    the next block in the background.

    Requests are only ever made ``blocksize`` at a time, and are
    stored in an LRU cache. The least recently accessed block is
    discarded when more than ``maxblocks`` are stored. If the
    next block is not in cache, it is loaded in a separate thread
    in non-blocking way.

    Parameters
    ----------
    blocksize : int
        The number of bytes to store in each block.
        Requests are only ever made for ``blocksize``, so this
        should balance the overhead of making a request against
        the granularity of the blocks.
    fetcher : Callable
    size : int
        The total size of the file being cached.
    maxblocks : int
        The maximum number of blocks to cache for. The maximum memory
        use for this cache is then ``blocksize * maxblocks``.
    """
    name: ClassVar[str] = 'background'

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int, maxblocks: int=32) -> None:
        super().__init__(blocksize, fetcher, size)
        self.nblocks = math.ceil(size / blocksize)
        self.maxblocks = maxblocks
        self._fetch_block_cached = UpdatableLRU(self._fetch_block, maxblocks)
        self._thread_executor = ThreadPoolExecutor(max_workers=1)
        self._fetch_future_block_number: int | None = None
        self._fetch_future: Future[bytes] | None = None
        self._fetch_future_lock = threading.Lock()

    def cache_info(self) -> UpdatableLRU.CacheInfo:
        """
        The statistics on the block cache.

        Returns
        -------
        NamedTuple
            Returned directly from the LRU Cache used internally.
        """
        return self._fetch_block_cached.cache_info()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__
        del state['_fetch_block_cached']
        del state['_thread_executor']
        del state['_fetch_future_block_number']
        del state['_fetch_future']
        del state['_fetch_future_lock']
        return state

    def __setstate__(self, state) -> None:
        self.__dict__.update(state)
        self._fetch_block_cached = UpdatableLRU(self._fetch_block, state['maxblocks'])
        self._thread_executor = ThreadPoolExecutor(max_workers=1)
        self._fetch_future_block_number = None
        self._fetch_future = None
        self._fetch_future_lock = threading.Lock()

    def _fetch_block(self, block_number: int, log_info: str='sync') -> bytes:
        """
        Fetch the block of data for `block_number`.
        """
        start = block_number * self.blocksize
        end = min(start + self.blocksize, self.size)
        self.miss_count += 1
        self.total_requested_bytes += end - start
        data = self.fetcher(start, end)
        
        # Pre-fetch next block in background
        with self._fetch_future_lock:
            next_block = block_number + 1
            if next_block < self.nblocks and next_block not in self._fetch_block_cached._cache:
                self._fetch_future_block_number = next_block
                self._fetch_future = self._thread_executor.submit(self._fetch_block, next_block, 'async')
        
        return data

    def _read_cache(self, start: int, end: int, start_block_number: int, end_block_number: int) -> bytes:
        """
        Read from our block cache.

        Parameters
        ----------
        start, end : int
            The start and end byte positions.
        start_block_number, end_block_number : int
            The start and end block numbers.
        """
        result = []
        for block_number in range(start_block_number, end_block_number + 1):
            with self._fetch_future_lock:
                if self._fetch_future_block_number == block_number and self._fetch_future:
                    block = self._fetch_future.result()
                    self._fetch_block_cached._cache[block_number] = block
                    self._fetch_future_block_number = None
                    self._fetch_future = None
                else:
                    block = self._fetch_block_cached(block_number)
            
            if block_number == start_block_number:
                result.append(block[start % self.blocksize:])
            elif block_number == end_block_number:
                result.append(block[:end % self.blocksize or None])
            else:
                result.append(block)
        
        return b''.join(result)
caches: dict[str | None, type[BaseCache]] = {None: BaseCache}

def register_cache(cls: type[BaseCache], clobber: bool=False) -> None:
    """'Register' cache implementation.

    Parameters
    ----------
    clobber: bool, optional
        If set to True (default is False) - allow to overwrite existing
        entry.

    Raises
    ------
    ValueError
    """
    if cls.name in caches and not clobber:
        raise ValueError(f"Cache {cls.name} already registered. Use clobber=True to overwrite.")
    caches[cls.name] = cls
for c in (BaseCache, MMapCache, BytesCache, ReadAheadCache, BlockCache, FirstChunkCache, AllBytes, KnownPartsOfAFile, BackgroundBlockCache):
    register_cache(c)
