from __future__ import annotations
import os
import pickle
import time
from typing import TYPE_CHECKING
from fsspec.utils import atomic_write
try:
    import ujson as json
except ImportError:
    if not TYPE_CHECKING:
        import json
if TYPE_CHECKING:
    from typing import Any, Dict, Iterator, Literal
    from typing_extensions import TypeAlias
    from .cached import CachingFileSystem
    Detail: TypeAlias = Dict[str, Any]

class CacheMetadata:
    """Cache metadata.

    All reading and writing of cache metadata is performed by this class,
    accessing the cached files and blocks is not.

    Metadata is stored in a single file per storage directory in JSON format.
    For backward compatibility, also reads metadata stored in pickle format
    which is converted to JSON when next saved.
    """

    def __init__(self, storage: list[str]):
        """

        Parameters
        ----------
        storage: list[str]
            Directories containing cached files, must be at least one. Metadata
            is stored in the last of these directories by convention.
        """
        if not storage:
            raise ValueError('CacheMetadata expects at least one storage location')
        self._storage = storage
        self.cached_files: list[Detail] = [{}]
        self._force_save_pickle = False

    def _load(self, fn: str) -> Detail:
        """Low-level function to load metadata from specific file"""
        try:
            with open(fn, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            # Fallback to pickle for backward compatibility
            with open(fn, "rb") as f:
                return pickle.load(f)

    def _save(self, metadata_to_save: Detail, fn: str) -> None:
        """Low-level function to save metadata to specific file"""
        with atomic_write(fn, mode="w") as f:
            json.dump(metadata_to_save, f)

    def _scan_locations(self, writable_only: bool=False) -> Iterator[tuple[str, str, bool]]:
        """Yield locations (filenames) where metadata is stored, and whether
        writable or not.

        Parameters
        ----------
        writable: bool
            Set to True to only yield writable locations.

        Returns
        -------
        Yields (str, str, bool)
        """
        for i, storage in enumerate(self._storage):
            fn = os.path.join(storage, "cache")
            writable = i == len(self._storage) - 1
            if writable_only and not writable:
                continue
            yield storage, fn, writable

    def check_file(self, path: str, cfs: CachingFileSystem | None) -> Literal[False] | tuple[Detail, str]:
        """If path is in cache return its details, otherwise return ``False``.

        If the optional CachingFileSystem is specified then it is used to
        perform extra checks to reject possible matches, such as if they are
        too old.
        """
        for storage, _, _ in self._scan_locations():
            detail = self.cached_files[storage].get(path)
            if detail:
                if cfs and cfs.check_files:
                    if cfs.fs.modified(path) > detail["time"]:
                        continue
                return detail, storage
        return False

    def clear_expired(self, expiry_time: int) -> tuple[list[str], bool]:
        """Remove expired metadata from the cache.

        Returns names of files corresponding to expired metadata and a boolean
        flag indicating whether the writable cache is empty. Caller is
        responsible for deleting the expired files.
        """
        expired_files = []
        now = time.time()
        for storage, _, writable in self._scan_locations():
            for path, detail in list(self.cached_files[storage].items()):
                if now - detail["time"] > expiry_time:
                    expired_files.append(os.path.join(storage, detail["fn"]))
                    del self.cached_files[storage][path]
        
        writable_cache = self.cached_files[self._storage[-1]]
        return expired_files, len(writable_cache) == 0

    def load(self) -> None:
        """Load all metadata from disk and store in ``self.cached_files``"""
        self.cached_files = {}
        for storage, fn, _ in self._scan_locations():
            if os.path.exists(fn):
                self.cached_files[storage] = self._load(fn)
            else:
                self.cached_files[storage] = {}

    def on_close_cached_file(self, f: Any, path: str) -> None:
        """Perform side-effect actions on closing a cached file.

        The actual closing of the file is the responsibility of the caller.
        """
        if f.mode == 'rb':
            return
        fn = f.cache.cache_path
        blocks = f.cache.blocks
        if not blocks:
            return
        detail = {
            "fn": fn,
            "blocks": blocks,
            "time": time.time(),
            "size": f.size,
        }
        self.cached_files[self._storage[-1]][path] = detail
        self.save()

    def pop_file(self, path: str) -> str | None:
        """Remove metadata of cached file.

        If path is in the cache, return the filename of the cached file,
        otherwise return ``None``.  Caller is responsible for deleting the
        cached file.
        """
        for storage in reversed(self._storage):
            if path in self.cached_files[storage]:
                detail = self.cached_files[storage].pop(path)
                return os.path.join(storage, detail["fn"])
        return None

    def save(self) -> None:
        """Save metadata to disk"""
        for storage, fn, writable in self._scan_locations():
            if writable:
                self._save(self.cached_files[storage], fn)

    def update_file(self, path: str, detail: Detail) -> None:
        """Update metadata for specific file in memory, do not save"""
        self.cached_files[self._storage[-1]][path] = detail
