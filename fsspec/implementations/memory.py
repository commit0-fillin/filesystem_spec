from __future__ import annotations
import logging
from datetime import datetime, timezone
from errno import ENOTEMPTY
from io import BytesIO
from pathlib import PurePath, PureWindowsPath
from typing import Any, ClassVar
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import stringify_path
logger = logging.getLogger('fsspec.memoryfs')

class MemoryFileSystem(AbstractFileSystem):
    """A filesystem based on a dict of BytesIO objects

    This is a global filesystem so instances of this class all point to the same
    in memory filesystem.
    """
    store: ClassVar[dict[str, Any]] = {}
    pseudo_dirs = ['']
    protocol = 'memory'
    root_marker = '/'

    def pipe_file(self, path, value, **kwargs):
        """Set the bytes of given file

        Avoids copies of the data if possible
        """
        if isinstance(value, bytes):
            self.store[path] = MemoryFile(self, path, value)
        elif isinstance(value, str):
            self.store[path] = MemoryFile(self, path, value.encode())
        elif hasattr(value, 'read'):
            if hasattr(value, 'seekable') and value.seekable():
                value.seek(0)
            self.store[path] = MemoryFile(self, path, value.read())
        else:
            raise ValueError("Cannot pipe value of type {}".format(type(value)))
        
        # Update parent directories
        parts = path.split('/')
        for i in range(1, len(parts)):
            parent = '/'.join(parts[:i])
            if parent not in self.pseudo_dirs:
                self.pseudo_dirs.append(parent)

class MemoryFile(BytesIO):
    """A BytesIO which can't close and works as a context manager

    Can initialise with data. Each path should only be active once at any moment.

    No need to provide fs, path if auto-committing (default)
    """

    def __init__(self, fs=None, path=None, data=None):
        logger.debug('open file %s', path)
        self.fs = fs
        self.path = path
        self.created = datetime.now(tz=timezone.utc)
        self.modified = datetime.now(tz=timezone.utc)
        if data:
            super().__init__(data)
            self.seek(0)

    def __enter__(self):
        return self
