import json
import os
import pickle
import shutil
import tempfile
import pytest
import fsspec
from fsspec.compression import compr
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cache_mapper import BasenameCacheMapper, HashCacheMapper, create_cache_mapper
from fsspec.implementations.cached import CachingFileSystem, LocalTempFile, WholeFileCacheFileSystem
from fsspec.implementations.local import make_path_posix
from fsspec.implementations.zip import ZipFileSystem
from fsspec.tests.conftest import win
from .test_ftp import FTPFileSystem

def test_equality(tmpdir):
    """Test sane behaviour for equality and hashing.

    Make sure that different CachingFileSystem only test equal to each other
    when they should, and do not test equal to the filesystem they rely upon.
    Similarly, make sure their hashes differ when they should and are equal
    when they should not.

    Related: GitHub#577, GitHub#578
    """
    from fsspec.implementations.cached import CachingFileSystem
    from fsspec.implementations.memory import MemoryFileSystem

    fs1 = CachingFileSystem(target_protocol='memory', cache_storage=str(tmpdir))
    fs2 = CachingFileSystem(target_protocol='memory', cache_storage=str(tmpdir))
    fs3 = CachingFileSystem(target_protocol='memory', cache_storage=str(tmpdir), cache_check=20)
    mem_fs = MemoryFileSystem()

    assert fs1 == fs2
    assert fs1 != fs3
    assert fs1 != mem_fs

    assert hash(fs1) == hash(fs2)
    assert hash(fs1) != hash(fs3)
    assert hash(fs1) != hash(mem_fs)

def test_str():
    """Test that the str representation refers to correct class."""
    from fsspec.implementations.cached import CachingFileSystem

    fs = CachingFileSystem(target_protocol='memory', cache_storage='/tmp/cache')
    assert 'CachingFileSystem' in str(fs)
    assert 'target_protocol=memory' in str(fs)
    assert 'cache_storage=/tmp/cache' in str(fs)
