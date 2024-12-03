from __future__ import annotations
import os
import shutil
import tarfile
import tempfile
from io import BytesIO
from pathlib import Path
import pytest
import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.cached import WholeFileCacheFileSystem
from fsspec.implementations.tar import TarFileSystem
from fsspec.implementations.tests.test_archive import archive_data, temptar

@pytest.mark.parametrize('recipe', [{'mode': 'w', 'suffix': '.tar', 'magic': b'a\x00\x00\x00\x00'}, {'mode': 'w:gz', 'suffix': '.tar.gz', 'magic': b'\x1f\x8b\x08\x08'}, {'mode': 'w:bz2', 'suffix': '.tar.bz2', 'magic': b'BZh91AY'}, {'mode': 'w:xz', 'suffix': '.tar.xz', 'magic': b'\xfd7zXZ\x00\x00'}], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_compressions(recipe):
    """
    Run tests on all available tar file compression variants.
    """
    with temptar(**recipe) as fn:
        with open(fn, 'rb') as f:
            assert f.read(4) == recipe['magic']
        
        fs = TarFileSystem(fn)
        assert fs.ls('/') == ['a', 'b', 'deeply']
        assert fs.ls('/deeply/nested') == ['path']
        
        with fs.open('/b') as f:
            assert f.read() == b'hello'
        
        with fs.open('/deeply/nested/path') as f:
            assert f.read() == b'stuff'

@pytest.mark.parametrize('recipe', [{'mode': 'w', 'suffix': '.tar', 'magic': b'a\x00\x00\x00\x00'}, {'mode': 'w:gz', 'suffix': '.tar.gz', 'magic': b'\x1f\x8b\x08\x08'}, {'mode': 'w:bz2', 'suffix': '.tar.bz2', 'magic': b'BZh91AY'}, {'mode': 'w:xz', 'suffix': '.tar.xz', 'magic': b'\xfd7zXZ\x00\x00'}], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_filesystem_direct(recipe, tmpdir):
    """
    Run tests through a real fsspec filesystem implementation.
    Here: `LocalFileSystem`.
    """
    with temptar(**recipe) as fn:
        fs = fsspec.filesystem("file")
        tar_fs = TarFileSystem(fs.open(fn, "rb"))
        
        assert tar_fs.ls('/') == ['a', 'b', 'deeply']
        assert tar_fs.ls('/deeply/nested') == ['path']
        
        with tar_fs.open('/b') as f:
            assert f.read() == b'hello'
        
        with tar_fs.open('/deeply/nested/path') as f:
            assert f.read() == b'stuff'

@pytest.mark.parametrize('recipe', [{'mode': 'w', 'suffix': '.tar', 'magic': b'a\x00\x00\x00\x00'}, {'mode': 'w:gz', 'suffix': '.tar.gz', 'magic': b'\x1f\x8b\x08\x08'}, {'mode': 'w:bz2', 'suffix': '.tar.bz2', 'magic': b'BZh91AY'}, {'mode': 'w:xz', 'suffix': '.tar.xz', 'magic': b'\xfd7zXZ\x00\x00'}], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_filesystem_cached(recipe, tmpdir):
    """
    Run tests through a real, cached, fsspec filesystem implementation.
    Here: `TarFileSystem` over `WholeFileCacheFileSystem` over `LocalFileSystem`.
    """
    with temptar(**recipe) as fn:
        fs = fsspec.filesystem("file")
        cached_fs = WholeFileCacheFileSystem(fs=fs, cache_storage=str(tmpdir))
        tar_fs = TarFileSystem(cached_fs.open(fn, "rb"))
        
        assert tar_fs.ls('/') == ['a', 'b', 'deeply']
        assert tar_fs.ls('/deeply/nested') == ['path']
        
        with tar_fs.open('/b') as f:
            assert f.read() == b'hello'
        
        with tar_fs.open('/deeply/nested/path') as f:
            assert f.read() == b'stuff'
        
        # Check if the file is cached
        assert len(cached_fs.cache) == 1
        assert list(cached_fs.cache.keys())[0] == fn

@pytest.mark.parametrize('compression', ['', 'gz', 'bz2', 'xz'], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_ls_with_folders(compression: str, tmp_path: Path):
    """
    Create a tar file that doesn't include the intermediate folder structure,
    but make sure that the reading filesystem is still able to resolve the
    intermediate folders, like the ZipFileSystem.
    """
    mode = 'w:' + (compression or '')
    fn = tmp_path / f"test.tar{'.'+compression if compression else ''}"
    
    data = {
        'a': b'content of a',
        'b/c': b'content of b/c',
        'b/d/e': b'content of b/d/e'
    }
    
    with tarfile.open(fn, mode=mode) as tar:
        for path, content in data.items():
            info = tarfile.TarInfo(name=path)
            info.size = len(content)
            tar.addfile(info, BytesIO(content))
    
    fs = TarFileSystem(str(fn))
    
    assert fs.ls('/') == ['a', 'b']
    assert fs.ls('/b') == ['c', 'd']
    assert fs.ls('/b/d') == ['e']
    
    with fs.open('/a') as f:
        assert f.read() == b'content of a'
    
    with fs.open('/b/c') as f:
        assert f.read() == b'content of b/c'
    
    with fs.open('/b/d/e') as f:
        assert f.read() == b'content of b/d/e'
