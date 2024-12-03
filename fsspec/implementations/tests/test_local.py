import bz2
import gzip
import os
import os.path
import pickle
import posixpath
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
import pytest
import fsspec
from fsspec import compression
from fsspec.core import OpenFile, get_fs_token_paths, open_files
from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.tests.test_utils import WIN
files = {'.test.accounts.1.json': b'{"amount": 100, "name": "Alice"}\n{"amount": 200, "name": "Bob"}\n{"amount": 300, "name": "Charlie"}\n{"amount": 400, "name": "Dennis"}\n', '.test.accounts.2.json': b'{"amount": 500, "name": "Alice"}\n{"amount": 600, "name": "Bob"}\n{"amount": 700, "name": "Charlie"}\n{"amount": 800, "name": "Dennis"}\n'}
csv_files = {'.test.fakedata.1.csv': b'a,b\n1,2\n', '.test.fakedata.2.csv': b'a,b\n3,4\n'}
odir = os.getcwd()

@contextmanager
def filetexts(d, open=open, mode='t'):
    """Dumps a number of textfiles to disk

    d - dict
        a mapping from filename to text like {'a.csv': '1,1
2,2'}

    Since this is meant for use in tests, this context manager will
    automatically switch to a temporary current directory, to avoid
    race conditions when running tests in parallel.
    """
    original_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as dirname:
        os.chdir(dirname)
        for filename, text in d.items():
            with open(filename, 'w' + mode) as f:
                f.write(text)
        yield
        os.chdir(original_dir)

def test_urlpath_expand_read():
    """Make sure * is expanded in file paths when reading."""
    with filetexts(csv_files):
        fn = sorted(csv_files)[0]
        fs = LocalFileSystem()
        files = open_files('./*.csv')
        assert len(files) == 2
        assert [f.path for f in files] == [os.path.abspath(p) for p in sorted(csv_files)]

        with open_files('./*.csv') as f:
            data = f[0].read()
            assert data == csv_files[fn]

def test_urlpath_expand_write():
    """Make sure * is expanded in file paths when writing."""
    tmpdir = tempfile.mkdtemp()
    try:
        fs = LocalFileSystem()
        files = open_files(os.path.join(tmpdir, "temp*.txt"), mode='w')
        assert len(files) == 1
        assert files[0].path == os.path.join(tmpdir, 'temp0.txt')

        files = open_files(os.path.join(tmpdir, "temp*.txt"), mode='w', num=2)
        assert len(files) == 2
        assert files[0].path == os.path.join(tmpdir, 'temp0.txt')
        assert files[1].path == os.path.join(tmpdir, 'temp1.txt')

        with open_files(os.path.join(tmpdir, "temp*.txt"), mode='w', num=2) as files:
            for file, text in zip(files, ['test1', 'test2']):
                file.write(text)

        with open_files(os.path.join(tmpdir, "temp*.txt"), mode='r') as files:
            assert files[0].read() == 'test1'
            assert files[1].read() == 'test2'
    finally:
        shutil.rmtree(tmpdir)
