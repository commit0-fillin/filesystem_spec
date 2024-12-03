import pytest
import os
import tempfile
from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.tests.abstract import AbstractFixtures

class LocalFixtures(AbstractFixtures):
    @pytest.fixture
    def fs(self):
        return LocalFileSystem()

    @pytest.fixture
    def root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield make_path_posix(tmpdir)

    @pytest.fixture
    def path1(self, root):
        return os.path.join(root, 'file1')

    @pytest.fixture
    def path2(self, root):
        return os.path.join(root, 'file2')

    @pytest.fixture
    def file1(self, fs, path1):
        with fs.open(path1, 'wb') as f:
            f.write(b'test data 1')
        return path1

    @pytest.fixture
    def file2(self, fs, path2):
        with fs.open(path2, 'wb') as f:
            f.write(b'test data 2')
        return path2

    @pytest.fixture
    def data1(self):
        return b'test data 1'

    @pytest.fixture
    def data2(self):
        return b'test data 2'
