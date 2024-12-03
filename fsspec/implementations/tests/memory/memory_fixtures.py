import pytest
from fsspec import filesystem
from fsspec.tests.abstract import AbstractFixtures

class MemoryFixtures(AbstractFixtures):
    protocol = "memory"
    
    @pytest.fixture
    def fs(self):
        return filesystem("memory")
    
    @pytest.fixture
    def fs2(self):
        return filesystem("memory")
    
    @pytest.fixture
    def create_hierarchy(self, fs):
        fs.mkdir("/root")
        fs.mkdir("/root/dir1")
        fs.mkdir("/root/dir2")
        fs.touch("/root/file1")
        fs.touch("/root/dir1/file2")
        fs.touch("/root/dir2/file3")
        return fs
    
    @pytest.fixture
    def temp_file(self, fs):
        fs.touch("/temp_file")
        return "/temp_file"
    
    @pytest.fixture
    def temp_dir(self, fs):
        fs.mkdir("/temp_dir")
        return "/temp_dir"
