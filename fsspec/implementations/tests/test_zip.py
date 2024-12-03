import collections.abc
import os.path
import pytest
import fsspec
from fsspec.implementations.tests.test_archive import archive_data, tempzip

def test_fsspec_get_mapper():
    """Added for #788"""
    with tempzip() as fn:
        mapper = fsspec.get_mapper(f"zip://{fn}")
        assert isinstance(mapper, collections.abc.MutableMapping)
        assert set(mapper.keys()) == set(archive_data.keys())
        for k, v in archive_data.items():
            assert mapper[k] == v
