import bz2
import gzip
import lzma
import os
import pickle
import tarfile
import tempfile
import zipfile
from contextlib import contextmanager
from io import BytesIO
import pytest
import fsspec
archive_data = {'a': b'', 'b': b'hello', 'deeply/nested/path': b'stuff'}

@contextmanager
def tempzip(data=None):
    """
    Provide test cases with temporary synthesized Zip archives.
    """
    if data is None:
        data = archive_data
    with tempfile.NamedTemporaryFile(suffix='.zip') as tmp:
        with zipfile.ZipFile(tmp, mode='w') as zf:
            for k, v in data.items():
                zf.writestr(k, v)
        tmp.flush()
        yield tmp.name

@contextmanager
def temparchive(data=None):
    """
    Provide test cases with temporary synthesized 7-Zip archives.
    """
    if data is None:
        data = archive_data
    with tempfile.NamedTemporaryFile(suffix='.7z') as tmp:
        with py7zr.SevenZipFile(tmp.name, mode='w') as archive:
            for k, v in data.items():
                archive.writestr(v, k)
        tmp.flush()
        yield tmp.name

@contextmanager
def temptar(data=None, mode='w', suffix='.tar'):
    """
    Provide test cases with temporary synthesized .tar archives.
    """
    if data is None:
        data = archive_data
    with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
        with tarfile.open(tmp.name, mode=mode) as tar:
            for k, v in data.items():
                info = tarfile.TarInfo(name=k)
                info.size = len(v)
                tar.addfile(info, BytesIO(v))
        tmp.flush()
        yield tmp.name

@contextmanager
def temptargz(data=None, mode='w', suffix='.tar.gz'):
    """
    Provide test cases with temporary synthesized .tar.gz archives.
    """
    with temptar(data, mode=mode + ':gz', suffix=suffix) as fn:
        yield fn

@contextmanager
def temptarbz2(data=None, mode='w', suffix='.tar.bz2'):
    """
    Provide test cases with temporary synthesized .tar.bz2 archives.
    """
    with temptar(data, mode=mode + ':bz2', suffix=suffix) as fn:
        yield fn

@contextmanager
def temptarxz(data=None, mode='w', suffix='.tar.xz'):
    """
    Provide test cases with temporary synthesized .tar.xz archives.
    """
    with temptar(data, mode=mode + ':xz', suffix=suffix) as fn:
        yield fn

class ArchiveTestScenario:
    """
    Describe a test scenario for any type of archive.
    """

    def __init__(self, protocol=None, provider=None, variant=None):
        self.protocol = protocol
        self.provider = provider
        self.variant = variant

def pytest_generate_tests(metafunc):
    """
    Generate test scenario parametrization arguments with appropriate labels (idlist).

    On the one hand, this yields an appropriate output like::

        fsspec/implementations/tests/test_archive.py::TestArchive::test_empty[zip] PASSED  # noqa

    On the other hand, it will support perfect test discovery, like::

        pytest fsspec -vvv -k "zip or tar or libarchive"

    https://docs.pytest.org/en/latest/example/parametrize.html#a-quick-port-of-testscenarios
    """
    if 'scenario' in metafunc.fixturenames:
        idlist = []
        argvalues = []
        for scenario in metafunc.cls.scenarios:
            idlist.append(f"{scenario.protocol}")
            if scenario.variant:
                idlist[-1] += f"-{scenario.variant}"
            argvalues.append(scenario)
        metafunc.parametrize('scenario', argvalues, ids=idlist, scope="class")
scenario_zip = ArchiveTestScenario(protocol='zip', provider=tempzip)
scenario_tar = ArchiveTestScenario(protocol='tar', provider=temptar)
scenario_targz = ArchiveTestScenario(protocol='tar', provider=temptargz, variant='gz')
scenario_tarbz2 = ArchiveTestScenario(protocol='tar', provider=temptarbz2, variant='bz2')
scenario_tarxz = ArchiveTestScenario(protocol='tar', provider=temptarxz, variant='xz')
scenario_libarchive = ArchiveTestScenario(protocol='libarchive', provider=temparchive)

class TestAnyArchive:
    """
    Validate that all filesystem adapter implementations for archive files
    will adhere to the same specification.
    """
    scenarios = [scenario_zip, scenario_tar, scenario_targz, scenario_tarbz2, scenario_tarxz, scenario_libarchive]
