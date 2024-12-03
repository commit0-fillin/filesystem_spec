import os
import shlex
import subprocess
import time
from tarfile import TarFile
import pytest
import fsspec
pytest.importorskip('paramiko')

def make_tarfile(files_to_pack, tmp_path):
    """Create a tarfile with some files."""
    tarfile_path = tmp_path / "test.tar"
    with TarFile.open(tarfile_path, "w") as tar:
        for file_name, content in files_to_pack.items():
            file_path = tmp_path / file_name
            with open(file_path, "w") as f:
                f.write(content)
            tar.add(file_path, arcname=file_name)
    return str(tarfile_path)
