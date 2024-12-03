import dask
from distributed.client import Client, _get_global_client
from distributed.worker import Worker
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import infer_storage_options

class DaskWorkerFileSystem(AbstractFileSystem):
    """View files accessible to a worker as any other remote file-system

    When instances are run on the worker, uses the real filesystem. When
    run on the client, they call the worker to provide information or data.

    **Warning** this implementation is experimental, and read-only for now.
    """

    def __init__(self, target_protocol=None, target_options=None, fs=None, client=None, **kwargs):
        super().__init__(**kwargs)
        if not (fs is None) ^ (target_protocol is None):
            raise ValueError('Please provide one of filesystem instance (fs) or target_protocol, not both')
        self.target_protocol = target_protocol
        self.target_options = target_options
        self.worker = None
        self.client = client
        self.fs = fs
        self._determine_worker()

class DaskFile(AbstractBufferedFile):

    def __init__(self, mode='rb', **kwargs):
        if mode != 'rb':
            raise ValueError('Remote dask files can only be opened in "rb" mode')
        super().__init__(**kwargs)

    def _initiate_upload(self):
        """Create remote file/upload"""
        if self.mode not in {'wb', 'ab'}:
            raise ValueError("File must be opened in write mode")
        self.buffer = io.BytesIO()
        self.offset = 0
        self.forced = False

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        if self.fs.worker is None:
            return self.fs.fs._fetch_range(self.path, start, end)
        else:
            return self.fs.worker.fetch_range(self.path, start, end)
