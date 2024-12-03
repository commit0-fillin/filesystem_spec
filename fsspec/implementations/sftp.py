import datetime
import logging
import os
import types
import uuid
from stat import S_ISDIR, S_ISLNK
import paramiko
from .. import AbstractFileSystem
from ..utils import infer_storage_options
logger = logging.getLogger('fsspec.sftp')

class SFTPFileSystem(AbstractFileSystem):
    """Files over SFTP/SSH

    Peer-to-peer filesystem over SSH using paramiko.

    Note: if using this with the ``open`` or ``open_files``, with full URLs,
    there is no way to tell if a path is relative, so all paths are assumed
    to be absolute.
    """
    protocol = ('sftp', 'ssh')

    def __init__(self, host, **ssh_kwargs):
        """

        Parameters
        ----------
        host: str
            Hostname or IP as a string
        temppath: str
            Location on the server to put files, when within a transaction
        ssh_kwargs: dict
            Parameters passed on to connection. See details in
            https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect
            May include port, username, password...
        """
        if self._cached:
            return
        super().__init__(**ssh_kwargs)
        self.temppath = ssh_kwargs.pop('temppath', '/tmp')
        self.host = host
        self.ssh_kwargs = ssh_kwargs
        self._connect()

    def _open(self, path, mode='rb', block_size=None, **kwargs):
        """
        block_size: int or None
            If 0, no buffering, if 1, line buffering, if >1, buffer that many
            bytes, if None use default from paramiko.
        """
        path = self._strip_protocol(path)
        
        if 'r' in mode:
            f = self.sftp.open(path, mode=mode)
        else:
            dir_path = self._parent(path)
            if dir_path:
                self.makedirs(dir_path, exist_ok=True)
            f = self.sftp.open(path, mode=mode)
        
        if block_size is not None:
            f.set_pipelined(block_size > 0)
        
        return f
