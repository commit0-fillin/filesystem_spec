import os
import sys
import uuid
import warnings
from ftplib import FTP, Error, error_perm
from typing import Any
from ..spec import AbstractBufferedFile, AbstractFileSystem
from ..utils import infer_storage_options, isfilelike

class FTPFileSystem(AbstractFileSystem):
    """A filesystem over classic FTP"""
    root_marker = '/'
    cachable = False
    protocol = 'ftp'

    def __init__(self, host, port=21, username=None, password=None, acct=None, block_size=None, tempdir=None, timeout=30, encoding='utf-8', **kwargs):
        """
        You can use _get_kwargs_from_urls to get some kwargs from
        a reasonable FTP url.

        Authentication will be anonymous if username/password are not
        given.

        Parameters
        ----------
        host: str
            The remote server name/ip to connect to
        port: int
            Port to connect with
        username: str or None
            If authenticating, the user's identifier
        password: str of None
            User's password on the server, if using
        acct: str or None
            Some servers also need an "account" string for auth
        block_size: int or None
            If given, the read-ahead or write buffer size.
        tempdir: str
            Directory on remote to put temporary files when in a transaction
        timeout: int
            Timeout of the ftp connection in seconds
        encoding: str
            Encoding to use for directories and filenames in FTP connection
        """
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.tempdir = tempdir or '/tmp'
        self.cred = (username, password, acct)
        self.timeout = timeout
        self.encoding = encoding
        if block_size is not None:
            self.blocksize = block_size
        else:
            self.blocksize = 2 ** 16
        self._connect()
        self.ftp = None

    def __del__(self):
        if self.ftp:
            try:
                self.ftp.quit()
            except Exception:
                self.ftp.close()

class TransferDone(Exception):
    """Internal exception to break out of transfer"""
    pass

class FTPFile(AbstractBufferedFile):
    """Interact with a remote FTP file with read/write buffering"""

    def __init__(self, fs, path, mode='rb', block_size='default', autocommit=True, cache_type='readahead', cache_options=None, **kwargs):
        super().__init__(fs, path, mode=mode, block_size=block_size, autocommit=autocommit, cache_type=cache_type, cache_options=cache_options, **kwargs)
        if not autocommit:
            self.target = self.path
            self.path = '/'.join([kwargs['tempdir'], str(uuid.uuid4())])

    def _fetch_range(self, start, end):
        """Get bytes between given byte limits

        Implemented by raising an exception in the fetch callback when the
        number of bytes received reaches the requested amount.

        Will fail if the server does not respect the REST command on
        retrieve requests.
        """
        out = io.BytesIO()
        
        def callback(data):
            out.write(data)
            if out.tell() >= end - start:
                raise TransferDone

        self.fs.ftp.retrbinary(f'RETR {self.path}', callback, rest=start)
        return out.getvalue()[:end-start]

def _mlsd2(ftp, path='.'):
    """
    Fall back to using `dir` instead of `mlsd` if not supported.

    This parses a Linux style `ls -l` response to `dir`, but the response may
    be platform dependent.

    Parameters
    ----------
    ftp: ftplib.FTP
    path: str
        Expects to be given path, but defaults to ".".
    """
    from datetime import datetime
    import re

    lines = []
    ftp.dir(path, lines.append)

    for line in lines:
        parts = line.split(None, 8)
        if len(parts) < 9:
            continue

        perms, _, user, group, size, month, day, year_or_time, name = parts

        if ':' in year_or_time:
            year = datetime.now().year
            time = year_or_time
        else:
            year = int(year_or_time)
            time = '00:00'

        dt_str = f'{month} {day} {year} {time}'
        mtime = datetime.strptime(dt_str, '%b %d %Y %H:%M')

        type_ = 'dir' if perms.startswith('d') else 'file'
        
        yield {
            'name': name,
            'type': type_,
            'size': int(size),
            'mtime': mtime.timestamp(),
        }
