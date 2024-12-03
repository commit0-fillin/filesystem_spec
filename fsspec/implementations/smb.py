"""
This module contains SMBFileSystem class responsible for handling access to
Windows Samba network shares by using package smbprotocol
"""
import datetime
import uuid
from stat import S_ISDIR, S_ISLNK
import smbclient
from .. import AbstractFileSystem
from ..utils import infer_storage_options

class SMBFileSystem(AbstractFileSystem):
    """Allow reading and writing to Windows and Samba network shares.

    When using `fsspec.open()` for getting a file-like object the URI
    should be specified as this format:
    ``smb://workgroup;user:password@server:port/share/folder/file.csv``.

    Example::

        >>> import fsspec
        >>> with fsspec.open(
        ...     'smb://myuser:mypassword@myserver.com/' 'share/folder/file.csv'
        ... ) as smbfile:
        ...     df = pd.read_csv(smbfile, sep='|', header=None)

    Note that you need to pass in a valid hostname or IP address for the host
    component of the URL. Do not use the Windows/NetBIOS machine name for the
    host component.

    The first component of the path in the URL points to the name of the shared
    folder. Subsequent path components will point to the directory/folder/file.

    The URL components ``workgroup`` , ``user``, ``password`` and ``port`` may be
    optional.

    .. note::

        For working this source require `smbprotocol`_ to be installed, e.g.::

            $ pip install smbprotocol
            # or
            # pip install smbprotocol[kerberos]

    .. _smbprotocol: https://github.com/jborean93/smbprotocol#requirements

    Note: if using this with the ``open`` or ``open_files``, with full URLs,
    there is no way to tell if a path is relative, so all paths are assumed
    to be absolute.
    """
    protocol = 'smb'

    def __init__(self, host, port=None, username=None, password=None, timeout=60, encrypt=None, share_access=None, register_session_retries=5, auto_mkdir=False, **kwargs):
        """
        You can use _get_kwargs_from_urls to get some kwargs from
        a reasonable SMB url.

        Authentication will be anonymous or integrated if username/password are not
        given.

        Parameters
        ----------
        host: str
            The remote server name/ip to connect to
        port: int or None
            Port to connect with. Usually 445, sometimes 139.
        username: str or None
            Username to connect with. Required if Kerberos auth is not being used.
        password: str or None
            User's password on the server, if using username
        timeout: int
            Connection timeout in seconds
        encrypt: bool
            Whether to force encryption or not, once this has been set to True
            the session cannot be changed back to False.
        share_access: str or None
            Specifies the default access applied to file open operations
            performed with this file system object.
            This affects whether other processes can concurrently open a handle
            to the same file.

            - None (the default): exclusively locks the file until closed.
            - 'r': Allow other handles to be opened with read access.
            - 'w': Allow other handles to be opened with write access.
            - 'd': Allow other handles to be opened with delete access.
        auto_mkdir: bool
            Whether, when opening a file, the directory containing it should
            be created (if it doesn't already exist). This is assumed by pyarrow
            and zarr-python code.
        """
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.encrypt = encrypt
        self.temppath = kwargs.pop('temppath', '')
        self.share_access = share_access
        self.register_session_retries = register_session_retries
        self.auto_mkdir = auto_mkdir
        self._connect()

    def _connect(self):
        smbclient.register_session(
            self.host,
            username=self.username,
            password=self.password,
            port=self.port,
            encrypt=self.encrypt,
            connection_timeout=self.timeout,
        )

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        info = smbclient.stat(self._strip_protocol(path))
        return datetime.datetime.fromtimestamp(info.create_time)

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        info = smbclient.stat(self._strip_protocol(path))
        return datetime.datetime.fromtimestamp(info.change_time)

    def _open(self, path, mode='rb', block_size=-1, autocommit=True, cache_options=None, **kwargs):
        """
        block_size: int or None
            If 0, no buffering, 1, line buffering, >1, buffer that many bytes

        Notes
        -----
        By specifying 'share_access' in 'kwargs' it is possible to override the
        default shared access setting applied in the constructor of this object.
        """
        path = self._strip_protocol(path)
        share_access = kwargs.get('share_access', self.share_access)
        
        if mode == 'rb':
            return smbclient.open_file(path, mode='rb', share_access=share_access, buffering=block_size)
        elif mode == 'wb':
            if self.auto_mkdir:
                self.makedirs(self._parent(path), exist_ok=True)
            temp = kwargs.pop('temp', self.temppath)
            return SMBFileOpener(path, temp, mode, port=self.port, block_size=block_size, **kwargs)
        else:
            raise NotImplementedError(f"File mode not supported: {mode}")

    def copy(self, path1, path2, **kwargs):
        """Copy within two locations in the same filesystem"""
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        smbclient.copyfile(path1, path2)

class SMBFileOpener:
    """writes to remote temporary file, move on commit"""

    def __init__(self, path, temp, mode, port=445, block_size=-1, **kwargs):
        self.path = path
        self.temp = temp
        self.mode = mode
        self.block_size = block_size
        self.kwargs = kwargs
        self.smbfile = None
        self._incontext = False
        self.port = port
        self._open()

    def commit(self):
        """Move temp file to definitive on success."""
        if self.smbfile:
            self.smbfile.close()
        smbclient.rename(self.temp, self.path)

    def discard(self):
        """Remove the temp file on failure."""
        if self.smbfile:
            self.smbfile.close()
        try:
            smbclient.remove(self.temp)
        except FileNotFoundError:
            pass

    def __fspath__(self):
        return self.path

    def __iter__(self):
        return self.smbfile.__iter__()

    def __getattr__(self, item):
        return getattr(self.smbfile, item)

    def __enter__(self):
        self._incontext = True
        return self.smbfile.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self._incontext = False
        self.smbfile.__exit__(exc_type, exc_value, traceback)
