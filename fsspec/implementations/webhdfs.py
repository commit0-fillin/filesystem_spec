import logging
import os
import secrets
import shutil
import tempfile
import uuid
from contextlib import suppress
from urllib.parse import quote
import requests
from ..spec import AbstractBufferedFile, AbstractFileSystem
from ..utils import infer_storage_options, tokenize
logger = logging.getLogger('webhdfs')

class WebHDFS(AbstractFileSystem):
    """
    Interface to HDFS over HTTP using the WebHDFS API. Supports also HttpFS gateways.

    Four auth mechanisms are supported:

    insecure: no auth is done, and the user is assumed to be whoever they
        say they are (parameter ``user``), or a predefined value such as
        "dr.who" if not given
    spnego: when kerberos authentication is enabled, auth is negotiated by
        requests_kerberos https://github.com/requests/requests-kerberos .
        This establishes a session based on existing kinit login and/or
        specified principal/password; parameters are passed with ``kerb_kwargs``
    token: uses an existing Hadoop delegation token from another secured
        service. Indeed, this client can also generate such tokens when
        not insecure. Note that tokens expire, but can be renewed (by a
        previously specified user) and may allow for proxying.
    basic-auth: used when both parameter ``user`` and parameter ``password``
        are provided.

    """
    tempdir = str(tempfile.gettempdir())
    protocol = ('webhdfs', 'webHDFS')

    def __init__(self, host, port=50070, kerberos=False, token=None, user=None, password=None, proxy_to=None, kerb_kwargs=None, data_proxy=None, use_https=False, session_cert=None, session_verify=True, **kwargs):
        """
        Parameters
        ----------
        host: str
            Name-node address
        port: int
            Port for webHDFS
        kerberos: bool
            Whether to authenticate with kerberos for this connection
        token: str or None
            If given, use this token on every call to authenticate. A user
            and user-proxy may be encoded in the token and should not be also
            given
        user: str or None
            If given, assert the user name to connect with
        password: str or None
            If given, assert the password to use for basic auth. If password
            is provided, user must be provided also
        proxy_to: str or None
            If given, the user has the authority to proxy, and this value is
            the user in who's name actions are taken
        kerb_kwargs: dict
            Any extra arguments for HTTPKerberosAuth, see
            `<https://github.com/requests/requests-kerberos/blob/master/requests_kerberos/kerberos_.py>`_
        data_proxy: dict, callable or None
            If given, map data-node addresses. This can be necessary if the
            HDFS cluster is behind a proxy, running on Docker or otherwise has
            a mismatch between the host-names given by the name-node and the
            address by which to refer to them from the client. If a dict,
            maps host names ``host->data_proxy[host]``; if a callable, full
            URLs are passed, and function must conform to
            ``url->data_proxy(url)``.
        use_https: bool
            Whether to connect to the Name-node using HTTPS instead of HTTP
        session_cert: str or Tuple[str, str] or None
            Path to a certificate file, or tuple of (cert, key) files to use
            for the requests.Session
        session_verify: str, bool or None
            Path to a certificate file to use for verifying the requests.Session.
        kwargs
        """
        if self._cached:
            return
        super().__init__(**kwargs)
        self.url = f'{('https' if use_https else 'http')}://{host}:{port}/webhdfs/v1'
        self.kerb = kerberos
        self.kerb_kwargs = kerb_kwargs or {}
        self.pars = {}
        self.proxy = data_proxy or {}
        if token is not None:
            if user is not None or proxy_to is not None:
                raise ValueError('If passing a delegation token, must not set user or proxy_to, as these are encoded in the token')
            self.pars['delegation'] = token
        self.user = user
        self.password = password
        if password is not None:
            if user is None:
                raise ValueError('If passing a password, the user must also be set in order to set up the basic-auth')
        elif user is not None:
            self.pars['user.name'] = user
        if proxy_to is not None:
            self.pars['doas'] = proxy_to
        if kerberos and user is not None:
            raise ValueError('If using Kerberos auth, do not specify the user, this is handled by kinit.')
        self.session_cert = session_cert
        self.session_verify = session_verify
        self._connect()
        self._fsid = f'webhdfs_{tokenize(host, port)}'

    def _open(self, path, mode='rb', block_size=None, autocommit=True, replication=None, permissions=None, **kwargs):
        """
        Parameters
        ----------
        path: str
            File location
        mode: str
            'rb', 'wb', etc.
        block_size: int
            Client buffer size for read-ahead or write buffer
        autocommit: bool
            If False, writes to temporary file that only gets put in final
            location upon commit
        replication: int
            Number of copies of file on the cluster, write mode only
        permissions: str or int
            posix permissions, write mode only
        kwargs

        Returns
        -------
        WebHDFile instance
        """
        return WebHDFile(self, path, mode=mode, block_size=block_size, autocommit=autocommit,
                         replication=replication, permissions=permissions, **kwargs)

    def content_summary(self, path):
        """Total numbers of files, directories and bytes under path"""
        path = self._strip_protocol(path)
        out = self._call('GETSUMMARY', path)
        return out['ContentSummary']

    def ukey(self, path):
        """Checksum info of file, giving method and result"""
        path = self._strip_protocol(path)
        out = self._call('GETFILECHECKSUM', path)
        return out['FileChecksum']

    def home_directory(self):
        """Get user's home directory"""
        out = self._call('GETHOMEDIRECTORY')
        return out['Path']

    def get_delegation_token(self, renewer=None):
        """Retrieve token which can give the same authority to other uses

        Parameters
        ----------
        renewer: str or None
            User who may use this token; if None, will be current user
        """
        if renewer:
            out = self._call('GETDELEGATIONTOKEN', renewer=renewer)
        else:
            out = self._call('GETDELEGATIONTOKEN')
        return out['Token']['urlString']

    def renew_delegation_token(self, token):
        """Make token live longer. Returns new expiry time"""
        out = self._call('RENEWDELEGATIONTOKEN', token=token)
        return out['long']

    def cancel_delegation_token(self, token):
        """Stop the token from being useful"""
        self._call('CANCELDELEGATIONTOKEN', token=token)

    def chmod(self, path, mod):
        """Set the permission at path

        Parameters
        ----------
        path: str
            location to set (file or directory)
        mod: str or int
            posix representation of permission, give as oct string, e.g, '777'
            or 0o777
        """
        path = self._strip_protocol(path)
        if isinstance(mod, str):
            mod = int(mod, 8)
        self._call('SETPERMISSION', path, permission=oct(mod)[2:])

    def chown(self, path, owner=None, group=None):
        """Change owning user and/or group"""
        path = self._strip_protocol(path)
        kwargs = {}
        if owner is not None:
            kwargs['owner'] = owner
        if group is not None:
            kwargs['group'] = group
        self._call('SETOWNER', path, **kwargs)

    def set_replication(self, path, replication):
        """
        Set file replication factor

        Parameters
        ----------
        path: str
            File location (not for directories)
        replication: int
            Number of copies of file on the cluster. Should be smaller than
            number of data nodes; normally 3 on most systems.
        """
        path = self._strip_protocol(path)
        self._call('SETREPLICATION', path, replication=replication)

class WebHDFile(AbstractBufferedFile):
    """A file living in HDFS over webHDFS"""

    def __init__(self, fs, path, **kwargs):
        super().__init__(fs, path, **kwargs)
        kwargs = kwargs.copy()
        if kwargs.get('permissions', None) is None:
            kwargs.pop('permissions', None)
        if kwargs.get('replication', None) is None:
            kwargs.pop('replication', None)
        self.permissions = kwargs.pop('permissions', 511)
        tempdir = kwargs.pop('tempdir')
        if kwargs.pop('autocommit', False) is False:
            self.target = self.path
            self.path = os.path.join(tempdir, str(uuid.uuid4()))

        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        pass
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        pass

    def _initiate_upload(self):
        """Create remote file/upload"""
        if self.mode not in {'wb', 'ab'}:
            raise ValueError("File must be in write or append mode")
        if self.mode == 'wb':
            self.fs._call('CREATE', self.path, overwrite=True)
        elif self.mode == 'ab':
            info = self.fs.info(self.path)
            self.offset = info['size']
