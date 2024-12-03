from __future__ import annotations
import importlib
import types
import warnings
__all__ = ['registry', 'get_filesystem_class', 'default']
_registry: dict[str, type] = {}
registry = types.MappingProxyType(_registry)
default = 'file'

def register_implementation(name, cls, clobber=False, errtxt=None):
    """Add implementation class to the registry

    Parameters
    ----------
    name: str
        Protocol name to associate with the class
    cls: class or str
        if a class: fsspec-compliant implementation class (normally inherits from
        ``fsspec.AbstractFileSystem``, gets added straight to the registry. If a
        str, the full path to an implementation class like package.module.class,
        which gets added to known_implementations,
        so the import is deferred until the filesystem is actually used.
    clobber: bool (optional)
        Whether to overwrite a protocol with the same name; if False, will raise
        instead.
    errtxt: str (optional)
        If given, then a failure to import the given class will result in this
        text being given.
    """
    if isinstance(cls, str):
        if name in known_implementations and not clobber:
            raise ValueError(f"Name {name} already in known_implementations")
        known_implementations[name] = {"class": cls, "err": errtxt}
    elif isinstance(cls, type):
        if name in _registry and not clobber:
            raise ValueError(f"Name {name} already in registry")
        _registry[name] = cls
    else:
        raise ValueError("cls must be a string or a class")
known_implementations = {'abfs': {'class': 'adlfs.AzureBlobFileSystem', 'err': 'Install adlfs to access Azure Datalake Gen2 and Azure Blob Storage'}, 'adl': {'class': 'adlfs.AzureDatalakeFileSystem', 'err': 'Install adlfs to access Azure Datalake Gen1'}, 'arrow_hdfs': {'class': 'fsspec.implementations.arrow.HadoopFileSystem', 'err': 'pyarrow and local java libraries required for HDFS'}, 'asynclocal': {'class': 'morefs.asyn_local.AsyncLocalFileSystem', 'err': "Install 'morefs[asynclocalfs]' to use AsyncLocalFileSystem"}, 'az': {'class': 'adlfs.AzureBlobFileSystem', 'err': 'Install adlfs to access Azure Datalake Gen2 and Azure Blob Storage'}, 'blockcache': {'class': 'fsspec.implementations.cached.CachingFileSystem'}, 'box': {'class': 'boxfs.BoxFileSystem', 'err': 'Please install boxfs to access BoxFileSystem'}, 'cached': {'class': 'fsspec.implementations.cached.CachingFileSystem'}, 'dask': {'class': 'fsspec.implementations.dask.DaskWorkerFileSystem', 'err': 'Install dask distributed to access worker file system'}, 'data': {'class': 'fsspec.implementations.data.DataFileSystem'}, 'dbfs': {'class': 'fsspec.implementations.dbfs.DatabricksFileSystem', 'err': 'Install the requests package to use the DatabricksFileSystem'}, 'dir': {'class': 'fsspec.implementations.dirfs.DirFileSystem'}, 'dropbox': {'class': 'dropboxdrivefs.DropboxDriveFileSystem', 'err': 'DropboxFileSystem requires "dropboxdrivefs","requests" and ""dropbox" to be installed'}, 'dvc': {'class': 'dvc.api.DVCFileSystem', 'err': 'Install dvc to access DVCFileSystem'}, 'file': {'class': 'fsspec.implementations.local.LocalFileSystem'}, 'filecache': {'class': 'fsspec.implementations.cached.WholeFileCacheFileSystem'}, 'ftp': {'class': 'fsspec.implementations.ftp.FTPFileSystem'}, 'gcs': {'class': 'gcsfs.GCSFileSystem', 'err': 'Please install gcsfs to access Google Storage'}, 'gdrive': {'class': 'gdrivefs.GoogleDriveFileSystem', 'err': 'Please install gdrivefs for access to Google Drive'}, 'generic': {'class': 'fsspec.generic.GenericFileSystem'}, 'git': {'class': 'fsspec.implementations.git.GitFileSystem', 'err': 'Install pygit2 to browse local git repos'}, 'github': {'class': 'fsspec.implementations.github.GithubFileSystem', 'err': 'Install the requests package to use the github FS'}, 'gs': {'class': 'gcsfs.GCSFileSystem', 'err': 'Please install gcsfs to access Google Storage'}, 'hdfs': {'class': 'fsspec.implementations.arrow.HadoopFileSystem', 'err': 'pyarrow and local java libraries required for HDFS'}, 'hf': {'class': 'huggingface_hub.HfFileSystem', 'err': 'Install huggingface_hub to access HfFileSystem'}, 'http': {'class': 'fsspec.implementations.http.HTTPFileSystem', 'err': 'HTTPFileSystem requires "requests" and "aiohttp" to be installed'}, 'https': {'class': 'fsspec.implementations.http.HTTPFileSystem', 'err': 'HTTPFileSystem requires "requests" and "aiohttp" to be installed'}, 'jlab': {'class': 'fsspec.implementations.jupyter.JupyterFileSystem', 'err': 'Jupyter FS requires requests to be installed'}, 'jupyter': {'class': 'fsspec.implementations.jupyter.JupyterFileSystem', 'err': 'Jupyter FS requires requests to be installed'}, 'lakefs': {'class': 'lakefs_spec.LakeFSFileSystem', 'err': 'Please install lakefs-spec to access LakeFSFileSystem'}, 'libarchive': {'class': 'fsspec.implementations.libarchive.LibArchiveFileSystem', 'err': 'LibArchive requires to be installed'}, 'local': {'class': 'fsspec.implementations.local.LocalFileSystem'}, 'memory': {'class': 'fsspec.implementations.memory.MemoryFileSystem'}, 'oci': {'class': 'ocifs.OCIFileSystem', 'err': 'Install ocifs to access OCI Object Storage'}, 'ocilake': {'class': 'ocifs.OCIFileSystem', 'err': 'Install ocifs to access OCI Data Lake'}, 'oss': {'class': 'ossfs.OSSFileSystem', 'err': 'Install ossfs to access Alibaba Object Storage System'}, 'reference': {'class': 'fsspec.implementations.reference.ReferenceFileSystem'}, 'root': {'class': 'fsspec_xrootd.XRootDFileSystem', 'err': "Install fsspec-xrootd to access xrootd storage system. Note: 'root' is the protocol name for xrootd storage systems, not referring to root directories"}, 's3': {'class': 's3fs.S3FileSystem', 'err': 'Install s3fs to access S3'}, 's3a': {'class': 's3fs.S3FileSystem', 'err': 'Install s3fs to access S3'}, 'sftp': {'class': 'fsspec.implementations.sftp.SFTPFileSystem', 'err': 'SFTPFileSystem requires "paramiko" to be installed'}, 'simplecache': {'class': 'fsspec.implementations.cached.SimpleCacheFileSystem'}, 'smb': {'class': 'fsspec.implementations.smb.SMBFileSystem', 'err': 'SMB requires "smbprotocol" or "smbprotocol[kerberos]" installed'}, 'ssh': {'class': 'fsspec.implementations.sftp.SFTPFileSystem', 'err': 'SFTPFileSystem requires "paramiko" to be installed'}, 'tar': {'class': 'fsspec.implementations.tar.TarFileSystem'}, 'wandb': {'class': 'wandbfs.WandbFS', 'err': 'Install wandbfs to access wandb'}, 'webdav': {'class': 'webdav4.fsspec.WebdavFileSystem', 'err': 'Install webdav4 to access WebDAV'}, 'webhdfs': {'class': 'fsspec.implementations.webhdfs.WebHDFS', 'err': 'webHDFS access requires "requests" to be installed'}, 'zip': {'class': 'fsspec.implementations.zip.ZipFileSystem'}}
assert list(known_implementations) == sorted(known_implementations), 'Not in alphabetical order'

def get_filesystem_class(protocol):
    """Fetch named protocol implementation from the registry

    The dict ``known_implementations`` maps protocol names to the locations
    of classes implementing the corresponding file-system. When used for the
    first time, appropriate imports will happen and the class will be placed in
    the registry. All subsequent calls will fetch directly from the registry.

    Some protocol implementations require additional dependencies, and so the
    import may fail. In this case, the string in the "err" field of the
    ``known_implementations`` will be given as the error message.
    """
    if protocol in _registry:
        return _registry[protocol]
    
    if protocol in known_implementations:
        try:
            cls = _import_class(known_implementations[protocol]['class'])
            register_implementation(protocol, cls)
            return cls
        except ImportError as e:
            err = known_implementations[protocol].get('err', str(e))
            raise ImportError(err) from e
    
    raise ValueError(f"Protocol {protocol} not known")
s3_msg = 'Your installed version of s3fs is very old and known to cause\nsevere performance issues, see also https://github.com/dask/dask/issues/10276\n\nTo fix, you should specify a lower version bound on s3fs, or\nupdate the current installation.\n'

def _import_class(fqp: str):
    """Take a fully-qualified path and return the imported class or identifier.

    ``fqp`` is of the form "package.module.klass" or
    "package.module:subobject.klass".

    Warnings
    --------
    This can import arbitrary modules. Make sure you haven't installed any modules
    that may execute malicious code at import time.
    """
    if ':' in fqp:
        module, klass = fqp.split(':')
    else:
        module, klass = fqp.rsplit('.', 1)
    
    mod = importlib.import_module(module)
    
    if ':' in fqp:
        return eval(f"mod.{klass}")
    else:
        return getattr(mod, klass)

def filesystem(protocol, **storage_options):
    """Instantiate filesystems for given protocol and arguments

    ``storage_options`` are specific to the protocol being chosen, and are
    passed directly to the class.
    """
    cls = get_filesystem_class(protocol)
    
    if protocol == 's3' and 's3fs' in cls.__module__:
        import s3fs
        if s3fs.__version__ < '0.5':
            warnings.warn(s3_msg, stacklevel=2)
    
    return cls(**storage_options)

def available_protocols():
    """Return a list of the implemented protocols.

    Note that any given protocol may require extra packages to be importable.
    """
    return list(_registry.keys()) + list(known_implementations.keys())
