import zipfile
import fsspec
from fsspec.archive import AbstractArchiveFileSystem

class ZipFileSystem(AbstractArchiveFileSystem):
    """Read/Write contents of ZIP archive as a file-system

    Keeps file object open while instance lives.

    This class is pickleable, but not necessarily thread-safe
    """
    root_marker = ''
    protocol = 'zip'
    cachable = False

    def __init__(self, fo='', mode='r', target_protocol=None, target_options=None, compression=zipfile.ZIP_STORED, allowZip64=True, compresslevel=None, **kwargs):
        """
        Parameters
        ----------
        fo: str or file-like
            Contains ZIP, and must exist. If a str, will fetch file using
            :meth:`~fsspec.open_files`, which must return one file exactly.
        mode: str
            Accept: "r", "w", "a"
        target_protocol: str (optional)
            If ``fo`` is a string, this value can be used to override the
            FS protocol inferred from a URL
        target_options: dict (optional)
            Kwargs passed when instantiating the target FS, if ``fo`` is
            a string.
        compression, allowZip64, compresslevel: passed to ZipFile
            Only relevant when creating a ZIP
        """
        super().__init__(self, **kwargs)
        if mode not in set('rwa'):
            raise ValueError(f"mode '{mode}' no understood")
        self.mode = mode
        if isinstance(fo, str):
            if mode == 'a':
                m = 'r+b'
            else:
                m = mode + 'b'
            fo = fsspec.open(fo, mode=m, protocol=target_protocol, **target_options or {})
        self.force_zip_64 = allowZip64
        self.of = fo
        self.fo = fo.__enter__()
        self.zip = zipfile.ZipFile(self.fo, mode=mode, compression=compression, allowZip64=allowZip64, compresslevel=compresslevel)
        self.dir_cache = None

    def __del__(self):
        if hasattr(self, 'zip'):
            self.close()
            del self.zip

    def close(self):
        """Commits any write changes to the file. Done on ``del`` too."""
        if hasattr(self, 'zip'):
            self.zip.close()
        if hasattr(self, 'of'):
            self.of.__exit__(None, None, None)
        self.dir_cache = None
