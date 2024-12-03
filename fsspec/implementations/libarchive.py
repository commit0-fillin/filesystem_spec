from contextlib import contextmanager
from ctypes import CFUNCTYPE, POINTER, c_int, c_longlong, c_void_p, cast, create_string_buffer
import libarchive
import libarchive.ffi as ffi
from fsspec import open_files
from fsspec.archive import AbstractArchiveFileSystem
from fsspec.implementations.memory import MemoryFile
from fsspec.utils import DEFAULT_BLOCK_SIZE
SEEK_CALLBACK = CFUNCTYPE(c_longlong, c_int, c_void_p, c_longlong, c_int)
read_set_seek_callback = ffi.ffi('read_set_seek_callback', [ffi.c_archive_p, SEEK_CALLBACK], c_int, ffi.check_int)
new_api = hasattr(ffi, 'NO_OPEN_CB')

@contextmanager
def custom_reader(file, format_name='all', filter_name='all', block_size=ffi.page_size):
    """Read an archive from a seekable file-like object.

    The `file` object must support the standard `readinto` and 'seek' methods.
    """
    def read_callback(archive, context, buffer, length):
        return file.readinto(ffi.buffer(buffer, length))

    def seek_callback(archive, context, offset, whence):
        return file.seek(offset, whence)

    with libarchive.read_memory_fd(
        None,
        block_size,
        format_name=format_name,
        filter_name=filter_name
    ) as archive:
        ffi.read_set_seek_callback(archive, SEEK_CALLBACK(seek_callback))
        ffi.read_set_read_callback(archive, ffi.READ_CALLBACK(read_callback))
        ffi.read_set_format(archive, format_name)
        ffi.read_set_filter(archive, filter_name)
        yield archive

class LibArchiveFileSystem(AbstractArchiveFileSystem):
    """Compressed archives as a file-system (read-only)

    Supports the following formats:
    tar, pax , cpio, ISO9660, zip, mtree, shar, ar, raw, xar, lha/lzh, rar
    Microsoft CAB, 7-Zip, WARC

    See the libarchive documentation for further restrictions.
    https://www.libarchive.org/

    Keeps file object open while instance lives. It only works in seekable
    file-like objects. In case the filesystem does not support this kind of
    file object, it is recommended to cache locally.

    This class is pickleable, but not necessarily thread-safe (depends on the
    platform). See libarchive documentation for details.
    """
    root_marker = ''
    protocol = 'libarchive'
    cachable = False

    def __init__(self, fo='', mode='r', target_protocol=None, target_options=None, block_size=DEFAULT_BLOCK_SIZE, **kwargs):
        """
        Parameters
        ----------
        fo: str or file-like
            Contains ZIP, and must exist. If a str, will fetch file using
            :meth:`~fsspec.open_files`, which must return one file exactly.
        mode: str
            Currently, only 'r' accepted
        target_protocol: str (optional)
            If ``fo`` is a string, this value can be used to override the
            FS protocol inferred from a URL
        target_options: dict (optional)
            Kwargs passed when instantiating the target FS, if ``fo`` is
            a string.
        """
        super().__init__(self, **kwargs)
        if mode != 'r':
            raise ValueError('Only read from archive files accepted')
        if isinstance(fo, str):
            files = open_files(fo, protocol=target_protocol, **target_options or {})
            if len(files) != 1:
                raise ValueError(f'Path "{fo}" did not resolve to exactly one file: "{files}"')
            fo = files[0]
        self.of = fo
        self.fo = fo.__enter__()
        self.block_size = block_size
        self.dir_cache = None
