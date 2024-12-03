"""Helper functions for a standard streaming compression API"""
from zipfile import ZipFile
import fsspec.utils
from fsspec.spec import AbstractBufferedFile
compr = {None: noop_file}

def register_compression(name, callback, extensions, force=False):
    """Register an "inferable" file compression type.

    Registers transparent file compression type for use with fsspec.open.
    Compression can be specified by name in open, or "infer"-ed for any files
    ending with the given extensions.

    Args:
        name: (str) The compression type name. Eg. "gzip".
        callback: A callable of form (infile, mode, **kwargs) -> file-like.
            Accepts an input file-like object, the target mode and kwargs.
            Returns a wrapped file-like object.
        extensions: (str, Iterable[str]) A file extension, or list of file
            extensions for which to infer this compression scheme. Eg. "gz".
        force: (bool) Force re-registration of compression type or extensions.

    Raises:
        ValueError: If name or extensions already registered, and not force.

    """
    global compr
    if name in compr and not force:
        raise ValueError(f"Compression {name} already registered")
    
    if isinstance(extensions, str):
        extensions = [extensions]
    
    for ext in extensions:
        if ext in fsspec.utils.compressions and not force:
            raise ValueError(f"Extension {ext} already registered")
    
    compr[name] = callback
    for ext in extensions:
        fsspec.utils.compressions[ext] = name
register_compression('zip', unzip, 'zip')
try:
    from bz2 import BZ2File
except ImportError:
    pass
else:
    register_compression('bz2', BZ2File, 'bz2')
try:
    from isal import igzip
    register_compression('gzip', isal, 'gz')
except ImportError:
    from gzip import GzipFile
    register_compression('gzip', lambda f, **kwargs: GzipFile(fileobj=f, **kwargs), 'gz')
try:
    from lzma import LZMAFile
    register_compression('lzma', LZMAFile, 'lzma')
    register_compression('xz', LZMAFile, 'xz')
except ImportError:
    pass
try:
    import lzmaffi
    register_compression('lzma', lzmaffi.LZMAFile, 'lzma', force=True)
    register_compression('xz', lzmaffi.LZMAFile, 'xz', force=True)
except ImportError:
    pass

class SnappyFile(AbstractBufferedFile):

    def __init__(self, infile, mode, **kwargs):
        import snappy
        super().__init__(fs=None, path='snappy', mode=mode.strip('b') + 'b', size=999999999, **kwargs)
        self.infile = infile
        if 'r' in mode:
            self.codec = snappy.StreamDecompressor()
        else:
            self.codec = snappy.StreamCompressor()

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        self.infile.seek(start)
        return self.codec.decompress(self.infile.read(end - start))
try:
    import snappy
    snappy.compress(b'')
    register_compression('snappy', SnappyFile, [])
except (ImportError, NameError, AttributeError):
    pass
try:
    import lz4.frame
    register_compression('lz4', lz4.frame.open, 'lz4')
except ImportError:
    pass
try:
    import zstandard as zstd
    register_compression('zstd', zstandard_file, 'zst')
except ImportError:
    pass

def available_compressions():
    """Return a list of the implemented compressions."""
    return list(compr.keys())
