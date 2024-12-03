import io
import json
import warnings
from .core import url_to_fs
from .utils import merge_offset_ranges

def open_parquet_file(path, mode='rb', fs=None, metadata=None, columns=None, row_groups=None, storage_options=None, strict=False, engine='auto', max_gap=64000, max_block=256000000, footer_sample_size=1000000, **kwargs):
    """
    Return a file-like object for a single Parquet file.

    The specified parquet `engine` will be used to parse the
    footer metadata, and determine the required byte ranges
    from the file. The target path will then be opened with
    the "parts" (`KnownPartsOfAFile`) caching strategy.

    Note that this method is intended for usage with remote
    file systems, and is unlikely to improve parquet-read
    performance on local file systems.

    Parameters
    ----------
    path: str
        Target file path.
    mode: str, optional
        Mode option to be passed through to `fs.open`. Default is "rb".
    metadata: Any, optional
        Parquet metadata object. Object type must be supported
        by the backend parquet engine. For now, only the "fastparquet"
        engine supports an explicit `ParquetFile` metadata object.
        If a metadata object is supplied, the remote footer metadata
        will not need to be transferred into local memory.
    fs: AbstractFileSystem, optional
        Filesystem object to use for opening the file. If nothing is
        specified, an `AbstractFileSystem` object will be inferred.
    engine : str, default "auto"
        Parquet engine to use for metadata parsing. Allowed options
        include "fastparquet", "pyarrow", and "auto". The specified
        engine must be installed in the current environment. If
        "auto" is specified, and both engines are installed,
        "fastparquet" will take precedence over "pyarrow".
    columns: list, optional
        List of all column names that may be read from the file.
    row_groups : list, optional
        List of all row-groups that may be read from the file. This
        may be a list of row-group indices (integers), or it may be
        a list of `RowGroup` metadata objects (if the "fastparquet"
        engine is used).
    storage_options : dict, optional
        Used to generate an `AbstractFileSystem` object if `fs` was
        not specified.
    strict : bool, optional
        Whether the resulting `KnownPartsOfAFile` cache should
        fetch reads that go beyond a known byte-range boundary.
        If `False` (the default), any read that ends outside a
        known part will be zero padded. Note that using
        `strict=True` may be useful for debugging.
    max_gap : int, optional
        Neighboring byte ranges will only be merged when their
        inter-range gap is <= `max_gap`. Default is 64KB.
    max_block : int, optional
        Neighboring byte ranges will only be merged when the size of
        the aggregated range is <= `max_block`. Default is 256MB.
    footer_sample_size : int, optional
        Number of bytes to read from the end of the path to look
        for the footer metadata. If the sampled bytes do not contain
        the footer, a second read request will be required, and
        performance will suffer. Default is 1MB.
    **kwargs :
        Optional key-word arguments to pass to `fs.open`
    """
    from fsspec import open_files
    from fsspec.core import url_to_fs
    from fsspec.utils import infer_compression
    
    if fs is None:
        fs, path = url_to_fs(path, **(storage_options or {}))
    
    compression = infer_compression(path)
    
    if engine == 'auto':
        try:
            import fastparquet
            engine = 'fastparquet'
        except ImportError:
            try:
                import pyarrow.parquet
                engine = 'pyarrow'
            except ImportError:
                raise ValueError("No parquet engine available. Install either 'fastparquet' or 'pyarrow'.")
    
    if engine == 'fastparquet':
        engine_obj = FastparquetEngine()
    elif engine == 'pyarrow':
        engine_obj = PyarrowEngine()
    else:
        raise ValueError(f"Unsupported engine: {engine}")
    
    if metadata is None:
        metadata = engine_obj.read_metadata(fs, path, footer_sample_size)
    
    byte_ranges = _get_parquet_byte_ranges_from_metadata(
        metadata, fs, engine, columns, row_groups, max_gap, max_block
    )
    
    f = fs.open(
        path,
        mode=mode,
        block_size=None,
        cache_type='parts',
        cache_options={'strict': strict, 'data': byte_ranges},
        compression=compression,
        **kwargs
    )
    
    return f

def _get_parquet_byte_ranges(paths, fs, metadata=None, columns=None, row_groups=None, max_gap=64000, max_block=256000000, footer_sample_size=1000000, engine='auto'):
    """Get a dictionary of the known byte ranges needed
    to read a specific column/row-group selection from a
    Parquet dataset. Each value in the output dictionary
    is intended for use as the `data` argument for the
    `KnownPartsOfAFile` caching strategy of a single path.
    """
    if engine == 'auto':
        try:
            import fastparquet
            engine = 'fastparquet'
        except ImportError:
            try:
                import pyarrow.parquet
                engine = 'pyarrow'
            except ImportError:
                raise ValueError("No parquet engine available. Install either 'fastparquet' or 'pyarrow'.")
    
    if engine == 'fastparquet':
        engine_obj = FastparquetEngine()
    elif engine == 'pyarrow':
        engine_obj = PyarrowEngine()
    else:
        raise ValueError(f"Unsupported engine: {engine}")
    
    byte_ranges = {}
    for path in paths:
        if metadata is None:
            file_metadata = engine_obj.read_metadata(fs, path, footer_sample_size)
        else:
            file_metadata = metadata
        
        byte_ranges[path] = _get_parquet_byte_ranges_from_metadata(
            file_metadata, fs, engine, columns, row_groups, max_gap, max_block
        )
    
    return byte_ranges

def _get_parquet_byte_ranges_from_metadata(metadata, fs, engine, columns=None, row_groups=None, max_gap=64000, max_block=256000000):
    """Simplified version of `_get_parquet_byte_ranges` for
    the case that an engine-specific `metadata` object is
    provided, and the remote footer metadata does not need to
    be transferred before calculating the required byte ranges.
    """
    if engine == 'fastparquet':
        engine_obj = FastparquetEngine()
    elif engine == 'pyarrow':
        engine_obj = PyarrowEngine()
    else:
        raise ValueError(f"Unsupported engine: {engine}")
    
    byte_ranges = engine_obj.get_byte_ranges(metadata, columns, row_groups)
    
    # Merge adjacent byte ranges
    from fsspec.utils import merge_offset_ranges
    paths, starts, ends = merge_offset_ranges(
        paths=[metadata.path] * len(byte_ranges),
        starts=[r[0] for r in byte_ranges],
        ends=[r[1] for r in byte_ranges],
        max_gap=max_gap,
        max_block=max_block
    )
    
    return list(zip(starts, ends))

class FastparquetEngine:

    def __init__(self):
        import fastparquet as fp
        self.fp = fp

class PyarrowEngine:

    def __init__(self):
        import pyarrow.parquet as pq
        self.pq = pq
