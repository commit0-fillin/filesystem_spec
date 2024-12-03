from __future__ import annotations
import inspect
import logging
import os
import shutil
import uuid
from typing import Optional
from .asyn import AsyncFileSystem, _run_coros_in_chunks, sync_wrapper
from .callbacks import DEFAULT_CALLBACK
from .core import filesystem, get_filesystem_class, split_protocol, url_to_fs
_generic_fs = {}
logger = logging.getLogger('fsspec.generic')
default_method = 'default'

def _resolve_fs(url, method=None, protocol=None, storage_options=None):
    """Pick instance of backend FS"""
    from .core import url_to_fs

    if method == 'default':
        return url_to_fs(url, **(storage_options or {}))
    elif method == 'generic':
        protocol = protocol or url.split('://', 1)[0]
        return _generic_fs.get(protocol), url
    elif method == 'current':
        protocol = protocol or url.split('://', 1)[0]
        cls = get_filesystem_class(protocol)
        return cls.current(), url
    else:
        raise ValueError(f"Method '{method}' not understood")

def rsync(source, destination, delete_missing=False, source_field='size', dest_field='size', update_cond='different', inst_kwargs=None, fs=None, **kwargs):
    """Sync files between two directory trees

    (experimental)

    Parameters
    ----------
    source: str
        Root of the directory tree to take files from. This must be a directory, but
        do not include any terminating "/" character
    destination: str
        Root path to copy into. The contents of this location should be
        identical to the contents of ``source`` when done. This will be made a
        directory, and the terminal "/" should not be included.
    delete_missing: bool
        If there are paths in the destination that don't exist in the
        source and this is True, delete them. Otherwise, leave them alone.
    source_field: str | callable
        If ``update_field`` is "different", this is the key in the info
        of source files to consider for difference. Maybe a function of the
        info dict.
    dest_field: str | callable
        If ``update_field`` is "different", this is the key in the info
        of destination files to consider for difference. May be a function of
        the info dict.
    update_cond: "different"|"always"|"never"
        If "always", every file is copied, regardless of whether it exists in
        the destination. If "never", files that exist in the destination are
        not copied again. If "different" (default), only copy if the info
        fields given by ``source_field`` and ``dest_field`` (usually "size")
        are different. Other comparisons may be added in the future.
    inst_kwargs: dict|None
        If ``fs`` is None, use this set of keyword arguments to make a
        GenericFileSystem instance
    fs: GenericFileSystem|None
        Instance to use if explicitly given. The instance defines how to
        to make downstream file system instances from paths.

    Returns
    -------
    dict of the copy operations that were performed, {source: destination}
    """
    if fs is None:
        fs = GenericFileSystem(**(inst_kwargs or {}))

    source_fs, source_path = fs._resolve_fs(source)
    dest_fs, dest_path = fs._resolve_fs(destination)

    source_files = source_fs.find(source_path)
    dest_files = dest_fs.find(dest_path)

    operations = {}

    for s_file in source_files:
        rel_path = s_file[len(source_path):].lstrip('/')
        d_file = f"{dest_path}/{rel_path}"

        if d_file not in dest_files:
            source_fs.get(s_file, d_file)
            operations[s_file] = d_file
        elif update_cond == 'always':
            source_fs.get(s_file, d_file)
            operations[s_file] = d_file
        elif update_cond == 'different':
            s_info = source_fs.info(s_file)
            d_info = dest_fs.info(d_file)
            s_value = s_info[source_field] if isinstance(source_field, str) else source_field(s_info)
            d_value = d_info[dest_field] if isinstance(dest_field, str) else dest_field(d_info)
            if s_value != d_value:
                source_fs.get(s_file, d_file)
                operations[s_file] = d_file

    if delete_missing:
        for d_file in dest_files:
            rel_path = d_file[len(dest_path):].lstrip('/')
            s_file = f"{source_path}/{rel_path}"
            if s_file not in source_files:
                dest_fs.rm(d_file)
                operations[s_file] = None

    return operations

class GenericFileSystem(AsyncFileSystem):
    """Wrapper over all other FS types

    <experimental!>

    This implementation is a single unified interface to be able to run FS operations
    over generic URLs, and dispatch to the specific implementations using the URL
    protocol prefix.

    Note: instances of this FS are always async, even if you never use it with any async
    backend.
    """
    protocol = 'generic'

    def __init__(self, default_method='default', **kwargs):
        """

        Parameters
        ----------
        default_method: str (optional)
            Defines how to configure backend FS instances. Options are:
            - "default": instantiate like FSClass(), with no
              extra arguments; this is the default instance of that FS, and can be
              configured via the config system
            - "generic": takes instances from the `_generic_fs` dict in this module,
              which you must populate before use. Keys are by protocol
            - "current": takes the most recently instantiated version of each FS
        """
        self.method = default_method
        super().__init__(**kwargs)

    def rsync(self, source, destination, **kwargs):
        """Sync files between two directory trees

        See `func:rsync` for more details.
        """
        return rsync(source, destination, fs=self, **kwargs)
    make_many_dirs = sync_wrapper(_make_many_dirs)
