import array
import logging
import posixpath
import warnings
from collections.abc import MutableMapping
from functools import cached_property
from fsspec.core import url_to_fs
logger = logging.getLogger('fsspec.mapping')

class FSMap(MutableMapping):
    """Wrap a FileSystem instance as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    root: string
        prefix for all the files
    fs: FileSystem instance
    check: bool (=True)
        performs a touch at the location, to check for write access.

    Examples
    --------
    >>> fs = FileSystem(**parameters)  # doctest: +SKIP
    >>> d = FSMap('my-data/path/', fs)  # doctest: +SKIP
    or, more likely
    >>> d = fs.get_mapper('my-data/path/')

    >>> d['loc1'] = b'Hello World'  # doctest: +SKIP
    >>> list(d.keys())  # doctest: +SKIP
    ['loc1']
    >>> d['loc1']  # doctest: +SKIP
    b'Hello World'
    """

    def __init__(self, root, fs, check=False, create=False, missing_exceptions=None):
        self.fs = fs
        self.root = fs._strip_protocol(root)
        self._root_key_to_str = fs._strip_protocol(posixpath.join(root, 'x'))[:-1]
        if missing_exceptions is None:
            missing_exceptions = (FileNotFoundError, IsADirectoryError, NotADirectoryError)
        self.missing_exceptions = missing_exceptions
        self.check = check
        self.create = create
        if create:
            if not self.fs.exists(root):
                self.fs.mkdir(root)
        if check:
            if not self.fs.exists(root):
                raise ValueError(f'Path {root} does not exist. Create  with the ``create=True`` keyword')
            self.fs.touch(root + '/a')
            self.fs.rm(root + '/a')

    @cached_property
    def dirfs(self):
        """dirfs instance that can be used with the same keys as the mapper"""
        from fsspec.implementations.dirfs import DirFS
        return DirFS(self.root, fs=self.fs)

    def clear(self):
        """Remove all keys below root - empties out mapping"""
        for key in self.keys():
            del self[key]

    def getitems(self, keys, on_error='raise'):
        """Fetch multiple items from the store

        If the backend is async-able, this might proceed concurrently

        Parameters
        ----------
        keys: list(str)
            They keys to be fetched
        on_error : "raise", "omit", "return"
            If raise, an underlying exception will be raised (converted to KeyError
            if the type is in self.missing_exceptions); if omit, keys with exception
            will simply not be included in the output; if "return", all keys are
            included in the output, but the value will be bytes or an exception
            instance.

        Returns
        -------
        dict(key, bytes|exception)
        """
        out = {}
        for key in keys:
            try:
                out[key] = self[key]
            except self.missing_exceptions:
                if on_error == 'raise':
                    raise KeyError(key)
                elif on_error == 'return':
                    out[key] = KeyError(key)
            except Exception as e:
                if on_error == 'raise':
                    raise
                elif on_error == 'return':
                    out[key] = e
        return out

    def setitems(self, values_dict):
        """Set the values of multiple items in the store

        Parameters
        ----------
        values_dict: dict(str, bytes)
        """
        for key, value in values_dict.items():
            self[key] = value

    def delitems(self, keys):
        """Remove multiple keys from the store"""
        for key in keys:
            try:
                del self[key]
            except KeyError:
                pass

    def _key_to_str(self, key):
        """Generate full path for the key"""
        return self.fs._strip_protocol(posixpath.join(self.root, key))

    def _str_to_key(self, s):
        """Strip path of to leave key name"""
        return posixpath.relpath(s, self.root)

    def __getitem__(self, key, default=None):
        """Retrieve data"""
        k = self._key_to_str(key)
        try:
            result = self.fs.cat(k)
        except self.missing_exceptions:
            if default is not None:
                return default
            raise KeyError(key)
        return result

    def pop(self, key, default=None):
        """Pop data"""
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is not None:
                return default
            raise

    def __setitem__(self, key, value):
        """Store value in key"""
        key = self._key_to_str(key)
        self.fs.mkdirs(self.fs._parent(key), exist_ok=True)
        self.fs.pipe_file(key, maybe_convert(value))

    def __iter__(self):
        return (self._str_to_key(x) for x in self.fs.find(self.root))

    def __len__(self):
        return len(self.fs.find(self.root))

    def __delitem__(self, key):
        """Remove key"""
        try:
            self.fs.rm(self._key_to_str(key))
        except:
            raise KeyError

    def __contains__(self, key):
        """Does key exist in mapping?"""
        path = self._key_to_str(key)
        return self.fs.isfile(path)

    def __reduce__(self):
        return (FSMap, (self.root, self.fs, False, False, self.missing_exceptions))

def get_mapper(url='', check=False, create=False, missing_exceptions=None, alternate_root=None, **kwargs):
    """Create key-value interface for given URL and options

    The URL will be of the form "protocol://location" and point to the root
    of the mapper required. All keys will be file-names below this location,
    and their values the contents of each key.

    Also accepts compound URLs like zip::s3://bucket/file.zip , see ``fsspec.open``.

    Parameters
    ----------
    url: str
        Root URL of mapping
    check: bool
        Whether to attempt to read from the location before instantiation, to
        check that the mapping does exist
    create: bool
        Whether to make the directory corresponding to the root before
        instantiating
    missing_exceptions: None or tuple
        If given, these exception types will be regarded as missing keys and
        return KeyError when trying to read data. By default, you get
        (FileNotFoundError, IsADirectoryError, NotADirectoryError)
    alternate_root: None or str
        In cases of complex URLs, the parser may fail to pick the correct part
        for the mapper root, so this arg can override

    Returns
    -------
    ``FSMap`` instance, the dict-like key-value store.
    """
    from fsspec import open_files

    of = open_files(url, **kwargs)[0]
    fs = of.fs
    path = alternate_root or of.path

    # Remove trailing slash from path
    path = path.rstrip('/')

    return FSMap(path, fs, check=check, create=create, missing_exceptions=missing_exceptions)
