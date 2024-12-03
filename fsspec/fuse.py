import argparse
import logging
import os
import stat
import threading
import time
from errno import EIO, ENOENT
from fuse import FUSE, FuseOSError, LoggingMixIn, Operations
from fsspec import __version__
from fsspec.core import url_to_fs
logger = logging.getLogger('fsspec.fuse')

class FUSEr(Operations):

    def __init__(self, fs, path, ready_file=False):
        self.fs = fs
        self.cache = {}
        self.root = path.rstrip('/') + '/'
        self.counter = 0
        logger.info('Starting FUSE at %s', path)
        self._ready_file = ready_file

def run(fs, path, mount_point, foreground=True, threads=False, ready_file=False, ops_class=FUSEr):
    """Mount stuff in a local directory

    This uses fusepy to make it appear as if a given path on an fsspec
    instance is in fact resident within the local file-system.

    This requires that fusepy by installed, and that FUSE be available on
    the system (typically requiring a package to be installed with
    apt, yum, brew, etc.).

    Parameters
    ----------
    fs: file-system instance
        From one of the compatible implementations
    path: str
        Location on that file-system to regard as the root directory to
        mount. Note that you typically should include the terminating "/"
        character.
    mount_point: str
        An empty directory on the local file-system where the contents of
        the remote path will appear.
    foreground: bool
        Whether or not calling this function will block. Operation will
        typically be more stable if True.
    threads: bool
        Whether or not to create threads when responding to file operations
        within the mounter directory. Operation will typically be more
        stable if False.
    ready_file: bool
        Whether the FUSE process is ready. The ``.fuse_ready`` file will
        exist in the ``mount_point`` directory if True. Debugging purpose.
    ops_class: FUSEr or Subclass of FUSEr
        To override the default behavior of FUSEr. For Example, logging
        to file.

    """
    fuse_ops = ops_class(fs, path, ready_file=ready_file)
    fuse = FUSE(
        fuse_ops,
        mount_point,
        foreground=foreground,
        nothreads=not threads,
        allow_other=True,
    )
    return fuse

def main(args):
    """Mount filesystem from chained URL to MOUNT_POINT.

    Examples:

    python3 -m fsspec.fuse memory /usr/share /tmp/mem

    python3 -m fsspec.fuse local /tmp/source /tmp/local \\
            -l /tmp/fsspecfuse.log

    You can also mount chained-URLs and use special settings:

    python3 -m fsspec.fuse 'filecache::zip::file://data.zip' \\
            / /tmp/zip \\
            -o 'filecache-cache_storage=/tmp/simplecache'

    You can specify the type of the setting by using `[int]` or `[bool]`,
    (`true`, `yes`, `1` represents the Boolean value `True`):

    python3 -m fsspec.fuse 'simplecache::ftp://ftp1.at.proftpd.org' \\
            /historic/packages/RPMS /tmp/ftp \\
            -o 'simplecache-cache_storage=/tmp/simplecache' \\
            -o 'simplecache-check_files=false[bool]' \\
            -o 'ftp-listings_expiry_time=60[int]' \\
            -o 'ftp-username=anonymous' \\
            -o 'ftp-password=xieyanbo'
    """
    parser = argparse.ArgumentParser(description='Mount filesystem from chained URL to MOUNT_POINT.')
    parser.add_argument('protocol', help='Filesystem protocol (e.g., memory, local, s3)')
    parser.add_argument('path', help='Path on the remote filesystem')
    parser.add_argument('mount_point', help='Local directory to mount the filesystem')
    parser.add_argument('-f', '--foreground', action='store_true', help='Run in foreground')
    parser.add_argument('-t', '--threads', action='store_true', help='Enable threading')
    parser.add_argument('-l', '--log', help='Log file path')
    parser.add_argument('-o', '--option', action='append', help='Additional options for the filesystem')
    
    args = parser.parse_args(args)
    
    # Set up logging
    if args.log:
        logging.basicConfig(filename=args.log, level=logging.INFO)
    
    # Parse additional options
    options = {}
    if args.option:
        for opt in args.option:
            key, value = opt.split('=', 1)
            if value.endswith('[int]'):
                options[key] = int(value[:-5])
            elif value.endswith('[bool]'):
                options[key] = value[:-6].lower() in ('true', 'yes', '1')
            else:
                options[key] = value
    
    # Create filesystem
    fs = filesystem(args.protocol, **options)
    
    # Mount filesystem
    run(fs, args.path, args.mount_point, foreground=args.foreground, threads=args.threads)
if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
