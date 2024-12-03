from collections import deque

class Transaction:
    """Filesystem transaction write context

    Gathers files for deferred commit or discard, so that several write
    operations can be finalized semi-atomically. This works by having this
    instance as the ``.transaction`` attribute of the given filesystem
    """

    def __init__(self, fs, **kwargs):
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        self.fs = fs
        self.files = deque()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction and commit, if exit is not due to exception"""
        self.complete(commit=exc_type is None)
        if self.fs:
            self.fs._intrans = False
            self.fs._transaction = None
            self.fs = None

    def start(self):
        """Start a transaction on this FileSystem"""
        if self.fs._intrans:
            raise ValueError("Nested transactions not supported")
        self.fs._intrans = True
        self.fs._transaction = self

    def complete(self, commit=True):
        """Finish transaction: commit or discard all deferred files"""
        while self.files:
            f = self.files.popleft()
            if commit:
                f.commit()
            else:
                f.discard()

class FileActor:

    def __init__(self):
        self.files = []

class DaskTransaction(Transaction):

    def __init__(self, fs):
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        import distributed
        super().__init__(fs)
        client = distributed.default_client()
        self.files = client.submit(FileActor, actor=True).result()

    def complete(self, commit=True):
        """Finish transaction: commit or discard all deferred files"""
        import distributed
        client = distributed.default_client()
        
        def process_files(files, commit):
            for f in files:
                if commit:
                    f.commit()
                else:
                    f.discard()
            return []

        client.submit(process_files, self.files, commit, actor=self.files).result()
