import base64
import io
from typing import Optional
from urllib.parse import unquote
from fsspec import AbstractFileSystem

class DataFileSystem(AbstractFileSystem):
    """A handy decoder for data-URLs

    Example
    -------
    >>> with fsspec.open("data:,Hello%2C%20World%21") as f:
    ...     print(f.read())
    b"Hello, World!"

    See https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/Data_URLs
    """
    protocol = 'data'

    def __init__(self, **kwargs):
        """No parameters for this filesystem"""
        super().__init__(**kwargs)

    @staticmethod
    def encode(data: bytes, mime: Optional[str]=None):
        """Format the given data into data-URL syntax

        This version always base64 encodes, even when the data is ascii/url-safe.
        """
        encoded_data = base64.b64encode(data).decode('ascii')
        mime_type = mime if mime else 'application/octet-stream'
        return f"data:{mime_type};base64,{encoded_data}"
