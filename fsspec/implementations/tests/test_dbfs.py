"""
Test-Cases for the DataBricks Filesystem.
This test case is somewhat special, as there is no "mock" databricks
API available. We use the [vcr(https://github.com/kevin1024/vcrpy)
package to record the requests and responses to the real databricks API and
replay them on tests.

This however means, that when you change the tests (or when the API
itself changes, which is very unlikely to occur as it is versioned),
you need to re-record the answers. This can be done as follows:

1. Delete all casettes files in the "./cassettes/test_dbfs" folder
2. Spin up a databricks cluster. For example,
   you can use an Azure Databricks instance for this.
3. Take note of the instance details (the instance URL. For example for an Azure
   databricks cluster, this has the form
   adb-<some-number>.<two digits>.azuredatabricks.net)
   and your personal token (Find out more here:
   https://docs.databricks.com/dev-tools/api/latest/authentication.html)
4. Set the two environment variables `DBFS_INSTANCE` and `DBFS_TOKEN`
5. Now execute the tests as normal. The results of the API calls will be recorded.
6. Unset the environment variables and replay the tests.
"""
import os
import sys
from urllib.parse import urlparse
import numpy
import pytest
import fsspec
if sys.version_info >= (3, 10):
    pytest.skip('These tests need to be re-recorded.', allow_module_level=True)
DUMMY_INSTANCE = 'my_instance.com'
INSTANCE = os.getenv('DBFS_INSTANCE', DUMMY_INSTANCE)
TOKEN = os.getenv('DBFS_TOKEN', '')

@pytest.fixture(scope='module')
def vcr_config():
    """
    To not record information in the instance and token details
    (which are sensitive), we delete them from both the
    request and the response before storing it.
    We also delete the date as it is likely to change
    (and will make git diffs harder).
    If the DBFS_TOKEN env variable is set, we record with VCR.
    If not, we only replay (to not accidentally record with a wrong URL).
    """
    def before_record_request(request):
        parsed = urlparse(request.uri)
        request.uri = parsed._replace(netloc=DUMMY_INSTANCE).geturl()
        if 'Authorization' in request.headers:
            request.headers['Authorization'] = 'Bearer <TOKEN>'
        return request

    def before_record_response(response):
        if 'Date' in response['headers']:
            del response['headers']['Date']
        return response

    return {
        'filter_headers': ['authorization'],
        'before_record_request': before_record_request,
        'before_record_response': before_record_response,
        'record_mode': 'once' if TOKEN else 'none'
    }
