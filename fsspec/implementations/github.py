import requests
import fsspec
from ..spec import AbstractFileSystem
from ..utils import infer_storage_options
from .memory import MemoryFile
from typing import List, Dict, Union

class GithubFileSystem(AbstractFileSystem):
    """Interface to files in github

    An instance of this class provides the files residing within a remote github
    repository. You may specify a point in the repos history, by SHA, branch
    or tag (default is current master).

    Given that code files tend to be small, and that github does not support
    retrieving partial content, we always fetch whole files.

    When using fsspec.open, allows URIs of the form:

    - "github://path/file", in which case you must specify org, repo and
      may specify sha in the extra args
    - 'github://org:repo@/precip/catalog.yml', where the org and repo are
      part of the URI
    - 'github://org:repo@sha/precip/catalog.yml', where the sha is also included

    ``sha`` can be the full or abbreviated hex of the commit you want to fetch
    from, or a branch or tag name (so long as it doesn't contain special characters
    like "/", "?", which would have to be HTTP-encoded).

    For authorised access, you must provide username and token, which can be made
    at https://github.com/settings/tokens
    """
    url = 'https://api.github.com/repos/{org}/{repo}/git/trees/{sha}'
    rurl = 'https://raw.githubusercontent.com/{org}/{repo}/{sha}/{path}'
    protocol = 'github'
    timeout = (60, 60)

    def __init__(self, org, repo, sha=None, username=None, token=None, timeout=None, **kwargs):
        super().__init__(**kwargs)
        self.org = org
        self.repo = repo
        if (username is None) ^ (token is None):
            raise ValueError('Auth required both username and token')
        self.username = username
        self.token = token
        if timeout is not None:
            self.timeout = timeout
        if sha is None:
            u = 'https://api.github.com/repos/{org}/{repo}'
            r = requests.get(u.format(org=org, repo=repo), timeout=self.timeout, **self.kw)
            r.raise_for_status()
            sha = r.json()['default_branch']
        self.root = sha
        self.ls('')

    @classmethod
    def repos(cls, org_or_user, is_org=True):
        """List repo names for given org or user

        This may become the top level of the FS

        Parameters
        ----------
        org_or_user: str
            Name of the github org or user to query
        is_org: bool (default True)
            Whether the name is an organisation (True) or user (False)

        Returns
        -------
        List of string
        """
        import requests

        if is_org:
            url = f"https://api.github.com/orgs/{org_or_user}/repos"
        else:
            url = f"https://api.github.com/users/{org_or_user}/repos"

        response = requests.get(url)
        response.raise_for_status()
        return [repo['name'] for repo in response.json()]

    @property
    def tags(self):
        """Names of tags in the repo"""
        url = f"https://api.github.com/repos/{self.org}/{self.repo}/tags"
        response = requests.get(url, timeout=self.timeout, **self.kw)
        response.raise_for_status()
        return [tag['name'] for tag in response.json()]

    @property
    def branches(self):
        """Names of branches in the repo"""
        url = f"https://api.github.com/repos/{self.org}/{self.repo}/branches"
        response = requests.get(url, timeout=self.timeout, **self.kw)
        response.raise_for_status()
        return [branch['name'] for branch in response.json()]

    @property
    def refs(self):
        """Named references, tags and branches"""
        return {
            'tags': self.tags,
            'branches': self.branches
        }

    def ls(self, path, detail=False, sha=None, _sha=None, **kwargs):
        """List files at given path

        Parameters
        ----------
        path: str
            Location to list, relative to repo root
        detail: bool
            If True, returns list of dicts, one per file; if False, returns
            list of full filenames only
        sha: str (optional)
            List at the given point in the repo history, branch or tag name or commit
            SHA
        _sha: str (optional)
            List this specific tree object (used internally to descend into trees)
        """
        if sha is None:
            sha = self.root
        if _sha is None:
            url = self.url.format(org=self.org, repo=self.repo, sha=sha)
            response = requests.get(url, timeout=self.timeout, **self.kw)
            response.raise_for_status()
            _sha = response.json()['sha']

        url = f"https://api.github.com/repos/{self.org}/{self.repo}/git/trees/{_sha}"
        response = requests.get(url, timeout=self.timeout, **self.kw)
        response.raise_for_status()
        tree = response.json()['tree']

        out = []
        for item in tree:
            if item['path'].startswith(path):
                if detail:
                    out.append({
                        'name': item['path'],
                        'size': item['size'] if item['type'] == 'blob' else None,
                        'type': 'file' if item['type'] == 'blob' else 'directory',
                        'sha': item['sha']
                    })
                else:
                    out.append(item['path'])

        return out
