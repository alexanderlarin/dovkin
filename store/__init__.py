from .base import BaseStore


def create_store(connection_uri):
    import urllib.parse
    url = urllib.parse.urlparse(connection_uri)

    if url.scheme == 'tinydb':
        from .tiny import TinyDBStore
        return TinyDBStore(path=urllib.parse.urljoin(url.netloc, url.path))
    else:
        raise NotImplementedError(f'store scheme={url.scheme}')
