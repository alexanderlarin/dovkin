from .base import BaseStore


async def connect_store(connection_uri):
    import urllib.parse
    url = urllib.parse.urlparse(connection_uri)

    if url.scheme == 'tinydb':
        from .tiny import TinyDBStore
        return TinyDBStore(path=urllib.parse.urljoin(url.netloc, url.path))
    elif url.scheme == 'postgres':
        from .postgres import PostgresDBStore
        return await PostgresDBStore.connect(connection_uri=connection_uri)
    else:
        raise NotImplementedError(f'store scheme={url.scheme}')
