import logging

from tinydb import TinyDB, where
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage

from .base import BaseStore

logger = logging.getLogger(__name__)


class TinyDBStore(BaseStore):
    def __init__(self, path):
        # IMPORTANT: read cache behavior, store.close() is never called on sigterm and ctrl-c
        caching_middleware = CachingMiddleware(JSONStorage)
        caching_middleware.WRITE_CACHE_SIZE = 1
        self._db = TinyDB(path, storage=caching_middleware)

    def close(self):
        logger.info('flush and close')
        self._db.close()

    @property
    def chats(self):
        return self._db.table('chats')

    @property
    def chat_wall_posts(self):
        return self._db.table('chat_wall_posts')

    @property
    def wall_posts(self):
        return self._db.table('wall_posts')

    async def add_chat(self, chat_id):
        if not self.chats.contains(where('chat_id') == chat_id):
            return self.chats.insert({'chat_id': chat_id})

    async def remove_chat(self, chat_id):
        if not self.chats.contains(where('chat_id') == chat_id):
            return self.chats.remove(where('chat_id') == chat_id)

    async def get_chats(self):
        for item in self.chats.all():
            yield item

    async def is_wall_post_exists(self, post_id, owner_id):
        return self.wall_posts.contains((where('post_id') == post_id) &
                                        (where('owner_id') == owner_id))

    async def add_wall_post(self, post_id, owner_id, **fields):
        if not await self.is_wall_post_exists(post_id, owner_id):
            return self.wall_posts.insert({'post_id': post_id, 'owner_id': owner_id, **fields})

    async def get_wall_posts(self, owner_id=None):
        posts = self.wall_posts.search(where('owner_id') == owner_id) if owner_id else self.wall_posts.all()
        posts = sorted(posts, key=lambda item: item['date'], reverse=True)  # TODO: it's not obvious
        for post in posts:
            yield post

    async def get_wall_posts_count(self, owner_id):
        return self.wall_posts.count(where('owner_id') == owner_id)

    async def add_chat_wall_post(self, chat_id, post_id, owner_id):
        if not await self.is_chat_wall_post_exists(chat_id, post_id, owner_id):
            return self.chat_wall_posts.insert({'chat_id': chat_id, 'post_id': post_id, 'owner_id': owner_id})

    async def is_chat_wall_post_exists(self, chat_id, post_id, owner_id):
        return self.chat_wall_posts.contains((where('chat_id') == chat_id) &
                                             (where('post_id') == post_id) &
                                             (where('owner_id') == owner_id))
