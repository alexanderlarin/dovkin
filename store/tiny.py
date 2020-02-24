import logging
from functools import reduce

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

    async def close(self):
        logger.info('flush and close')
        self._db.close()

    @property
    def chats(self):
        return self._db.table('chats')

    @property
    def groups(self):
        return self._db.table('groups')

    @property
    def subscriptions(self):
        return self._db.table('subscriptions')

    @property
    def chat_wall_posts(self):
        return self._db.table('chat_wall_posts')

    @property
    def wall_posts(self):
        return self._db.table('wall_posts')

    @staticmethod
    def get_filters(**filters):
        if filters:
            filters = {key: value for key, value in filters.items() if value is not None}

            def concat(q, key):
                cond = where(key) == filters[key]
                return (q & cond) if q else cond

            return reduce(concat, filters.keys(), None)

    def get_items(self, table_name, **filters):
        table = self._db.table(table_name)
        query = self.get_filters(**filters)
        if not query:
            return table.all()
        else:
            return table.search(query)

    async def get_chats(self):
        for item in self.chats.all():
            yield item

    async def upsert_chat(self, chat_id, **fields):
        return self.chats.upsert({'chat_id': chat_id, **fields}, where('chat_id') == chat_id)

    async def get_groups(self, is_member=None):
        for item in self.get_items('groups', is_member=is_member):
            yield item

    async def upsert_group(self, group_id, **fields):
        return self.groups.upsert({'group_id': group_id, **fields}, where('group_id') == group_id)

    async def get_subscriptions(self, chat_id=None):
        for item in self.get_items('subscriptions', chat_id=chat_id):
            yield item

    async def upsert_subscription(self, chat_id, group_id, **options):
        keys = {'chat_id': chat_id, 'group_id': group_id}
        return self.subscriptions.upsert({**keys, **options}, self.get_filters(**keys))

    async def upsert_wall_post(self, post_id, owner_id, **fields):
        keys = {'post_id': post_id, 'owner_id': owner_id}
        return self.wall_posts.upsert({**keys, **fields}, self.get_filters(**keys))

    async def get_wall_posts(self, owner_id=None):
        for post in self.get_items('wall_posts', owner_id=owner_id):
            yield post

    async def count_wall_posts(self, owner_id):
        return self.wall_posts.count(self.get_filters(owner_id=owner_id))

    async def next_chat_wall_post(self, chat_id, owner_id):
        items = [
            item async for item in self.get_wall_posts(owner_id=owner_id)
            if not self.chat_wall_posts.contains(
                self.get_filters(chat_id=chat_id, owner_id=owner_id, post_id=item['post_id']))
        ]
        return max(items, key=lambda post: post['date'])

    async def upsert_chat_wall_post(self, chat_id, post_id, owner_id, **fields):
        keys = {'chat_id': chat_id, 'post_id': post_id, 'owner_id': owner_id}
        return self.chat_wall_posts.upsert({**keys, **fields}, self.get_filters(**keys))
