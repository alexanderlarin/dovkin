import logging

from motor import motor_asyncio

from .base import BaseStore

logger = logging.getLogger(__name__)


class MongoDBStore(BaseStore):
    def __init__(self, connection_uri):
        self._db = motor_asyncio.AsyncIOMotorClient(connection_uri).get_database()

    async def get_chat_ids(self):
        async for doc in self._db.chats.find():
            yield doc['chat_id']

    async def add_chat(self, chat_id):
        doc = {'chat_id': chat_id}
        if not await self._db.chats.find_one(doc):
            return await self._db.chats.insert_one(doc)

    async def remove_chat(self, chat_id):
        return self._db.chats.find_one_and_delete({'chat_id': chat_id})

    async def get_wall_posts(self, owner_id=None):
        async for doc in self._db.wall_posts.find({'owner_id': owner_id} if owner_id else None):
            yield doc

    async def get_wall_posts_count(self, owner_id):
        return await self._db.wall_posts.count_documents({'owner_id': owner_id})

    async def is_wall_post_exists(self, post_id, owner_id):
        return await self._db.wall_posts.find_one({'post_id': post_id, 'owner_id': owner_id}) is not None

    async def add_wall_post(self, post_id, owner_id, date, photos):
        return await self._db.wall_posts.find_one_and_update(
            {'post_id': post_id, 'owner_id': owner_id}, {'$set': {'date': date, 'photos': photos}}, upsert=True)

    async def add_chat_post(self, chat_id, post_id, owner_id):
        doc = {'chat_id': chat_id, 'post_id': post_id, 'owner_id': owner_id}
        if not await self._db.chat_posts.find_one(doc):
            return await self._db.chat_posts.insert_one(doc)

    async def get_wall_post_to_send(self, chat_id, owner_id):
        find_filter = {'owner_id': owner_id} if owner_id else {}
        find_filter['photos'] = {'$ne': None}
        async for post in self._db.wall_posts.find(find_filter).sort('date', -1):
            if not await self._db.chat_posts.find_one({
                'chat_id': chat_id, 'post_id': post['post_id'], 'owner_id': post['owner_id']
            }):
                return post
