import asyncpg

from .base import BaseStore


class PostgresDBStore(BaseStore):
    @staticmethod
    async def prepare_connection(connection):
        import json

        await connection.set_type_codec(
            'json',
            # format='binary',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )

    @classmethod
    async def connect(cls, connection_uri):
        pool = await asyncpg.create_pool(connection_uri, init=cls.prepare_connection)

        async with pool.acquire() as connection:
            await connection.execute(
                'CREATE TABLE IF NOT EXISTS chat(chat_id INT, fields jsonb, PRIMARY KEY (chat_id));'
                'CREATE TABLE IF NOT EXISTS "group"('
                '   group_id INT,'
                '   is_member BOOLEAN NOT NULL DEFAULT FALSE,'
                '   fields jsonb NOT NULL DEFAULT \'{}\'::json,'
                '   PRIMARY KEY (group_id)'
                ');'
                'CREATE TABLE IF NOT EXISTS subscription('
                '   chat_id INT, group_id INT, options jsonb NOT NULL DEFAULT \'{}\'::json,'
                '   PRIMARY KEY (chat_id, group_id), FOREIGN KEY (chat_id) REFERENCES chat (chat_id) ON DELETE CASCADE,'
                '   FOREIGN KEY (group_id) REFERENCES "group" (group_id) ON DELETE CASCADE'
                ');'
                'CREATE TABLE IF NOT EXISTS wall_post('
                '   post_id INT, owner_id INT, date INT NOT NULL, fields jsonb NOT NULL DEFAULT \'{}\'::json,'
                '   PRIMARY KEY (post_id, owner_id)'
                ');'
                'CREATE TABLE IF NOT EXISTS chat_wall_post('
                '   chat_id  INT, post_id INT, owner_id INT, options jsonb,'
                '   PRIMARY KEY (chat_id, post_id, owner_id),'
                '   FOREIGN KEY (chat_id) REFERENCES chat (chat_id) ON UPDATE CASCADE,'
                '   FOREIGN KEY (post_id, owner_id) REFERENCES wall_post (post_id, owner_id) ON UPDATE CASCADE'
                ')')
        return PostgresDBStore(pool=pool)

    def __init__(self, pool: asyncpg.pool.Pool):
        self._pool = pool

    async def close(self):
        await self._pool.close()

    async def get_chats(self):
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                async for item in connection.cursor('SELECT chat_id, fields::json FROM chat'):
                    yield {'chat_id': item['chat_id'], **item['fields']}

    async def upsert_chat(self, chat_id, **fields):
        async with self._pool.acquire() as connection:
            return await connection.execute(
                'INSERT INTO chat (chat_id, fields)'
                'VALUES ($1, $2::json)'
                'ON CONFLICT (chat_id) DO UPDATE SET fields = chat.fields || excluded.fields',
                chat_id, fields
            )

    async def get_groups(self, is_member=None):
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                async for item in connection.cursor(
                        'SELECT group_id, fields::json FROM "group" WHERE $1::boolean IS NULL OR is_member = $1',
                        is_member):
                    yield {'group_id': item['group_id'], **item['fields']}

    async def upsert_group(self, group_id, is_member, **fields):
        async with self._pool.acquire() as connection:
            if is_member is not None:
                await connection.execute(
                    'INSERT INTO "group" (group_id, is_member)'
                    'VALUES ($1, $2)'
                    'ON CONFLICT (group_id) DO UPDATE SET is_member = $2',
                    group_id, is_member
                )
            return await connection.execute(
                'INSERT INTO "group" (group_id, fields)'
                'VALUES ($1, $2::json)'
                'ON CONFLICT (group_id) DO UPDATE SET fields = "group".fields || excluded.fields',
                group_id, fields
            )

    async def get_subscriptions(self, chat_id=None):
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                async for item in connection.cursor(
                        'SELECT chat_id, group_id, options::json FROM subscription WHERE chat_id IS NULL OR chat_id = $1',
                        chat_id):
                    yield {'chat_id': item['chat_id'], 'group_id': item['group_id'], **item['options']}

    async def upsert_subscription(self, chat_id, group_id, **options):
        async with self._pool.acquire() as connection:
            return await connection.execute(
                'INSERT INTO subscription (chat_id, group_id, options)'
                'VALUES ($1, $2, $3::json)'
                'ON CONFLICT (chat_id, group_id) DO UPDATE SET options = subscription.options || excluded.options',
                chat_id, group_id, options
            )

    async def get_wall_posts(self, owner_id=None):
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                async for item in connection.cursor(
                        'SELECT post_id, owner_id, fields::json FROM wall_post WHERE owner_id IS NULL OR owner_id = $1',
                        owner_id):
                    yield {'post_id': item['post_id'], 'owner_id': item['owner_id'], **item['fields']}

    async def count_wall_posts(self, owner_id):
        async with self._pool.acquire() as connection:
            return await connection.fetchval(
                'SELECT count(*) FROM wall_post WHERE owner_id = $1', owner_id
            )

    async def upsert_wall_post(self, post_id, owner_id, date, **fields):
        async with self._pool.acquire() as connection:
            return await connection.execute(
                'INSERT INTO wall_post (post_id, owner_id, date, fields)'
                'VALUES ($1, $2, $3, $4::json)'
                'ON CONFLICT (post_id, owner_id)'
                'DO UPDATE SET date = excluded.date, fields = wall_post.fields || excluded.fields',
                post_id, owner_id, date, fields
            )

    async def next_chat_wall_post(self, chat_id, owner_id):
        async with self._pool.acquire() as connection:
            item = await connection.fetchrow(
                'SELECT post_id, owner_id, date, fields::json FROM wall_post WHERE owner_id = $2 AND NOT EXISTS ('
                '   SELECT chat_id, post_id, owner_id FROM chat_wall_post'
                '   WHERE chat_id = $1 AND post_id = wall_post.post_id AND owner_id = wall_post.owner_id'
                ') ORDER BY date desc LIMIT 1',
                chat_id, owner_id)
            return {'post_id': item['post_id'], 'owner_id': item['owner_id'], 'date': item['date'], **item['fields']}

    async def upsert_chat_wall_post(self, chat_id, post_id, owner_id, **fields):
        async with self._pool.acquire() as connection:
            return await connection.execute(
                'INSERT INTO chat_wall_post (chat_id, post_id, owner_id, options)'
                'VALUES ($1, $2, $3, $4::json)'
                'ON CONFLICT (chat_id, post_id, owner_id)'
                'DO UPDATE SET options = chat_wall_post.options || excluded.options',
                chat_id, post_id, owner_id, fields
            )
