from abc import ABC, abstractmethod


class BaseStore(ABC):
    def close(self):
        pass

    @abstractmethod
    async def get_chats(self):
        yield

    @abstractmethod
    async def upsert_chat(self, chat_id, **fields):
        pass

    @abstractmethod
    async def get_groups(self, is_member=None):
        yield

    @abstractmethod
    async def upsert_group(self, group_id, **fields):
        pass

    @abstractmethod
    async def get_subscriptions(self, chat_id=None):
        yield

    @abstractmethod
    async def add_subscription(self, chat_id, group_id):
        pass

    # @abstractmethod
    # async def remove_subscription(self, chat_id, group_id):
    #     pass

    @abstractmethod
    async def get_wall_posts(self, owner_id=None):
        yield

    @abstractmethod
    async def get_wall_posts_count(self, owner_id):
        pass

    @abstractmethod
    async def is_wall_post_exists(self, post_id, owner_id):
        pass

    @abstractmethod
    async def add_wall_post(self, post_id, owner_id, **fields):
        pass

    @abstractmethod
    async def add_chat_wall_post(self, chat_id, post_id, owner_id):
        pass

    @abstractmethod
    async def is_chat_wall_post_exists(self, chat_id, post_id, owner_id):
        pass
