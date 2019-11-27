from abc import ABC, abstractmethod


class BaseStore(ABC):
    def close(self):
        pass

    @abstractmethod
    def get_chat_ids(self):
        pass

    @abstractmethod
    def add_chat(self, chat_id):
        pass

    @abstractmethod
    def remove_chat(self, chat_id):
        pass

    @abstractmethod
    def get_wall_posts(self, owner_id=None):
        pass

    @abstractmethod
    def get_wall_posts_count(self, owner_id):
        pass

    @abstractmethod
    def is_wall_post_exists(self, post_id, owner_id):
        pass

    @abstractmethod
    def add_wall_post(self, post_id, owner_id, date, photos):
        pass

    @abstractmethod
    def add_chat_post(self, chat_id, post_id, owner_id):
        pass

    @abstractmethod
    def get_wall_post_to_send(self, chat_id, owner_id):
        pass
