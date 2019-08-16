from tinydb import TinyDB, where


class Store:
    def __init__(self, path):
        self._db = TinyDB(path)

    @property
    def chats(self):
        return self._db.table('chats')

    @property
    def posts(self):
        return self._db.table('posts')

    def add_chat_id(self, chat_id):
        if not self.chats.contains(where('chat_id') == chat_id):
            return self.chats.insert({'chat_id': chat_id})

    def get_chat_ids(self):
        return (item['chat_id'] for item in self.chats.all())

    def is_post_sent(self, chat_id, post_id, owner_id):
        return self.posts.contains((where('chat_id') == chat_id) &
                                   (where('post_id') == post_id) &
                                   (where('owner_id') == owner_id))

    def set_post_sent(self, chat_id, post_id, owner_id):
        if not self.is_post_sent(chat_id, post_id, owner_id):
            return self.posts.insert({'chat_id': chat_id, 'post_id': post_id, 'owner_id': owner_id})
