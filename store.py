from tinydb import TinyDB, where


class Store:
    def __init__(self, path):
        self._db = TinyDB(path)

    def close(self):
        self._db.close()

    @property
    def chats(self):
        return self._db.table('chats')

    @property
    def chat_posts(self):
        return self._db.table('chat_posts')

    @property
    def wall_posts(self):
        return self._db.table('wall_posts')

    def add_chat(self, chat_id):
        if not self.chats.contains(where('chat_id') == chat_id):
            return self.chats.insert({'chat_id': chat_id})

    def remove_chat(self, chat_id):
        if not self.chats.contains(where('chat_id') == chat_id):
            return self.chats.remove(where('chat_id') == chat_id)

    def get_chat_ids(self):
        return (item['chat_id'] for item in self.chats.all())

    def add_chat_post(self, chat_id, post_id, owner_id):
        if not self.is_chat_post_exists(chat_id, post_id, owner_id):
            return self.chat_posts.insert({'chat_id': chat_id, 'post_id': post_id, 'owner_id': owner_id})

    def is_chat_post_exists(self, chat_id, post_id, owner_id):
        return self.chat_posts.contains((where('chat_id') == chat_id) &
                                        (where('post_id') == post_id) &
                                        (where('owner_id') == owner_id))

    def add_wall_post(self, post_id, owner_id, date, photos):
        if not self.is_wall_post_exists(post_id, owner_id):
            return self.wall_posts.insert({'post_id': post_id, 'owner_id': owner_id,
                                           'date': date, 'photos': photos})

    def is_wall_post_exists(self, post_id, owner_id):
        return self.wall_posts.contains((where('post_id') == post_id) &
                                        (where('owner_id') == owner_id))

    def get_wall_post_ids(self, owner_id):
        items = self.wall_posts.search(where('owner_id') == owner_id)
        return (item['post_id'] for item in items)

    def get_wall_posts_count(self, owner_id):
        return self.wall_posts.count(where('owner_id') == owner_id)

    def get_wall_post_to_send(self, chat_id, owner_id):
        items = sorted(self.wall_posts.search(where('owner_id') == owner_id),
                       key=lambda post: post['date'], reverse=True)
        return next((item for item in items
                     if not self.is_chat_post_exists(chat_id=chat_id,
                                                     post_id=item['post_id'], owner_id=owner_id)), None)
