import asyncio
import aiofiles
import aiohttp
import aiogram
import aiovk
import argparse
import backoff
import json
import logging
import logging.handlers
import os

from store import PostItem, Store
from vk import walk_wall_posts

logger = logging.getLogger('bot')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Telegram bot for vk.com photos gathering in messenger chat',
                                     epilog='Let the tool do the work!')
    parser.add_argument('--config', default='config.json', help='path to JSON-formatted config file')
    parser.add_argument('--vk-app-id', help='vk.com registered app ID')
    parser.add_argument('--vk-app-scope', help='vk.com registered app SCOPE in "SCOPE_1, SCOPE_2...SCOPE_N" format')
    parser.add_argument('--vk-user-auth', help='vk.com user credentials in "login:password" format')
    parser.add_argument('--vk-group-id')
    parser.add_argument('--send-posts-timeout')
    parser.add_argument('--walk-posts-timeout')
    parser.add_argument('--update-posts-timeout')
    parser.add_argument('--store-photos-timeout')
    parser.add_argument('--telegram-bot-token', help='telegram bot token given by BotFather bot')
    parser.add_argument('--proxy-url', help='proxy server url')
    parser.add_argument('--proxy-auth', help='proxy server credentials in <login:password> format')
    parser.add_argument('--store-file', default='db.tinydb', help='path to JSON-formatted local store file')
    parser.add_argument('--store-photos-dir', help='path to downloaded photos store')
    parser.add_argument('--log-file', default='bot.log', help='log file path')

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
                        datefmt='%H:%M:%S',
                        handlers=[logging.StreamHandler(),
                                  logging.handlers.TimedRotatingFileHandler(args.log_file,
                                                                            when='h', interval=24,
                                                                            backupCount=2)])

    logger.info(f'init store in {args.store_file}')
    store = Store(args.store_file)

    config = {}
    if args.config:
        logger.info(f'read vars from config={args.config}')
        if os.path.exists(args.config) and os.path.isfile(args.config):
            with open(args.config) as stream:
                config = json.load(stream)
        else:
            logger.error(f'config={args.config} not found')
    else:
        logger.info(f'no config set, read from args')

    def get_config_value(key, required=False):
        value = config.get(key) if config else args[key]
        if not value and required:
            raise ValueError(f'{key} value is required but was missed')
        return value

    def get_username_password(auth):
        values = auth.split(':')
        if len(values) != 2:
            raise ValueError(f'auth {auth} should be in "username:password" format')
        return values[0], values[1]

    token = get_config_value('telegram_bot_token', required=True)
    logger.info(f'init bot with token={token}')

    proxy_params = {}
    proxy_url = get_config_value('proxy_url')
    if proxy_url:
        proxy_params['proxy'] = proxy_url
        logger.info(f'use proxy with proxy_url={proxy_url}')
    proxy_auth = get_config_value('proxy_auth')
    if proxy_auth:
        username, password = get_username_password(proxy_auth)
        proxy_params['proxy_auth'] = aiohttp.BasicAuth(login=username, password=password)
        logger.info(f'use proxy_auth={username}:{password}')

    bot = aiogram.Bot(token=token, **proxy_params)

    app_id = get_config_value('vk_app_id', required=True)
    logger.info(f'init vk session for app_id={app_id}')
    app_scope = get_config_value('vk_app_scope')
    if not isinstance(app_scope, list):
        app_scope = [scope for scope in app_scope.split(',') if scope]
    logger.info(f'use vk session app_scope={app_scope}')
    user_auth = get_config_value('vk_user_auth', required=True)
    username, password = get_username_password(user_auth)
    logger.info(f'use vk session user_auth={username}:{password}')

    session = aiovk.ImplicitSession(login=username, password=password,
                                    app_id=app_id, scope=app_scope)

    api = aiovk.API(session=session)

    group_id = get_config_value('vk_group_id', required=True)
    owner_id = -group_id  # Because it's a group id
    logger.info(f'use vk owner_id={owner_id}')

    store_photos_dir = get_config_value('store_photos_dir')
    if store_photos_dir:
        logger.info(f'use store_photos_dir={store_photos_dir}')
        if not os.path.exists(store_photos_dir) or not os.path.isdir(store_photos_dir):
            logger.warning(f'create store_photos_dir={store_photos_dir}')
            os.makedirs(store_photos_dir)
    else:
        logger.warning(f'store_photos_dir is empty, no photos will be stored')

    send_posts_timeout = get_config_value('send_posts_timeout', required=True)
    logger.info(f'use send_posts_timeout={send_posts_timeout}')
    walk_posts_timeout = get_config_value('walk_posts_timeout', required=True)
    logger.info(f'use walk_posts_timeout={walk_posts_timeout}')
    update_posts_timeout = get_config_value('update_posts_timeout', required=True)
    logger.info(f'use update_posts_timeout={update_posts_timeout}')
    store_photos_timeout = get_config_value('store_photos_timeout', required=store_photos_dir)
    logger.info(f'use store_photos_timeout={store_photos_timeout}')

    def get_photo_url(item):
        return item.get('photo_2560', item.get('photo_1280', item.get('photo_807', None)))

    def get_photo_urls(item):
        return [url for url in (get_photo_url(attach['photo'])
                                for attach in item.get('attachments', []) if attach['type'] == 'photo') if url]


    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    async def send_chat_posts(chat_id):
        logger.info(f'search posts for chat_id={chat_id}')
        item = store.get_wall_post_to_send(chat_id=chat_id, owner_id=owner_id)
        if item:
            post = PostItem(**item)
            logger.info(f'send posts to chat_id={chat_id} post_id={post.post_id}, photos={post.photos}')
            media = aiogram.types.MediaGroup()
            for photo in post.photos:
                media.attach_photo(photo['url'])
            await bot.send_media_group(chat_id, media=media)

            store.add_chat_post(chat_id=chat_id, owner_id=owner_id, post_id=post.post_id)
            logger.info(f'post sent chat_id={chat_id}, owner_id={owner_id}, post_id={post.post_id}')

    async def send_posts():
        chat_ids = list(store.get_chat_ids())
        logger.info(f'send posts to chat_ids={chat_ids}')

        for chat_id in chat_ids:
            try:
                await send_chat_posts(chat_id)
            except Exception as ex:
                logger.error(f'send posts to chat_id={chat_id} failed')
                logger.exception(ex)

    async def store_photos(max_store_photos=10):
        logger.info(f'store photos for owner_id={owner_id}')
        wall_posts = store.get_wall_posts(owner_id=owner_id)
        store_photos_count = 0
        for item in wall_posts:
            for photo in item['photos']:
                photo_url = photo['url']
                _, ext = os.path.splitext(photo_url)
                post_id = item['post_id']
                photo_id = photo['id']
                filename = os.path.join(store_photos_dir,
                                        f'{owner_id}_{post_id}_{photo_id}{ext}')
                if not os.path.exists(filename) or not os.path.isfile(filename):
                    logger.debug(f'store photo photo_url={photo_url} to {filename}')
                    _, data = await session.driver.get_bin(photo_url, params={})  # TODO: use separate session?
                    async with aiofiles.open(filename, mode='wb') as s:
                        await s.write(data)

                    store_photos_count += 1
                    logger.info(f'store photos count={store_photos_count} of {max_store_photos}')

            if store_photos_count >= max_store_photos:
                break

    async def watch_send_posts():
        while True:
            try:
                logger.info('watch send posts started')
                await send_posts()
            except Exception as ex:
                logger.error(f'watch send posts failed with error {ex!r}')
                logger.exception(ex)

            logger.info(f'watch send posts sleep for {send_posts_timeout}secs')
            await asyncio.sleep(send_posts_timeout)

    async def watch_update_posts():
        while True:
            # Sleep before dut to watch posts may be in progress
            logger.info(f'watch update posts sleep for {update_posts_timeout}secs')
            await asyncio.sleep(update_posts_timeout)

            try:
                logger.info('watch update posts started')
                await walk_wall_posts(api, store, owner_id, max_posts_offset=30)
            except Exception as ex:
                logger.error(f'watch update posts failed with error {ex!r}')
                logger.exception(ex)

    async def watch_walk_posts():
        while True:
            try:
                logger.info('watch walk posts started')
                await walk_wall_posts(api, store, owner_id)
            except Exception as ex:
                logger.error(f'watch walk posts failed with error {ex!r}')
                logger.exception(ex)

            logger.info(f'watch walk posts sleep for {walk_posts_timeout}secs')
            await asyncio.sleep(walk_posts_timeout)


    async def watch_store_photos():
        while True:
            try:
                logger.info('watch store photos')
                await store_photos()
            except Exception as ex:
                logger.error(f'watch store photos failed with error {ex!r}')
                logger.exception(ex)

            logger.info(f'watch store photos sleeps for {store_photos_timeout}secs')
            await asyncio.sleep(store_photos_timeout)

    dispatcher = aiogram.Dispatcher(bot=bot)

    @dispatcher.message_handler(commands=['start', ])
    async def start(message: aiogram.types.Message):
        await bot.send_message(message.chat.id,
                               "Hi!\nI'm DoVkIn!\nPowered by @alexanderolarin\nJust type /subscribe command")


    @dispatcher.message_handler(commands=['subscribe', ])
    async def subscribe(message: aiogram.types.Message):
        store.add_chat(message.chat.id)
        await bot.send_message(message.chat.id,
                               "Keep Nude and Panties Off!\nYou're subscribed")
        asyncio.ensure_future(send_chat_posts(message.chat.id))
        logger.info(f'subscribe chat: {message.chat.id}, send immediately')

    @dispatcher.message_handler(commands=['unsubscribe', ])
    async def unsubscribe(message: aiogram.types.Message):
        store.remove_chat(message.chat.id)
        await bot.send_message(message.chat.id,
                               "Oh no!\nYou're unsubscribed")
        logger.info(f'unsubscribe chat: {message.chat.id}')

    async def startup(_):
        logger.info('startup callbacks')
        asyncio.ensure_future(watch_send_posts())
        asyncio.ensure_future(watch_walk_posts())
        asyncio.ensure_future(watch_update_posts())
        if store_photos_dir:
            asyncio.ensure_future(watch_store_photos())

    async def shutdown(_):
        logger.info('shutdown callbacks')
        await session.close()
        store.close()

    logger.info('init polling')
    aiogram.executor.start_polling(
        dispatcher, on_startup=startup, on_shutdown=shutdown)
