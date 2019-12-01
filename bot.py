import asyncio
import aiohttp
import aiogram
import aiovk
import argparse
import json
import logging.handlers
import os

from handlers import apply_handlers
from jobs import send_post, store_photos, sync_groups_membership, walk_wall_posts
from store import BaseStore, create_store
from vk import ImplicitSession

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
    parser.add_argument('--store', default='tinydb://db.tinydb', help='path to JSON-formatted local store file')
    parser.add_argument('--store-photos-dir', help='path to downloaded photos store')
    parser.add_argument('--log-file', default='bot.log', help='log file path')

    args = parser.parse_args()

    # TODO: move logging config to JSON
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
                        datefmt='%H:%M:%S',
                        handlers=[logging.StreamHandler(),
                                  logging.handlers.TimedRotatingFileHandler(args.log_file,
                                                                            when='h', interval=24,
                                                                            backupCount=2)])

    logger.info(f'init store connection_uri={args.store}')
    store: BaseStore = create_store(args.store)
    # store: BaseStore = MongoDBStore("mongodb://localhost:27017/dovkin")

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
    max_requests_period = get_config_value("vk_max_requests_period", required=True)
    max_requests_per_period = get_config_value("vk_max_requests_per_period", required=True)
    logger.info(
        f'use vk api max_requests_period={max_requests_period} max_requests_per_period={max_requests_per_period}')

    session = ImplicitSession(
        login=username, password=password, app_id=app_id, scope=app_scope,
        max_requests_period=max_requests_period, max_requests_per_period=max_requests_per_period)

    api = aiovk.API(session=session)

    group_ids = get_config_value('vk_group_ids', required=True)
    logger.info(f'use vk group_ids={group_ids}')

    store_photos_path = get_config_value('store_photos_path')
    if store_photos_path:
        logger.info(f'use store_photos_path={store_photos_path}')
        if not os.path.exists(store_photos_path) or not os.path.isdir(store_photos_path):
            logger.warning(f'create store_photos_path={store_photos_path}')
            os.makedirs(store_photos_path)
    else:
        logger.warning(f'store_photos_path is empty, no photos will be stored')

    send_posts_timeout = get_config_value('send_posts_timeout', required=True)
    logger.info(f'use send_posts_timeout={send_posts_timeout}')
    walk_posts_timeout = get_config_value('walk_posts_timeout', required=True)
    logger.info(f'use walk_posts_timeout={walk_posts_timeout}')
    update_posts_timeout = get_config_value('update_posts_timeout', required=True)
    logger.info(f'use update_posts_timeout={update_posts_timeout}')
    store_photos_timeout = get_config_value('store_photos_timeout', required=store_photos_path)
    logger.info(f'use store_photos_timeout={store_photos_timeout}')

    member_group_ids = None  # TODO: remove global variable

    async def send_posts():
        async for chat in store.get_chats():
            chat_id = chat['chat_id']
            try:
                logger.info(f'send post to chat_id={chat_id}')
                await send_post(bot, store, chat_id=chat_id, group_ids=member_group_ids)

            except Exception as ex:
                logger.error(f'send posts to chat_id={chat_id} failed')
                logger.exception(ex)

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
            # Sleep before due to watch posts may be in progress
            logger.info(f'watch update posts sleep for {update_posts_timeout}secs')
            await asyncio.sleep(update_posts_timeout)

            try:
                logger.info('watch update posts started')
                global member_group_ids
                member_group_ids = await sync_groups_membership(api, group_ids=group_ids)
                for group_id in member_group_ids:
                    await walk_wall_posts(api, store, owner_id=-group_id, max_offset=30)

            except Exception as ex:
                logger.error(f'watch update posts failed with error {ex!r}')
                logger.exception(ex)

    async def watch_walk_posts():
        while True:
            try:
                logger.info('watch walk posts started')
                global member_group_ids
                member_group_ids = await sync_groups_membership(api, group_ids=group_ids)
                for group_id in member_group_ids:
                    await walk_wall_posts(api, store, owner_id=-group_id)

            except Exception as ex:
                logger.error(f'watch walk posts failed with error {ex!r}')
                logger.exception(ex)

            logger.info(f'watch walk posts sleep for {walk_posts_timeout}secs')
            await asyncio.sleep(walk_posts_timeout)

    async def watch_store_photos():
        while True:
            try:
                logger.info('watch store photos started')
                await store_photos(session, store,
                                   store_photos_path=store_photos_path)

            except Exception as ex:
                logger.error(f'watch store photos failed with error {ex!r}')
                logger.exception(ex)

            logger.info(f'watch store photos sleeps for {store_photos_timeout}secs')
            await asyncio.sleep(store_photos_timeout)

    logger.info(f'init dispatcher with message handlers')
    dispatcher = apply_handlers(
        dispatcher=aiogram.Dispatcher(bot=bot), store=store)

    async def startup(_):
        logger.info('startup callbacks')
        asyncio.ensure_future(watch_send_posts())
        asyncio.ensure_future(watch_walk_posts())
        asyncio.ensure_future(watch_update_posts())
        if store_photos_path:
            asyncio.ensure_future(watch_store_photos())

    async def shutdown(_):
        logger.info('shutdown callbacks')
        await session.close()
        store.close()

    logger.info('init polling')
    aiogram.executor.start_polling(
        dispatcher, on_startup=startup, on_shutdown=shutdown)
