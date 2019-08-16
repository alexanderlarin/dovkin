import asyncio
import aiohttp
import aiogram
import aiovk
import argparse
import json
import logging
import os

from itertools import repeat

from store import Store

logger = logging.getLogger('bot')

MAX_COUNT = 5


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Telegram bot for vk.com photos gathering in messenger chat',
                                     epilog='Let the tool do the work!')
    parser.add_argument('--config', default='config.json', help='path to JSON-formatted config file')
    parser.add_argument('--vk-app-id', help='vk.com registered app ID')
    parser.add_argument('--vk-app-scope', help='vk.com registered app SCOPE in "SCOPE_1, SCOPE_2...SCOPE_N" format')
    parser.add_argument('--vk-user-auth', help='vk.com user credentials in "login:password" format')
    parser.add_argument('--vk-group-id')
    parser.add_argument('--telegram-bot-token', help='telegram bot token given by BotFather bot')
    parser.add_argument('--proxy-url', help='proxy server url')
    parser.add_argument('--proxy-auth', help='proxy server credentials in <login:password> format')
    parser.add_argument('--store-file', default='db.tinydb', help='path to JSON-formatted local store file')
    parser.add_argument('--log-file', default='bot.log', help='log file path')

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
                        datefmt='%H:%M:%S',
                        handlers=[logging.StreamHandler(), logging.FileHandler(args.log_file)])

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
        value = config[key] if config else args[key]
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
    logger.info(f'use vk session user_auth{username}:{password}')

    session = aiovk.ImplicitSession(login=username, password=password,
                                    app_id=app_id, scope=app_scope)

    async def send_photos(chat_id, photo_urls):
        media = aiogram.types.MediaGroup()
        for photo_url in photo_urls:
            media.attach_photo(photo_url)

        await bot.send_media_group(chat_id, media=media)

    api = aiovk.API(session=session)

    group_id = get_config_value('vk_group_id', required=True)
    group_id = f'-{group_id}'
    logger.info(f'use vk group_id={group_id}')

    def get_photo_url(item):
        return item.get('photo_2560', item.get('photo_1280', item.get('photo_807', None)))

    def get_photo_urls(item):
        return [url for url in (get_photo_url(attach['photo'])
                                for attach in item.get('attachments', []) if attach['type'] == 'photo') if url]

    async def publish_posts():
        chat_ids = list(store.get_chat_ids())
        sent_posts_count = dict(zip(chat_ids, repeat(0, len(chat_ids))))
        logger.info(f'publish posts chat_ids: {chat_ids}')

        offset = 0

        while all(count < MAX_COUNT for count in sent_posts_count.values()):
            await asyncio.sleep(10)  # TODO: replace with items per limit from aiovk

            response = await api.wall.get(owner_id=group_id, offset=offset, count=3, filter='all', extended=0)

            items = response['items']

            logger.debug(f'received posts offset={offset}, count={len(items)}')

            def should_publish(item):
                if not store.is_post_sent(chat_id, item['id'], item['owner_id']):
                    return item and get_photo_urls(item)

            while True:  # Collect if it's possible and request more another case
                send_post_futures = []
                for chat_id in sent_posts_count.keys():
                    if sent_posts_count[chat_id] >= MAX_COUNT:
                        continue
                    post = next((item for item in items if should_publish(item)), None)
                    photo_urls = post and get_photo_urls(post)
                    logging.debug(f'post photos chat_id={chat_id}, photo_urls={photo_urls}')
                    if photo_urls:
                        send_post_futures.append(send_photos(chat_id, photo_urls))
                        store.set_post_sent(chat_id, post['id'], post['owner_id'])  # TODO: not so good time before sending
                        sent_posts_count[chat_id] += 1

                logger.debug(f'send photos futures count={len(send_post_futures)}')
                if send_post_futures:  # TODO: OMFG what is cycle in cycle duplication
                    await asyncio.gather(*send_post_futures)
                else:
                    break

            logger.info(f'publish posts count: {sent_posts_count}')

            if not items:
                return sent_posts_count
            else:
                offset += len(items)


    async def watch_send_photos():
        while True:
            logger.info('publish posts started')
            await publish_posts()
            logger.info('publish posts completed')
            await asyncio.sleep(500)


    dispatcher = aiogram.Dispatcher(bot=bot)


    @dispatcher.message_handler(commands=['start', ])
    async def subscribe(message: aiogram.types.Message):
        logger.info(f'subscribe chat: {message.chat.id}')
        store.add_chat_id(message.chat.id)
        await bot.send_message(message.chat.id,
                               "Hi!\nI'm dovkin!\nPowered by @alexanderolarin.\nYou're subscribed")


    loop = asyncio.get_event_loop()
    loop.create_task(watch_send_photos())

    logger.info('init polling')
    aiogram.executor.start_polling(dispatcher, loop=loop)
    session.close()

