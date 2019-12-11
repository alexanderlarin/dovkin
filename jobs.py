import aiofiles
import aiogram
import aiovk
import backoff
import enum
import logging
import os

from store.base import BaseStore
from vk import get_photos

logger = logging.getLogger(__name__)

MAX_POSTS_COUNT = 100


class Membership(enum.Enum):
    MEMBER = 'member'
    REQUEST = 'request'


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
async def check_group_membership(api: aiovk.API, group_id):
    response = await api.groups.isMember(group_id=group_id, extended=1)
    for membership in Membership:
        if response.get(membership.value):
            logger.debug(f'check group_id={group_id} membership={membership}')
            return membership


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
async def sync_group_membership(api: aiovk.API, group_id):
    membership = await check_group_membership(api, group_id=group_id)
    if not membership:
        response = await api.groups.join(group_id=group_id)
        logger.debug(f'request group_id={group_id} membership={response}')
        membership = await check_group_membership(api, group_id=group_id)
    return membership == Membership.MEMBER


async def sync_groups_membership(session: aiovk.TokenSession, store: BaseStore):
    api = aiovk.API(session=session)
    async for item in store.get_groups(is_member=False):
        logger.info(f'sync membership group_id={item["group_id"]}')
        is_member = await sync_group_membership(api, group_id=item['group_id'])
        await store.upsert_group(group_id=item['group_id'], is_member=is_member)


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
async def get_wall_posts(api: aiovk.API, owner_id, offset, limit):
    logger.debug(f'wall posts query owner_id={owner_id} offset={offset} limit={limit}')
    return await api.wall.get(owner_id=owner_id, offset=offset, count=limit, extended=0, filter='all')


async def generate_wall_posts(api: aiovk.API, owner_id, limit):
    offset = 0

    while True:
        response = await get_wall_posts(api, owner_id=owner_id, offset=offset, limit=limit)

        items = response['items']
        count = response['count']

        for item in items:
            yield item, offset, count
            offset += 1

        if not items or len(items) < limit:
            break


async def walk_wall_posts(session: aiovk.TokenSession, store: BaseStore, owner_id, max_offset=None):
    logger.info(f'walk wall posts owner_id={owner_id} max_offset={max_offset}')

    api = aiovk.API(session=session)
    async for fields, offset, count in generate_wall_posts(api, owner_id=owner_id, limit=MAX_POSTS_COUNT):
        post_id = fields.pop('id')
        owner_id = fields.pop('owner_id')
        if not await store.is_wall_post_exists(post_id=post_id, owner_id=owner_id):
            await store.add_wall_post(post_id=post_id, owner_id=owner_id, **fields)

        if not max_offset or offset < max_offset:
            logger.info(f'walk store posts owner_id={owner_id}'
                        f' count=[{await store.get_wall_posts_count(owner_id)}/{count}], continue')
        else:
            logger.info(f'walk wall posts ends due to offset={offset}'
                        f' is greater than max_posts_offset={max_offset}, break')
            break


async def store_photos(session: aiovk.TokenSession, store: BaseStore, store_photos_path: str, max_count=10):
    logger.info(f'store photos max_count={max_count}')

    store_count = 0
    async for wall_post in store.get_wall_posts():
        post_id = wall_post['post_id']
        owner_id = wall_post['owner_id']

        for photo in get_photos(wall_post):
            photo_id = photo['id']
            photo_url = photo['url']

            _, ext = os.path.splitext(photo_url)
            filename = os.path.join(store_photos_path, f'{owner_id}_{post_id}_{photo_id}{ext}')

            if not os.path.exists(filename) or not os.path.isfile(filename):
                _, data = await session.driver.get_bin(photo_url, params={})
                async with aiofiles.open(filename, mode='wb') as s:
                    await s.write(data)
                store_count += 1
                logger.info(f'store photos count=[{store_count}/{max_count}] url={photo_url} to {filename}')

        if store_count >= max_count:
            break


async def send_post(bot: aiogram.Bot, store: BaseStore, chat_id, owner_id):
    logger.info(f'search post from owner_id={owner_id} to chat_id={chat_id}')

    async for wall_post in store.get_wall_posts(owner_id=owner_id):
        post_id = wall_post['post_id']
        owner_id = wall_post['owner_id']  # Important: cause in this step we should have real owner_id not None
        if not await store.is_chat_wall_post_exists(chat_id=chat_id, post_id=post_id, owner_id=owner_id):
            photos = get_photos(wall_post)
            if photos:
                logger.info(f'send post_id={post_id} owner_id={owner_id} with photos={photos} to chat_id={chat_id}')
                media = aiogram.types.MediaGroup()
                for photo in photos:
                    media.attach_photo(photo['url'])
                await bot.send_media_group(chat_id, media=media)
                await store.add_chat_wall_post(owner_id=owner_id, post_id=post_id, chat_id=chat_id)

                return wall_post

    logger.warning(f'can\'t send post to chat_id={chat_id}')
