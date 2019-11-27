import aiofiles
import aiogram
import aiovk
import backoff
import enum
import logging
import os
import random

from store.base import BaseStore
from vk import get_photos, ImplicitSession

logger = logging.getLogger(__name__)

MAX_POSTS_COUNT = 100


class Membership(enum.Enum):
    MEMBER = 'member'
    REQUEST = 'request'


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
async def check_group_membership(api: aiovk.API, group_id):
    response = await api.groups.isMember(group_id=group_id, extended=1)
    for membership in Membership:
        if response[membership.value]:
            logger.debug(f'check group_id={group_id} membership={membership}')
            return membership


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
async def sync_group_membership(api: aiovk.API, group_id):
    membership = await check_group_membership(api, group_id=group_id)
    if not membership:
        response = await api.groups.join(group_id=group_id)
        logger.debug(f'request group_id={group_id} membership={response}')
        membership = await check_group_membership(api, group_id=group_id)
    if membership == Membership.MEMBER:
        return True


async def sync_groups_membership(api: aiovk.API, group_ids):
    member_group_ids = []
    logger.info(f'sync group_ids={group_ids} membership')
    for group_id in group_ids:
        is_member = await sync_group_membership(api, group_id=group_id)
        if is_member:
            member_group_ids.append(group_id)
    logger.info(f'member group_ids={member_group_ids} count=[{len(member_group_ids)}/{len(group_ids)}]')
    return member_group_ids


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


async def walk_wall_posts(api: aiovk.API, store: BaseStore, owner_id, max_offset=None):
    logger.info(f'walk wall posts owner_id={owner_id} max_offset={max_offset}')

    async for item, offset, count in generate_wall_posts(api, owner_id=owner_id, limit=MAX_POSTS_COUNT):
        post_item = {
            'post_id': item['id'],
            'owner_id': item['owner_id'],
            'date': item['date'],
            'photos': get_photos(item)
        }
        if post_item['photos'] and not await store.is_wall_post_exists(
                post_id=post_item['post_id'], owner_id=post_item['owner_id']):
            await store.add_wall_post(**post_item)

        if not max_offset or offset < max_offset:
            logger.info(f'walk store posts owner_id={owner_id}'
                        f' count=[{await store.get_wall_posts_count(owner_id)}/{count}], continue')
        else:
            logger.info(f'walk wall posts ends due to offset={offset}'
                        f' is greater than max_posts_offset={max_offset}, end')
            break


async def store_photos(session: ImplicitSession, store: BaseStore, store_photos_path: str, max_count=10):
    logger.info(f'store photos max_count={max_count}')

    store_count = 0
    async for item in store.get_wall_posts():
        post_id = item['post_id']
        owner_id = item['owner_id']

        for photo in item['photos']:
            photo_id = photo['id']
            photo_url = photo['url']

            _, ext = os.path.splitext(photo_url)
            filename = os.path.join(store_photos_path, f'{owner_id}_{post_id}_{photo_id}{ext}')

            if not os.path.exists(filename) or not os.path.isfile(filename):
                logger.debug(f'store photo url={photo_url} to {filename}')
                _, data = await session.driver.get_bin(photo_url, params={})
                async with aiofiles.open(filename, mode='wb') as s:
                    await s.write(data)
                store_count += 1
                logger.info(f'store photos count=[{store_count}/{max_count}]')

        if store_count >= max_count:
            break


async def send_post(bot: aiogram.Bot, store: BaseStore, chat_id, group_ids):
    owner_id = -group_ids[random.randint(0, len(group_ids) - 1)] if group_ids else None
    logger.info(f'search post owner_id={owner_id} for chat_id={chat_id}')
    item = await store.get_wall_post_to_send(chat_id=chat_id, owner_id=owner_id)
    if item:
        post_id = item['post_id']
        owner_id = item['owner_id']
        photos = item['photos']

        logger.info(f'send posts to chat_id={chat_id} post_id={post_id}, photos={photos}')
        media = aiogram.types.MediaGroup()
        for photo in photos:
            media.attach_photo(photo['url'])
        await bot.send_media_group(chat_id, media=media)

        await store.add_chat_post(chat_id=chat_id, owner_id=owner_id, post_id=post_id)
        logger.info(f'post sent chat_id={chat_id}, owner_id={owner_id}, post_id={post_id}')
