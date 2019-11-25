import aiovk
import backoff
import enum
import logging

from store import Store
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


async def walk_wall_posts(api: aiovk.API, store: Store, owner_id, max_offset=None):
    logger.info(f'walk wall posts owner_id={owner_id} max_offset={max_offset}')

    async for item, offset, count in generate_wall_posts(api, owner_id=owner_id, limit=MAX_POSTS_COUNT):
        post_item = {
            'post_id': item['id'],
            'owner_id': item['owner_id'],
            'date': item['date'],
            'photos': get_photos(item)
        }
        if not store.is_wall_post_exists(post_id=post_item['post_id'], owner_id=post_item['owner_id']):
            store.add_wall_post(**post_item)

        if not max_offset or offset < max_offset:
            logger.info(f'walk store posts owner_id={owner_id}'
                        f' count=[{store.get_wall_posts_count(owner_id)}/{count}], continue')
        else:
            logger.info(f'walk wall posts ends due to offset={offset}'
                        f' is greater than max_posts_offset={max_offset}, end')
            break
