import asyncio
import aiovk
import logging

from collections import namedtuple

from store import Store

logger = logging.getLogger(__name__)


PostItem = namedtuple('PostItem', ['post_id', 'owner_id', 'date', 'photos'])


def get_photo_url(item):
    return item.get('photo_2560', item.get('photo_1280', item.get('photo_807', item.get('photo_604', None))))


def get_photos(item):
    photos = (attachment['photo'] for attachment in item.get('attachments', []) if attachment['type'] == 'photo')
    photos = ({'id': photo['id'], 'url': get_photo_url(photo)} for photo in photos)
    return [photo for photo in photos if photo['url']]


async def get_wall_posts(api, owner_id, offset, limit):
    logger.debug(f'wall posts query owner_id={owner_id} offset={offset} count={limit}')
    response = await api.wall.get(owner_id=owner_id, offset=offset, count=limit, extended=0, filter='all')
    items = response['items']
    count = response['count']
    logger.debug(f'wall posts items owner_id={owner_id} count={len(items)} of {count}')
    return response['items'], response['count']


async def walk_wall_posts(api: aiovk.API, store: Store,
                          owner_id, loop_to_end, posts_per_request_limit=30, timeout_between_requests=10):
    offset = 0

    while True:
        items, count = await get_wall_posts(api, owner_id=owner_id, offset=offset, limit=posts_per_request_limit)

        if not len(items):
            logger.info(f'walk wall posts owner_id={owner_id}, end of wall is reached count={count}')
            break
        else:
            store_items = (PostItem(post_id=item['id'],
                                    owner_id=item['owner_id'],
                                    date=item['date'],
                                    photos=get_photos(item)) for item in items)

            store_items = [item for item in store_items if item.photos
                           and not store.is_wall_post_exists(post_id=item.post_id, owner_id=item.owner_id)]

            for item in store_items:
                store.add_wall_post(**item._asdict())

            if not loop_to_end and not len(store_items):
                logger.info(f'walk wall posts ends due to nothing to store')
                break
            else:
                logger.info(f'walk store posts owner_id={owner_id} '
                            f'count={len(store_items)} of {store.get_wall_posts_count(owner_id)}, continue')

            offset += len(items)

            logger.debug(f'wall posts sleep for {timeout_between_requests}secs')
            await asyncio.sleep(timeout_between_requests)
