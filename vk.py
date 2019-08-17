import asyncio
import aiovk
import logging

from store import Store

logger = logging.getLogger(__name__)


def select_photo_url(item):
    return item.get('photo_2560', item.get('photo_1280', item.get('photo_807', item.get('photo_604', None))))


def get_photos(item):
    photos = (attachment['photo'] for attachment in item.get('attachments', []) if attachment['type'] == 'photo')
    photos = ({'id': photo['id'], 'url': select_photo_url(photo)} for photo in photos)
    return [photo for photo in photos if photo['url']]


async def get_wall_posts(api: aiovk.API, store: Store, owner_id, request_posts_limit, required_posts_count):
    offset = 0
    posts_saved_count = 0

    while posts_saved_count < required_posts_count:
        logger.debug(f'wall posts query offset={offset}, count={request_posts_limit}')

        response = await api.wall.get(owner_id=owner_id,
                                      offset=offset,
                                      count=request_posts_limit, extended=0, filter='all')

        count = response['count']
        items = response['items']

        logger.debug(f'wall posts owner_id={owner_id} items count={len(items)} received (total count={count})')

        if store.get_wall_posts_count(owner_id=owner_id) >= count:
            logger.info(f'wall posts owner_id={owner_id} are saved')
            return False

        items = ({
            'post_id': item['id'],
            'date': item['date'],
            'photos': get_photos(item)
        } for item in response['items']
            if not store.is_wall_post_exists(post_id=item['id'], owner_id=owner_id))
        items = [item for item in items if item['photos']]

        for item in items:
            store.add_wall_post(owner_id=owner_id, **item)

        offset += len(response['items'])
        posts_saved_count += len(items)
        logger.debug(
            f'wall posts owner_id={owner_id} items count={len(items)} saved (total count={posts_saved_count})')

        sleep_for_secs = 10
        logger.debug(f'wall posts sleep for {sleep_for_secs}secs')
        await asyncio.sleep(sleep_for_secs)


async def watch_wall_posts(api: aiovk.API, store: Store, owner_id, get_posts_timeout,
                           request_posts_limit=10, required_posts_count=30):
    while True:
        await get_wall_posts(api, store, owner_id, request_posts_limit, required_posts_count)

        logger.info(
            f'watch wall posts owner_id={owner_id} items count={store.get_wall_posts_count(owner_id)} total saved')

        logger.info(f'watch wall posts sleep for {get_posts_timeout}secs')
        await asyncio.sleep(get_posts_timeout)
