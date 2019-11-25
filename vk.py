import aiovk.utils
import logging

logger = logging.getLogger(__name__)


class ImplicitSession(aiovk.ImplicitSession):
    def __init__(self, login: str, password: str, app_id: int, scope: str or int or list,
                 max_requests_per_period: int, max_requests_period: int,
                 timeout: int = 10, num_of_attempts: int = 5, driver=None):
        super().__init__(login, password, app_id, scope, timeout, num_of_attempts, driver)
        self._queue = aiovk.utils.TaskQueue(max_requests_per_period, max_requests_period)

    @aiovk.utils.wait_free_slot
    async def send_api_request(self, method_name, params=None, timeout=None) -> dict:
        return await super().send_api_request(method_name, params, timeout)


def get_photo_url(item):
    return item.get('photo_2560', item.get('photo_1280', item.get('photo_807', item.get('photo_604', None))))


def get_photos(item):
    photos = (attachment['photo'] for attachment in item.get('attachments', []) if attachment['type'] == 'photo')
    photos = ({'id': photo['id'], 'url': get_photo_url(photo)} for photo in photos)
    return [photo for photo in photos if photo['url']]
