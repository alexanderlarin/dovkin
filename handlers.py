import aiogram
import aiovk
import asyncio
import logging
import urllib.parse

from aiogram.dispatcher.filters.state import State
from jobs import send_post
from store import BaseStore

logger = logging.getLogger(__name__)

subscribe_url_state = State('subscribe_url')


def apply_handlers(dispatcher: aiogram.Dispatcher, store: BaseStore, api: aiovk.API):
    bot = dispatcher.bot

    @dispatcher.message_handler(commands=['start', ])
    async def start(message: aiogram.types.Message):
        await store.add_chat(chat_id=message.chat.id)
        await bot.send_message(
            message.chat.id, 'Hi!\nI\'m DoVkIn!\nPowered by @alexanderolarin\n'
                             'Just type /subscribe command to subscribe wall posts from VK-group')

    @dispatcher.message_handler(commands=['subscribe', ])
    async def subscribe(message: aiogram.types.Message):
        await bot.send_message(
            message.chat.id, "Type URL for VK-group you want to subscribe:")
        await subscribe_url_state.set()

    @dispatcher.message_handler(state=subscribe_url_state)
    async def subscribe_url(message: aiogram.types.Message, state: aiogram.dispatcher.FSMContext):
        try:
            await aiogram.types.ChatActions.typing()  # TODO: should depends on real execution time

            group_url = message.text
            short_name = urllib.parse.urlparse(group_url).path.strip('/')

            response = await api.groups.getById(group_id=short_name)
            group_id = response[0]['id']

            await store.add_subscription(chat_id=message.chat.id, group_id=group_id)
            await bot.send_message(chat_id=message.chat.id, text=f'You\'re subscribed to {group_url}')

            logger.info(f'subscribe chat_id={message.chat.id} to vk group_id={group_id} short_name={short_name}')

            await state.finish()

        except Exception as ex:
            logger.error(f'trouble in message={message.text} processing')
            logger.exception(ex)

            await message.reply(text='Something is wrong!...\nTry again or type /cancel ')

    # @dispatcher.message_handler(commands=['unsubscribe', ])
    # async def unsubscribe(message: aiogram.types.Message):
    #     await store.remove_chat(chat_id=message.chat.id)
    #     await bot.send_message(
    #         message.chat.id, "Oh no!\nYou're unsubscribed")
    #     logger.info(f'unsubscribe chat_id={message.chat.id}')

    return dispatcher
