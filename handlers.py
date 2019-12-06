import aiogram
import aiovk
import logging
import urllib.parse

from aiogram.dispatcher.filters.state import State
from jobs import sync_group_membership
from store import BaseStore

logger = logging.getLogger(__name__)

subscribe_url_state = State('subscribe_url')


def apply_handlers(dispatcher: aiogram.Dispatcher, store: BaseStore, api: aiovk.API):
    bot = dispatcher.bot

    @dispatcher.message_handler(commands=['start', ])
    async def start(message: aiogram.types.Message):
        await store.upsert_chat(chat_id=message.chat.id, username=message.chat.username)
        await bot.send_message(
            message.chat.id, 'Hi!\nI\'m DoVkIn!\nPowered by @alexanderolarin\n'
                             'Just type /subscribe command to subscribe wall posts from VK-group')

    @dispatcher.message_handler(commands=['cancel'], state='*')
    @dispatcher.message_handler(aiogram.filters.Text(equals='cancel', ignore_case=True), state='*')
    async def cancel(message: aiogram.types.Message, state: aiogram.dispatcher.FSMContext):
        current_state = await state.get_state()
        if current_state:
            logging.info(f'cancel chat_id={message.chat.id} state=={current_state}')
            await state.finish()

    @dispatcher.message_handler(commands=['subscribe', ])
    async def subscribe(message: aiogram.types.Message):
        await store.upsert_chat(chat_id=message.chat.id, username=message.chat.username)
        await bot.send_message(
            message.chat.id, "Type URL for VK-group you want to subscribe or type /cancel:")
        await subscribe_url_state.set()

    @dispatcher.message_handler(state=subscribe_url_state)
    async def subscribe_url(message: aiogram.types.Message, state: aiogram.dispatcher.FSMContext):
        try:
            await aiogram.types.ChatActions.typing()  # TODO: should depends on real execution time

            group_url = message.text

            short_name = urllib.parse.urlparse(group_url).path.strip('/')

            await bot.send_message(chat_id=message.chat.id, text=f'Wait! We\'re trying to subscribe...')
            await state.finish()  # TODO: or it's too early

            await aiogram.types.ChatActions.typing()  # TODO: should depends on real execution time
            group = (await api.groups.getById(group_id=short_name))[0]
            group_fields = {
                'name': group['name'],
                'short_name': short_name,
                'is_member': await sync_group_membership(api, group_id=group['id'])
            }
            await store.upsert_group(group_id=group['id'], **group_fields)
            logger.info(f'upsert group_id={group["id"]} with short_name={short_name}')

            await store.add_subscription(chat_id=message.chat.id, group_id=group['id'])
            logger.info(f'add subscription chat_id={message.chat.id} group_id={group["id"]}')
            await bot.send_message(chat_id=message.chat.id, text=f'You\'re subscribed to {group_url}')

            # TODO: send post immediately and run wall posts update

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
