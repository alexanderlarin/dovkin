import aiogram
import aiovk
import asyncio
import logging
import urllib.parse

from aiogram.dispatcher.filters.state import State
from jobs import send_post, sync_group_membership, walk_wall_posts, MAX_POSTS_COUNT
from store import BaseStore

logger = logging.getLogger(__name__)

subscribe_url_state = State('subscribe_url')


def apply_handlers(dispatcher: aiogram.Dispatcher, store: BaseStore, session: aiovk.TokenSession):
    api = aiovk.API(session=session)
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

            # TODO: add url processing without SCHEME prefix
            short_name = urllib.parse.urlparse(group_url).path.strip('/')

            await bot.send_message(chat_id=message.chat.id, text=f'Wait! We\'re trying to subscribe...')
            await state.finish()  # TODO: or it's too early

            await aiogram.types.ChatActions.typing()  # TODO: should depends on real execution time
            group = (await api.groups.getById(group_id=short_name))[0]
            group_fields = {
                'name': group['name'],
                'url': group_url,
                'is_member': await sync_group_membership(api, group_id=group['id'])
            }
            await store.upsert_group(group_id=group['id'], **group_fields)
            logger.info(f'upsert group_id={group["id"]} with short_name={short_name}')

            await store.add_subscription(chat_id=message.chat.id, group_id=group['id'])
            logger.info(f'add subscription chat_id={message.chat.id} group_id={group["id"]}')
            await bot.send_message(chat_id=message.chat.id, text=f'You\'re subscribed to {group_url}')

            async def walk_posts_and_send_one():
                owner_id = -group['id']
                if group_fields['is_member']:
                    logger.info(f'send post from owner_id={owner_id} to chat_id={message.chat.id} immediately')
                    await walk_wall_posts(session, store, owner_id=-group['id'], max_offset=MAX_POSTS_COUNT)
                    await send_post(bot, store, chat_id=message.chat.id, owner_id=-group['id'])
                else:
                    logger.warning(f'can\'t send post from owner_id={owner_id} because fake user is not group member')

            asyncio.ensure_future(walk_posts_and_send_one())

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
