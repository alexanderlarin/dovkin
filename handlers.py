import aiogram
import aiovk
import asyncio
import enum
import logging
import urllib.parse

from aiogram.dispatcher.filters.state import State
from jobs import check_group_membership, send_post, sync_group_membership, walk_wall_posts, MAX_POSTS_COUNT
from store import BaseStore

logger = logging.getLogger(__name__)

subscribe_url_state = State('subscribe_url')
subscriptions_state = State('subscriptions_state')
subscription_state = State('subscription_state')


class Action(enum.Enum):
    back = 'back'
    remove = 'remove'


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

    async def get_subscriptions_message(chat_id):
        reply_markup = aiogram.types.InlineKeyboardMarkup(row_width=1)
        async for s in store.get_subscriptions(chat_id=chat_id):
            reply_markup.add(
                aiogram.types.InlineKeyboardButton(text=s['group']['name'], callback_data=s['group']['group_id']))
        return 'Choose a subscription from the list below:', reply_markup

    @dispatcher.message_handler(commands=['subscriptions', ])
    async def get_subscriptions(message: aiogram.types.Message):
        text, reply_markup = await get_subscriptions_message(message.chat.id)
        await bot.send_message(chat_id=message.chat.id, text=text, reply_markup=reply_markup)
        await subscriptions_state.set()

    @dispatcher.callback_query_handler(
        aiogram.filters.Text(equals=Action.back.value), state=subscriptions_state)
    async def back_subscriptions(callback_query: aiogram.types.CallbackQuery, state: aiogram.dispatcher.FSMContext):
        text, reply_markup = await get_subscriptions_message(callback_query.message.chat.id)
        await callback_query.message.edit_text(text=text, reply_markup=reply_markup)
        await subscriptions_state.set()
        await state.reset_data()

    @dispatcher.callback_query_handler(state=subscriptions_state)
    async def get_subscription(callback_query: aiogram.types.CallbackQuery, state: aiogram.dispatcher.FSMContext):
        async for s in store.get_subscriptions(chat_id=callback_query.message.chat.id):
            if s['group']['group_id'] == int(callback_query.data):
                membership = await check_group_membership(api, group_id=s['group']['group_id'])

                await callback_query.answer()

                reply_markup = aiogram.types.InlineKeyboardMarkup(row_width=2)
                reply_markup.add(aiogram.types.InlineKeyboardButton(text='Remove', callback_data=Action.remove.value))
                reply_markup.add(aiogram.types.InlineKeyboardButton(text='«Back', callback_data=Action.back.value))

                await callback_query.message.edit_text(
                    text=f'Here it is:'
                         f'\n{s["group"]["url"]}'
                         f'\nMembership: {membership.value.title()}',
                    reply_markup=reply_markup
                )
                await state.update_data(s)
                await subscriptions_state.set()

    @dispatcher.callback_query_handler(
        aiogram.filters.Text(equals='remove'), state=subscriptions_state)
    async def delete_subscription(callback_query: aiogram.types.CallbackQuery, state: aiogram.dispatcher.FSMContext):
        async with state.proxy() as data:
            await store.remove_subscription(chat_id=callback_query.message.chat.id, group_id=data['group']['group_id'])
        await callback_query.answer(text='Removed')
        await state.reset_data()

        text, reply_markup = await get_subscriptions_message(callback_query.message.chat.id)
        await callback_query.message.edit_text(text=text, reply_markup=reply_markup)
        await subscriptions_state.set()

    @dispatcher.message_handler(commands=['subscribe', ], state='*')
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

            await store.upsert_subscription(chat_id=message.chat.id, group_id=group['id'])
            logger.info(f'add subscription chat_id={message.chat.id} group_id={group["id"]}')
            await bot.send_message(chat_id=message.chat.id, text=f'You\'re subscribed to {group_url}')

            # TODO: refactor, look like a job
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

    return dispatcher
