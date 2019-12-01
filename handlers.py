import aiogram
import asyncio
import logging

from jobs import send_post
from store import BaseStore

logger = logging.getLogger(__name__)


def apply_handlers(dispatcher: aiogram.Dispatcher, store: BaseStore):
    bot = dispatcher.bot

    @dispatcher.message_handler(commands=['start', ])
    async def start(message: aiogram.types.Message):
        await bot.send_message(
            message.chat.id, "Hi!\nI'm DoVkIn!\nPowered by @alexanderolarin\nJust type /subscribe command")

    @dispatcher.message_handler(commands=['subscribe', ])
    async def subscribe(message: aiogram.types.Message):
        await store.add_chat(chat_id=message.chat.id)
        await bot.send_message(message.chat.id,
                               "Keep Nude and Panties Off!\nYou're subscribed")
        asyncio.ensure_future(send_post(bot, store, chat_id=message.chat.id, group_ids=member_group_ids))
        logger.info(f'subscribe chat_id={message.chat.id}, send first post immediately')

    @dispatcher.message_handler(commands=['unsubscribe', ])
    async def unsubscribe(message: aiogram.types.Message):
        await store.remove_chat(chat_id=message.chat.id)
        await bot.send_message(
            message.chat.id, "Oh no!\nYou're unsubscribed")
        logger.info(f'unsubscribe chat_id={message.chat.id}')

    return dispatcher
