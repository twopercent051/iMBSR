from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import hcode


async def get_id(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    text = f'chat_id: {hcode(chat_id)} || user_id: {hcode(user_id)}'

    await message.answer(text)





def register_echo(dp: Dispatcher):
    dp.register_message_handler(get_id, commands='get_id')
