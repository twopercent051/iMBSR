from aiogram import Dispatcher
from aiogram.types import Message, CallbackQuery
from aiogram.dispatcher import FSMContext
from aiogram_calendar import simple_cal_callback, SimpleCalendar
from aiogram.utils.markdown import hspoiler, hitalic
import aiogram_calendar

from tgbot.misc.states import FSMUser
from tgbot.keyboards.user_inline import *
from tgbot.models.sql_connector import *
from tgbot.misc.datetimer import next_step_timer
from tgbot.handlers.testing import test_descriptor
from create_bot import bot

from datetime import datetime
from math import ceil
import time


admin_group = config.misc.admin_group


async def user_start(message: Message):
    user_id = message.from_user.id
    is_user = await check_user_sql(user_id)
    if is_user['c'] == 0:
        text = 'Введите Ваше имя'
        kb = None
        await FSMUser.name.set()
    else:
        test_result = await get_test_result_sql(user_id, 0)
        if test_result is None:
            text = '<span class="tg-spoiler">💛 <b>Для</b> продолжения курса оцените ваше текущее состояние - пройдите тест 👉</span>'
            kb = user_start_test_kb(0)
        else:
            text = 'Главное меню'
            kb = main_menu_kb()
            await FSMUser.menu.set()
    await message.answer(text, reply_markup=kb)


async def get_name(message: Message, state: FSMContext):
    text = 'Введите город'
    await FSMUser.city.set()
    async with state.proxy() as data:
        data['name'] = message.text
    await message.answer(text)


async def get_city(message: Message, state: FSMContext):
    text = 'Введите E-mail'
    await FSMUser.email.set()
    async with state.proxy() as data:
        data['city'] = message.text
    await message.answer(text)


async def get_email(message: Message, state: FSMContext):
    if len(message.text.split('@')) == 2:
        text = 'Выберите ваш часовой пояс, чтобы мы не присылали сообщения ночью'
        kb = user_timezone_kb()
        await FSMUser.timezone.set()
        async with state.proxy() as data:
            data['email'] = message.text
        await message.answer(text, reply_markup=kb)
    else:
        text = 'Неверный формат электронной почты. Попробуйте снова'
        await message.answer(text)


async def get_timezone(callback: CallbackQuery, state: FSMContext):
    text = 'Каковы ваши ожидания от курса?'
    timezone = callback.data.split(':')[1]
    await FSMUser.expectations.set()
    async with state.proxy() as data:
        data['timezone'] = timezone
    await callback.message.answer(text)
    await bot.answer_callback_query(callback.id)


async def get_expectations(message: Message, state: FSMContext):
    user_id = message.from_user.id
    text = '💛 Для продолжения курса оцените ваше текущее состояние - пройдите тест 👉'
    kb = user_start_test_kb(0)
    async with state.proxy() as data:
        name = data.as_dict()['name']
        city = data.as_dict()['city']
        email = data.as_dict()['email']
        timezone = data.as_dict()['timezone']
    await create_user_sql(user_id, name, city, email, timezone, message.text)
    await message.answer(text, reply_markup=kb)


async def main_menu(callback: CallbackQuery):
    text = 'Главное меню'
    kb = main_menu_kb()
    await FSMUser.menu.set()
    await callback.message.answer(text, reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def get_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    profile = await get_profile_sql(user_id)
    text = [
        '💻 Профиль',
        f'👤 Имя: <i>{profile["name"]}</i>',
        f'🏢 Город: <i>{profile["city"]}</i>',
        f'✉️ Почта: <i>{profile["email"]}</i>',
        '📋 Ваши ожидания:',
        f'<i>{profile["expectations"]}</i>',
    ]
    kb = profile_kb()
    await callback.message.answer('\n'.join(text), reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def edit_profile_start(callback: CallbackQuery):
    text = 'Что будем менять?'
    kb = edit_profile_kb()
    await callback.message.answer(text, reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def edit_profile_enter(callback: CallbackQuery):
    field = callback.data.split(':')[1]
    text, kb = None, menu_kb()
    time_text = [
        '🕓 Выберите время для выполнения ежедневной практики (медитация/йога) по 30 минут. В выбранное время пришлём',
        'вам напоминалку 🔔. (Введите время в формате hh:mm через двоеточие не позднее 21:00)'
    ]
    if field == 'name':
        await FSMUser.edit_name.set()
        text = 'Введите новое имя'
    if field == 'city':
        await FSMUser.edit_city.set()
        text = 'Введите новый город'
    if field == 'email':
        await FSMUser.edit_email.set()
        text = 'Введите новый E-mail'
    if field == 'expectations':
        await FSMUser.edit_expectations.set()
        text = 'Обновите свои ожидания'
    if field == 'timezone':
        await FSMUser.edit_timezone.set()
        text = 'Выберите часовой пояс'
        kb = user_timezone_kb()
    if field == 'time_menu':
        await FSMUser.edit_time_menu.set()
        text = ''.join(time_text)
    if field == 'time_task':
        await FSMUser.edit_time_task.set()
        text = ''.join(time_text)
    if field == 'date':
        text = 'Выберите дату начала курса'
        kb = await aiogram_calendar.SimpleCalendar().start_calendar()
    await callback.message.answer(text, reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def edit_profile_finish_msg(message: Message, state: FSMContext):
    user_id = message.from_user.id
    stat_cor = await state.get_state()
    stat = stat_cor.split(':')[1]
    value = message.text
    text = None
    kb = menu_kb()
    if stat == 'edit_name':
        text = 'Имя изменено'
    if stat == 'edit_city':
        text = 'Город изменён'
    if stat == 'edit_email':
        if len(message.text.split('@')) == 2:
            text = 'Электронная почта изменена'
        else:
            text = 'Неверный формат электронной почты. Попробуйте снова'
            await message.answer(text, reply_markup=kb)
            return
    if stat == 'edit_expectations':
        text = 'Ожидания изменены'
    if stat == 'edit_time_task' or stat == 'edit_time_menu':
        text_error = 'Вы ввели неправильный формат времени'
        try:
            hour = int(message.text.split(':')[0])
            minute = int(message.text.split(':')[1])
            if 0 <= hour <= 20 and 0 <= minute <= 59 and len(message.text.split(':')) == 2:
                text = 'Время обновлено'
                await edit_profile_sql(user_id, 'remind_hour', hour)
                await edit_profile_sql(user_id, 'remind_min', minute)
                await message.answer(text, reply_markup=kb)
                if stat == 'edit_time_task':
                    profile = await get_profile_sql(user_id)
                    user_tz = profile['timezone']
                    remind_1_time = await next_step_timer(user_tz, 1, hour, minute)
                    await edit_profile_sql(user_id, 'day', 1)
                    await edit_profile_sql(user_id, 'remind_meditation_time', remind_1_time)
                return
            else:
                await message.answer(text_error, reply_markup=kb)
                return
        except ValueError:
            await message.answer(text_error, reply_markup=kb)
            return
    await edit_profile_sql(user_id, stat.split('_')[1], value)
    await message.answer(text, reply_markup=kb)


async def edit_profile_finish_clb(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    stat_cor = await state.get_state()
    stat = stat_cor.split(':')[1]
    value = int(callback.data.split(':')[1])
    text = 'Часовой пояс изменен'
    kb = menu_kb()
    await edit_profile_sql(user_id, stat.split('_')[1], value)
    await callback.message.answer(text, reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def calender(callback: CallbackQuery):
    text = [
        '📆 Выберите дату начала курса.',
        'В этот день мы пришлём:',
        '✔ Инструкцию по началу курса,',
        '✔ Теорию первой недели,',
        '✔ Практики на неделю',
        '✔ Дневник для заполнения вручную.'
    ]
    kb = await aiogram_calendar.SimpleCalendar().start_calendar()
    await callback.message.answer('\n'.join(text), reply_markup=kb)


async def edit_date(callback: CallbackQuery, callback_data):
    user_id = callback.from_user.id
    selected, date = await SimpleCalendar().process_selection(callback, callback_data)
    user_profile = await get_profile_sql(user_id)
    timezone = user_profile['timezone']
    if selected:
        if date.timestamp() - time.time() < (timezone - 24) * 3600:
            text = ['Вы ввели дату, которая уже прошла']
            kb = await aiogram_calendar.SimpleCalendar().start_calendar()
        else:
            await edit_profile_sql(user_id, 'start_date', date.timestamp())
            if -24 * 3600 < time.time() + timezone * 3600 - date.timestamp() < 24 * 3600:
                await edit_profile_sql(user_id, 'next_step_time', time.time())
            else:
                days_offset = ceil((date.timestamp() - time.time() - timezone * 3600) / (24 * 3600)) - 1
                next_step = await next_step_timer(timezone, days_offset, 10, 0)
                await edit_profile_sql(user_id, 'next_step_time', next_step)
            await edit_profile_sql(user_id, 'next_step_name', 'week_1:task')
            text = [
                f'✅ Вы выбрали дату {date.strftime("%d.%m.%Y")}',
                'Накануне мы пришлём материалы первой недели курса.'
            ]
            kb = menu_kb()
        await callback.message.answer('\n'.join(text), reply_markup=kb)


async def practice_counter(callback: CallbackQuery):
    user_id = callback.from_user.id
    week_id = callback.data.split(':')[1]
    practice = await get_practices_sql(user_id, week_id)
    if practice is None:
        practice_count = 0
    else:
        practice_count = practice['counter']
    if week_id == 'not':
        text = f'Ничего страшного, в следующий раз получится 💪\n✔ Выполнено практик на неделе: {practice_count}'
    else:
        if practice is None:
            await create_practices_sql(user_id, week_id)
        else:
            await edit_practices_sql(user_id, week_id, practice_count + 1)
        text = f'Отлично, вы молодец 🥳\n✔ Выполнено практик на неделе: {practice_count + 1}'
    await callback.message.answer(text)


async def current_result(callback: CallbackQuery):
    user_id = callback.from_user.id
    profile = await get_profile_sql(user_id)
    if profile['week_id'] < 3:
        test_week_id = 0
    elif profile['week_id'] < 8:
        test_week_id = 3
    else:
        test_week_id = 8
    tests = await get_test_result_sql(user_id, test_week_id)
    practices = await get_practices_sql(user_id, profile['week_id'])
    if profile['start_date'] is None:
        start_date = 'Не выбрана'
    else:
        start_date = datetime.fromtimestamp(profile['start_date']).strftime('%d.%m.%Y')
    if profile['remind_hour'] is None:
        practice_time = 'Не выбрано'
    else:
        if profile["remind_min"] < 10:
            minutes = f'0{profile["remind_min"]}'
        else:
            minutes = profile["remind_min"]
        practice_time = f'{profile["remind_hour"]}:{minutes}'
    if profile['week_id'] == 0:
        current_week = 'Вы ещё не начали курс'
    else:
        current_week = profile['week_id']
    if practices is None:
        counter = 0
    else:
        counter = practices['counter']
    text = [
        '💪 <b><u>Текущие результаты курса</u></b>\n',
        f'🏁 <b>Дата начала курса:</b> {start_date}',
        f'🕒 <b>Время практики:</b> {practice_time}',
        f'📆 <b>Текущая неделя:</b> {current_week}',
        f'✔ <b>Выполнено практик на неделе:</b> {counter}\n',

    ]
    if tests is not None:
        desc = test_descriptor(tests['anxiety'], tests['depression'])
        text_test = [
            '-' * 10,
            '\n⭐️ <b><u>Текущая оценка состояния</u></b>\n',
            f'<b>Тревога:</b> {tests["anxiety"]} баллов',
            f'{desc[0]}\n',
            f'<b>Депрессия:</b> {tests["depression"]} баллов',
            f'{desc[1]}\n'
        ]
        text.extend(text_test)
    if profile['next_step_time'] is None or profile['next_step_time'] > time.time():
        kb = current_result_kb(True)
    else:
        kb = current_result_kb(False)
    await callback.message.answer('\n'.join(text), reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def user_reset(callback: CallbackQuery):
    text, kb = None, None
    if callback.data.split(':')[1] == 'request':
        text = 'Вы действительно хотите удалить прогресс курса и начать заново? Восстановить данные будет невозможно'
        kb = reset_kb()
    if callback.data.split(':')[1] == 'accept':
        user_id = callback.from_user.id
        text = '💛 Для продолжения курса оцените ваше текущее состояние - пройдите тест 👉'
        kb = user_start_test_kb(0)
        await reset_user_sql(user_id)
    await callback.message.answer(text, reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def practices(callback: CallbackQuery):
    if callback.data.split(':')[1] == 'menu':
        user_id = callback.from_user.id
        profile = await get_profile_sql(user_id)
        if profile['start_date'] is None:
            text = [
                '❌ Вы ещё не начали курс',
                '✅ Чтобы получить доступ к материалам первой недели - выберите дату начала курса 👇'
            ]
            kb = get_date_kb()
        else:
            if profile['week_id'] == 0:
                date = datetime.fromtimestamp(profile['start_date'])
                text = [
                    f'✅ Вы выбрали дату {date.strftime("%d.%m.%Y")}',
                    'Накануне мы пришлём материалы первой недели курса.'
                ]
                kb = menu_kb()
            else:
                text = ['Для просмотра задания выберите неделю курса']
                kb = practices_kb(profile['week_id'])
    else:
        week_id = int(callback.data.split(':')[1])
        text_request = await get_text_sql(week_id)
        text = [text_request['task']]
        kb = menu_kb()
    await callback.message.answer('\n'.join(text), reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def support_start(callback: CallbackQuery):
    if callback.data.split(':')[1] == 'support':
        text = [
            '✏️ <b>Напишите в ответ, о чём хотите узнать или в чём нужна помощь.</b>',
            '💁 На связи наш технический <i>специалист Виталий Суворов</i> - поможет разобраться в технических деталях.',
            '💁‍♀️ А также сертифицированный <i>коуч Светлана Шевоцукова</i> - ответит на вопросы по курсу и поможет в '
            'его прохождении\n',
            '<i>*Предложения по курсу также можете писать сюда</i>'
        ]
        kb = menu_kb()
        await FSMUser.support.set()
    else:
        text = [
            '✏️ <b>Напишите в ответ, свой отзыв о курсе</b> <i>(чем помог, что нового узнали, в чём возникли '
            'сложности и т.п.)</i>',
            '👍 Мы всегда рады обратной связи и готовы меняться в лучшую сторону!'
        ]
        kb = menu_kb()
        await FSMUser.feedback.set()
    await callback.message.answer('\n'.join(text), reply_markup=kb)
    await bot.answer_callback_query(callback.id)


async def support_finish(message: Message):
    user_id = message.from_user.id
    user_username = message.from_user.username
    user_text = 'Обращение отправлено'
    user_kb = menu_kb()
    admin_text = [
        f'Обращение от пользователя @{user_username}',
        '-' * 10,
        message.text
    ]
    admin_kb = support_kb(user_id)
    await message.answer(user_text, reply_markup=user_kb)
    await bot.send_message(admin_group, '\n'.join(admin_text), reply_markup=admin_kb)


async def feedback_finish(message: Message):
    user_username = message.from_user.username
    user_text = 'Отзыв отправлен'
    user_kb = menu_kb()
    admin_text = [
        f'Отзыв от пользователя @{user_username}',
        '-' * 10,
        message.text
    ]
    await message.answer(user_text, reply_markup=user_kb)
    await bot.send_message(admin_group, '\n'.join(admin_text))


async def donate(callback: CallbackQuery):
    if callback.data.split(':')[1] == 'start':
        text = [
            '🤲 Поддержите наш проект\n',
            '❤️ Мы очен стараемся, чтобы наш проект был полезен для широкого круга лиц и был доступен каждому, '
            'поэтому курс iMBSR <b>бесплатный.</b> Чтобы он оставался таким как можно дольше, пожалуйста, поддержите '
            'нас любой суммой 🙏'
        ]
        kb = donate_kb()
    else:
        text = ['РЕКВИЗИТЫ ДЛЯ ДОНАТА']
        kb = menu_kb()
    await callback.message.answer('\n'.join(text), reply_markup=kb)
    await bot.answer_callback_query(callback.id)


def register_user(dp: Dispatcher):
    edit_fsm_list = [FSMUser.edit_name, FSMUser.edit_city, FSMUser.edit_email, FSMUser.edit_expectations,
                     FSMUser.edit_time_task, FSMUser.edit_time_menu]

    dp.register_message_handler(user_start, commands=["start"], state="*")
    dp.register_message_handler(get_name, content_types='text', state=FSMUser.name)
    dp.register_message_handler(get_city, content_types='text', state=FSMUser.city)
    dp.register_message_handler(get_email, content_types='text', state=FSMUser.email)
    dp.register_message_handler(get_expectations, content_types='text', state=FSMUser.expectations)
    dp.register_message_handler(edit_profile_finish_msg, content_types='text', state=edit_fsm_list)
    dp.register_message_handler(support_finish, content_types='text', state=FSMUser.support)
    dp.register_message_handler(feedback_finish, content_types='text', state=FSMUser.feedback)

    dp.register_callback_query_handler(get_timezone, lambda x: x.data.split(':')[0] == 'tz', state=FSMUser.timezone)
    dp.register_callback_query_handler(main_menu, lambda x: x.data == 'main_menu', state='*')
    dp.register_callback_query_handler(get_profile, lambda x: x.data == 'profile', state='*')
    dp.register_callback_query_handler(edit_profile_start, lambda x: x.data == 'edit_profile', state='*')
    dp.register_callback_query_handler(edit_profile_enter, lambda x: x.data.split(':')[0] == 'edit', state='*')
    dp.register_callback_query_handler(edit_profile_finish_clb, lambda x: x.data.split(':')[0] == 'tz',
                                       state=FSMUser.edit_timezone)
    dp.register_callback_query_handler(edit_date, simple_cal_callback.filter(), state='*')
    dp.register_callback_query_handler(practice_counter, lambda x: x.data.split(':')[0] == 'done', state='*')
    dp.register_callback_query_handler(current_result, lambda x: x.data == 'current_result', state='*')
    dp.register_callback_query_handler(calender, lambda x: x.data == 'edit:date', state='*')
    dp.register_callback_query_handler(user_reset, lambda x: x.data.split(':')[0] == 'reset', state='*')
    dp.register_callback_query_handler(practices, lambda x: x.data.split(':')[0] == 'practices', state='*')
    dp.register_callback_query_handler(support_start, lambda x: x.data.split(':')[0] == 'support', state='*')
    dp.register_callback_query_handler(donate, lambda x: x.data.split(':')[0] == 'donate', state='*')
