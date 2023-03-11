import time

from aiogram import Dispatcher
from aiogram.types import CallbackQuery
from aiogram.dispatcher import FSMContext
from aiogram.utils.markdown import hstrikethrough, hspoiler

from tgbot.misc.states import FSMUser
from tgbot.keyboards.test_inline import test_keyboard, finish_test_kb
from tgbot.models.sql_connector import *
from create_bot import bot


def test_descriptor(anxiety, depression):
    if anxiety <= 7:
        anxiety_text = [
            '<span class="tg-spoiler"><i>Норма</i> - отсутствие достоверно выраженных симптомов',
            'тревоги.\n<b><i>Рекомендуется<i><b> - 3 раза в неделю уделять себе время и делать практики нашего курса',
            'для поддержания уровня тревоги в норме.</span>'
        ]
    elif 8 <= anxiety <= 10:
        anxiety_text = [
            '<span class="tg-spoiler"><i>Средне-выраженная тревога</i> (умеренная степень тревожности, эмоциональное',
            'напряжение, нет возможности расслабится в полной мере).\n<b><i>Рекомендуется</i></b> - ежедневно уделять',
            'себе время и делать практики нашего курса.</span>'
        ]
    else:
        anxiety_text = [
            '<span class="tg-spoiler"><i>Клинически выраженная тревога</i> (высокая степень тревожности, сильное',
            'эмоциональное напряжение, состояние близкое к паническим атакам).\n<b><i>Рекомендуется</i></b> - помимо',
            'данного курса обратиться к психотерапевту или неврологу.</span>'
        ]
    if depression <= 7:
        depression_text = [
            '<span class="tg-spoiler"><i>Норма</i> - отсутствие достоверно выраженных симптомов',
            'депрессии.\n<b><i>Рекомендуется<i><b> - 3 раза в неделю уделять себе время и делать практики нашего курса',
            'для поддержания уровня депрессии в норме.</span>'
        ]
    elif 8 <= depression <= 10:
        depression_text = [
            '<span class="tg-spoiler"><i>Средне-выраженная депрессия</i> (умеренная степень тревожности, эмоциональное',
            'напряжение, нет возможности расслабится в полной мере).\n<b><i>Рекомендуется</i></b> - помимо данного',
            'курса обратиться к психотерапевту.</span>'
        ]
    else:
        depression_text = [
            '<span class="tg-spoiler"><i>Клинически выраженная депрессия</i> (снижение настроения, высокая',
            'утомляемость, пессимистичное восприятие, нарушение сна и аппетита).\n<b><i>Рекомендуется</i></b> - помимо',
            'данного курса обратиться к психотерапевту.'
        ]
    return ''.join(anxiety_text), ''.join(depression_text)


async def questions(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    question_id = None  # id вопроса, которое показывается в сообщении
    text_dict = {
        'q1': '✓ <i>Вопрос 1/14</i>\nЯ напряжен. Мне не по себе',
        'q2': '✓ <i>Вопрос 2/14</i>\nТо, что приносило мне большое удовольствие, и сейчас вызывает такое же чувство',
        'q3': '✓ <i>Вопрос 3/14</i>\nМне страшно. Кажется, будто что-то ужасное может вот-вот случиться',
        'q4': '✓ <i>Вопрос 4/14</i>\nЯ способен рассмеяться и увидеть в том или ином событии смешное',
        'q5': '✓ <i>Вопрос 5/14</i>\nБеспокойные мысли крутятся у меня в голове',
        'q6': '✓ <i>Вопрос 6/14</i>\nЯ чувствую себя бодрым',
        'q7': '✓ <i>Вопрос 7/14</i>\nЯ легко могу сесть и расслабиться',
        'q8': '✓ <i>Вопрос 8/14</i>\nМне кажется, что я стал все делать очень медленно',
        'q9': '✓ <i>Вопрос 9/14</i>\nЯ испытываю внутренне напряжение или дрожь',
        'q10': '✓ <i>Вопрос 10/14</i>\nЯ не слежу за своей внешностью',
        'q11': '✓ <i>Вопрос 11/14</i>\nЯ не могу усидеть на месте, словно мне постоянно нужно двигаться',
        'q12': '✓ <i>Вопрос 12/14</i>\nЯ считаю, что мои дела (занятия, увлечения) могут принести мне чувство '
               'удовлетворения',
        'q13': '✓ <i>Вопрос 13/14</i>\nУ меня бывает внезапное чувство паники',
        'q14': '✓ <i>Вопрос 14/14</i>\nЯ могу получить удовольствие от хорошей книги, фильма, радио- или телепрограммы',
    }
    if callback.data.split(':')[0] == 'start_testing':
        week_id = int(callback.data.split(':')[1])
        question_id = 1
        async with state.proxy() as data:
            data['week_id'] = week_id
    if callback.data.split('_')[0] == 'question':
        question_id = int(callback.data.split(':')[0].split('_')[1]) + 1
        async with state.proxy() as data:
            data[f'answer_{question_id - 1}'] = callback.data.split(':')[1]
    if callback.data.split(':')[0] == 'back':
        question_id = int(callback.data.split(':')[1]) - 1
    if question_id < 15:
        text = text_dict[f'q{question_id}']
        kb = test_keyboard(question_id)
        await callback.message.answer(text, reply_markup=kb)
    else:
        anxiety = 0
        depression = 0
        text = None
        async with state.proxy() as data:
            week_id = int(data.as_dict()['week_id'])
        for i in range(1, 15):
            async with state.proxy() as data:
                result = int(data.as_dict()[f'answer_{i}'])
            if i % 2 == 1:
                anxiety += result
            else:
                depression += result
        description = test_descriptor(anxiety, depression)
        if week_id == 0:
            text = [
                'Тест завершён!\n',
                '⭐️ <b><u>Текущая оценка состояния</u></b>\n',
                f'<b>Тревога:</b> {anxiety} баллов',
                f'{description[0]}\n',
                f'<b>Депрессия:</b> {depression} баллов',
                description[1],
                'Текущий результат обновлён',
                'Теперь подумайте, когда хотели бы начать прохождение курса?'
            ]
            await FSMUser.new_date.set()
        if week_id == 3:
            await edit_profile_sql(user_id, 'next_step_name', 'week_4:task')
            await edit_profile_sql(user_id, 'next_step_time', time.time())
            old_result = await get_test_result_sql(user_id, 0)
            text = [
                'Тест завершён!\n',
                '⭐️ <b><u>Текущая оценка состояния</u></b>\n',
                f'<b>Тревога:</b> {hstrikethrough(old_result["anxiety"])} → {anxiety} баллов',
                f'{description[0]}\n',
                f'<b>Депрессия:</b> {hstrikethrough(old_result["depression"])} → {depression} баллов',
                description[1],
                'Текущий результат обновлён'
            ]
        if week_id == 8:
            await edit_profile_sql(user_id, 'next_step_time', 0)
            text = [
                'Поздравляем с завершением курса!!\n',
                '⭐️ <b><u>Текущая оценка состояния</u></b>\n',
                f'<b>Тревога:</b> {anxiety} баллов',
                f'{description[0]}\n',
                f'<b>Депрессия:</b> {depression} баллов',
                description[1],
            ]
        await create_test_result(user_id, week_id, anxiety, depression)
        kb = finish_test_kb(week_id)
        await callback.message.answer('\n'.join(text), reply_markup=kb)
    await edit_profile_sql(user_id, 'remind_meditation_time', 0)
    await bot.answer_callback_query(callback.id)


def register_testing(dp: Dispatcher):
    dp.register_callback_query_handler(questions, lambda x: x.data.split(':')[0] == 'start_testing', state='*')
    dp.register_callback_query_handler(questions, lambda x: x.data.split('_')[0] == 'question', state='*')
    dp.register_callback_query_handler(questions, lambda x: x.data.split(':')[0] == 'back', state='*')


