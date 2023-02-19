from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def menu_kb():
    mailing_button = InlineKeyboardButton(text='✉️ Рассылка', callback_data='mailing')
    metrics_button = InlineKeyboardButton(text='📊 Метрики', callback_data='metrics')
    edition_button = InlineKeyboardButton(text='📝 Редактура текстов', callback_data='edition')
    keyboard = InlineKeyboardMarkup(row_width=1).add(mailing_button, metrics_button, edition_button)
    return keyboard


def home_kb():
    home_button = InlineKeyboardButton(text='🏠 Домой', callback_data='home')
    keyboard = InlineKeyboardMarkup(row_width=1).add(home_button)
    return keyboard


def edition_kb():
    keyboard = InlineKeyboardMarkup(row_width=1)
    for i in range(1, 9):
        week_button = InlineKeyboardButton(text=f'Неделя {i}', callback_data=f'edit_week:{i}')
        keyboard.add(week_button)
    home_button = InlineKeyboardButton(text='🏠 Домой', callback_data='home')
    keyboard.add(home_button)
    return keyboard


def week_kb(week_id):
    other_keyboards = {
        1: 'Рутинные дела',
        2: 'Хорошие моменты',
        3: 'Стоп-Прислушайся к себе',
        4: 'Практика благодарности',
        5: 'Рутинные дела',
        6: 'Хорошие моменты',
        7: 'Стоп-Прислушайся к себе',
        8: 'Стоп-Прислушайся к себе'
    }
    keyboard = InlineKeyboardMarkup(row_width=1)
    task_button = InlineKeyboardButton(text='Задание', callback_data=f'edit_text:week_{week_id}:task')

    remind_1_button = InlineKeyboardButton(text='Медитации',
                                           callback_data=f'edit_text:week_{week_id}:remind_meditation')
    remind_2_button = InlineKeyboardButton(text=other_keyboards[week_id],
                                           callback_data=f'edit_text:week_{week_id}:remind_other')
    remind_3_button = InlineKeyboardButton(text='Дневник', callback_data=f'edit_text:week_{week_id}:remind_daily')
    keyboard.add(task_button, remind_1_button, remind_2_button, remind_3_button)
    if week_id == 1:
        other_button = InlineKeyboardButton(text='Видео-кружок', callback_data=f'edit_text:week_{week_id}:other')
        keyboard.add(other_button)
    elif week_id in [3, 6, 8]:
        other_button = InlineKeyboardButton(text='Донат', callback_data=f'edit_text:week_{week_id}:other')
        keyboard.add(other_button)
    home_button = InlineKeyboardButton(text='🏠 Домой', callback_data='home')
    keyboard.add(home_button)
    return keyboard
