from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def user_timezone_kb():
    msk_button = InlineKeyboardButton(text='Московское время', callback_data='tz:0')
    a_button = InlineKeyboardButton(text='-1', callback_data='tz:-1')
    b_button = InlineKeyboardButton(text='+1', callback_data='tz:1')
    c_button = InlineKeyboardButton(text='+2', callback_data='tz:2')
    d_button = InlineKeyboardButton(text='+3', callback_data='tz:3')
    e_button = InlineKeyboardButton(text='+4', callback_data='tz:4')
    f_button = InlineKeyboardButton(text='+5', callback_data='tz:5')
    g_button = InlineKeyboardButton(text='+6', callback_data='tz:6')
    h_button = InlineKeyboardButton(text='+7', callback_data='tz:7')
    i_button = InlineKeyboardButton(text='+8', callback_data='tz:8')
    j_button = InlineKeyboardButton(text='+9', callback_data='tz:9')
    keyboard = InlineKeyboardMarkup(row_width=1).add(msk_button).row(a_button, b_button, c_button, d_button, e_button).\
        row(f_button, g_button, h_button, i_button, j_button)
    return keyboard


def user_start_test_kb(week_id):
    start_test_button = InlineKeyboardButton(text='✔️ Пройти тест', callback_data=f'start_testing:{week_id}')
    keyboard = InlineKeyboardMarkup(row_width=1).add(start_test_button)
    return keyboard


def main_menu_kb():
    profile_button = InlineKeyboardButton(text='👨🏿‍💻 Профиль', callback_data='profile')
    current_result_button = InlineKeyboardButton(text='💪 Текущий результат', callback_data='current_result')
    practices_button = InlineKeyboardButton(text='🧘‍♂️ Практики', callback_data='practices:menu')
    course_questions_button = InlineKeyboardButton(text='❔ Вопросы по курсу', callback_data='support:support')
    support_project_button = InlineKeyboardButton(text='💰 Поддержать проект', callback_data='donate:start')
    leave_feedback_button = InlineKeyboardButton(text='💬 Написать отзыв', callback_data='feedback:support')
    keyboard = InlineKeyboardMarkup(row_width=1).add(profile_button, current_result_button, practices_button,
                                                     course_questions_button, support_project_button,
                                                     leave_feedback_button)
    return keyboard


def profile_kb():
    edit_profile_button = InlineKeyboardButton(text='📝 Редактировать', callback_data='edit_profile')
    menu_button = InlineKeyboardButton(text='⬅️ Назад в меню', callback_data='main_menu')
    keyboard = InlineKeyboardMarkup(row_width=1).add(edit_profile_button, menu_button)
    return keyboard


def edit_profile_kb():
    name_button = InlineKeyboardButton(text='Имя', callback_data='edit:name')
    city_button = InlineKeyboardButton(text='Город', callback_data='edit:city')
    email_button = InlineKeyboardButton(text='E-mail', callback_data='edit:email')
    timezone_button = InlineKeyboardButton(text='Часовой пояс', callback_data='edit:timezone')
    expectations_button = InlineKeyboardButton(text='Ожидания', callback_data='edit:expectations')
    menu_button = InlineKeyboardButton(text='⬅️ Назад в меню', callback_data='main_menu')
    keyboard = InlineKeyboardMarkup(row_width=1).add(name_button, city_button, email_button, timezone_button,
                                                     expectations_button, menu_button)
    return keyboard


def current_result_kb(is_date: bool):
    keyboard = InlineKeyboardMarkup(row_width=1)
    date_button = InlineKeyboardButton(text='📆 Изменить дату курса', callback_data='edit:date')
    time_button = InlineKeyboardButton(text='🕒 Изменить время', callback_data='edit:time_menu')
    reset_button = InlineKeyboardButton(text='🛑 Сбросить прогресс курса', callback_data='reset:request')
    menu_button = InlineKeyboardButton(text='⬅️ Главное меню', callback_data='main_menu')
    if is_date:
        keyboard.add(date_button)
    keyboard.add(time_button, reset_button, menu_button)
    return keyboard


def reset_kb():
    accept_button = InlineKeyboardButton(text='🛑 Да, я уверен', callback_data='reset:accept')
    refuse_button = InlineKeyboardButton(text='⬅️ Нет, я передумал', callback_data='main_menu')
    keyboard = InlineKeyboardMarkup(row_width=1).add(accept_button, refuse_button)
    return keyboard


def menu_kb():
    menu_button = InlineKeyboardButton(text='⬅️ Назад в меню', callback_data='main_menu')
    keyboard = InlineKeyboardMarkup(row_width=1).add(menu_button)
    return keyboard


def time_kb():
    time_button = InlineKeyboardButton(text='🕒 Выбрать время для 30-минутной практики', callback_data='edit:time_task')
    keyboard = InlineKeyboardMarkup(row_width=1).add(time_button)
    return keyboard


def remind_meditation_kb(week_id):
    done_button = InlineKeyboardButton(text='✔️ Получилось', callback_data=f'done:{week_id}')
    not_button = InlineKeyboardButton(text='🕒 В следующий раз', callback_data='done:not')
    keyboard = InlineKeyboardMarkup(row_width=1).add(done_button, not_button)
    return keyboard


def get_date_kb():
    date_button = InlineKeyboardButton(text='📆 Выбрать дату начала курса', callback_data='edit:date')
    menu_button = InlineKeyboardButton(text='⬅️ Главное меню', callback_data='main_menu')
    keyboard = InlineKeyboardMarkup(row_width=1).add(date_button, menu_button)
    return keyboard


def practices_kb(week_id):
    keyboard = InlineKeyboardMarkup(row_width=1)
    for i in range(1, week_id):
        week_button = InlineKeyboardButton(text=f'Неделя {i}', callback_data=f'practices:{i}')
        keyboard.add(week_button)
    return keyboard


def support_kb(user_id):
    answer_button = InlineKeyboardButton(text='✉️ Ответить на обращение', callback_data=f'support:{user_id}')
    home_button = InlineKeyboardButton(text='🏠 Домой', callback_data='home')
    keyboard = InlineKeyboardMarkup(row_width=1).add(answer_button, home_button)
    return keyboard


def donate_kb():
    donate_button = InlineKeyboardButton(text='💰 Поддержать проект', callback_data='donate:finish')
    keyboard = InlineKeyboardMarkup(row_width=1).add(donate_button)
    return keyboard
