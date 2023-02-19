from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def question_1_kb():
    button_1 = InlineKeyboardButton(text='всё время', callback_data='question_1:3')
    button_2 = InlineKeyboardButton(text='часто', callback_data='question_1:2')
    button_3 = InlineKeyboardButton(text='время от времени, иногда', callback_data='question_1:1')
    button_4 = InlineKeyboardButton(text='совсем не испытываю', callback_data='question_1:0')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4)
    return keyboard


def question_2_kb():
    button_1 = InlineKeyboardButton(text='определенно это так', callback_data='question_2:0')
    button_2 = InlineKeyboardButton(text='наверное, это так', callback_data='question_2:1')
    button_3 = InlineKeyboardButton(text='лишь в очень малой степени это так', callback_data='question_2:2')
    button_4 = InlineKeyboardButton(text='это совсем не так', callback_data='question_2:3')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:2')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_3_kb():
    button_1 = InlineKeyboardButton(text='определенно это так, и страх очень сильный', callback_data='question_3:3')
    button_2 = InlineKeyboardButton(text='да, это так, но страх не очень сильный', callback_data='question_3:2')
    button_3 = InlineKeyboardButton(text='иногда, но это меня не беспокоит', callback_data='question_3:1')
    button_4 = InlineKeyboardButton(text='совсем не испытываю', callback_data='question_3:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:3')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_4_kb():
    button_1 = InlineKeyboardButton(text='определенно, это так', callback_data='question_4:0')
    button_2 = InlineKeyboardButton(text='наверное, это так', callback_data='question_4:1')
    button_3 = InlineKeyboardButton(text='лишь в очень малой степени это так', callback_data='question_4:2')
    button_4 = InlineKeyboardButton(text='совсем не способен', callback_data='question_4:3')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:4')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_5_kb():
    button_1 = InlineKeyboardButton(text='постоянно', callback_data='question_5:3')
    button_2 = InlineKeyboardButton(text='большую часть времени', callback_data='question_5:2')
    button_3 = InlineKeyboardButton(text='время от времени', callback_data='question_5:1')
    button_4 = InlineKeyboardButton(text='только иногда', callback_data='question_5:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:5')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_6_kb():
    button_1 = InlineKeyboardButton(text='совсем не чувствую', callback_data='question_6:3')
    button_2 = InlineKeyboardButton(text='очень редко', callback_data='question_6:2')
    button_3 = InlineKeyboardButton(text='иногда', callback_data='question_6:1')
    button_4 = InlineKeyboardButton(text='практически все время', callback_data='question_6:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:6')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_7_kb():
    button_1 = InlineKeyboardButton(text='определенно, это так', callback_data='question_7:0')
    button_2 = InlineKeyboardButton(text='наверное, это так', callback_data='question_7:1')
    button_3 = InlineKeyboardButton(text='лишь изредка это так', callback_data='question_7:2')
    button_4 = InlineKeyboardButton(text='совсем не могу', callback_data='question_7:3')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:7')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_8_kb():
    button_1 = InlineKeyboardButton(text='практически все время', callback_data='question_8:3')
    button_2 = InlineKeyboardButton(text='часто', callback_data='question_8:2')
    button_3 = InlineKeyboardButton(text='иногда', callback_data='question_8:1')
    button_4 = InlineKeyboardButton(text='совсем нет', callback_data='question_8:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:8')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_9_kb():
    button_1 = InlineKeyboardButton(text='совсем не испытываю', callback_data='question_9:0')
    button_2 = InlineKeyboardButton(text='иногда', callback_data='question_9:1')
    button_3 = InlineKeyboardButton(text='часто', callback_data='question_9:2')
    button_4 = InlineKeyboardButton(text='очень часто', callback_data='question_9:3')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:9')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_10_kb():
    button_1 = InlineKeyboardButton(text='определенно это так', callback_data='question_10:3')
    button_2 = InlineKeyboardButton(text='я не уделяю этому столько времени, сколько нужно',
                                    callback_data='question_10:2')
    button_3 = InlineKeyboardButton(text='может быть, я стал меньше уделять этому внимания',
                                    callback_data='question_10:1')
    button_4 = InlineKeyboardButton(text='я слежу за собой так же, как и раньше', callback_data='question_10:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:10')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_11_kb():
    button_1 = InlineKeyboardButton(text='определенно, это так', callback_data='question_11:3')
    button_2 = InlineKeyboardButton(text='наверное, это так', callback_data='question_11:2')
    button_3 = InlineKeyboardButton(text='лишь в очень малой степени это так', callback_data='question_11:1')
    button_4 = InlineKeyboardButton(text='совсем не так', callback_data='question_11:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:11')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_12_kb():
    button_1 = InlineKeyboardButton(text='точно так, как и обычно', callback_data='question_12:0')
    button_2 = InlineKeyboardButton(text='да, но не в той степени, как раньше', callback_data='question_12:1')
    button_3 = InlineKeyboardButton(text='значительно меньше, чем раньше', callback_data='question_12:2')
    button_4 = InlineKeyboardButton(text='совсем не считаю', callback_data='question_12:3')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:12')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_13_kb():
    button_1 = InlineKeyboardButton(text='действительно, очень часто', callback_data='question_13:3')
    button_2 = InlineKeyboardButton(text='довольно часто', callback_data='question_13:2')
    button_3 = InlineKeyboardButton(text='не так уж часто', callback_data='question_13:1')
    button_4 = InlineKeyboardButton(text='совсем не бывает', callback_data='question_13:0')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:13')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def question_14_kb():
    button_1 = InlineKeyboardButton(text='часто', callback_data='question_14:0')
    button_2 = InlineKeyboardButton(text='иногда', callback_data='question_14:1')
    button_3 = InlineKeyboardButton(text='редко', callback_data='question_14:2')
    button_4 = InlineKeyboardButton(text='очень редко', callback_data='question_14:3')
    back_button = InlineKeyboardButton(text='⬅️ назад', callback_data='back:14')
    keyboard = InlineKeyboardMarkup(row_width=1).add(button_1, button_2, button_3, button_4, back_button)
    return keyboard


def test_keyboard(question_id):
    if question_id == 1:
        return question_1_kb()
    if question_id == 2:
        return question_2_kb()
    if question_id == 3:
        return question_3_kb()
    if question_id == 4:
        return question_4_kb()
    if question_id == 5:
        return question_5_kb()
    if question_id == 6:
        return question_6_kb()
    if question_id == 7:
        return question_7_kb()
    if question_id == 8:
        return question_8_kb()
    if question_id == 9:
        return question_9_kb()
    if question_id == 10:
        return question_10_kb()
    if question_id == 11:
        return question_11_kb()
    if question_id == 12:
        return question_12_kb()
    if question_id == 13:
        return question_13_kb()
    if question_id == 14:
        return question_14_kb()


def finish_test_kb(week_id):
    if week_id == 0:
        date_button = InlineKeyboardButton(text='📆 Выбрать дату начала курса', callback_data='edit:date')
        menu_button = InlineKeyboardButton(text='⬅️ Назад в меню', callback_data='main_menu')
        keyboard = InlineKeyboardMarkup(row_width=1).add(date_button, menu_button)
    else:
        menu_button = InlineKeyboardButton(text='⬅️ Назад в меню', callback_data='main_menu')
        keyboard = InlineKeyboardMarkup(row_width=1).add(menu_button)
    return keyboard
