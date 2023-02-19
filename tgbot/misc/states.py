from aiogram.dispatcher.filters.state import State, StatesGroup


class FSMUser(StatesGroup):
    home = State()
    name = State()
    city = State()
    email = State()
    timezone = State()
    expectations = State()
    testing = State()
    new_date = State()
    menu = State()
    edit_name = State()
    edit_city = State()
    edit_email = State()
    edit_timezone = State()
    edit_expectations = State()
    edit_time_task = State()
    edit_time_menu = State()
    support = State()
    feedback = State()


class FSMAdmin(StatesGroup):
    home = State()
    mailing = State()
    edit = State()
    support = State()
