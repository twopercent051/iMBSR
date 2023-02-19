import time
from random import randint

from tgbot.models.sql_connector import *
from tgbot.keyboards.user_inline import *
from tgbot.misc.datetimer import next_step_timer
from create_bot import scheduler, bot


async def tasker(user):
    user_id = user['user_id']
    step = user['next_step_name']
    user_tz = user['timezone']
    day = user['day']
    week_id = int(step.split(':')[0].split('_')[1])
    step_name = step.split(':')[1]
    contain_request = await get_text_sql(week_id)
    contain = None
    if step_name != 'test':
        contain = contain_request[step_name]
    kb = None
    next_step_time = None

    if week_id == 1:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = await next_step_timer(user_tz, 1, randint(12, 18), randint(0, 59))
            await edit_profile_sql(user_id, 'next_step_name', 'week_1:remind_other')
            await edit_profile_sql(user_id, 'week_id', 1)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            next_step_time = await next_step_timer(user_tz, 0, 21, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_1:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, randint(12, 18), randint(0, 59))
                await edit_profile_sql(user_id, 'next_step_name', 'week_1:remind_other')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 1)
                await edit_profile_sql(user_id, 'next_step_name', 'week_1:other')

    if week_id == 2:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = await next_step_timer(user_tz, 1, randint(12, 18), randint(0, 59))
            await edit_profile_sql(user_id, 'next_step_name', 'week_2:remind_other')
            await edit_profile_sql(user_id, 'week_id', 2)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            next_step_time = await next_step_timer(user_tz, 0, 21, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_2:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, randint(12, 18), randint(0, 59))
                await edit_profile_sql(user_id, 'next_step_name', 'week_2:remind_other')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 20)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:task')

    if week_id == 3:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = await next_step_timer(user_tz, 1, 11, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:1')
            await edit_profile_sql(user_id, 'week_id', 3)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            counter = int(step.split(':')[2])
            if counter == 1:
                if day == 2:
                    next_step_time = await next_step_timer(user_tz, 0, 13, 0)
                    await edit_profile_sql(user_id, 'next_step_name', 'week_3:other')
                else:
                    next_step_time = await next_step_timer(user_tz, 0, 15, 0)
                    await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:2')
            if counter == 2:
                next_step_time = await next_step_timer(user_tz, 0, 19, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:3')
            if counter == 3:
                next_step_time = await next_step_timer(user_tz, 0, 21, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, 11, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:1')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 20)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:test')
        if step_name == 'other':
            next_step_time = await next_step_timer(user_tz, 0, 15, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:2')
        if step_name == 'test':
            contain = '💛 Для продолжения курса оцените ваше текущее состояние - пройдите тест 👉'
            kb = user_start_test_kb(3)
            next_step_time = 0

    if week_id == 4:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = next_step_timer(user_tz, 1, 20, 30)
            await edit_profile_sql(user_id, 'next_step_name', 'week_4:remind_other')
            await edit_profile_sql(user_id, 'week_id', 4)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            next_step_time = await next_step_timer(user_tz, 0, 21, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_4:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, 20, 30)
                await edit_profile_sql(user_id, 'next_step_name', 'week_4:remind_other')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 20)
                await edit_profile_sql(user_id, 'next_step_name', 'week_5:task')

    if week_id == 5:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = await next_step_timer(user_tz, 1, randint(12, 18), randint(0, 59))
            await edit_profile_sql(user_id, 'next_step_name', 'week_5:remind_other')
            await edit_profile_sql(user_id, 'week_id', 5)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            next_step_time = await next_step_timer(user_tz, 0, 21, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_5:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, randint(12, 18), randint(0, 59))
                await edit_profile_sql(user_id, 'next_step_name', 'week_5:remind_other')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 1)
                await edit_profile_sql(user_id, 'next_step_name', 'week_6:task')

    if week_id == 6:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = await next_step_timer(user_tz, 1, 11, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_6:remind_other:1')
            await edit_profile_sql(user_id, 'week_id', 6)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            counter = int(step.split(':')[2])
            if counter == 1:
                if day == 2:
                    next_step_time = await next_step_timer(user_tz, 0, 13, 0)
                    await edit_profile_sql(user_id, 'next_step_name', 'week_3:other')
                else:
                    next_step_time = await next_step_timer(user_tz, 0, 15, 0)
                    await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:2')
            if counter == 2:
                next_step_time = await next_step_timer(user_tz, 0, 19, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:3')
            if counter == 3:
                next_step_time = await next_step_timer(user_tz, 0, 21, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, 11, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:1')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 20)
                await edit_profile_sql(user_id, 'next_step_name', 'week_7:task')
        if step_name == 'other':
            next_step_time = await next_step_timer(user_tz, 0, 15, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_3:remind_other:2')

    if week_id == 7:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = next_step_timer(user_tz, 1, 20, 30)
            await edit_profile_sql(user_id, 'next_step_name', 'week_7:remind_other')
            await edit_profile_sql(user_id, 'week_id', 7)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            next_step_time = await next_step_timer(user_tz, 0, 21, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_7:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, 20, 30)
                await edit_profile_sql(user_id, 'next_step_name', 'week_7:remind_other')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 20)
                await edit_profile_sql(user_id, 'next_step_name', 'week_8:task')

    if week_id == 8:
        if step_name == 'task':
            kb = time_kb()
            next_step_time = await next_step_timer(user_tz, 1, 11, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_8:remind_other:1')
            await edit_profile_sql(user_id, 'week_id', 8)
            await edit_profile_sql(user_id, 'day', 1)
        if step_name == 'remind_other':
            counter = int(step.split(':')[2])
            if counter == 1:
                if day == 2:
                    next_step_time = await next_step_timer(user_tz, 0, 13, 0)
                    await edit_profile_sql(user_id, 'next_step_name', 'week_8:other')
                else:
                    next_step_time = await next_step_timer(user_tz, 0, 15, 0)
                    await edit_profile_sql(user_id, 'next_step_name', 'week_8:remind_other:2')
            if counter == 2:
                next_step_time = await next_step_timer(user_tz, 0, 19, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_8:remind_other:3')
            if counter == 3:
                next_step_time = await next_step_timer(user_tz, 0, 21, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_8:remind_daily')
        if step_name == 'remind_daily':
            if day < 7:
                next_step_time = await next_step_timer(user_tz, 1, 11, 0)
                await edit_profile_sql(user_id, 'next_step_name', 'week_8:remind_other:1')
                await edit_profile_sql(user_id, 'day', day + 1)
            else:
                next_step_time = await next_step_timer(user_tz, 0, 21, 20)
                await edit_profile_sql(user_id, 'next_step_name', 'week_8:test')
        if step_name == 'other':
            next_step_time = await next_step_timer(user_tz, 0, 15, 0)
            await edit_profile_sql(user_id, 'next_step_name', 'week_8:remind_other:2')
        if step_name == 'test':
            contain = '💛 Для завершения курса оцените ваше текущее состояние - пройдите тест 👉'
            kb = user_start_test_kb(8)

    if step_name == 'other' and week_id == 1:
        next_step_time = await next_step_timer(user_tz, 0, 21, 20)
        await edit_profile_sql(user_id, 'next_step_name', 'week_2:task')
        await edit_profile_sql(user_id, 'week_id', 2)
        await edit_profile_sql(user_id, 'day', 1)
        await bot.send_video_note(user_id, contain)
    else:
        await bot.send_message(user_id, contain, reply_markup=kb)

    await edit_profile_sql(user_id, 'next_step_time', next_step_time)


async def reminder(user):
    user_id = user['user_id']
    week_id = user['week_id']
    timezone = user['timezone']
    hour, minute = user['remind_hour'], user['remind_min']
    contain_request = await get_text_sql(week_id)
    contain = contain_request['remind_meditation']
    kb = remind_meditation_kb(week_id)
    next_step_time = await next_step_timer(timezone, 1, hour, minute)
    await edit_profile_sql(user_id, 'remind_meditation_time', next_step_time)
    await bot.send_message(user_id, contain, reply_markup=kb)


async def user_scheduler():
    user_list = await get_users_sql()
    for user in user_list:
        if user['next_step_time'] != 0:
            if user['next_step_time'] < time.time():
                await tasker(user)
        if user['remind_meditation_time'] != 0:
            if user['remind_meditation_time'] < time.time():
                await reminder(user)


def scheduler_jobs():
    scheduler.add_job(user_scheduler, "interval", seconds=5, max_instances=3)
