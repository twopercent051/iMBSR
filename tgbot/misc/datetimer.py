from datetime import datetime, timedelta


async def next_step_timer(user_tz: int, days_offset: int, tm_hours: int, tm_minutes: int):
    utc_offset = 3 + user_tz
    user_timestamp = datetime.utcnow().timestamp() + (3600 * utc_offset)
    user_date = datetime.fromtimestamp(user_timestamp)
    user_next_day = user_date + timedelta(days=days_offset)
    user_next_day_mod = user_next_day.replace(hour=tm_hours, minute=tm_minutes).timestamp()
    utc_result = user_next_day_mod - (user_tz + 3) * 3600
    return int(utc_result)
