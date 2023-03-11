import asyncio
import contextvars

import aiomysql

from create_bot import config

connection = contextvars.ContextVar('connection')


async def connection_init():
    host = config.db.host
    user = config.db.user
    password = config.db.password
    db_name = config.db.database
    print('Try connect')
    connection.set(await aiomysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        db=db_name,
        cursorclass=aiomysql.cursors.DictCursor
    ))
    print('connected')
    return connection.get()


async def sql_start():
    # connection.set(await connection_init())
    await connection_init()
    async with connection.get().cursor() as cursor:
        print('In cursor')
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS users(
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
            user_id VARCHAR(40),
            name VARCHAR(50),
            city VARCHAR(50),
            email VARCHAR(40),
            timezone INT,
            expectations TEXT,
            week_id INT DEFAULT 0,
            next_step_time INT DEFAULT 0,
            next_step_name VARCHAR(50),
            start_date INT,
            remind_hour INT,
            remind_min INT,
            day INT,
            remind_meditation_time INT DEFAULT 0
            );
            """)
        print('Table users OK')
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS tests(
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
            user_id VARCHAR(40),
            week_id INT,
            anxiety INT,
            depression INT
            );
            """)
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS practices(
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
            user_id VARCHAR(40),
            week_id INT,
            counter INT DEFAULT 0
            );
            """)
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS texts(
            week_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
            task TEXT,
            remind_meditation TEXT,
            remind_other TEXT,
            remind_daily TEXT,
            other TEXT
            );
            """)
        # for i in range(1, 9):
        #     await cursor.execute(f'INSERT IGNORE INTO texts (week_id) VALUES ({i});')
        await cursor.execute("ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4")
        await cursor.execute("ALTER TABLE tests CONVERT TO CHARACTER SET utf8mb4")
        await cursor.execute("ALTER TABLE practices CONVERT TO CHARACTER SET utf8mb4")
        await cursor.execute("ALTER TABLE texts CONVERT TO CHARACTER SET utf8mb4")
        print('MySQL connected OK')
        await connection.get().commit()
        # connection.close()


async def create_user_sql(user_id, name, city, email, timezone, expectations):
    # connection = await connection_init()
    query = 'INSERT INTO users (user_id, name, city, email, timezone, expectations) VALUES (%s, %s, %s, %s, %s, %s);'
    query_tuple = (user_id, name, city, email, timezone, expectations)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
    await connection.get().commit()
    # connection.close()


async def check_user_sql(user_id):
    # connection = await connection_init()
    query = 'SELECT COUNT(user_id) AS c FROM users WHERE user_id = (%s);'
    query_tuple = (user_id,)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
        result = await cursor.fetchone()
    # connection.close()
    return result


async def get_users_sql():
    # connection = await connection_init()
    query = 'SELECT * FROM users;'
    async with connection.get().cursor() as cursor:
        await cursor.execute(query)
        result = await cursor.fetchall()
    # connection.close()
    return result


async def create_test_result(user_id, week_id, anxiety, depression):
    # connection = await connection_init()
    query = 'INSERT INTO tests (user_id, week_id, anxiety, depression) VALUES (%s, %s, %s, %s);'
    query_tuple = (user_id, week_id, anxiety, depression)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
    await connection.get().commit()
    # connection.close()


async def get_test_result_sql(user_id, week_id):
    # connection = await connection_init()
    query = 'SELECT * FROM tests WHERE user_id = (%s) AND week_id = (%s);'
    query_tuple = (user_id, week_id)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
        result = await cursor.fetchone()
    await connection.get().commit()
    # connection.close()
    return result


async def get_profile_sql(user_id):
    # connection = await connection_init()
    query = 'SELECT * FROM users WHERE user_id = (%s);'
    query_tuple = (user_id,)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
        result = await cursor.fetchone()
    await connection.get().commit()
    # connection.close()
    return result


async def edit_profile_sql(user_id, field, value):
    # connection = await connection_init()
    query = f'UPDATE users SET {field} = (%s) WHERE user_id = (%s);'
    query_tuple = (value, user_id)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
    await connection.get().commit()
    # connection.close()


async def get_practices_sql(user_id, week_id):
    # connection = await connection_init()
    query = 'SELECT counter FROM practices WHERE user_id = (%s) AND week_id = (%s);'
    query_tuple = (user_id, week_id)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
        result = await cursor.fetchone()
    # connection.close()
    return result


async def create_practices_sql(user_id, week_id):
    # connection = await connection_init()
    query = 'INSERT INTO practices (user_id, week_id, counter) VALUES (%s, %s, 1);'
    query_tuple = (user_id, week_id)
    async with connection.cursor() as cursor:
        await cursor.execute(query, query_tuple)
    await connection.get().commit()
    # connection.close()


async def edit_practices_sql(user_id, week_id, counter):
    # connection = await connection_init()
    query = f'UPDATE practices SET counter = (%s) WHERE user_id = (%s) AND week_id = (%s);'
    query_tuple = (counter, user_id, week_id)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
    await connection.get().commit()
    # connection.close()


async def edit_text_sql(week_id, field, value):
    # connection = await connection_init()
    query = f'UPDATE texts SET {field} = (%s) WHERE week_id = (%s);'
    query_tuple = (value, week_id)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
    await connection.get().commit()
    # connection.close()


async def get_text_sql(week_id):
    # connection = await connection_init()
    query = 'SELECT * FROM texts WHERE week_id = (%s);'
    query_tuple = (week_id,)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query, query_tuple)
        result = await cursor.fetchone()
    # connection.close()
    return result


async def reset_user_sql(user_id):
    # connection = await connection_init()
    query_users = """
        UPDATE users SET 
        week_id = 0,
        next_step_time = 0,
        start_date = NULL,
        remind_hour = NULL,
        remind_min = NULL,
        day = 0,
        remind_meditation_time = 0
        WHERE user_id = (%s);
        """
    query_practices = 'DELETE FROM practices WHERE user_id = (%s);'
    query_tests = 'DELETE FROM tests WHERE user_id = (%s);'
    query_tuple = (user_id,)
    async with connection.get().cursor() as cursor:
        await cursor.execute(query_users, query_tuple)
        await cursor.execute(query_practices, query_tuple)
        await cursor.execute(query_tests, query_tuple)
    await connection.get().commit()
    # connection.close()


async def close_sql():
    connection.get().close()
