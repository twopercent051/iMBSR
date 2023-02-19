import asyncio

import pymysql

from create_bot import config


def connection_init():
    host = config.db.host
    user = config.db.user
    password = config.db.password
    db_name = config.db.database
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def sql_start():
    connection = connection_init()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
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
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tests(
                id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
                user_id VARCHAR(40),
                week_id INT,
                anxiety INT,
                depression INT
                );
                """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS practices(
                id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
                user_id VARCHAR(40),
                week_id INT,
                counter INT DEFAULT 0
                );
                """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS texts(
                week_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
                task TEXT,
                remind_meditation TEXT,
                remind_other TEXT,
                remind_daily TEXT,
                other TEXT
                );
                """)
            for i in range(1, 9):
                cursor.execute(f'INSERT IGNORE INTO texts (week_id) VALUES ({i});')
            cursor.execute("ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4")
            cursor.execute("ALTER TABLE tests CONVERT TO CHARACTER SET utf8mb4")
            cursor.execute("ALTER TABLE practices CONVERT TO CHARACTER SET utf8mb4")
            cursor.execute("ALTER TABLE texts CONVERT TO CHARACTER SET utf8mb4")
        print('MySQL connected OK')
    finally:
        connection.commit()
        connection.close()


async def create_user_sql(user_id, name, city, email, timezone, expectations):
    connection = connection_init()
    query = 'INSERT INTO users (user_id, name, city, email, timezone, expectations) VALUES (%s, %s, %s, %s, %s, %s);'
    query_tuple = (user_id, name, city, email, timezone, expectations)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
    finally:
        connection.commit()
        connection.close()


async def check_user_sql(user_id):
    connection = connection_init()
    query = 'SELECT COUNT(user_id) AS c FROM users WHERE user_id = (%s);'
    query_tuple = (user_id,)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
            result = cursor.fetchone()
    finally:
        connection.commit()
        connection.close()
        return result


async def get_users_sql():
    connection = connection_init()
    query = 'SELECT * FROM users;'
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.commit()
        connection.close()
        return result


async def create_test_result(user_id, week_id, anxiety, depression):
    connection = connection_init()
    query = 'INSERT INTO tests (user_id, week_id, anxiety, depression) VALUES (%s, %s, %s, %s);'
    query_tuple = (user_id, week_id, anxiety, depression)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
    finally:
        connection.commit()
        connection.close()


async def get_test_result_sql(user_id, week_id):
    connection = connection_init()
    query = 'SELECT * FROM tests WHERE user_id = (%s) AND week_id = (%s);'
    query_tuple = (user_id, week_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
            result = cursor.fetchone()
    finally:
        connection.commit()
        connection.close()
        return result


async def get_profile_sql(user_id):
    connection = connection_init()
    query = 'SELECT * FROM users WHERE user_id = (%s);'
    query_tuple = (user_id,)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
            result = cursor.fetchone()
    finally:
        connection.commit()
        connection.close()
        return result


async def edit_profile_sql(user_id, field, value):
    connection = connection_init()
    query = f'UPDATE users SET {field} = (%s) WHERE user_id = (%s);'
    query_tuple = (value, user_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
    finally:
        connection.commit()
        connection.close()


async def get_practices_sql(user_id, week_id):
    connection = connection_init()
    query = 'SELECT counter FROM practices WHERE user_id = (%s) AND week_id = (%s);'
    query_tuple = (user_id, week_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
            result = cursor.fetchone()
    finally:
        connection.commit()
        connection.close()
        return result


async def create_practices_sql(user_id, week_id):
    connection = connection_init()
    query = 'INSERT INTO practices (user_id, week_id, counter) VALUES (%s, %s, 1);'
    query_tuple = (user_id, week_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
    finally:
        connection.commit()
        connection.close()


async def edit_practices_sql(user_id, week_id, counter):
    connection = connection_init()
    query = f'UPDATE practices SET counter = (%s) WHERE user_id = (%s) AND week_id = (%s);'
    query_tuple = (counter, user_id, week_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
    finally:
        connection.commit()
        connection.close()


async def edit_text_sql(week_id, field, value):
    connection = connection_init()
    query = f'UPDATE texts SET {field} = (%s) WHERE week_id = (%s);'
    query_tuple = (value, week_id)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
    finally:
        connection.commit()
        connection.close()


async def get_text_sql(week_id):
    connection = connection_init()
    query = 'SELECT * FROM texts WHERE week_id = (%s);'
    query_tuple = (week_id,)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, query_tuple)
            result = cursor.fetchone()
    finally:
        connection.close()
        return result


async def reset_user_sql(user_id):
    connection = connection_init()
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
    try:
        with connection.cursor() as cursor:
            cursor.execute(query_users, query_tuple)
            cursor.execute(query_practices, query_tuple)
            cursor.execute(query_tests, query_tuple)
    finally:
        connection.commit()
        connection.close()

