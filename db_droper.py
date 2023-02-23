import pymysql
from create_bot import config


def connection_init(host, user, password, db_name):
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def deleter():
    host = config.db.host
    user = config.db.user
    password = config.db.password
    db_name = config.db.database
    connection = connection_init(host, user, password, db_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE tests')
            cursor.execute('DROP TABLE users')
            # # cursor.execute('DROP TABLE texts')
            cursor.execute('DROP TABLE practices')
    finally:
        connection.close()


deleter()

