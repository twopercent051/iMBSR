import redis

from create_bot import config


r = redis.Redis(host=config.rds.host, port=config.rds.port, db=config.rds.db)

def redis_start():
    pass
