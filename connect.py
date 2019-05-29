import redis
import time


def log_info(tag, attempt, sleep=None):
    if sleep:
        print(
            "{tag} attempt: {attempt} sleep: {sleep}".format(
                tag=tag, attempt=attempt, sleep=sleep
            )
        )
    else:
        print("{tag} attempt: {attempt}".format(tag=tag, attempt=attempt))


def connect(pool):
    max_iter = 5
    i = 0
    s = 0

    while i < max_iter:
        try:
            conn = redis.StrictRedis(connection_pool=pool)
            return conn
        except redis.exceptions.ConnectionError:
            s = s ** 2 or 2
            log_info(tag="redis.unavailable", attempt=i, sleep=s)
            time.sleep(s)

    log_info(tag="redis.connect.error", attempt=i)
