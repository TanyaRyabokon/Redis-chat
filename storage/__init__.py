import random
from enum import Enum

USERS_REDIS_KEY = "USERS_REDIS_KEY"
ONLINE_USERS_REDIS_KEY = "ONLINE_USERS_REDIS_KEY"
PUBSUB_SPAM_KEY = "SPAM"
PUBSUB_LOGIN_KEY = "LOGIN"
PUBSUB_LOGOUT_KEY = 'LOGOUT'


class UserRole(Enum):
    user = 0
    admin = 1


class UserStatus(Enum):
    online = 0
    offline = 1


class MessageStatus(Enum):
    created = 0
    queued = 1
    blocked_spam = 2
    sent = 3
    received = 4


def get_instance_id(r):
    instance_id = random.randint(1, 10 ** 26)
    while r.hexists(USERS_REDIS_KEY, instance_id):
        instance_id = random.randint(1, 10 ** 26)
    return instance_id
