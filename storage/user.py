import time
import json
import datetime

from storage import get_instance_id
from storage import USERS_REDIS_KEY
from storage import ONLINE_USERS_REDIS_KEY
from storage import MessageStatus
from storage import UserRole
from storage import PUBSUB_SPAM_KEY
from storage import PUBSUB_LOGIN_KEY
from storage import PUBSUB_LOGOUT_KEY
from storage.message_queue import MessageQueue
from storage.message_queue import get_message_queue_redis_key
from storage.message_queue import make_message
from utils import has_permission


def get_all_users(r):
    users = list(map(json.loads, r.hvals(USERS_REDIS_KEY)))
    return [(user["username"], user["user_id"]) for user in users]


def get_online_users(r):
    return set(
        map(lambda u: u.decode("utf-8"), r.smembers(ONLINE_USERS_REDIS_KEY))
    )


def get_all_usernames(r):
    all_users = get_all_users(r)
    return list(map(lambda u: u[0], all_users))


class User:
    def __init__(self, r, username, role=UserRole.user.value):
        self.redis_client = r
        if self.redis_client.hexists(USERS_REDIS_KEY, username):
            print("Good to see you again,", username)
            user_data = json.loads(
                self.redis_client.hget(USERS_REDIS_KEY, username)
            )
        else:
            user_id = get_instance_id(self.redis_client)
            user_data = {
                "user_id": user_id,
                "username": username,
                "role": role,
            }
            self.redis_client.hset(
                USERS_REDIS_KEY, username, json.dumps(user_data)
            )
        self.username = user_data["username"]
        self.role = user_data["role"]

        self.user_id = user_data["user_id"]
        self.created_messages_queue = MessageQueue(
            self.redis_client,
            get_message_queue_redis_key(
                self.username, MessageStatus.created.name
            ),
        )
        self.blocked_messages_queue = MessageQueue(
            self.redis_client,
            get_message_queue_redis_key(
                self.username, MessageStatus.blocked_spam.name
            ),
        )
        self.sent_messages_queue = MessageQueue(
            self.redis_client,
            get_message_queue_redis_key(
                self.username, MessageStatus.sent.name
            ),
        )
        self.online()
        if self.role == UserRole.admin.value:
            self.p = self.redis_client.pubsub()
            self.p.subscribe([PUBSUB_SPAM_KEY, PUBSUB_LOGIN_KEY, PUBSUB_LOGOUT_KEY])

    def online(self):
        self.redis_client.sadd(ONLINE_USERS_REDIS_KEY, self.username)
        self.redis_client.publish(PUBSUB_LOGIN_KEY, self.username)

    def offline(self):
        self.redis_client.srem(ONLINE_USERS_REDIS_KEY, self.username)
        self.redis_client.publish(PUBSUB_LOGOUT_KEY, self.username)

    def send_message(self, message_content, receiver):
        message_id = get_instance_id(self.redis_client)
        created_messages_queue = MessageQueue(
            self.redis_client,
            get_message_queue_redis_key(
                self.username, MessageStatus.created.name
            ),
        )
        message = make_message(
            message_id,
            message_content,
            receiver,
            self.username,
            datetime.datetime.now().isoformat(),
            MessageStatus.created.name,
        )
        created_messages_queue.add_message(message, message_id)

    def get_sent_messages(self):
        match_filter = "user:{username}:*".format(username=self.username)
        messages = []
        for queue in self.redis_client.scan_iter(match=match_filter):
            messages.extend(self.redis_client.zscan(queue.decode("utf-8")))
        return messages

    def get_new_messages(self):
        queue_name = get_message_queue_redis_key(
            self.username, MessageStatus.received.name
        )
        index, messages = self.redis_client.zscan(queue_name)
        return list(map(lambda m: json.loads(m[0]), messages))

    def get_sent_messages_count(self):
        return self.sent_messages_queue.count()

    def get_created_messages_count(self):
        return self.created_messages_queue.count()

    def get_blocked_messages_count(self):
        return self.blocked_messages_queue.count()

    def get_messages_count_grouped_by_status(self):
        return {
            MessageStatus.created.name: {
                "count": self.get_created_messages_count(),
                "messages": self.created_messages_queue.get_all_messages(),
            },
            MessageStatus.blocked_spam.name: {
                "count": self.get_blocked_messages_count(),
                "messages": self.blocked_messages_queue.get_all_messages(),
            },
            MessageStatus.sent.name: {
                "count": self.get_sent_messages_count(),
                "messages": self.sent_messages_queue.get_all_messages(),
            },
        }

    @has_permission
    def get_users_online(self):
        return set(
            map(
                lambda u: u.decode("utf-8"),
                self.redis_client.smembers(ONLINE_USERS_REDIS_KEY),
            )
        )

    @has_permission
    def get_most_common(self, queue_status, n=3):
        users = get_all_users(self.redis_client)
        most_common_senders = sorted(
            users,
            key=lambda user: MessageQueue(
                self.redis_client,
                get_message_queue_redis_key(user[0], queue_status),
            ).count(),
        )
        return list(map(lambda sender: sender[0], most_common_senders))[:n]

    @has_permission
    def get_events_log(self):
        logs = []
        while True:
            log = self.p.get_message()
            if not log:
                break
            logs.append({
                'channel': log['channel'],
                'data': log['data'],
            })
            time.sleep(0.001)
        return logs
