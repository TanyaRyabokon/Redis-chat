import json
import time
from storage import get_instance_id
from datetime import timedelta


def get_message_queue_redis_key(username, status):
    return "user:{username}:{status}".format(username=username, status=status)


def make_message(message_id, content, receiver, sender, date, status):
    return {
        "message_id": message_id,
        "content": content,
        "receiver": receiver,
        "sender": sender,
        "date": date,
        "status": status,
    }


class MessageQueue:
    def __init__(self, redis_client, message_redis_key):
        self.redis_client = redis_client
        self.message_redis_key = message_redis_key

    def add_message(self, message, message_id=None, score=None):
        if not message_id:
            message_id = get_instance_id(self.redis_client)
        if not score:
            score = int(time.time() * 1000)
        self.redis_client.zadd(
            self.message_redis_key, {json.dumps(message): score}
        )
        return message_id

    def get_all_messages(self):
        index, messages = self.redis_client.zscan(self.message_redis_key)
        return list(map(lambda m: json.loads(m[0]), messages))

    def popmax(self):
        message_bytes, score = self.redis_client.zpopmax(
            self.message_redis_key
        )[0]
        return json.loads(message_bytes), score

    def count(self):
        return self.redis_client.zcount(
            self.message_redis_key, 0, timedelta.max.total_seconds()
        )
