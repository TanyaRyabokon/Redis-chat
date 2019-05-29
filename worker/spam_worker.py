import time
import json
import random

from storage.user import get_all_usernames
from storage import MessageStatus
from storage import PUBSUB_SPAM_KEY
from storage.message_queue import MessageQueue
from storage.message_queue import get_message_queue_redis_key


class SpamWorker:
    def __init__(self, redis_client, kill_evt):
        self.redis_client = redis_client
        self.kill_evt = kill_evt
        self.p = redis_client.pubsub()

    def get_messages_queue_name_from_user(self, username, status):
        match_filter = "user:{username}:{status}".format(
            username=username, status=status
        )
        index, queue = self.redis_client.scan(match=match_filter)
        if not len(queue):
            return None
        return queue[0].decode("utf-8")

    def check_messages_on_spam(self, username, queue):
        blocked_messages_queue = MessageQueue(
            self.redis_client,
            get_message_queue_redis_key(
                username, MessageStatus.blocked_spam.name
            ),
        )
        sent_messages_queue = MessageQueue(
            self.redis_client,
            get_message_queue_redis_key(username, MessageStatus.sent.name),
        )
        while queue.count():
            message, score = queue.popmax()
            is_spam = random.choice([True, False])
            if is_spam:
                message["status"] = MessageStatus.blocked_spam.name
                blocked_messages_queue.add_message(message, score=score)
                self.redis_client.publish(PUBSUB_SPAM_KEY, json.dumps(message))
            else:
                message["status"] = MessageStatus.sent.name
                sent_messages_queue.add_message(message, score=score)
                receiver = message["receiver"]
                receive_messages_queue = MessageQueue(
                    self.redis_client,
                    get_message_queue_redis_key(
                        receiver, MessageStatus.received.name
                    ),
                )
                receive_messages_queue.add_message(message, score=score)

    def start(self):
        all_usernames = get_all_usernames(self.redis_client)
        print('\nRegistered users: ', all_usernames)
        while not self.kill_evt.is_set():
            for username in all_usernames:
                created_messages_queue = MessageQueue(
                    self.redis_client,
                    get_message_queue_redis_key(
                        username, MessageStatus.created.name
                    ),
                )
                if not created_messages_queue.count():
                    continue
                self.check_messages_on_spam(username, created_messages_queue)
            time.sleep(5)
