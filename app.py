import pprint
import threading
from threading import Thread

import redis

from connect import connect
from emulation import Emulation
from storage.user import User
from storage import UserRole
from utils import messages_to_string
from utils import display_users_grouped_messages
from worker.spam_worker import SpamWorker
from storage import MessageStatus

HOST = "127.0.0.1"
pp = pprint.PrettyPrinter(indent=4)
kill_evt = threading.Event()


def worker(kill_evt, conn_pool):
    redis_client = connect(conn_pool)
    spam_worker = SpamWorker(redis_client, kill_evt)
    spam_worker.start()


def main(conn_pool):
    redis_client = connect(conn_pool)
    # redis_client.flushdb()
    init_cmd = input("Ui or emulation?")
    if init_cmd == "ui":
        # redis_client.flushdb()
        text = input("Do you have an account? y/n").strip()
        username = input("Please, enter your username: ").strip()
        if text == "y":
            user = User(redis_client, username)
        else:
            role = int(input("Input 0 for ordinary user or 1 for admin role:"))
            user = User(redis_client, username, role)

        command_help = (
            "Commands: `received`, `send`, " "`sent msgs` or `q` for exit\n"
            if user.role == UserRole.user
            else "Commands: `received`, `send`, `sent msgs`, "
                 "`online users`, `most senders`, `most spamers`, `events log` "
                 "or `q` for exit\n"
        )
        text = input(command_help)
        while len(text) != 0 and text[0] != "q":
            try:
                cmd = text.strip()
                if cmd == "received":
                    print(messages_to_string(user.get_new_messages()))
                if cmd == "send":
                    receiver_username = input("Please, enter receiver username: ")
                    message_content = input("Enter message:")
                    user.send_message(message_content, receiver_username)
                if cmd == "sent msgs":
                    display_users_grouped_messages(
                        user.get_messages_count_grouped_by_status()
                    )
                if user.role == UserRole.admin.value:
                    if cmd == "online users":
                        print(user.get_users_online())
                    if cmd == "most senders":
                        print(user.get_most_common(MessageStatus.sent.name))
                    if cmd == "most spamers":
                        print(
                            user.get_most_common(MessageStatus.blocked_spam.name)
                        )
                    if cmd == "events log":
                        print(user.get_events_log())

            except Exception as e:
                print(e)
            finally:
                text = input(command_help)
        user.offline()
        kill_evt.set()
    else:
        emulation = Emulation(kill_evt, redis_client, 5)
        emulation.emul()


if __name__ == "__main__":
    pool = redis.ConnectionPool(host=HOST, port=6379, db=0)
    worker_thread = Thread(target=worker, args=(kill_evt, pool))
    main_thread = Thread(target=main, args=(pool,))
    main_thread.start()
    worker_thread.start()

    main_thread.join()
    worker_thread.join()
