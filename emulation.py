import random
import time
from faker import Faker
from threading import Thread

from storage.user import User

fake = Faker()


class Emulation:
    def __init__(self, kill_event, redis_client, length):
        self.redis_client = redis_client
        self.kill_event = kill_event
        self.length = length
        self.usernames = []
        self.users = []
        for i in range(self.length):
            role = random.choice([0, 1])
            username = self._generate_name()
            user = User(self.redis_client, username, role)
            print("User: " + user.username + " logged in to chat.")
            self.users.append(user)

    def _generate_name(self) -> str:
        username = fake.name()
        while username in self.usernames:
            username = fake.name()
        self.usernames.append(username)
        return username

    def emul(self):
        main_thread = Thread(target=self.start)
        main_thread.start()

    def start(self):
        while len(self.users) > 0:
            sender = random.choice(self.users)
            receiver = random.choice(self.users)
            msg = fake.text()
            sender.send_message(msg, receiver.username)
            print("User: " + sender.username + " sent message to User: " + receiver.username)
            print(msg + "\n\n")
            should_log_out = random.choice([True, False])
            if should_log_out:
                sender.offline()
                print("User: " + sender.username + " logged out from chat")
                self.users.remove(sender)
            sleep_time = random.randrange(0, 5)
            time.sleep(sleep_time)
        self.kill_event.set()
