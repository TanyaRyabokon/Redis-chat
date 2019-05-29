from functools import wraps

from storage import UserRole


def messages_to_string(messages):
    messages_string = ""
    if not messages:
        return "No messages"
    for message in messages:
        messages_string += "From {}:\n{}\nDate: {}\n\n".format(
            message["sender"], message["content"], message["date"]
        )
    return messages_string


def display_users_grouped_messages(msgs_counts):
    for key, value in msgs_counts.items():
        print(
            "{}: {} messages\n{}\n".format(
                key, value["count"], messages_to_string(value["messages"])
            )
        )


def has_permission(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if args[0].role != UserRole.admin.value:
            raise Exception("User has not enough permissions.")
        return f(*args, **kwargs)

    return wrapper
