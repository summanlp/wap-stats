

def create_messages(messages):
    pass


class UserData:

    def __init__(self, name, messages):
        self.name = name
        self.messages = create_messages(messages)
