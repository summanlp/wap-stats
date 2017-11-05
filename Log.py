
import wordcloud
from abc import ABCMeta, abstractmethod
from operator import add
from text_cleanner import clean_text
from graph import Graph

LANGUAGE = "spanish"

CHAT_WORDCLOUD_WIDTH = 1000
CHAT_WORDCLOUD_HEIGHT = 1000

USER_WORDCLOUD_WIDTH = 500
USER_WORDCLOUD_HEIGHT = 500


class Log:
    __metaclass__ = ABCMeta

    def __init__(self, name, messages):
        self.name = name.encode('utf-8')
        self.messages = messages

    def export_word_cloud(self, width, height):
        log_as_string = clean_text(self.to_string(), LANGUAGE)
        wc = wordcloud.WordCloud(width=width, height=height).generate(log_as_string)
        wc.to_file(self.name + ".png")

    @abstractmethod
    def to_string(self):
        return ""

    @abstractmethod
    def get_messages(self):
        """ :return: rdd of (datetime <datetime>, message <str>) without user information """
        return None

    def count_messages(self):
        return self.messages.count()

    def count_words(self, clean=False):
        """Clean flags set stopwords, digits and punctuation removal"""
        if clean:
            return self.get_messages().map(lambda t: len(clean_text(t[1], LANGUAGE).split())).reduce(add)
        return self.get_messages().map(lambda t: len(t[1].split())).reduce(add)



class ChatLog(Log):
    """
    name: name of the conversation
    messages: rdd with the log of a WhatsApp Chat.
    Rdd: [(user_name <str>, (datetime <datetime>, message <str>),
        (user_name <str>, (datetime <datetime>, message <str>)
        ]
    """
    def get_users_data(self):
        user_datas = []
        for user in self.get_users_names():
            user_log = self.messages.filter(lambda t: t[0] == user).map(lambda t: t[1])
            user_log.persist()
            user_datas.append(UserLog(user, user_log))
        return user_datas

    def export_word_cloud(self):
        super(ChatLog, self).export_word_cloud(CHAT_WORDCLOUD_WIDTH, CHAT_WORDCLOUD_HEIGHT)

    def to_string(self):
        """Combines the whole log in one string"""
        return self.messages.map(lambda t: t[1][1]).reduce(lambda a, b: a + " " + b)

    def get_messages(self):
        return self.messages.map(lambda t: t[1])

    def get_users_names(self):
        return self.messages.keys().distinct().collect()

    def get_response_graph(self):
        graph = build_graph(self.get_users_names())

        last_speaker = self.messages.take(1)[0][0]

        increment = 1

        for message in self.messages.toLocalIterator():
            current_speaker = message[0]
            if current_speaker == last_speaker: continue

            edge = last_speaker, current_speaker
            if graph.has_edge(edge):
                weight = graph.get_edge_properties(edge)[graph.WEIGHT_ATTRIBUTE_NAME]
                graph.set_edge_properties(edge, weight=weight+increment)
            else:
                graph.add_edge(edge, wt=increment)

            last_speaker = current_speaker

        return graph


def build_graph(users):
    graph = Graph()
    for user in users:
        graph.add_node(user)
    return graph



class UserLog(Log):
    """
    Stores all the messages of a given user.

    :param name: name of the given user
    :param messages: rdd: [(datetime <datetime>, message <str>),
                            (datetime <datetime>, message <str>)
                        ]

    """
    def export_word_cloud(self):
        super(UserLog, self).export_word_cloud(USER_WORDCLOUD_WIDTH, USER_WORDCLOUD_HEIGHT)

    def to_string(self):
        return self.messages.map(lambda t: t[1]).reduce(lambda a, b: a + " " + b)

    def get_messages(self):
        return self.messages
