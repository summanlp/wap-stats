
import wordcloud
import matplotlib.pyplot as plt
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
        """:returns: all the messages together as one string"""
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

    def get_word_count(self, words_amount=200, clean=True):
        """:returns: dict with word count of the most words_amount words"""
        split_function = lambda x: clean_text(x[1], LANGUAGE).split() if clean else lambda x: x[1].split()
        return self.get_messages().flatMap(split_function) \
                                    .map(lambda word: (word, 1)) \
                                    .reduceByKey(add) \
                                    .sortBy(lambda x: x[1],ascending=False) \
                                    .zipWithIndex().filter(lambda t: t[1] < words_amount) \
                                    .map(lambda x: (x[0][0],x[0][1])) \
                                    .collectAsMap()

    def get_messages_by_hour(self):
        """
        :returns: dict {hour <int>, amount <int>}
        hour is from 0 to 23
        """
        return self.get_messages().map(lambda t: (t[0].hour, 1)).reduceByKey(add).collectAsMap()

    def get_messages_by_hour_histogram(self, figure_filename="hour_histogram.png"):
        messages_by_hour = self.get_messages_by_hour()
        draw_bar_graph(figure_filename, messages_by_hour.keys(), messages_by_hour.values())

    def get_messages_by_day_of_the_week(self):
        """
        :returns: dict {(day_number, day_name) <tuple>, amount <int>}
        """
        days = {
            0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"
        }

        return self.get_messages().map(lambda t: ((t[0].weekday(), days[t[0].weekday()]), 1))\
                                    .reduceByKey(add)\
                                    .collectAsMap()

    def get_messages_by_day_of_the_week_histogram(self, figure_filename="day_histogram.png"):
        messages_by_days = self.get_messages_by_day_of_the_week()

        sorted_by_day = sorted(messages_by_days.iteritems(), cmp=lambda a, b: cmp(a[0][0], b[0][0]))
        labels = [item[0][1] for item in sorted_by_day]
        values = [item[1] for item in sorted_by_day]

        draw_bar_graph(figure_filename, labels, values)


def draw_bar_graph(figure_filename, labels, values):
    """
    :param figure_filename: to store the image
    :param labels: list of labels
    :param values: list of values
    Pre: len(labels) == len(values)
    """
    plt.clf()
    plt.bar(range(len(labels)), values, align='center')
    plt.xticks(range(len(labels)), labels)
    plt.savefig(figure_filename)


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

    def get_ice_breakers(self, quiet_hours=1):
        """
        Get who is the person that starts conversations (breaks the ice) after quiet_hours of silence.
        :param quiet_hours: amount of hours between messages to say that the user "broke the ice"
        :return: dict of user name and amount of times he/she broke the ice
        """
        ice_breakers = {}
        threshold = quiet_hours * 3600

        last_message = self.messages.take(1)[0]

        for message in self.messages.toLocalIterator():
            time_delta = message[1][0] - last_message[1][0]

            last_message = message

            if time_delta.total_seconds() <= threshold: continue

            user_name = message[0]
            if user_name in ice_breakers:
                ice_breakers[user_name] += 1
            else:
                ice_breakers[user_name] = 1

        return ice_breakers


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
