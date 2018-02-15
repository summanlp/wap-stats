
from pyspark import SparkContext
import argparse
import re
from datetime import datetime
from Log import ChatLog
from text_cleanner import to_unicode
from export import gexf_export_from_graph
from user_comparator import get_users_similarity

ANDROID = "android"
IOS = "ios"

DATE_MESSAGE_SEPARATOR = " - "
USER_MESSAGE_SEPARATOR = ": "

ANDROID_DATETIME_FORMAT_EN = "%m/%d/%y, %H:%M"
ANDROID_DATETIME_FORMAT_ES = "%d/%m/%y, %H:%M"
IOS_DATETIME_FORMAT_ES = "%d/%m/%y %H:%M:%S"
IOS_DATETIME_FORMAT_EN = "%d/%m/%y %H:%M:%S"

SPANISH = "es"
ENGLISH = "en"

EXCLUDED = ["Media omitted", "http", "omitido", "omitida"]


date_format = ANDROID_DATETIME_FORMAT_ES


def remove_invalid_lines(text_rdd, device_type):
    if device_type != ANDROID:
        text_rdd = text_rdd.map(lambda line: line.replace(": ", DATE_MESSAGE_SEPARATOR, 1))

    # removes lines with invalid format
    valid_line = re.compile("\A\d+/\d+/\d+,? \d+:\d+(:\d+)?\s-\s.*: .*")
    rdd = text_rdd.filter(lambda line: valid_line.match(line) is not None)

    # removes lines with excluded terms
    for term in EXCLUDED:
        rdd = rdd.filter(lambda line: term not in line)
        rdd.persist()

    return rdd.map(to_unicode).map(lambda line: line.rstrip())


def create_user_tuple(line, device_type):
    """ returns a tuple of (user_name <str>, datetime <datetime>, message <str>)"""
    date_end_index = line.find(DATE_MESSAGE_SEPARATOR)
    date = line[:date_end_index]
    name_end_index = line.find(USER_MESSAGE_SEPARATOR)
    name = line[date_end_index + len(DATE_MESSAGE_SEPARATOR):name_end_index]
    message = line[name_end_index + len(USER_MESSAGE_SEPARATOR):]

    return name, (datetime.strptime(date, date_format), message)


def process_chat(file_path, device_type):
    sc = SparkContext(appName="wap-stats")

    text_rdd = sc.textFile(file_path)
    text_rdd = remove_invalid_lines(text_rdd, device_type)

    return ChatLog(file_path, text_rdd.map(lambda t: create_user_tuple(t, device_type)))


def word_reporting(users_data):

    uwc = [(user.name, user.count_messages(), user.count_words(), user.count_words(clean=True)) for user in users_data]
    uwc = sorted(uwc, cmp=lambda a,b: cmp(b[1], a[1]))

    f = open("report.csv", "w")

    header = "user,messages,words,word message ratio,words (without stopwords),word message ratio\n"
    messages_acum, words_acum, words_nostop_acum = 0, 0, 0
    print(header)
    f.write(header)
    for t in uwc:
        word_message_ratio = t[2] / float(t[1])
        word_nostop_message_ratio = t[3] / float(t[1])
        s = "{},{},{},{a:.2f},{c},{b:.2f}\n".format(t[0], t[1], t[2], a=word_message_ratio, c=t[3], b=word_nostop_message_ratio)
        print(s)
        f.write(s)

        messages_acum += t[1]
        words_acum += t[2]
        words_nostop_acum += t[3]

    s = "Total,{},{},{a:.2f},{c},{b:.2f}\n".format(messages_acum, words_acum, a=words_acum / float(messages_acum),
                                                   c=words_nostop_acum, b=words_nostop_acum / float(messages_acum))
    print(s)
    f.write(s)

    f.close()


def word_count(chat_log):
    word_amount = 50
    wc = chat_log.get_word_count(words_amount=word_amount)
    swc = sorted([(k, v) for k, v in wc.iteritems()], cmp=lambda a, b: cmp(b[1], a[1]))

    f = open("most_common_words.csv", "w")

    print "{} Most common words".format(word_amount)

    for k, v in swc:
        s = "{},{}\n".format(v, k.encode("utf-8"))
        print s
        f.write(s)

    f.close()


def graph_export(chat_log):
    graph = chat_log.get_response_graph()
    gexf_export_from_graph(graph, path="views/test.gexf")


def set_date_format(device, language):
    key_generator = lambda a, b: a + "_" + b
    formats = {
        key_generator(ANDROID, SPANISH): ANDROID_DATETIME_FORMAT_ES,
        key_generator(ANDROID, ENGLISH): ANDROID_DATETIME_FORMAT_EN,
        key_generator(IOS, SPANISH): IOS_DATETIME_FORMAT_ES,
        key_generator(IOS, ENGLISH): IOS_DATETIME_FORMAT_EN
    }

    global date_format
    date_format = formats[key_generator(device, language)]


def print_dict(title, dict):
    sorted_by_value = sorted(dict.iteritems(), cmp=lambda x,y: cmp(y[1], x[1]))
    print title
    for k,v in sorted_by_value:
        print "{} - {}".format(v, k.encode('utf-8'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze WhatsApp Chats')
    parser.add_argument('-f', '--file', help='Chat log file name', required=True)
    parser.add_argument('-d', '--device', help='Device where the chat was taken:', choices=[ANDROID,IOS], required=True)
    parser.add_argument('-l', '--language', help='Language of the phone:', choices=[ENGLISH, SPANISH])

    args = parser.parse_args()

    set_date_format(args.device, args.language)

    chat_log = process_chat(args.file, args.device)

    matrix = get_users_similarity(chat_log.get_users_data())

    for user, row in matrix.iteritems():
        minimum = min(row.items(), key=lambda x: x[1][0])
        most_similar = minimum[0]
        data = minimum[1]
        print "Most similar of {} is {}: in common: {}, different: {}, distance: {}"\
            .format(user, most_similar, data[1], data[2], data[0])

    # trending_topics = chat_log.get_trending_topics()
    #
    # for t in trending_topics:
    #     print "\nWeek {}".format(t[0])
    #     print [k for k in t[1]]

    # print_dict("After 3 hour:", chat_log.get_ice_breakers(3))

    # word_reporting(chat_log.get_users_data())
    #
    # word_count(chat_log)
    #
    # chat_log.export_word_cloud()
    #
    # graph_export(chat_log)
    # chat_log.get_messages_by_hour_histogram(args.file + "_hour_histogram.png")
    # chat_log.get_messages_by_day_of_the_week_histogram(args.file + "_days_histogram.png")
