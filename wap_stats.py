
from pyspark import SparkContext
import argparse
import re
import wordcloud
import UserData
from text_cleanner import to_unicode

ANDROID = "android"
IOS = "ios"

DATE_MESSAGE_SEPARATOR = " - "
USER_MESSAGE_SEPARATOR = ": "

EXCLUDED = ["Media omitted", "http", "omitido", "omitida"]


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


def create_user_tuple(line):
    """ returns a tuple of (user_name, timestamp, message)"""
    date_end_index = line.find(DATE_MESSAGE_SEPARATOR)
    date = line[:date_end_index]
    name_end_index = line.find(USER_MESSAGE_SEPARATOR)
    name = line[date_end_index + len(DATE_MESSAGE_SEPARATOR):name_end_index]
    message = line[name_end_index + len(USER_MESSAGE_SEPARATOR):]
    return (name, (date, message))


def process_chat(file_path, device_type):
    sc = SparkContext(appName="wap-stats")

    text_rdd = sc.textFile(file_path)
    text_rdd = remove_invalid_lines(text_rdd, device_type)

    user_messages_rdd = text_rdd.map(create_user_tuple)

    return user_messages_rdd.groupByKey().mapValues(list).collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze WhatsApp Chats')
    parser.add_argument('-f', '--file', help='Chat log file name', required=True)
    parser.add_argument('-d', '--device', help='Device where the chat was taken:', choices=[ANDROID,IOS], required=True)

    args = parser.parse_args()

    # list of [(user_name, [(timestamp, msg), (timestamp, msg), ...],
    #           (user_name, [(timestamp, msg), (timestamp, msg), ...]]
    users_messages = process_chat(args.file, args.device)

    users = [UserData(name, messages) for name, messages in users_messages]

    print users[0]



    # for item in users:
    #     wc = wordcloud.WordCloud(width=500, height=500).generate(item[1])
    #     wc.to_file(item[0] + ".png")
    #
    # whole_log = ""
    # for item in users: whole_log += item[1] + " "
    #
    # wc = wordcloud.WordCloud(max_words=50,width=1000, height=1000).generate(whole_log)
    # wc.to_file(args.file.split(".")[0] + ".png")



    # fededict = wc.process_text(user_messages[0][1])
    # word_count = [(k, fededict[k]) for k in fededict]
    # print sorted(word_count, cmp=lambda a,b: cmp(b[1], a[1]))[:25]

