
from pyspark import SparkContext
import argparse
import re
from datetime import datetime
from Log import ChatLog
from text_cleanner import to_unicode

ANDROID = "android"
IOS = "ios"

DATE_MESSAGE_SEPARATOR = " - "
USER_MESSAGE_SEPARATOR = ": "

ANDROID_DATETIME_FORMAT = "%m/%d/%y, %H:%M"
IOS_DATETIME_FORMAT = "%d/%m/%y %H:%M:%S"

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


def create_user_tuple(line, device_type):
    """ returns a tuple of (user_name <str>, datetime <datetime>, message <str>)"""
    date_end_index = line.find(DATE_MESSAGE_SEPARATOR)
    date = line[:date_end_index]
    name_end_index = line.find(USER_MESSAGE_SEPARATOR)
    name = line[date_end_index + len(DATE_MESSAGE_SEPARATOR):name_end_index]
    message = line[name_end_index + len(USER_MESSAGE_SEPARATOR):]

    if device_type == IOS:
        return name, (datetime.strptime(date, IOS_DATETIME_FORMAT), message)
    return name, (datetime.strptime(date, ANDROID_DATETIME_FORMAT), message)


def process_chat(file_path, device_type):
    sc = SparkContext(appName="wap-stats")

    text_rdd = sc.textFile(file_path)
    text_rdd = remove_invalid_lines(text_rdd, device_type)

    return ChatLog(file_path, text_rdd.map(lambda t: create_user_tuple(t, device_type)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze WhatsApp Chats')
    parser.add_argument('-f', '--file', help='Chat log file name', required=True)
    parser.add_argument('-d', '--device', help='Device where the chat was taken:', choices=[ANDROID,IOS], required=True)

    args = parser.parse_args()

    chat_log = process_chat(args.file, args.device)

    users_data = chat_log.get_users_data()

    print "Total palabras: " + str(chat_log.count_words())

    uwc = [(user.name, user.count_messages(), user.count_words(), user.count_words(clean=True)) for user in users_data]
    uwc = sorted(uwc, cmp=lambda a,b: cmp(b[1], a[1]))

    f = open("data.csv","w")

    header = "user,messages,words,word message ratio,words (without stopwords),word message ratio\n"
    print header
    f.write(header)
    for t in uwc:
        word_message_ratio = t[2] / float(t[1])
        word_nostop_message_ratio = t[3] / float(t[1])
        s = "{},{},{},{a:.2f},{c},{b:.2f}\n".format(t[0], t[1], t[2], a=word_message_ratio, c=t[3], b=word_nostop_message_ratio)
        print s
        f.write(s)

    f.close()



