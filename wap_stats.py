
from pyspark import SparkContext
import argparse
import wordcloud
from text_cleanner import to_unicode, clean_string

DATE_MESSAGE_SEPARATOR = " - "
USER_MESSAGE_SEPARATOR = ": "
EXCLUDED = ["Media omitted"]
LANGUAGE = "spanish"


def clean_text(text_rdd):
    # removes lines with different format
    rdd = text_rdd.filter(
        lambda line: DATE_MESSAGE_SEPARATOR in line and USER_MESSAGE_SEPARATOR in line.split(DATE_MESSAGE_SEPARATOR)[1]
    )

    # removes lines with excluded terms
    for term in EXCLUDED:
        rdd = rdd.filter(lambda line: term not in line)
        rdd.persist()

    return rdd.map(to_unicode).map(lambda line: line.rstrip())


def create_user_tuple(line):
    user_and_message = line.split(DATE_MESSAGE_SEPARATOR)[1]
    index = user_and_message.find(USER_MESSAGE_SEPARATOR)
    return user_and_message[:index], user_and_message[index + len(USER_MESSAGE_SEPARATOR):]


def process_chat(file_path):
    sc = SparkContext(appName="wap-stats")

    text_rdd = sc.textFile(file_path)
    text_rdd = clean_text(text_rdd)

    user_messages_rdd = text_rdd.map(create_user_tuple).reduceByKey(lambda a,b: a + " " + b)

    return user_messages_rdd.map(lambda t: (t[0], clean_string(t[1], LANGUAGE))).collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze WhatsApp Chats')
    parser.add_argument('-f', '--file', help='Chat log file name', required=True)

    args = parser.parse_args()

    # list of (user, messages)
    user_messages = process_chat(args.file)

    for item in user_messages:
        wc = wordcloud.WordCloud(width=500, height=500).generate(item[1])
        wc.to_file(item[0] + ".png")


    whole_log = ""
    for item in user_messages: whole_log += item[1] + " "

    wc = wordcloud.WordCloud(width=1000, height=1000).generate(whole_log)
    wc.to_file(args.file.split(".")[0] + ".png")



    # fededict = wc.process_text(user_messages[0][1])
    # word_count = [(k, fededict[k]) for k in fededict]
    # print sorted(word_count, cmp=lambda a,b: cmp(b[1], a[1]))[:25]

