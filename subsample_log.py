#!/usr/bin/env python
# -*- coding: utf-8 -*-

from wap_stats import process_chat, set_date_format

DEVICE = "ios"
LANGUAGE = "es"
CHAT_FILE = "chat.txt"

# Example: replace User A for real user name
TARGETS = {"User A", "User B"}


set_date_format(DEVICE, LANGUAGE)

chat_log = process_chat(CHAT_FILE, DEVICE)

rdd = chat_log.messages.filter(lambda t: t[0] in TARGETS)\
                        .map(lambda t: ":".join([t[0], t[1][1]]))\
                        .saveAsTextFile("subsample.txt")

