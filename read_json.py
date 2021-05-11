import os, sys
import datetime
import time
import collections
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json 
from json import loads
from time import sleep
from json import dumps
from _thread import *
import threading

file = open('messages/user1_user2.json', 'rb')

data = json.load(file)

print(data)

print(dict(sorted(data.items(), key = lambda kv:(kv[1]['time_stamp'], kv[0]))))

file.close()

data['1234'] = {}
file = open('messages/user1_user2.json', 'wb')
# json.dump(data, file)
with open("messages/user1_user2.json", "w") as outfile:
    json.dump(data, outfile)
file.close()
