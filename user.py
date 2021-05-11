from kafka import KafkaConsumer
from json import loads
from time import sleep
from json import dumps
from kafka import KafkaProducer
from _thread import *
import json 

import sys
import datetime

import time
import collections

from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
import threading

topic_num = 0
chance = 0
msgid = 1
servercount = 1

def select_server(msgid):
    global servercount
    return msgid%servercount

def check_typeof_receiver(message):
    recv = message.split("_")[1]
    f = open('group.txt','r')
    lines= f.readlines()
    for line in lines:
        if(line.split('-')[0] == recv):
            return line.split('\n')[0].split('-')[1:]
    r_list = []
    r_list.append(recv)
    return r_list

def consumer_t(topic):
    
    global chance, producer, msgid
    consumer = KafkaConsumer(topic,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))


    for message in consumer:
        message = message.value
        print(message)
        
            
user_id=""
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

while(1):
    
    if(topic_num==0):
        print(" User running .. ")
        topic= input()
        user_id=topic
        t1 = threading.Thread(target=consumer_t,args=(topic,))
        t1.start()
        topic_num+=1
    
    else:    
        recv = input("Receiver?  ")
        data = "Hi Yash"
        topic1 = "loadbalancer"
        dict_send={'op_type':"send",'uid1':user_id,'uid2':recv,'msg':data}
        producer.send(topic1, value=dict_send)
        new_msg = "Update yash"
        dict_send2={'op_type':"update",'is_Group':"True",'uid1':user_id,'uid2':recv, 'msgid':1, 'data':new_msg}
        producer.send(topic1, value=dict_send2)
        # sleep(1)