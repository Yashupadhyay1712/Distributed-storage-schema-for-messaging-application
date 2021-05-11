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
import threading

topic_num = 0
chance = 0
msgid = 1
servercount = 1

file = open('msgid.txt', 'r')
msgid = int(file.read()) + 1
file.close()

def select_server(msgid):
    global servercount
    return msgid%servercount

def check_typeof_receiver(message):
    recv = message.split("_")[2]
    f = open('group.txt','r')
    lines= f.readlines()
    for line in lines:
        if(line.split('-')[0] == recv):
            return line.split('\n')[0].split('-')[1:]
    r_list = []
    r_list.append(recv)
    return r_list

def fun_send(message,dict_serverid):
    global msgid,producer
    recv_dict = message.split("_")
    receiver_list = check_typeof_receiver(message)
    cur_server = select_server(msgid)
    # print("server# : ",cur_server)
    recv_str = '_'.join(receiver_list)
    # print("recv_str ",recv_str)
    # print(recv_dict)
    timestamp=str(datetime.datetime.now())
    format_of_msg_server = str(msgid)+"_"+message +"_"+timestamp +"_/_"+recv_str

    dict_ack = {}
    dict_ack['ack'] = '1'
    dict_ack['uid1'] = recv_dict[1]
    dict_ack['uid2'] = recv_dict[2]
    dict_ack['timestamp'] = timestamp
    dict_ack['msgid'] = str(msgid)
    dict_ack['text'] = message.split("_")[-1]

    producer.send(dict_serverid[cur_server], value=format_of_msg_server)    
    producer.send(recv_dict[1], value=dict_ack)    
    msgid += 1
    file = open('msgid.txt', 'w')
    file.write(str(msgid))
    file.close() 

def fun_send_group(message,dict_serverid):
    global msgid,producer
    recv_dict = message.split("_")
    receiver_list = check_typeof_receiver(message)
    cur_server = select_server(msgid)
    # print("server# : ",cur_server)
    recv_str = '_'.join(receiver_list)
    # print("recv_str ",recv_str)
    # print(recv_dict)
    timestamp=str(datetime.datetime.now())
    format_of_msg_server = str(msgid)+"_"+message +"_"+timestamp + "_/_"+recv_str

    dict_ack = {}
    dict_ack['ack'] = '1'
    dict_ack['uid1'] = recv_dict[1]
    dict_ack['uid2'] = recv_dict[2]
    dict_ack['timestamp'] = timestamp
    dict_ack['msgid'] = str(msgid)
    dict_ack['text'] = message.split("_")[-1]

    producer.send(dict_serverid[cur_server], value=format_of_msg_server)    
    producer.send(recv_dict[1], value=dict_ack)    
    msgid += 1
    file = open('msgid.txt', 'w')
    file.write(str(msgid))
    file.close() 

def fun_delete(message,dict_serverid):
    global msgid,producer
    cur_server = select_server(msgid)

    dict_ack = {}
    dict_ack['ack'] = '6'
    dict_ack['uid1'] = message[2]
    dict_ack['uid2'] = message[3]
    dict_ack['msgid'] = str(message[4])
    
    format_of_msg_server = str(message[-1])+"_"+message[0]+"_"+message[2]+"_"+message[3]+"_"+message[1]
    producer.send(dict_serverid[cur_server], value=format_of_msg_server)

    producer.send(message[2], value=dict_ack)
    dict_ack['ack'] = '7'
    producer.send(message[3], value=dict_ack)  
  

def fun_fetch_msg(message,dict_serverid):
    global msgid,producer
    cur_server = select_server(msgid)
    # print(message)
    format_of_msg_server = str(message[1])+"_"+message[0]+"_"+message[2]+"_"+message[3]
    # print(format_of_msg_server)
    producer.send(dict_serverid[cur_server], value=format_of_msg_server)    

def fun_update(message,dict_serverid):
    global msgid,producer
    cur_server = select_server(msgid)
    timestamp=str(datetime.datetime.now())
    format_of_msg_server = (message[1])+"_"+message[0]+"_"+message[2]+"_"+message[3]+"_"+str(message[4])+"_"+message[5]+"_"+timestamp
    # print(format_of_msg_server)
    dict_ack = {}
    dict_ack['ack'] = '4'
    dict_ack['uid1'] = message[2]
    dict_ack['uid2'] = message[3]
    dict_ack['timestamp'] = timestamp
    dict_ack['msgid'] = str(message[4])
    dict_ack['text'] = message[5]
    producer.send(dict_serverid[cur_server], value=format_of_msg_server)  
    format_of_msg_server2 = str(message[4])+"_"+"send2_"+message[2]+"_"+message[3]+"_"+message[5] +"_"+timestamp +"_/_"+message[3]
    # print(format_of_msg_server2)
    producer.send(dict_serverid[cur_server], value=format_of_msg_server2)  
    producer.send(message[2], value=dict_ack)

def consumer_t(topic):
    
    global chance, producer, msgid
    consumer = KafkaConsumer(topic,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

    recv = "Yash"
    dict_serverid = {0:"server0",1:"server1"}

    for msg in consumer:
        recv_dict = msg.value
        print("recv ",(recv_dict))
        # continue
        if(recv_dict['op_type']=='send'):
            # recv_dict.pop('op_type')
            message='_'.join(recv_dict.values())
            print("Send msg: ", message)
            fun_send(message,dict_serverid)

        elif(recv_dict['op_type']=='send3'):
            # recv_dict.pop('op_type')
            message='_'.join(recv_dict.values())
            print("Group msg: ", message)
            fun_send_group(message,dict_serverid)

        elif(recv_dict['op_type']=='fetchmsg'):
            message=list(recv_dict.values())
            print('fetch msg: ',message)
            fun_fetch_msg(message,dict_serverid)
            print()

        elif(recv_dict['op_type']=='delete'):
            message=list(recv_dict.values())
            print('delete: ',message)
            fun_delete(message,dict_serverid)
            print()

        elif(recv_dict['op_type']=='update'):
            message=list(recv_dict.values())
            print('update: ',message)
            fun_update(message,dict_serverid)
            print()
            
        elif(recv_dict['op_type']=='fetch_grp'):
            # recv_dict.pop('op_type')
            print()
        elif(recv_dict['op_type']=='fetch_user'):
            # recv_dict.pop('op_type')
            print()


            
user_id=""
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

while(1):
    
    if(topic_num==0):
        print(" Load balancer running .. ")
        topic= "loadbalancer"
        user_id=topic
        t1 = threading.Thread(target=consumer_t,args=(topic,))
        t1.start()
        topic_num+=1
    
    else:    
        recv = input("Receiver?  ")
        data = "Hi Yash"
        
        data=user_id+"_"+recv+"_"+data
        producer.send(recv, value=data)
        sleep(1)


