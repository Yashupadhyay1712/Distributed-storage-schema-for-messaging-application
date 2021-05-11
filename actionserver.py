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
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb1 = myclient["GlobalDB1"]
mydb2 = myclient["GlobalDB2"]
mydb3 = myclient["GlobalDB3"]

topic_num = 0
chance = 0
msgid = 1
servercount = 1

def select_server(msgid):
    global servercount
    return msgid%servercount

def check_typeof_receiver(message):
    recv = message.split("_/_")[1].split("_")
    if(len(recv)>1):
        return "group"
    return "singleuser"

def update_database(templist,flg):
    table_name=""
    print("templist  ",templist)

    if flg==1:
        list_1 = [templist[2], templist[3]]
        list_1.sort()
        table_name = str(list_1[0])+"_"+str(list_1[1])
    else:
        table_name=str(templist[3])

    Timestamp = templist[-1]
    # print(Timestamp, " time")
    mycol1 = mydb1[table_name]
    mycol2 = mydb2[table_name]
    mycol3 = mydb3[table_name]

    mydict = { "Msg ID": templist[0], "Sender ID": templist[2], "Text": templist[4],"Timestamp":Timestamp}

    x1 = mycol1.insert_one(mydict)
    x2 = mycol2.insert_one(mydict)
    x3 = mycol3.insert_one(mydict)

def fun_delete(table_name,msgid):
    mycol1 = mydb1[table_name]
    mycol2 = mydb2[table_name]
    mycol3 = mydb3[table_name]
    myquery = { "Msg ID": msgid }

    mydoc1 = mycol1.find(myquery)
    for x in mydoc1:
      mycol1.delete_one(x)

    mydoc2 = mycol2.find(myquery)
    for x in mydoc2:
      mycol2.delete_one(x)

    mydoc3 = mycol3.find(myquery)
    for x in mydoc3:
      mycol3.delete_one(x)
    print("Deleted.... ")

def fun_fetch_msg(userid, tablename):
    global producer
    mycol1 = mydb1[tablename]
    dict_ack2 = {}
    dict_ack2['ack'] = '8'
    temp = tablename.split("_")
    uid2 = None
    if(userid == temp[0]):
        uid2 = temp[1]
    else:
        uid2 = temp[0]
    dict_ack2['uid2'] = uid2
    dict_ack2['msgs'] = []
    for x in mycol1.find({}, {"_id":0, "Msg ID": 1, "Sender ID": 1,"Text": 1,"Timestamp": 1 }): 
        # print(x)
        dict_ack = {}
        dict_ack['msgid'] = x["Msg ID"]
        dict_ack['uid1'] = userid
        dict_ack['uid2'] = x["Sender ID"]
        dict_ack['text'] = x["Text"]
        dict_ack['timestamp'] = x["Timestamp"]
        dict_ack2['msgs'].append(dict_ack)
    # print(dict_ack2)
    producer.send(userid, value=dict_ack2)

def fun_update(table_name,msgid,new_msg):
	mycol1 = mydb1[table_name]
	mycol2 = mydb2[table_name]
	mycol3 = mydb3[table_name]
	myquery = { "Msg ID": str(msgid) }
	# print("msgid:: ",(msgid))
	# print("new_msg:: ",new_msg)
	newvalues = { "$set": { "Text": new_msg } }

	mycol1.update_many(myquery, newvalues)
	mycol2.update_many(myquery, newvalues)
	mycol3.update_many(myquery, newvalues)

	myquery = { "Msg ID": msgid }

	mydoc1 = mycol1.find({},{"_id":0, "Msg ID": 1, "Sender ID": 1,"Text": 1,"Timestamp": 1 })
	# for x in mydoc1:
	# 	print("entry  ",x)

def consumer_t(topic):
    
    global chance, producer, msgid
    consumer = KafkaConsumer(topic,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message = message.value
        # print("loop ",message)
        op_type = message.split("_")[1]
        if(op_type=='send'): #broadcast
            receiver_type = check_typeof_receiver(message)
            print("[Send query]:", message)
            # print(" receiver_type ",receiver_type)
            if(receiver_type=="group"):
                # print("its grp")
                recv = message.split("_/_")[1].split("_")
                # print("if grp ",recv)
                update_database(message.split("_/_")[0].split("_"),0)
                format_of_msg_server = " ".join(message.split("_/_")[0].split("_"))
                for recvr in recv:
                    dict_ack = {}
                    dict_ack['ack'] = '0'
                    dict_ack['uid1'] = message.split("_/_")[0].split("_")[2]
                    dict_ack['uid2'] = recvr
                    dict_ack['timestamp'] = message.split("_/_")[0].split("_")[-1]
                    dict_ack['msgid'] = message.split("_/_")[0].split("_")[0]
                    dict_ack['text'] = message.split("_/_")[0].split("_")[-2]
                    # print(recvr, dict_ack)
                    producer.send(recvr.split('\n')[0], value=dict_ack)

            else:
                # print("nothing")
                recv = message.split("_/_")[1].split("_")
                # print("if not ",recv)
                update_database(message.split("_/_")[0].split("_"),1)
                format_of_msg_server = " ".join(message.split("_/_")[0].split("_"))
                # print(format_of_msg_server)
                for recvr in recv:
                    dict_ack = {}
                    dict_ack['ack'] = '0'
                    dict_ack['uid1'] = message.split("_/_")[0].split("_")[2]
                    dict_ack['uid2'] = recvr
                    dict_ack['timestamp'] = message.split("_/_")[0].split("_")[-1]
                    dict_ack['msgid'] = message.split("_/_")[0].split("_")[0]
                    dict_ack['text'] = message.split("_/_")[0].split("_")[-2]
                    producer.send(recvr, value=dict_ack)
                    # producer.send(recvr, value=format_of_msg_server)

        elif(op_type=="send2"):
            receiver_type = check_typeof_receiver(message)
            # print(" receiver_type send2",receiver_type, message)
            print("Update - send2")
            if(receiver_type=="group"):
                # print("its grp")
                recv = message.split("_/_")[1].split("_")
                # print("if grp ",recv)
                # update_database(message.split("_/_")[0].split("_"),0)
                format_of_msg_server = " ".join(message.split("_/_")[0].split("_"))
                for recvr in recv:
                    dict_ack = {}
                    dict_ack['ack'] = '5'
                    dict_ack['uid1'] = message.split("_/_")[0].split("_")[2]
                    dict_ack['uid2'] = recvr
                    dict_ack['timestamp'] = message.split("_/_")[0].split("_")[-1]
                    dict_ack['msgid'] = message.split("_/_")[0].split("_")[0]
                    dict_ack['text'] = message.split("_/_")[0].split("_")[-2]
                    producer.send(recvr.split('\n')[0], value=dict_ack)

            else:
                # print("nothing")
                recv = message.split("_/_")[1].split("_")
                # print("if not ",recv)
                # update_database(message.split("_/_")[0].split("_"),1)
                format_of_msg_server = " ".join(message.split("_/_")[0].split("_"))
                # print(format_of_msg_server)
                for recvr in recv:
                    dict_ack = {}
                    dict_ack['ack'] = '5'
                    dict_ack['uid1'] = message.split("_/_")[0].split("_")[2]
                    dict_ack['uid2'] = recvr
                    dict_ack['timestamp'] = message.split("_/_")[0].split("_")[-1]
                    dict_ack['msgid'] = message.split("_/_")[0].split("_")[0]
                    dict_ack['text'] = message.split("_/_")[0].split("_")[-2]
                    producer.send(recvr, value=dict_ack)
                    # producer.send(recvr, value=format_of_msg_server)
    
        if(op_type=='send3'): #group
            print("Group - send3", message)
            recv = message.split("_/_")[1].split("_")
            # print("if grp ",recv)
            update_database(message.split("_/_")[0].split("_"),0)
            format_of_msg_server = " ".join(message.split("_/_")[0].split("_"))
            # print("message", message)
            for recvr in recv:
                dict_ack = {}
                dict_ack['ack'] = '9'
                dict_ack['uid1'] = message.split("_/_")[0].split("_")[3]
                dict_ack['uid2'] = message.split("_/_")[0].split("_")[2]
                dict_ack['timestamp'] = message.split("_/_")[0].split("_")[-1]
                dict_ack['msgid'] = message.split("_/_")[0].split("_")[0]
                dict_ack['text'] = message.split("_/_")[0].split("_")[-2]
                # print(recvr, dict_ack)
                producer.send(recvr.split('\n')[0], value=dict_ack)


          

        elif(op_type=="fetchmsg"):
            # fun_fetchmsg(message)
            recv_msg = message.split("_")
            tablename = ""
            print("fetchmsg: ", recv_msg)
            # print(recv_msg)
            if(recv_msg[0]=="false"):
                list_1 = [recv_msg[2], recv_msg[3]]
                list_1.sort()
                tablename = str(list_1[0])+"_"+str(list_1[1])
                # tablename = recv_msg[2]+"_"+recv_msg[3]
            else:
                tablename = recv_msg[3]
            # print("tablename : ", tablename)
            # print(tablename, recv_msg[2])
            fun_fetch_msg(recv_msg[2],tablename)
    
            
        elif(op_type=='delete'):
            
            recv_msg = message.split("_")
            tablename = ""
            print("delete: ", recv_msg)
            if(recv_msg[-1]=="false"):
                list_1 = [recv_msg[2], recv_msg[3]]
                list_1.sort()
                tablename = str(list_1[0])+"_"+str(list_1[1])
            else:
                tablename = recv_msg[3]
            # print(tablename)
            msgid = recv_msg[0]
            fun_delete(tablename,msgid)

        elif(op_type=='update'):
            
            recv_msg = message.split("_")
            tablename = ""
            print("update", recv_msg)
            if(recv_msg[0]=="false"):
                list_1 = [recv_msg[2], recv_msg[3]]
                list_1.sort()
                tablename = str(list_1[0])+"_"+str(list_1[1])
            else:
                tablename = recv_msg[3]
            # print(tablename)
            msgid = recv_msg[4]
            fun_update(tablename,msgid, recv_msg[5])

                        
    
            
user_id=""
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))


while(1):
    
    if(topic_num==0):
        print(" Action Server running ..")
        topic= "server0"
        user_id=topic
        t1 = threading.Thread(target=consumer_t,args=(topic,))
        t1.start()
        topic_num+=1
    
    else:    
        recv = input("Receiver?  ")
        data = "Hi Yash"
        
        data=user_id+" sent you-   "+data
        producer.send(recv, value=data)
        sleep(1)