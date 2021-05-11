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
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# f = db.createCollection("user_info")
# print(f)
user_db = myclient["authentication"]
user_table = user_db["user_info"]

def login_authenticate():
	topic_login = "login"
	consumer = KafkaConsumer(topic_login,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     # auto_offset_reset='earliest',
	 group_id=None,
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

	for message in consumer:
		message = message.value
		query = user_table.find({"user_id": message['uid']})
		flag = 0
		for x in query:
			# print("[query : ]", x)
			if(x["password"]  == message["password"]):
				flag=1
			else:
				flag=0
			break

		# print("[message : ]", message)
		print("Login: ", message)
		res = {
			"uid":message['uid'],
			"ack":flag
		}
		res=dict(res)
		#producer.flush()
		time.sleep(0.5)
		producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
		producer.send("login_ack", json.dumps(res).encode('utf-8')) 
		producer.flush()

def register_user():
	topic_register = "register"
	consumer = KafkaConsumer(topic_register,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     # auto_offset_reset='earliest',
	 group_id=None,
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))
	for message in consumer:
		message = message.value
		reg_dict = { 
					"user_id": message['uid'],
					"email": message['email'], 
					"password": message['password']
				 }

		x1 = user_table.insert_one(reg_dict)
		# print(x1)
		print("Register: ", message)
		res = {
			"uid":message['uid'],
			"ack":"ok"
		}
		time.sleep(0.5)
		producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
		producer.send("register_ack", json.dumps(res).encode('utf-8'))
		producer.flush()

def fetch_users():
	topic_register = "fetch_users"
	print("[fetch_users]")
	consumer = KafkaConsumer(topic_register,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     # auto_offset_reset='earliest',
	 group_id=None,
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))
	for message in consumer:
		message = message.value
		# print(message)
		user_id = message['uid']
		user_list = {}
		query = user_table.find()
		user_list = []
		for x in query:
			# print("[query : ]", x["user_id"])
			user_list.append(x["user_id"])
		# print(user_list)
		res = {
			"ack":'2',
			"users":user_list
		}

		time.sleep(0.5)
		producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
		producer.send(user_id, json.dumps(res).encode('utf-8'))
		producer.flush()



def main():
	t1 = threading.Thread(target=login_authenticate)
	t2 = threading.Thread(target=register_user)
	t3 = threading.Thread(target=fetch_users)
	t1.start()
	t2.start()
	t3.start()
	t1.join()
	t2.join()
	t3.join()
	print("Done!")

if __name__ == '__main__':
	main()