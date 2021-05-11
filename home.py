from flask import Flask, render_template, url_for, request, session, redirect  
import pymongo
import os, time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json 
from json import loads
from time import sleep
from json import dumps
import threading

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
app.secret_key = 'any random string'

users = 0
users_data = {}
user_threads = {}

def user_handle(user_id):
	global users_data, user_threads
	consumer = KafkaConsumer(user_id,
	     bootstrap_servers=['localhost:9092'],
	     auto_offset_reset='latest',
	     enable_auto_commit=True,
	     value_deserializer=lambda x: loads(x.decode('utf-8')))
	print("[", user_id,"]: ", "started")
	for msg in consumer:
		if user_id in user_threads:
			# print("[", user_id,"]: ","entered")
			recv_dict = msg.value
			# print("[", user_id,"]: ", recv_dict)
			# print("[", user_id,"]: ", recv_dict['ack'])

			if(recv_dict['ack'] == '2'): # fetch users
				users_data[user_id]['user_list'] = recv_dict['users']
			elif(recv_dict['ack'] == '3'): # fetch groups
				pass
			elif(recv_dict['ack'] == '1'): # send same user
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				if uid2 not in users_data[user_id]['msg_list']:
					users_data[user_id]['msg_list'][uid2] = {}
				users_data[user_id]['msg_list'][uid2][msg_id] = {}
				users_data[user_id]['msg_list'][uid2][msg_id]['uid'] = uid1
				users_data[user_id]['msg_list'][uid2][msg_id]['time_stamp'] = recv_dict['timestamp']
				users_data[user_id]['msg_list'][uid2][msg_id]['text'] = recv_dict['text']
			elif (recv_dict['ack'] == '0'): # send different user
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				if uid1 not in users_data[user_id]['msg_list']:
					users_data[user_id]['msg_list'][uid1] = {}
				users_data[user_id]['msg_list'][uid1][msg_id] = {}
				users_data[user_id]['msg_list'][uid1][msg_id]['uid'] = uid1
				users_data[user_id]['msg_list'][uid1][msg_id]['time_stamp'] = recv_dict['timestamp']
				users_data[user_id]['msg_list'][uid1][msg_id]['text'] = recv_dict['text']

			elif (recv_dict['ack'] == '4'): # update same user
				# print(users_data)
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				timestamp = recv_dict['timestamp']
				users_data[user_id]['msg_list'][uid2][msg_id]['text'] = recv_dict['text']
				users_data[user_id]['msg_list'][uid2][msg_id]['time_stamp'] = recv_dict['timestamp']

			elif (recv_dict['ack'] == '5'): # update different user
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				timestamp = recv_dict['timestamp']
				users_data[user_id]['msg_list'][uid1][msg_id]['time_stamp'] = recv_dict['timestamp']
				users_data[user_id]['msg_list'][uid1][msg_id]['text'] = recv_dict['text']
			elif (recv_dict['ack'] == '6'): # delete same user
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				users_data[user_id]['msg_list'][uid2].pop(msg_id)
			elif (recv_dict['ack'] == '7'): # delete different user
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				users_data[user_id]['msg_list'][uid1].pop(msg_id)
			elif (recv_dict['ack'] == '8'): # fetch msgs same user
				msgs = recv_dict['msgs']
				uid2 = recv_dict['uid2']
				if uid2 not in users_data[user_id]['msg_list']:
					users_data[user_id]['msg_list'][uid2] = {}
				# print("fetch_msg : ", recv_dict)
				for msg in msgs:
					msg_id = str(msg['msgid'])
					users_data[user_id]['msg_list'][uid2][msg_id] = {}
					users_data[user_id]['msg_list'][uid2][msg_id]['uid'] = msg['uid2']
					users_data[user_id]['msg_list'][uid2][msg_id]['time_stamp'] = msg['timestamp']
					users_data[user_id]['msg_list'][uid2][msg_id]['text'] = msg['text']
			elif (recv_dict['ack'] == '9'): # group
				msg_id = recv_dict['msgid']
				uid1 = recv_dict['uid1']
				uid2 = recv_dict['uid2']
				if uid1 not in users_data[user_id]['msg_list']:
					users_data[user_id]['msg_list'][uid1] = {}
				users_data[user_id]['msg_list'][uid1][msg_id] = {}
				users_data[user_id]['msg_list'][uid1][msg_id]['uid'] = uid2
				users_data[user_id]['msg_list'][uid1][msg_id]['time_stamp'] = recv_dict['timestamp']
				users_data[user_id]['msg_list'][uid1][msg_id]['text'] = recv_dict['text']


		else:
			break
	print("[", user_id,"]: ", "stopped")
		    


@app.route("/")
@app.route("/home")
def home():
	return render_template("home.html")
    

@app.route("/login",methods=["GET","POST"])
def login():
	return render_template("login.html")

@app.route("/login_check",methods=["GET","POST"])
def login_check():
	global producer, users, users_data, user_threads
	if request.method=="POST":
		req=request.form
		req=dict(req)
		# session['uid'] = req['uid']
		uid = str(req['uid'])
		# print(req)
		n = len(req)
		# print(n)
		
		topic = "login"
		topic_ack = "login_ack"
		
		# print(topic)
		consumer = KafkaConsumer(topic_ack,
	     bootstrap_servers=['localhost:9092'],
	     auto_offset_reset='latest',
	     # auto_offset_reset='earliest',
	     group_id=None,
	     enable_auto_commit=True,
	     value_deserializer=lambda x: loads(x.decode('utf-8')))
		# print(topic_ack)
		producer.send(topic, json.dumps(req).encode('utf-8'))
		for message in consumer:
			message = message.value
			# print(message)
			break
		if(message['ack']==1):
			users += 1 
			users_data[uid] = {}
			users_data[uid]['cid']=None
			users_data[uid]['user_list']=[]
			users_data[uid]['group_list']=[]
			users_data[uid]['msg_list']={}
			t1 = threading.Thread(target=user_handle, args=(uid,))
			user_threads[uid]=True
			t1.start()
			return (redirect("/dashboard/" + str(uid)))
		else:
			return render_template("invalid.html")
	return (redirect("/login"))

@app.route("/register",methods=["GET","POST"])
def register():
	return render_template("register.html")

@app.route("/register_check",methods=["GET","POST"])
def register_check():
	global producer, users, users_data, user_threads
	if request.method=="POST":
		req=request.form
		req=dict(req)
		# session['uid'] = req['uid']
		uid = str(req['uid'])
		# print(req)
		n = len(req)
		# print(n)
		
		topic = "register"
		topic_ack = "register_ack"

		# print(topic)
		consumer = KafkaConsumer(topic_ack,
	     bootstrap_servers=['localhost:9092'],
	     auto_offset_reset='latest',
	     # auto_offset_reset='earliest',
	     group_id=None,
	     enable_auto_commit=True,
	     value_deserializer=lambda x: loads(x.decode('utf-8')))
		# print(topic_ack)
		producer.send(topic, json.dumps(req).encode('utf-8'))
		for message in consumer:
			message = message.value
			# print(message)
			break

		users += 1 
		users_data[uid] = {}
		users_data[uid]['cid']=None
		users_data[uid]['user_list']=[]
		users_data[uid]['group_list']=[]
		users_data[uid]['msg_list']={}
		t1 = threading.Thread(target=user_handle, args=(uid,))
		user_threads[uid]=True
		t1.start()
		return (redirect("/dashboard/" + str(uid)))
	return (redirect("/register"))


@app.route("/dashboard/<string:user_id>",methods=["GET","POST"])
def dashboard(user_id):
	global producer, users, users_data
	
	if user_id in users_data:
		cid = users_data[user_id]['cid']
		if cid in users_data[user_id]['msg_list']:
			data4 = dict(sorted(users_data[user_id]['msg_list'][cid].items(), key = lambda kv:(int(kv[0]), kv[1]['time_stamp'])))
			return render_template("dashboard.html", uid=user_id, users=users_data[user_id]['user_list'], groups=users_data[user_id]['group_list'], msgs=data4, cid=users_data[user_id]['cid']) 
		else:
			return render_template("dashboard.html", uid=user_id, users=users_data[user_id]['user_list'], groups=users_data[user_id]['group_list'], msgs={}, cid=users_data[user_id]['cid']) 
	return (redirect("/home"))


@app.route("/logout/<string:user_id>",methods=["POST"])
def logout(user_id):
	global users_data, users, user_threads, producer
	users -= 1
	users_data.pop(user_id)
	user_threads.pop(user_id)
	producer.send(user_id, json.dumps({}).encode('utf-8'))
	return (redirect("/"))


@app.route("/fetch_users/<string:user_id>", methods=['POST'])
def fetch_users(user_id):
	global producer, users, users_data
	print('fetch users')
	# file = open('mappings/user.txt', 'r')
	# data = file.read().splitlines()
	# file.close()
	# print(data)
	# print(users_data)
	# users_data[user_id]['user_list'] = data
	dict_send = {
		"uid":user_id
	}
	producer.send("fetch_users", json.dumps(dict_send).encode('utf-8'))
	sleep(1)
	return (redirect("/dashboard/" + str(user_id)))


@app.route("/fetch_groups/<string:user_id>", methods=['POST'])
def fetch_groups(user_id):
	global producer, users, users_data
	print('fetch groups')
	file = open('group.txt', 'r')
	data = file.read().splitlines()
	file.close()
	# print(data)
	data2 = []
	for i in data:
		data2.append(i.split('-')[0])
	# print(data2)
	users_data[user_id]['group_list'] = data2
	sleep(1)
	return (redirect("/dashboard/" + str(user_id)))

@app.route("/fetch_msg/<string:user_id>", methods=['POST'])
def fetch_msg(user_id):
	global producer, users, users_data
	# print('fetch msg')
	chat_id = users_data[user_id]['cid']
	print("[fetch msg] : ", user_id, chat_id)

	dict_send = {
		"op_type":"fetchmsg",
		"isGroup":"false",
		"uid1":user_id,
		"uid2":chat_id,
	}

	if(chat_id.startswith("group")):
		dict_send["isGroup"] = "true"

	topic1 = "loadbalancer"
	producer.send(topic1, json.dumps(dict_send).encode('utf-8'))
	sleep(1)
	return (redirect("/dashboard/" + str(user_id)))




@app.route("/update_cid/<string:user_id>/<string:chat_id>", methods=['GET', 'POST'])
def update_cid(user_id, chat_id):
	global producer, users, users_data
	print("[update_cid] : ", user_id, chat_id)
	users_data[user_id]['cid'] = chat_id
	return (redirect("/dashboard/" + str(user_id)))



@app.route("/send/<string:user_id>", methods=['GET', 'POST'])
def send(user_id):
	global producer, users, users_data
	#print(users_data)
	chat_id = users_data[user_id]['cid']
	print("[send] : ", user_id, chat_id)
	if request.method=="POST":
		req=request.form
		req=dict(req)
		# print("[send] : ", req)
		file_name = ""
		topic1 = "loadbalancer"
		dict_send = {}
		if(chat_id.startswith("group")):
			dict_send={'op_type':"send3",'uid1':user_id,'uid2':chat_id,'msg':req['typed_msg']}
		else:
			dict_send={'op_type':"send",'uid1':user_id,'uid2':chat_id,'msg':req['typed_msg']}
		producer.send(topic1, json.dumps(dict_send).encode('utf-8'))
		sleep(0.5)

	return (redirect("/dashboard/" + str(user_id)))

@app.route("/update_msg/<string:user_id>", methods=['GET', 'POST'])
def update_msg(user_id):
	global producer, users, users_data
	#print(users_data)
	chat_id = users_data[user_id]['cid']

	print("[update_msg] : ", user_id, chat_id)
	if request.method=="POST":
		req=request.form
		req=dict(req)
		# print("[update_msg] : ", req)
		msg_id = req['msg_id']
		# users_data[user_id]['msg_list'][uid2][msg_id]['text'] = req['updated_msg']
		topic1 = "loadbalancer"
		dict_send = {}
		if(chat_id.startswith("group")):
			dict_send={'op_type':"update", 'isGroup':'true' ,'uid1':user_id,'uid2':chat_id, 'msgid':msg_id,'msg':req['updated_msg']}
		else:
			dict_send={'op_type':"update", 'isGroup':'false' ,'uid1':user_id,'uid2':chat_id, 'msgid':msg_id,'msg':req['updated_msg']}
		producer.send(topic1, json.dumps(dict_send).encode('utf-8'))
		sleep(0.5)

	return (redirect("/dashboard/" + str(user_id)))

@app.route("/delete_msg/<string:user_id>", methods=['GET', 'POST'])
def delete_msg(user_id):
	global producer, users, users_data
	#print(users_data)
	chat_id = users_data[user_id]['cid']

	print("[delete_msg] : ", user_id, chat_id)
	if request.method=="POST":
		req=request.form
		req=dict(req)
		# print("[delete_msg] : ", req)
		msg_id = req['msg_id']
		# users_data[user_id]['msg_list'][uid2].pop()
		topic1 = "loadbalancer"
		dict_send = {}
		if(chat_id.startswith("group")):
			dict_send={'op_type':"delete", 'isGroup':'true', 'uid1':user_id, 'uid2':chat_id, 'msgid':msg_id}
		else:
			dict_send={'op_type':"delete", 'isGroup':'false', 'uid1':user_id, 'uid2':chat_id, 'msgid':msg_id}
		producer.send(topic1, json.dumps(dict_send).encode('utf-8'))
		sleep(0.5)

	return (redirect("/dashboard/" + str(user_id)))




if __name__ == "__main__":
    app.run(debug=True, threaded=True)