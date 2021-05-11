from flask import Flask, render_template, url_for, request, session, redirect  
import pymongo
import os
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json 
from json import loads
from time import sleep
from json import dumps

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
app.secret_key = 'any random string'
uid = None
data1 = []
data2 = []
data3 = []
msgs = []
cid = None


@app.route("/")
@app.route("/home")
def home():
	if 'uid' in session:
		return render_template("signedin.html", uid=session['uid'])
	return render_template("home.html")
    

@app.route("/login",methods=["GET","POST"])
def login():
	if 'uid' in session:
		return render_template("signedin.html", uid=session['uid'])
	return render_template("login.html")


@app.route("/register",methods=["GET","POST"])
def register():
	if 'uid' in session:
		return render_template("signedin.html", uid=session['uid'])
	return render_template("register.html")


@app.route("/dashboard",methods=["GET","POST"])
def dashboard():
	global producer, uid, data1, data2, data3, cid

	if request.method=="POST":
		req=request.form
		req=dict(req)
		session['uid'] = req['uid']
		uid = req['uid']
		print(req)
		n = len(req)
		print(n)
		topic=""
		topic_ack=""
		if n==2:
			topic = "login"
			topic_ack = "login_ack"
		else:
			topic = "register"
			topic_ack = "register_ack"
		print(topic)
		consumer = KafkaConsumer(topic_ack,
	     bootstrap_servers=['localhost:9092'],
	     auto_offset_reset='latest',
	     # auto_offset_reset='earliest',
	     group_id=None,
	     enable_auto_commit=True,
	     value_deserializer=lambda x: loads(x.decode('utf-8')))
		print(topic_ack)
		producer.send(topic, json.dumps(req).encode('utf-8'))
		for message in consumer:
			message = message.value
			print(message)
			break


	if 'uid' not in session:
		return render_template("invalid.html")

	uid = session['uid']
	return render_template("dashboard.html", uid=uid, users=data1, groups=data2, msgs=data3, cid=cid) #(redirect(request.url)) 

@app.route("/logout/",methods=["POST"])
def logout():
	global uid, data1, data2, data3, cid
	session.pop('uid', None)
	uid = None
	data1 = []
	data2 = []
	data3 = []
	cid = None
	#return render_template("home.html")
	return (redirect("/"))

@app.route("/fetch_users/", methods=['POST'])
def fetch_users():
	global data1
	# print('fetch users')
	file = open('user.txt', 'r')
	data1 = file.read().splitlines()
	file.close()
	# print(data)
	# return render_template("dashboard.html", uid=uid, users=data1, groups=data2)
	return (redirect("/dashboard")) 

@app.route("/fetch_groups/", methods=['POST'])
def fetch_groups():
	global data2
	# print('fetch users')
	file = open('group.txt', 'r')
	data2 = file.read().splitlines()
	file.close()
	# print(data)
	# return render_template("dashboard.html", uid=uid, users=data1, groups=data2)
	return (redirect("/dashboard"))

@app.route("/update_cid/<string:chat_id>", methods=['GET', 'POST'])
def update_cid(chat_id):
	global cid
	# print('fetch users')
	# print("hello", chat_id)
	cid = chat_id
	fetch_msgs()
	# print(data)
	# return render_template("dashboard.html", uid=uid, users=data1, groups=data2)
	return (redirect("/dashboard"))

@app.route("/fetch_msgs/", methods=['POST'])
def fetch_msgs():
	global data3, cid
	# print('fetch users')
	file_name = ""
	if(cid.startswith("group")):
		file_name = "messages/" + str(cid) + ".txt"
	else:
		file_name = "messages/" + str(uid) + "_" + str(cid) + ".txt"
	print(file_name)
	if(os.path.isfile(file_name)):
		file = open(file_name, 'r')
		data3 = file.read().splitlines()
		file.close()
	# print(data)
	return (redirect("/dashboard"))

if __name__ == "__main__":
    app.run(debug=True, threaded=True)