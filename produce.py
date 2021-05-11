from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
from sys import argv
import os

def send_message(topic,msg):
	producer.send(topic, msg.encode('utf-8'))

if(len(argv)<=1):
	print("enter topic")
	os._exit(0)

topic = argv[1]
while(1):
	data = input("> ")
	send_message(topic, data)