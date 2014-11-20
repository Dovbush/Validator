# -*- coding: utf-8 -*-
import logging
import pika

LOG_LOCATION= "/opt/lv128/log/Validation.log"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

QUEUE_VALIDATION = "validation.messages"
QUEUE_HTTPLISTENER = "httplistener"
QUEUE_MSG_ALL = "message.all"
RABBITMQ_SERVER = "localhost"

GOOD_MSG = "Response 200 - OK"
BAD_MSG = "Error 400 - Bad requst"
MAX_LENGTH = 128


class Validation():
	"""
	the message form validation queue is moving to the next queue 
	where message is ready to be sent to consumer.
	In other case - it moves to the queue with refused messages
	"""
	def __init__(self):
		self.log = logging.getLogger(LOG_LOCATION)
		self.log.setLevel(logging.INFO)
		log_hand = logging.FileHandler(LOG_LOCATION)
		log_hand.setLevel(logging.INFO)
		formatter = logging.Formatter(LOG_FORMAT)
		log_hand.setFormatter(formatter)
		self.log.addHandler(log_hand)


		self.connection = pika.BlockingConnection(
			pika.ConnectionParameters(RABBITMQ_SERVER))
		
		if self.connection.is_open:
			self.log.info("connected to rabbitmq")
		else:
			self.log.error("no connection to rabbitmq")

	def valid(self):
		tmp_msg = self.get_msg(QUEUE_VALIDATION)
		if len(tmp_msg) < MAX_LENGTH:
			self.log.info(GOOD_MSG, tmp_msg)
			self.send_msg(QUEUE_MSG_ALL, tmp_msg)
			self.send_msg(QUEUE_HTTPLISTENER, GOOD_MSG)
		else:
			self.send_msg(QUEUE_HTTPLISTENER, BAD_MSG)
			self.log.error(BAD_MSG)
		
	def get_msg(self, my_queue):
		channel = self.connection.channel()
		for method_frame, properties, body in channel.consume(my_queue):
			self.log.info(body)
			channel.basic_ack(method_frame.delivery_tag)
			return body

	def send_msg(self, my_queue, msg_body):
		channel = self.connection.channel()
		channel.queue_declare(queue=my_queue)
		channel.basic_publish(exchange='', routing_key=my_queue, body=msg_body)


if __name__ == '__main__':
	v = Validation()
	v.valid()
	
