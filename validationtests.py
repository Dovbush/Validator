import unittest
from validation import Validation 
import pika

QUEUE_VALIDATION = "validation.messages"
QUEUE_HTTPLISTENER = "httplistener"
QUEUE_MSG_ALL = "message.all"
RABBITMQ_SERVER = "localhost"

GOOD_MSG = "Response 200 - OK"
BAD_MSG = "Error 400 - Bad requst"

TEST_MSG_SHORT = "1234567890"
TEST_MSG_LONG = "1234567890"*13

con = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_SERVER))

def my_send_to_queue(conn, msg, QU):
	channel = conn.channel()
	channel.queue_declare(queue=QU)
	channel.basic_publish(exchange='', routing_key=QU, body=msg)

def my_get_msg(conn, my_queue):	
	channel = conn.channel()
	for method_frame, properties, body in channel.consume(my_queue):
		channel.basic_ack(method_frame.delivery_tag)
		return body

class Test_Validation(unittest.TestCase):
	def setUp(self):
		self.val = Validation()

	def test_GetMsg_short(self):
		my_send_to_queue(con, TEST_MSG_SHORT, QUEUE_VALIDATION)
		self.val.valid()
		tmsg = my_get_msg(con, QUEUE_MSG_ALL)
		tm = my_get_msg(con, QUEUE_HTTPLISTENER)
		self.assertEqual(tmsg, TEST_MSG_SHORT)

	def test_GetMsg_long(self):
		my_send_to_queue(con, TEST_MSG_LONG, QUEUE_VALIDATION)
		self.val.valid()
		tmsg = my_get_msg(con, QUEUE_HTTPLISTENER)
		self.assertEqual(tmsg, BAD_MSG)

	def test_SendMsg(self):
		self.valid.send_msg(TEST_MSG_LONG, QUEUE_VALIDATION)
		tmsg = my_get_msg(con, QUEUE_HTTPLISTENER)
		self.assertEqual(tmsg, GOOD_MSG)

if __name__ == '__main__':
	unittest.main()	