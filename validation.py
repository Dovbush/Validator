# -*- coding: utf-8 -*-

import logging
import pika
from time import sleep

LOG_LOCATION= "validation.log"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

QUEUE_VALIDATION = "validation.messages"
QUEUE_HTTPLISTENER = "httplistener"
QUEUE_MSG_ALL = "message.all"
QUEUE_UUID = "message"
RABBITMQ_SERVER = "localhost"
CONNECT_ON = "connected to rabbitmq"
CONNECT_OFF = "no connection to rabbitmq"
EMPTY = "can't consume - queue is empty"
MAX_LENGTH = 128
MAX_NUMBER_FIELD = 3
GOOD_MSG = "Response 200 - OK"
BAD_LENGTH = "Error 400 - Bad requst, message is longer than %s" % MAX_LENGTH
MISSING_ELEMENTS = "Error 400 - Bad requst, hex, token or message are missing"
MODE = 2


class Validation():
    """
    the message form validation queue is moving to the next queue 
    where message is ready to be sent to consumer.
    In other case - it moves to the queue with refused messages
    """
      
    def __init__(self):
        credentials = pika.PlainCredentials('lv128', 'lv128')
        parameters = pika.ConnectionParameters('localhost',
                                       5672,
                                       '/',
                                       credentials)
        
        self.log = logging.getLogger(LOG_LOCATION)
        self.log.setLevel(logging.INFO)
        log_hand = logging.FileHandler(LOG_LOCATION)
        log_hand.setLevel(logging.INFO)
        formatter = logging.Formatter(LOG_FORMAT)
        log_hand.setFormatter(formatter)
        self.log.addHandler(log_hand)
        try:
            self.connection = pika.BlockingConnection(parameters)
            self.log.info(CONNECT_ON)
            self.channel = self.connection.channel()
        except:
            self.log.exception(CONNECT_OFF)
            raise
            quit()

    def valid(self):
        """Every messages must have 3 elements: hex, token and message
           Every message is checked for length"""

        my_msg = self.get_msg(QUEUE_VALIDATION)
        test_msg = my_msg.split(":")
        queue_uuid = test_msg[1]
        my_message = test_msg[2]
        if len(test_msg) == MAX_NUMBER_FIELD and len(my_message) < MAX_LENGTH :
            self.log.info(GOOD_MSG + " " + my_message)
            self.send_msg(QUEUE_MSG_ALL, my_message)
            self.send_msg((QUEUE_UUID + "-" + queue_uuid), my_message)
            self.send_msg(QUEUE_HTTPLISTENER, GOOD_MSG)
        else:
            if len(test_msg) != MAX_NUMBER_FIELD :
                self.send_msg(QUEUE_HTTPLISTENER, MISSING_ELEMENTS)                
                self.log.error(MISSING_ELEMENTS)
            elif len(my_message) < MAX_LENGTH :
                self.send_msg(QUEUE_HTTPLISTENER, BAD_LENGTH)
                self.log.error(BAD_LENGTH)
        
    def get_msg(self, my_queue):
        """The function takes message from the queue"""

        self.channel.queue_declare(my_queue)
        for method_frame, properties, body in self.channel.consume(my_queue):
            print body
            self.log.info(CONNECT_ON)
            self.log.info(body)
            self.channel.basic_ack(method_frame.delivery_tag)
            return body
        
    def send_msg(self, my_queue, msg_body):
        """The function send message to another queue"""

        self.channel.queue_declare(my_queue)
        self.channel.basic_publish(exchange='', routing_key=my_queue, body=msg_body, 
                                properties=pika.BasicProperties(delivery_mode=MODE))


if __name__ == '__main__':
    v = Validation()
    while True:
        v.valid()
        sleep(0.5)

