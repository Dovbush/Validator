# -*- coding: utf-8 -*-
import logging
import pika

LOG_LOCATION= "validation.log"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

QUEUE_VALIDATION = "validation.messages"
QUEUE_HTTPLISTENER = "httplistener"
QUEUE_MSG_ALL = "message.all"
RABBITMQ_SERVER = "localhost"
CONNECT_ON = "connected to rabbitmq"
CONNECT_OFF = "no connection to rabbitmq"
EMPTY = "can't consume - queue is empty"

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
        except:
            self.log.exception(CONNECT_OFF)
            raise
            quit()

    def valid(self):
        tmp_msg = self.get_msg(QUEUE_VALIDATION)

        if len(tmp_msg) < MAX_LENGTH:
            self.log.info(GOOD_MSG + " " + tmp_msg)
            self.send_msg(QUEUE_MSG_ALL, tmp_msg)
            self.send_msg(QUEUE_HTTPLISTENER, GOOD_MSG)
        else:
            self.send_msg(QUEUE_HTTPLISTENER, BAD_MSG)
            self.log.error(BAD_MSG)
        
    def get_msg(self, my_queue):
        channel = self.connection.channel()
        for method_frame, properties, body in channel.consume(my_queue):
            self.log.info(CONNECT_ON)
            self.log.info(body)
            channel.basic_ack(method_frame.delivery_tag)
            return body
        
    def send_msg(self, my_queue, msg_body):
        channel = self.connection.channel()
        channel.basic_publish(exchange='', routing_key=my_queue, body=msg_body)


if __name__ == '__main__':
    v = Validation()
    v.valid()
    
    
    
