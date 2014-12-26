import pika
import logging
import mysql.connector
from types import *
from time import sleep

LOG_LOCATION= "/opt/lv128/log/validation.log"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
QUEUE_VALIDATION = "validation.messages"
QUEUE_HTTPLISTENER = "httplistener"
QUEUE_MSG_ALL = "message.all"
QUEUE_UUID = "message."

RABBITMQ_SERVER = "127.0.0.1"
RABBITMQ_USER = "lv128"
RABBITMQ_PASSWORD = "lv128"
RABBITMQ_PORT = 5672

CONNECT_ON = "connected to rabbitmq"
CONNECT_OFF = "no connection to rabbitmq"
SQL_CONNECT_ON = "connected to sql"
SQL_CONNECT_OFF = "no connection to sql"
EMPTY = "can't consume - queue is empty"
MAX_LENGTH = 128
MAX_NUMBER_FIELD = 3

DB_SERVER = "127.0.0.1"
DB_USER = "root"
DB_PASSWORD = ''
DB_NAME = 'yaps'

GOOD_MSG = "Response 200 - OK"
BAD_LENGTH = "Error 400 - Bad requst, message is longer than %s" % MAX_LENGTH
INVALID_TOKEN = "Error 406 - Invalid token."
MISSING_ELEMENTS = "Error 400 - Bad requst, hex, token or message are missing"
GET_USER_COUNTERS = "SELECT total_msg_counter, success_msg_counter, failed_msg_counter from my_app_msg WHERE user_id={0}"
UPDATE_USER_COUNTERS = "UPDATE my_app_msg SET total_msg_counter={0}, success_msg_counter = {1}, failed_msg_counter = {2} WHERE user_id ={3}"
FIND_PROFILE_BY_TOKEN = "SELECT * FROM my_app_profile WHERE token={}"
SELECT_COUNTERS_BY_ID = "SELECT total_msg_counter, success_msg_counter, failed_msg_counter FROM my_app_msg WHERE user_id={0}"
SELECT_USER_BY_TOKEN = "SELECT * FROM my_app_profile WHERE token = {0}"
class Validation():
    """
    the message form validation queue is moving to the next queue
    where message is ready to be sent to consumer.
    In other case - it moves to the queue with refused messages
    """

    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(RABBITMQ_SERVER,
                                       RABBITMQ_PORT,
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
            
            self.sql_conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_SERVER,
                              database=DB_NAME)

            self.sql_cursor = self.sql_conn.cursor()
            self.log.info(SQL_CONNECT_ON)
        except:
            self.log.exception(SQL_CONNECT_OFF)
            raise
        try:
            self.connection = pika.BlockingConnection(parameters)
            self.log.info(CONNECT_ON)
            self.channel = self.connection.channel()
        except:
            self.log.exception(CONNECT_OFF)
            raise

    def update_user_counters(self, record, is_valid):
        id = record[0]
        token = record[1]
        user_id = record[2]
        self.sql_cursor.execute(SELECT_COUNTERS_BY_ID.format(user_id))
        result = self.sql_cursor.fetchone()
        total = result[0] + 1
        success = result[1]
        fail = result[2]
        if is_valid:
            success = success + 1
        else:
            fail = fail + 1 
        self.sql_cursor.execute(UPDATE_USER_COUNTERS.format(str(total), str(success), str(fail), str(user_id)))
        self.sql_conn.commit()
        id, token, user_id = record
        self.sql_cursor.execute(GET_USER_COUNTERS.format(str(user_id)))
        total, success, fail = self.sql_cursor.fetchone()
        self.sql_cursor.execute(UPDATE_USER_COUNTERS.format(str(total), str(success), str(fail), str(user_id)))
    
    def get_valid_record(self, token):
        self.sql_cursor.execute(SELECT_USER_BY_TOKEN.format(str(token)))
        result = self.sql_cursor.fetchone()
        return result

    def valid(self):
        """Every messages must have 3 elements: hex, token and message
           Every message is checked for length"""

        my_msg = self.get_msg(QUEUE_VALIDATION)
        test_msg = my_msg.split(":")
        queue_uuid = test_msg[1]
        my_message = test_msg[2]
        valid_record = self.get_valid_record(queue_uuid)
        if len(test_msg) == MAX_NUMBER_FIELD and len(my_message) < MAX_LENGTH and not  (type(valid_record) is NoneType):
                self.update_user_counters(valid_record, 1)
                self.log.info(GOOD_MSG + " " + my_message)
                self.send_msg(QUEUE_MSG_ALL, my_message)
                self.send_msg((QUEUE_UUID + queue_uuid), my_message)
                self.send_msg(QUEUE_HTTPLISTENER, GOOD_MSG)
        else:
            if len(test_msg) != MAX_NUMBER_FIELD :
                self.send_msg(QUEUE_HTTPLISTENER, MISSING_ELEMENTS)
                self.log.error(MISSING_ELEMENTS)
                self.update_user_counters(valid_record, False)
            elif len(my_message) > MAX_LENGTH :
                self.send_msg(QUEUE_HTTPLISTENER, BAD_LENGTH)
                self.log.error(BAD_LENGTH)
                self.update_user_counters(valid_record, False)
            else:
                self.log.error(INVALID_TOKEN)
                self.send_msg(QUEUE_HTTPLISTENER, INVALID_TOKEN)

    def get_msg(self, my_queue):
        """The function takes message from the queue"""

        self.channel.queue_declare(my_queue)
        try:
            for method_frame, properties, body in self.channel.consume(my_queue):
                self.log.info(CONNECT_ON)
                self.log.info(body)
                self.channel.basic_ack(method_frame.delivery_tag)
                return body
        except pika.exceptions, err_msg:
            self.log.error(err_msg)
            return False

    def send_msg(self, my_queue, msg_body):
        """The function send message to another queue"""

        self.channel.queue_declare(my_queue)
        self.channel.basic_publish(exchange='', routing_key=my_queue, body=msg_body)


if __name__ == '__main__':
    v = Validation()
    while True:
        v.valid()
        sleep(0.05)
