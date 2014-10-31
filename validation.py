#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pika

valid_mes = "Hello everybody!" 			#here should a reference on a real message 
queue_valid = 'validation.messages1'    #this is a queue the messages come from
queue_httpL = 'httpListener'            #this is the next queue the messages go to
queue_all = 'all.messages1'
#queue_tocken = 'tocken'.join(id)...
#id = triplet (the reference on this variable comes from HTTPListener)
confirmation = 'Response 200 - OK'
error400 = 'Error 400 - Bad requst'   
max_length = 128
mode = 2 
mes_in_work = 1								#this is an internal rabbitMQ's settings           


connection = pika.BlockingConnection(pika.ConnectionParameters(
              'localhost'))  						
channel = connection.channel()                      #open connection

def callback(ch, method, properties, body):
	"""this function consume messages and acknowledge them"""
	ch.basic_ack(delivery_tag = method.delivery_tag)

def pushing():
	"""function is cheking for messages length,
		if True, the message form validation queue is moving to the next queue 
		where message is ready to be sent to consumer.
		In other case - it moves to the queue with refused messages"""
	channel.basic_qos(prefetch_count=mes_in_work)
	channel.basic_consume(callback,
               				queue=queue_valid)
	if len(valid_mes) < max_length:
		channel.queue_declare(queue=queue_all)
		channel.basic_publish(exchange='',
                   				routing_key=queue_all,
                   				body=valid_mes,
                   				properties=pika.BasicProperties(
                  				delivery_mode = mode,                    # make message persistent
                   				))
		channel.queue_declare(queue=queue_httpL)
		channel.basic_publish(exchange='',
                   				routing_key=queue_httpL,
                   				body=confirmation,
                   				properties=pika.BasicProperties(
                   				delivery_mode = mode,                    # make message persistent
                   				))
		logging.basicConfig(format='%(asctime)s %(message)s', filename='info.log', level=logging.INFO)
		logging.info(confirmation, valid_mes)
		#we need to add queue - #queue_tocken = 'tocken'.join(id)...
	else:
		channel.queue_declare(queue=queue_httpL)
		channel.basic_publish(exchange='',
                   				routing_key=queue_httpL,
                   				body=error400,
                   				properties=pika.BasicProperties(
                   				delivery_mode = mode,                    # make message persistent
                   				))
		logging.basicConfig(format='%(asctime)s %(message)s', filename='info.log', level=logging.INFO)
		logging.info(error400)
			
pushing()
channel.start_consuming()
#connection.close()


