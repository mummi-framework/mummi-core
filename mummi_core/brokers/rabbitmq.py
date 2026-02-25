#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights
# reserved. LLNL-CODE-827197. This work was produced at the Lawrence Livermore
# National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44)
# between the U.S. Department of Energy (DOE) and Lawrence Livermore National
# Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers,
# notice of U.S. Government Rights and license terms and conditions.
# -----------------------------------------------------------------------------

from abc import ABC, abstractmethod
import json
import logging
import numpy as np
import pika
import traceback
from typing import Callable, Any, List
import ssl
import uuid
import functools
import sys
import os

from .base import BrokerInterface, JSON
from .utils import NumpyEncoder

LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
class RabbitMQBroker(BrokerInterface):
    """
    A RabbitMQBroker takes a path containing the credentials (JSON) file 
    having the following structure:
        {
            "service-port": 11,
            "service-host": "host",
            "rabbitmq-erlang-cookie": "cookie",
            "rabbitmq-name": "name",
            "rabbitmq-password": "pass",
            "rabbitmq-user": "user",
            "rabbitmq-vhost": "vhost"
        }
    Then, it needs a CA certificate that you can generate with OpenSSL:
        openssl s_client -connect $REMOTE_HOST:$REMOTE_PORT \
            -showcerts < /dev/null 2>/dev/null | \
            sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > cacert.crt

    Finally, you have to specify a queue and a default routing key (optional).
    """
    def __init__(self,
            credentials: str,
            cacert: str,
            queue: str,
            exchange: str = '',
            routing_key: str = None,
            prefetch_count: int = 1,
            logger: logging.Logger = LOGGER
            ) -> None:
        super().__init__()
        self.id = str(uuid.uuid4())[:8]
        self.credentials: str = credentials
        self.cacert: str = cacert
        self.queue = queue
        self.exchange: str = exchange
        self.routing_key: str = routing_key
        # Maximum message count which will be processing at the same time
        self.prefetch_count = max(prefetch_count, 1)
        
        self.channel: pika.BlockingChannel = None
        self.connection_parameters: pika.ConnectionParameters = None
        self.connection: pika.BlockingConnection = None
        self.logger = logger if logger else logging.getLogger(__name__)

    def _parse_credentials(self, json_file: str) -> JSON:
        """ A function that just read a JSON file. """
        data = {}
        with open(json_file, 'r') as f:
            data = json.load(f)
        return data

    def __str__(self) -> str:
        return f"{__class__.__name__}(queue={self.queue}, exchange={self.exchange}, routing_key={self.routing_key})"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        self.connect()
        return self
         
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
        self.close()

    def connect(self,
            credentials: str = None,
            cacert: str = None) -> pika.BlockingConnection:
        """
        Connect to the RabbitMQ server, initialize a channel, 
        create a queue and return the connection if applicable.
        """
        credentials = credentials if credentials else self.credentials
        cacert = cacert if cacert else self.cacert

        conn = self._parse_credentials(credentials)
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cacert)

        # These credentials are based on what Livermore Computing PDS uses
        # TODO: adapt those to Oak Ridge and to any given services.
        plain_credentials = pika.PlainCredentials(
            conn["rabbitmq-user"], 
            conn["rabbitmq-password"]
        )

        self.connection_parameters = pika.ConnectionParameters(
            host=conn["service-host"],
            port=conn["service-port"],
            virtual_host=conn["rabbitmq-vhost"],
            credentials=plain_credentials,
            ssl_options=pika.SSLOptions(context)
        )

        self.logger.info(f"Connecting to {conn['service-host']} ...")
        # We connect to RabbitMQ
        self.connection = pika.BlockingConnection(self.connection_parameters)
        # We create a channel (automatically managed py Pika)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count = self.prefetch_count)
        # We declare the queue
        # Warning:
        #   if no queue is specified then RabbitMQ will NOT hold messages that are not routed to queues.
        if self.queue == '':
            # Warning: in order to receive messages, 
            #   - the receiver will have to be started BEFORE the sender
            #     Otherwise the message will be lost.
            #   - Or a named queue must be created, which are doing here
            self.queue = __class__.__name__+"."+str(self.id)
        result = self.channel.queue_declare(queue = self.queue, auto_delete = False, exclusive = False)
        self.queue = result.method.queue
        self.logger.info(f"Queue \"{self.queue}\" declared.")

        if self.exchange == '':
            raise ValueError("Exchange cannot be empty")
        # We are binding the exchange to a specific routing_key
        # This operation is NOT permitted on the default exchange ''
        self.channel.exchange_declare(
            exchange = self.exchange,
            exchange_type = "direct",
            auto_delete = True
        )
        self.logger.info(f"Created \"direct\" exchange \"{self.exchange}\"")
    
        self.channel.queue_bind(
            exchange = self.exchange,
            queue = self.queue,
            routing_key = self.routing_key,
        )
        self.logger.info(f"Binding exchange \"{self.exchange}\" to routing_key=\"{self.routing_key}\"")

        # We are ready to publish and receive messages!
        return self.connection

    def purge(self, queue_name: str = None) -> None:
        """ Remove all the messages from the queue (be careful!). """
        queue_name = queue_name if queue_name else self.queue
        if self.channel and self.channel.is_open:
            self.channel.queue_purge(queue_name)

    def delete_queue(self, queue_name: str = None) -> None:
        """ Delete the queue (if it's unused and empty) """
        queue_name = queue_name if queue_name else self.queue
        self.channel.queue_delete(
            queue = queue_name,
            if_unused = True, 
            if_empty = True
        )

    def queue_exists(self, queue_name: str) -> bool:
        """ Return True if the queue exists. """
        try:
            self.channel.queue_declare(
                queue = queue_name,
                passive = True
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
            else:
                raise e
        return True

    def exchange_exists(self, exchange_name: str) -> bool:
        """ Return True if the exchange exists. """
        try:
            self.channel.exchange_declare(
                exchange = exchange_name,
                passive = True
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
            else:
                raise e
        return True

    def delete_exchange(self, exchange_name: str, if_unused : bool = True) -> None:
        """ Delete the exchange """
        exchange_name = exchange_name if exchange_name else self.exchange
        self.channel.exchange_delete(
            exchange = exchange_name, 
            if_unused = if_unused,
        )

    def queue_unbind(self, queue, routing_key = None):
        """ Unbind a queue to an exchange """
        routing_key = routing_key if routing_key else self.routing_key
        if self.channel and self.channel.is_open:
            self.channel.queue_unbind(self.queue, routing_key)

    def stop(self) -> None:
        """ 
        Stop the broker from consumming messages,
        allowing us to exit gracefully
        """
        self.channel.stop_consuming()

    def close(self) -> None:
        """ Close the connection with the broker. """
        if not self.channel.is_closed:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def send(self, data: str, exchange: str = '', routing_key: str = None):
        """ Close the connection with the broker. """
        routing_key = routing_key if routing_key else self.routing_key
        exchange = exchange if exchange else self.exchange
        
        if not (isinstance(data, str) and isinstance(exchange, str) and isinstance(routing_key, str)):
            raise TypeError("the data being sent, exchange and routing_key must be a string!")

        if self.channel:
            self.channel.basic_publish(exchange = exchange, routing_key = routing_key, body = data)
            self.logger.info(f"Sent \"{data}\" on queue=\"{self.queue}\" exchange=\"{exchange}\" routing_key=\"{routing_key}\"")
        else:
            self.logger.warning(f"That client is not connected to the broker, aborting...")

    def defaut_callback(self, ch, method, properties, body, args) -> None:
        """ Example of a simple callback. """
        data = body.decode()
        self.logger.warning(f"Received \"{data}\" from exchange=\"{method.exchange}\" routing_key=\"{method.routing_key}\" with args = {args}")

    def receive(self, nb_msg: int = None, callback: Callable = None, args: Any = None) -> None:
        """
        Consume a message on the queue and call the callback.
            - if nb_msg is None, this call will block for ever and will process all messages that arrives
            - if nb_msg = 1 for example, this function will block until one message has been processed.
        """
        callback = callback if callback else self.defaut_callback
        if self.channel and self.channel.is_open:
            self.logger.info(f"Starting to consume messages from queue={self.queue}, routing_key={self.routing_key} ...")
            # we will consume only nb_msg and requeue all other messages
            # if there are more messages in the queue.
            # It will block as long as nb_msg did not get read
            if nb_msg:
                nb_msg = max(nb_msg, 0)
                message_consumed = 0
                # Comsume nb_msg messages and break out
                for method_frame, properties, body in self.channel.consume(self.queue):
                    # Call the call on the message parts
                    try:
                        callback(
                            ch = self.channel,
                            method = method_frame,
                            properties = properties,
                            body = body,
                            args = args
                        )
                    except Exception as e:
                        self.logger.error(f"Exception {type(e)}: {e}")
                        self.logger.debug(traceback.format_exc())
                    finally:
                        # Acknowledge the message even on failure
                        self.channel.basic_ack(delivery_tag = method_frame.delivery_tag)
                    self.logger.warning(f"Consumed message {message_consumed+1}/{method_frame.delivery_tag} (exchange={method_frame.exchange}, routing_key={method_frame.routing_key})")
                    message_consumed += 1
                    # Escape out of the loop after nb_msg messages
                    if message_consumed == nb_msg:
                        # Cancel the consumer and return any pending messages
                        self.channel.cancel()
                        break
            else:
                # Basically if nb_msg = None, it means that nb_msg = inf
                # We bock for ever waiting for message
                self.logger.info(f"Starting to consume all messages from queue={self.queue}, routing_key={self.routing_key} ...")
                # Useful trick to add an extra arguments to the callback
                cb = functools.partial(callback, args = args)
                self.channel.basic_consume(
                    queue = self.queue,
                    on_message_callback = cb,
                    auto_ack = True)
                self.channel.start_consuming()
        else:
            self.logger.error(f"That client is not connected to the broker, aborting...")

def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)

def on_request(ch, method, props, body, args):
    n = int(body)

    print(f" [.] fib(n) (extra args = {args})")
    response = fib(n)

    # TODO: check is msg is below max message size
    msg = json.dumps(response, cls = NumpyEncoder)

    ch.basic_publish(exchange='',
                routing_key = props.reply_to,
                properties = pika.BasicProperties(correlation_id = props.correlation_id),
                body = msg
    )

    ch.basic_ack(delivery_tag = method.delivery_tag)

class RPCServer(BrokerInterface):
    """
    A RPCServer takes a path containing the credentials (JSON) file 
    having the following structure:
        {
            "service-port": 11,
            "service-host": "host",
            "rabbitmq-erlang-cookie": "cookie",
            "rabbitmq-name": "name",
            "rabbitmq-password": "pass",
            "rabbitmq-user": "user",
            "rabbitmq-vhost": "vhost"
        }
    Then, it needs a CA certificate that you can generate with OpenSSL:
        openssl s_client -connect $REMOTE_HOST:$REMOTE_PORT \
            -showcerts < /dev/null 2>/dev/null | \
            sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > cacert.crt

    Finally, you have to specify a queue and a default routing key (optional).
    """
    def __init__(self,
            credentials: str,
            cacert: str,
            queue: str,
            prefetch_count: int = 1,
            logger: logging.Logger = LOGGER
            ) -> None:
        super().__init__()
        self.credentials: str = credentials
        self.cacert: str = cacert
        self.queue = queue
        # Maximum message count which will be processing at the same time
        self.prefetch_count = max(prefetch_count, 1)
        
        self.channel: pika.BlockingChannel = None
        self.connection_parameters: pika.ConnectionParameters = None
        self.connection: pika.BlockingConnection = None
        self.logger = logger if logger else logging.getLogger(__name__)

    def _parse_credentials(self, json_file: str) -> JSON:
        """ A function that just read a JSON file. """
        data = {}
        with open(json_file, 'r') as f:
            data = json.load(f)
        return data

    def __str__(self) -> str:
        return f"{__class__.__name__}(queue={self.queue})"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        self.connect()
        return self
         
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
        self.purge()
        self.close()

    def connect(self,
            credentials: str = None,
            cacert: str = None) -> pika.BlockingConnection:
        """
        Connect to the RabbitMQ server, initialize a channel, 
        create a queue and return the connection if applicable.
        """
        credentials = credentials if credentials else self.credentials
        cacert = cacert if cacert else self.cacert

        conn = self._parse_credentials(credentials)
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cacert)

        # These credentials are based on what Livermore Computing PDS uses
        # TODO: adapt those to Oak Ridge and to any given services.
        plain_credentials = pika.PlainCredentials(
            conn["rabbitmq-user"], 
            conn["rabbitmq-password"]
        )

        self.connection_parameters = pika.ConnectionParameters(
            host = conn["service-host"],
            port = conn["service-port"],
            virtual_host = conn["rabbitmq-vhost"],
            credentials = plain_credentials,
            ssl_options = pika.SSLOptions(context)
        )

        self.logger.info(f"Connecting to {conn['service-host']} ...")
        # We connect to RabbitMQ
        self.connection = pika.BlockingConnection(self.connection_parameters)
        # We create a channel (automatically managed py Pika)
        self.channel = self.connection.channel()
        self.logger.info(f"Channel \"{self.channel}\" declared.")

        self.channel.basic_qos(prefetch_count = self.prefetch_count)

        self.channel.queue_declare(queue = self.queue)
        self.logger.info(f"Queue \"{self.queue}\" declared.")

        self.channel.basic_qos(prefetch_count = self.prefetch_count)

        return self.connection

    def run(self, callback: Callable = on_request, args: List[Any] = None):
        if self.channel and self.channel.is_open:
            cb = functools.partial(callback, args = args)
            self.channel.basic_consume(queue = self.queue, on_message_callback = cb)
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                self.stop()
                self.purge()
                self.close()
                self.logger.error("Interrupted. Got KeyboardInterrupt")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
        else:
            self.logger.warning(f"Channel {self.channel} is not open. Ignored call.")

    def purge(self, queue_name: str = None) -> None:
        """ Remove all the messages from the queue (be careful!). """
        queue_name = queue_name if queue_name else self.queue
        if self.channel and self.channel.is_open:
            self.channel.queue_purge(queue_name)

    def stop(self) -> None:
        """ 
        Stop the broker from consumming messages,
        allowing us to exit gracefully
        """
        self.channel.stop_consuming()

    def close(self) -> None:
        """ Close the connection with the broker. """
        if not self.channel.is_closed:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def send(self, data: str, exchange: str = '', routing_key: str = None):
        raise NotImplementedError("send is not implemented for RPC server")

    def receive(self, nb_msg: int = None, callback: Callable = None, args: Any = None) -> None:
        raise NotImplementedError("receive is not implemented for RPC server")

class RPCClient(object):
    """
    A RPCClient that can make RPC requests (remote procedure call)
    to a RPCServer that will run the function and send back the results.
    """
    def __init__(self,
            credentials: str,
            cacert: str,
            routing_key: str,
            logger: logging.Logger = LOGGER) -> None:
        self.credentials: str = credentials
        self.cacert: str = cacert
        self.routing_key = routing_key
        self.response = None
        self.corr_id = None

        self.channel: pika.BlockingChannel = None
        self.connection_parameters: pika.ConnectionParameters = None
        self.connection: pika.BlockingConnection = None
        self.logger = logger if logger else logging.getLogger(__name__)

        conn = self._parse_credentials(credentials)
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cacert)

        plain_credentials = pika.PlainCredentials(
            conn["rabbitmq-user"], 
            conn["rabbitmq-password"]
        )

        self.connection_parameters = pika.ConnectionParameters(
            host = conn["service-host"],
            port = conn["service-port"],
            virtual_host = conn["rabbitmq-vhost"],
            credentials = plain_credentials,
            ssl_options = pika.SSLOptions(context)
        )

        self.logger.info(f"Starting client, connecting to {conn['service-host']} ...")
        # We connect to RabbitMQ
        self.connection = pika.BlockingConnection(self.connection_parameters)
        # We create a channel (automatically managed py Pika)
        self.channel = self.connection.channel()
        # self.logger.info(f"Channel \"{self.channel}\" declared.")

        self.channel.basic_qos(prefetch_count = self.prefetch_count)

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.logger.info(f"Callback Queue \"{self.callback_queue}\" declared.")

        self.channel.basic_consume(
            queue = self.callback_queue,
            on_message_callback = self.on_response,
            auto_ack = True)

    def _parse_credentials(self, json_file: str) -> JSON:
        """ A function that just read a JSON file. """
        data = {}
        with open(json_file, 'r') as f:
            data = json.load(f)
        return data

    def __str__(self) -> str:
        return f"{__class__.__name__}(routing_key={self.routing_key})"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        self.connect()
        return self
         
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
        # self.purge()
        self.close()

    def queue_exists(self, queue_name: str) -> bool:
        """ Return True if the queue exists. """
        try:
            self.channel.queue_declare(
                queue = queue_name,
                passive = True
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
            else:
                raise e
        return True

    def purge(self, queue_name: str = None) -> None:
        """ Remove all the messages from the queue (be careful!). """
        if self.channel and self.channel.is_open:
            self.channel.queue_purge(queue_name)

    def stop(self) -> None:
        """ 
        Stop the broker from consumming messages,
        allowing us to exit gracefully
        """
        self.channel.stop_consuming()

    def close(self) -> None:
        """ Close the connection with the broker. """
        if not self.channel.is_closed:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.channel.basic_publish(
                exchange='',
                routing_key = self.routing_key,
                properties = pika.BasicProperties(
                    reply_to = self.callback_queue,
                    correlation_id = self.corr_id,
                ),
                body = str(n)
        )
        self.connection.process_data_events(time_limit = None)
        return json.loads(self.response)
