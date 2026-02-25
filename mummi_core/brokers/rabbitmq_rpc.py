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

import json
import logging
import pika
from typing import Callable, Any, List
import ssl
import uuid
import functools
import sys
import os
import time
import traceback
import threading

from .base import BrokerInterface, JSON
from .utils import NumpyEncoder

LOGGER = logging.getLogger(__name__)

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

        self._threads = []

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
        if os.path.isfile(cacert):
            # We try to connect with TLS if there is a certificate
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = False
            context.load_verify_locations(cacert)
            ssl_options = pika.SSLOptions(context)
        else:
            # No TLS certificate, we try a plain connecion (unsafe)
            self.logger.warning(f"Using Plain connection (unsafe): TLS certificate {cacert} does not exist")
            ssl_options = None

        # These credentials are based on what Livermore Computing PDS uses
        # TODO: adapt those to Oak Ridge and to any given services.
        plain_credentials = pika.PlainCredentials(
            conn["rabbitmq-user"], 
            conn["rabbitmq-password"]
        )

        # NOTE: We deactivate heartbeat because the threads we are using are long running threads
        # and Pika does not like when a thread block its ioloop. if the ioloop is blocked then
        # no heartbeats is sent and RabbitMQ thinks the connection is dead.
        self.connection_parameters = pika.ConnectionParameters(
            host = conn["service-host"],
            port = conn["service-port"],
            virtual_host = conn["rabbitmq-vhost"],
            credentials = plain_credentials,
            ssl_options = ssl_options,
            # heartbeat = 0,
            # blocked_connection_timeout=3600,
        )

        self.logger.info(f"Connecting to {conn['service-host']} ...")
        # We connect to RabbitMQ
        self.connection = pika.BlockingConnection(self.connection_parameters)
        # We create a channel (automatically managed py Pika)
        self.channel = self.connection.channel()
        self.logger.info(f"Channel \"{self.channel}\" declared.")

        res = self.channel.queue_declare(queue = self.queue)
        self.logger.info(f"Queue \"{self.queue}\" declared.")

        self.message_count = res.method.message_count
        self.consumer_count = res.method.consumer_count
        if self.message_count > 0:
            self.logger.warning(
                f"Queue \"{self.queue}\" has already "
                f"{self.message_count} messages (probably from another run). "
                f"Queue will be purged.")
            self.purge()

        self.channel.basic_qos(prefetch_count = self.prefetch_count)

        return self.connection

    def ack_message(self, ch, delivery_tag):
        """
        Note that `ch` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if ch.is_open:
            ch.basic_ack(delivery_tag)
            self.logger.debug(f"Sent ack with tag={delivery_tag} back to consumer")
        else:
            self.logger.error(f"Could not send ack, channel is not open :{ch}")

    def rpc_send_result(self, ch, props, msg):
        """
        Note that `ch` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if ch.is_open:
            # TODO: check is msg is below max message size 
            #       if not chunk it to RABBITMQ_MAX (must be defined)
            ch.basic_publish(exchange='',
                routing_key = props.reply_to,
                properties = pika.BasicProperties(
                                correlation_id = props.correlation_id
                ),
                body = msg
            )
            self.logger.debug(f"Sent results back to consumer {props.reply_to}")
        else:
            self.logger.error(f"Could not send result, channel is not open :{ch}")

    def on_request(self, ch, method_frame, properties, body, args):
        assert args != None and len(args) > 0
        func = args[1]
        work_args = args[2:]
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(
            target = self.worker,
            args = (ch, properties, delivery_tag, body, func, work_args)
        )
        t.start()
        self._threads.append(t)
        self.logger.debug(f"#processes (alive or dead) = {len(self._threads)}")

    def worker(self, ch, properties, delivery_tag, body, func, args):
        pid = threading.get_ident()
        self.logger.debug(f"\n"
                f" Process ID     : {pid}\n"
                f"   > Delivery tag : {delivery_tag}\n"
                f"   > Properties   : {properties}\n"
                f"   > Message body : {body}\n"
                f"   > Function     : {func.__name__}\n"
                f"   > Arguments    : {args[0]}\n"
        )
        # we ack the message to RabbitMQ
        data = json.loads(body)
        try:
            args[0]['body'] = data
            self.logger.info(f"[{pid}] Workflow requested {data['k_samples']} samples")
            response = func(**args[0])
        except Exception as _:
            error = traceback.format_exc()
            msg = json.dumps(error, cls = NumpyEncoder)
            self.logger.error(f"[pid={pid}] worker({func.__name__}) (extra args = {args}) =>")
            self.logger.error(msg)
        else:
            msg = json.dumps(response, cls = NumpyEncoder)
        finally:
            # We send the result to the consumer
            ack_cb = functools.partial(self.ack_message, ch, delivery_tag)
            ch.connection.add_callback_threadsafe(ack_cb)
            rpc_cb = functools.partial(self.rpc_send_result, ch, properties, msg)
            ch.connection.add_callback_threadsafe(rpc_cb)

    def run(self, callback: Callable = None, args: Any = None):
        """
        Run the serserver waiting for requests. The callback is executed 
        when the request is received. It  does the work and send the response back.
        args is extra arguments that can be transmitted to the callback if needed.
        """
        if self.channel and self.channel.is_open and callback:
            cb = functools.partial(self.on_request, args=(self._threads, callback, args))
            self.channel.basic_consume(queue = self.queue, on_message_callback = cb)
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                self.stop()
                self.purge()
                self.close()
                self.logger.error("Interrupted.")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
        else:
            self.logger.warning(
                f"Channel ({self.channel}) is not open "
                f"or callback is not provided (={callback}). "
                f"Ignored call.")

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
        if self.channel:
            self.channel.stop_consuming()
        for thread in self._threads:
            thread.join()

    def close(self) -> None:
        """ Close the connection with the broker. """
        if self.channel and not self.channel.is_closed:
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
            timeout_retry: int = 5,
            max_retry: int = 100,
            logger: logging.Logger = LOGGER) -> None:
        self.credentials: str = credentials
        self.cacert: str = cacert
        self.routing_key = routing_key
        self.timeout_retry = timeout_retry
        self.max_retry = max_retry

        self.response = None
        self.corr_id = None

        self.channel: pika.BlockingChannel = None
        self.connection_parameters: pika.ConnectionParameters = None
        self.connection: pika.BlockingConnection = None
        self.logger = logger if logger else logging.getLogger(__name__)

        conn = self._parse_credentials(credentials)


        if os.path.isfile(cacert):
            # We try to connect with TLS if there is a certificate
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cacert)
            ssl_options = pika.SSLOptions(context)
        else:
            # No TLS certificate, we try a plain connecion (unsafe)
            self.logger.warning(f"Using Plain connection (unsafe): TLS certificate {cacert} does not exist")
            ssl_options = None


        plain_credentials = pika.PlainCredentials(
            conn["rabbitmq-user"], 
            conn["rabbitmq-password"]
        )

        self.connection_parameters = pika.ConnectionParameters(
            host = conn["service-host"],
            port = conn["service-port"],
            virtual_host = conn["rabbitmq-vhost"],
            credentials = plain_credentials,
            ssl_options = ssl_options,
            # heartbeat = 0
        )

        self.logger.info(f"Starting client, connecting to {conn['service-host']} ...")
        # We connect to RabbitMQ
        self.connection = pika.BlockingConnection(self.connection_parameters)
        # We create a channel (automatically managed py Pika)
        self.channel = self.connection.channel()
        if self.channel.is_open:
            self.logger.info(f"Channel \"{self.channel}\" declared and open.")
        else:
            self.logger.error(f"Channel \"{self.channel}\" declared but not open. error")
            exit(1)

        res = self.channel.queue_declare(queue = routing_key)

        # We need at list one RPCServer listening
        retry = 1
        while self.num_servers() == 0:
            self.logger.warning(f"[retry={retry}/{max_retry}] No RPC server is listening on queue {self.routing_key}, retrying in {timeout_retry} secondes ... ")
            time.sleep(timeout_retry)
            retry += 1
            if retry > max_retry:
                self.logger.critical(f"We did not find any RPC server listening on queue {self.routing_key}, aborting...")
                if self.channel.is_open:
                    self.channel.close()
                try:
                    self.connection.close()
                except pika.exceptions.StreamLostError as e:
                    pass
                exit(1)

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.logger.debug(f"Callback Queue \"{self.callback_queue}\" declared.")

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

    def num_servers(self) -> int:
        """ Return the number of RPC servers listening to routing key """
        if not self.queue_exists(self.routing_key):
            return 0
        result = self.channel.queue_declare(queue = self.routing_key, passive=True)
        return result.method.consumer_count

    def queue_exists(self, queue_name: str) -> bool:
        """ Return True if the queue exists. """
        try:
            self.channel.queue_declare(
                queue = queue_name,
                passive = True
            )
        except pika.exceptions.ChannelWrongStateError as e:
            return False
        except pika.exceptions.ChannelClosedByBroker as e:
            self.logger.debug(f"Trying to check if queue {queue_name}: {e}")
            if e.reply_code == 404:
                return False
            else:
                raise e
        except Exception as e:
            self.logger.debug(f"Trying to check if queue {queue_name}: {e}")
            return False
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
        if self.channel and not self.channel.is_closed:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, input: dict):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.channel.basic_publish(
                exchange='',
                routing_key = self.routing_key,
                properties = pika.BasicProperties(
                    reply_to = self.callback_queue,
                    correlation_id = self.corr_id,
                ),
                body = json.dumps(input)
        )
        self.connection.process_data_events(time_limit = None)
        return json.loads(self.response)
