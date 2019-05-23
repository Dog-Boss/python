#!/usr/bin/env python
# coding: utf-8
"""
Created on 2018/1/23
@author: jiangrz
@description:
"""

import os
import sys
import re
import time
from datetime import datetime
import traceback
import logging
import logging.config
import argparse
import json
import signal
import random
import string

import pika
import msgpack
import pymqi
from pymqi import CMQC
from daemons import daemonizer

MQCNO_RECONNECT_Q_MGR = 0x04000000

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, "logs"))
MQFAIL_LOG_DIR = os.path.abspath(os.path.join(LOG_DIR, "mqfail"))

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

if not os.path.exists(MQFAIL_LOG_DIR):
    os.makedirs(MQFAIL_LOG_DIR)

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--name", help="mqproxy name", type=str, required=True)
parser.add_argument("-c", "--config", help="config file", type=argparse.FileType('r', 0), required=True)
parser.add_argument("-p", "--pidfile", help="pid file", type=str, required=True)
args = parser.parse_args()

pid_file = args.pidfile
cfg_file = args.config

app_config = json.load(cfg_file)
proxy_name = args.name

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[%(name)s][%(asctime)s][%(levelname)5s] - %(message)s'
        },
        'mq_fail': {
            'format': '%(asctime)s - %(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, 'mqproxy_%s.log' % proxy_name),
            'when': 'midnight',
            'interval': 1,
            'backupCount': 7,
        },
        'MQFail': {
            'level': 'INFO',
            'formatter': 'mq_fail',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(MQFAIL_LOG_DIR, '%s_fail.log' % proxy_name),
            'when': 'midnight',
            'interval': 1,
            'backupCount': 14,
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
        'MQFail': {
            'handlers': ['MQFail'],
            'level': 'INFO',
            'propagate': False
        }
    }
}
logging.config.dictConfig(LOGGING)

logger = logging.getLogger(proxy_name)
mqfail = logging.getLogger("MQFail")


class UndefinedEventType(Exception):
    pass


class MQEmitter(object):
    MQCNO_RECONNECT_Q_MGR = 0x04000000

    IMQ_URL = re.compile("imq://(?P<host>[^:]+):(?P<port>\d+)/(?P<q_mgr>\\w+)/(?P<channel>\w+)/\?q=(?P<queue>\w+)")

    def __init__(self, conf):
        self.mq_conf = conf
        # self.fail_logger = logging.getLogger(conf.get("fail_logger"))

        self.url = conf.get("url")
        m = MQEmitter.IMQ_URL.match(self.url)
        if m:
            self.conn_info = str("{0}({1})".format(m.group('host'), m.group('port')))
            self.q_mgr_name = str(m.group('q_mgr'))
            self.chl_name = str(m.group('channel'))
            self.q_name = str(m.group("queue"))
        else:
            raise NameError

        self._q_mgr = None

        self.put_counter = 0
        self.get_counter = 0
        self.queue = None

    def connect(self):
        cd = pymqi.CD()

        cd.ConnectionName = self.conn_info
        cd.ChannelName = self.chl_name
        cd.ChannelType = CMQC.MQCHT_CLNTCONN
        cd.TransportType = CMQC.MQXPT_TCP

        connect_options = CMQC.MQCNO_HANDLE_SHARE_BLOCK | self.MQCNO_RECONNECT_Q_MGR

        try:
            self._q_mgr = pymqi.QueueManager(None)
            self._q_mgr.connect_with_options(self.q_mgr_name, cd=cd, opts=connect_options)

            self.queue = pymqi.Queue(self._q_mgr, self.q_name)
            logger.info("IBM MQ connected (%s/?q=%s)", self.url, self.q_name)
        except Exception as exc:
            logger.warning("Fail to connect to Queue, error: %s", traceback.format_exc(exc))

    def close(self):
        if self.queue:
            try:
                self.queue.close()
            except Exception:
                pass

        try:
            if self._q_mgr and self._q_mgr.is_connected:
                self._q_mgr.disconnect()
                self._q_mgr = None

        except Exception as exc:
            logger.warning("[{}] Fail to close Queue, error: {}".format(self.__repr__(),
                                                                        traceback.format_exc(exc)))

    def emit(self, msg):
        put_opts = pymqi.PMO(Options=CMQC.MQPMO_NEW_MSG_ID +
                                     CMQC.MQPMO_NEW_CORREL_ID + CMQC.MQPMO_NO_SYNCPOINT)
        put_mds = pymqi.MD()
        put_mds.Format = "MQSTR"
        put_mds.Persistent = CMQC.MQPER_NOT_PERSISTENT

        def _escape_chr(s):
            def f(x):
                ret = x
                try:
                    if x in '\\;=&':
                        ret = "\\%s" % x
                    elif x not in string.printable:
                        ret = "\\%03d" % ord(x)
                except Exception:
                    pass

                return ret

            try:
                if type(s).__name__ not in ['str', 'unicode']:
                    return s
                r = "".join([f(c) for c in s])
            except Exception:
                r = s
                logger.error("fail to escape string: {}({}), error: {}".format(s, type(s), traceback.format_exc()))

            return r

        def pack_event(msg_in):
            status_type = msg_in.get("Acct-Status-Type")
            evh_timestamp = time.mktime(datetime.now().timetuple())
            evh_corr_id = random.randint(1, 65535)
            if status_type == 1:
                evh_event_type = 5
            elif status_type == 2:
                evh_event_type = 6
            else:
                logger.warning("[{}] Undefined Acct-Status-Type: {}".format(self.__repr__(), status_type))
                raise UndefinedEventType

            evh = "CORRELATION_ID=%d&DELIVER_MODE=0&EVENT_ID=#FIX#&EXPIRATION=0&" \
                  "NODE_ID=#FIX#&REDELIVERED=0&REPLY_TO=#FIX#&" \
                  "TIMESTAMP=%d&DECODE_TYPE=1&EVENT_TYPE=%d" % (
                      evh_corr_id,
                      evh_timestamp,
                      evh_event_type
                  )
            ev = '&'.join(['%s=%s' % (key, _escape_chr(value)) for (key, value) in msg_in.items()])
            return "{};{}".format(evh, ev)

        try:
            data = pack_event(msg)
            logger.debug("[{}] proxy msg: {}".format(self.__repr__(), data))
            try:
                self.queue.put(data, put_mds, put_opts)
            except Exception as exc:
                logger.warning(
                    "[{}] fail to put msg to queue, error: {}".format(
                        self.__repr__(),
                        traceback.format_exc())
                )
                mqfail.info(data)
        except UndefinedEventType:
            pass

    def __repr__(self):
        return "<{} {}?{}>".format(self.__class__.__name__, self.url, self.q_name)


class AcctConsumer(object):
    def __init__(self, config):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._frontend_cfg = config['mqproxy']['frontend']
        self._backend_cfg = config['mqproxy']['backend']
        self.name = proxy_name

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = self._frontend_cfg['url']
        self.exchange = self._frontend_cfg['exchange']
        self.routing_key = self._frontend_cfg['routing_key']
        self.queue_name = "{}::{}".format(self.exchange, self.name)
        self.emitter = MQEmitter(self._backend_cfg)

        self.last_report = datetime.now()
        self.put_counter = 0
        self.get_counter = 0

    def __repr__(self):
        return "AcctConsumer::{}".format(self.queue_name)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        logger.debug("Opening connection")
        self.add_on_connection_close_callback()
        self.open_channel()
        logger.info('Connection opened')

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self.emitter.close()
            self._connection.ioloop.stop()
        else:
            logger.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # stop MQEmitter client
        self.emitter.close()
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new MQEmitter connection
            self.emitter.connect()
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.debug("Opening channel")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_queue()
        logger.debug('Channel opened')

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        logger.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_queue(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info('Declaring queue %s', self.queue_name)
        self._channel.queue_declare(self.on_queue_declareok, exclusive=False, durable=True, queue=self.queue_name)
        return

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        if isinstance(self.routing_key, str) or isinstance(self.routing_key, unicode):
            logger.info('Binding <Exchange::%s> ==> <Queue::%s>(%s)',
                        self.exchange, self.queue_name, self.routing_key)
            self._channel.queue_bind(self.on_bindok,
                                     queue=self.queue_name,
                                     exchange=self.exchange,
                                     routing_key=self.routing_key)
        elif isinstance(self.routing_key, list) or isinstance(self.routing_key, tuple):
            for item in self.routing_key:
                logger.info('Binding <Exchange::%s> ==> <Queue::%s>(%s)',
                            self.exchange, self.queue_name, item)
                if isinstance(item, str) or isinstance(item, unicode):
                    self._channel.queue_bind(self.on_bindok,
                                             queue=self.queue_name,
                                             exchange=self.exchange,
                                             routing_key=item)
                else:
                    raise TypeError("item in routing_key list must be str")
        else:
            raise TypeError("routing_key must be one of [str, list, tuple]")

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        logger.info('Queue bound <Queue::%s> ==> <Exchange::%s>', self.queue_name, self.exchange)
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        logger.info('Start consuming with queue: %s', self.queue_name)
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queue_name)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        self.acknowledge_message(basic_deliver.delivery_tag)
        logger.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)

        try:
            # json_body = json.loads(body)
            # response_messages = self.callback_func(body)
            self.get_counter += 1
            msg = msgpack.unpackb(body, encoding='utf-8')
            logger.debug(
                "[{}] received message #{} from exchange {}: {}".format(
                    self.__repr__(),
                    basic_deliver.delivery_tag,
                    self.exchange,
                    msg))

            self.emitter.emit(msg)
            self.put_counter += 1
        except ValueError:
            logger.exception(
                "Invalid MsgPack received from exchange {}, dropping message, msg: {}".format(self.exchange, body))
        except Exception:
            # todo: better exception handling this is not good practice
            logger.exception(
                "Unexpected error, received from exchange %s, dropping message", self.exchange)

        t = datetime.now()
        td = t - self.last_report
        if td.total_seconds() >= 600:
            logger.info("MQProxy[%s] counter report: %d/%d (get/put) in last 600 sec(s)",
                        proxy_name,
                        self.get_counter,
                        self.put_counter)
            self.last_report = t
            self.put_counter = 0
            self.get_counter = 0

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        logger.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        try:
            self.emitter.connect()
            self._connection = self.connect()
            self._connection.ioloop.start()
        except Exception:
            logger.error("mqproxy error: {}".format(traceback.format_exc()))
            raise

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        logger.info('Stopped')

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        logger.info('Closing connection')
        self._connection.close()


if __name__ == '__main__':

    @daemonizer.run(pidfile=pid_file)
    def main():
        bot = AcctConsumer(app_config)

        def signal_handler(signum, frame):
            logger.info("stopped mqproxy::%s by signal %d", proxy_name, signum)
            if bot:
                bot.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            bot.run()
        except KeyboardInterrupt:
            bot.stop()
        except Exception:
            logger.warning("mqproxy running error: {}".format(traceback.format_exc()))
            sys.exit(-1)


    main()