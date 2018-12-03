import json
import threading
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested
import sys
import logging
from datetime import datetime


class Collector():
    def __init__(self, broker_cloud, time_collect):
        # ----->configure logging <-----
        # if not os.path.exists('logging'):
        #     os.makedirs('logging')
        # handler = logging.handlers.RotatingFileHandler('logging/driver.log', maxBytes=200,
        #                               backupCount=1)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt='[%(asctime)s - %(levelname)s - %(name)s] - %(message)s',
                                      datefmt='%m-%d-%Y %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        # -----> end configure logging <-----

        self.time_collect = time_collect
        self.producer_connection = Connection(broker_cloud)
        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")
        self.list_platform_id = []

    def collect(self):
        # print("Collect the states of the devices")
        for platform_id in self.list_platform_id:
            self.collect_by_platform_id(platform_id)
        # threading.Timer(self.time_collect, self.collect).start()

    def collect_by_platform_id(self, platform_id):
        self.logger.info('Collect data from platform_id: ', str(platform_id))
        message_request = {
            'header': {
                'reply_to': 'driver.response.collector.api_get_states',
                'PlatformId': platform_id,
                'mode': 'PULL'
            }
        }

        queue_name = 'driver.request.api_get_states'
        self.publish_messages(message_request, self.producer_connection, queue_name, self.exchange)

    def handle_collect_by_platform_id(self, body, message):
        self.logger.info('Received state from platform_id: ', json.loads(body)['header']['PlatformId'])
        states = json.loads(body)['body']['states']
        self.logger.debug('State : {}'.format(states))
        points = []
        for state in states:
            points.append({
                "MetricId": state['MetricId'],
                "DataType": state['DataPoint']['DataType'],
                "Value": state['DataPoint']['Value'],
                "TimeCollect": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        message = {
            'header': {},
            'body': {
                'data_points': points
            }
        }
        queue_name = 'dbwriter.request.api_write_db'
        self.publish_messages(message, self.producer_connection, queue_name, self.exchange)

    def get_list_platforms(self):
        self.logger.info("Get list platforms from Registry")
        message = {
            "header": {
                'reply_to': 'registry.response.collector.api_get_list_platforms',
                'PlatformStatus': "active"
            }
        }

        queue_name = 'registry.request.api_get_list_platforms'
        self.publish_messages(message, self.producer_connection, queue_name, self.exchange)

    def handle_get_list(self, body, message):
        list_platforms = json.loads(body)['body']['list_platforms']
        temp = []
        for platform in list_platforms:
            temp.append(platform['PlatformId'])

        self.list_platform_id = temp
        self.collect()
        self.logger.info('Updated list of platform_id: ', str(self.list_platform_id))

    def handle_notification(self, body, message):
        self.logger.info('Have Notification')
        if json.loads(body)['notification'] == 'Have Platform_id change':
            self.get_list_platforms()

    def publish_messages(self, message, conn, queue_name, exchange, routing_key=None, queue_routing_key=None):
        self.logger.debug("Message publish to queue {}: {}".format(queue_name, message))
        if queue_routing_key is None:
            queue_routing_key = queue_name
        if routing_key is None:
            routing_key = queue_name

        queue_publish = Queue(name=queue_name, exchange=exchange, routing_key=queue_routing_key, message_ttl=20)

        conn.ensure_connection()
        with Producer(conn) as producer:
            producer.publish(
                json.dumps(message),
                exchange=exchange.name,
                routing_key=routing_key,
                declare=[queue_publish],
                retry=True
            )

    def run(self):
        queue_notification = Queue(name='collector.request.notification', exchange=self.exchange,
                                   routing_key='collector.request.notification', message_ttl=20)
        queue_list_platforms = Queue(name='registry.response.collector.api_get_list_platforms', exchange=self.exchange,
                                     routing_key='registry.response.collector.api_get_list_platforms', message_ttl=20)
        queue_get_states = Queue(name='driver.response.collector.api_get_states', exchange=self.exchange,
                                 routing_key='driver.response.collector.api_get_states', message_ttl=20)

        self.get_list_platforms()

        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with nested(Consumer(self.consumer_connection, queues=queue_notification, callbacks=[self.handle_notification],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_list_platforms, callbacks=[self.handle_get_list],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_states,
                                     callbacks=[self.handle_collect_by_platform_id], no_ack=True)):
                    while True:
                        self.consumer_connection.drain_events()
            except (ConnectionRefusedError, exceptions.OperationalError):
                self.logger.error('Connection to Broker Cloud is lost')
            except self.consumer_connection.connection_errors:
                self.logger.error('Connection to Broker Cloud is error')


if __name__ == '__main__':

    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = "localhost"
        MODE = "PULL"
        TIME_COLLECT = 5
    else:
        BROKER_CLOUD = sys.argv[1]
        TIME_COLLECT = int(sys.argv[3])

    collector = Collector(BROKER_CLOUD, TIME_COLLECT)
    collector.run()