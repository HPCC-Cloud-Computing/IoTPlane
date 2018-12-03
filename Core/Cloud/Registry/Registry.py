import json
import uuid
import time
import threading
import copy
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested
import sys
from Cloud.Registry.db_communicator import DbCommunicator
import logging


class Registry:
    def __init__(self, broker_cloud, mode, time_inactive_platform, time_update_conf, time_check_platform_active):
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

        self.time_update_conf = time_update_conf
        self.time_check_platform_active = time_check_platform_active
        self.time_inactive_platform = time_inactive_platform
        self.mode = mode
        self.dbcommunitor = DbCommunicator("Registry", "root", "root", "0.0.0.0")

        self.producer_connection = Connection(broker_cloud)
        self.consumer_connection = Connection(broker_cloud)

        self.exchange = Exchange("IoT", type="direct")

    def update_config_changes_by_platform_id(self, platform_id):

        message = {
            'header': {
                'reply_to': 'driver.response.registry.api_check_configuration_changes',
                'PlatformId': platform_id,
                'mode': 'PULL'
            }
        }

        queue_name = 'driver.request.api_check_configuration_changes'
        self.publish_messages(message, self.producer_connection, queue_name, self.exchange)

    def check_platform_active(self):
        while 1:
            queue_name = 'driver.request.api_check_platform_active'
            list_platforms = self.dbcommunitor.get_platforms(platform_status='all')
            # print("list_platform: {}".format(list_platforms))
            for platform in list_platforms:
                self.logger.info("Send check active message to platform: {}".format(platform['PlatformId']))
                message = {
                    'header': {
                        'PlatformId': platform['PlatformId'],
                        'reply_to': 'driver.response.registry.api_check_platform_active'
                    }
                }
                self.publish_messages(message, self.producer_connection, queue_name, self.exchange)
                if (time.time() - platform['LastResponse']) > self.time_inactive_platform and platform['PlatformStatus'] == 'active':
                    self.logger.info(" Change status of platform {} - {} to inactive".format(platform['PlatformName'], platform['PlatformId']))
                    platform['PlatformStatus'] = 'inactive'
                    sources = self.dbcommunitor.get_sources(platform_id=platform['PlatformId'], get_source_id_of_metric=True)
                    for source in sources:
                        source['information']['SourceStatus'] = 'inactive'
                        for metric in source['metrics']:
                            metric['MetricStatus'] = 'inactive'
                            self.dbcommunitor.update_metric(info_metric=metric)
                        self.dbcommunitor.update_info_source(info=source['information'])
                    self.dbcommunitor.update_platform(info_platform=platform)

                    self.send_notification_to_collector()

            time.sleep(self.time_check_platform_active)

    def update_changes_to_db(self, new_info, platform_id):
        # print("Update change of {} to database".format(platform_id))
        now_info = self.dbcommunitor.get_sources(platform_id=platform_id, source_status="all", metric_status="all")
        inactive_sources = copy.deepcopy(now_info)

        for new_source in new_info:
            info_new_source = copy.deepcopy(new_source['information'])
            if 'SourceId' in info_new_source:
                for now_source in now_info:
                    info_now_source = copy.deepcopy(now_source['information'])
                    if info_now_source["SourceId"] == info_new_source["SourceId"]:
                        info_new_source['SourceStatus'] = 'active'
                        # if info_now_source['SourceType'] == 'Thing':
                        #     if (info_now_source['EndPoint'] != info_new_source['EndPoint']
                        #             or info_now_source['Description'] != info_new_source['Description']
                        #             or info_now_source['Label'] != info_new_source['Label']
                        #             or info_now_source['ThingName'] != info_new_source['ThingName']):

                        self.dbcommunitor.update_info_source(info=info_new_source)

                        # elif info_now_source['SourceType'] == 'Platform':
                        #     if (info_now_source['EndPoint'] != info_new_source['EndPoint']
                        #             or info_now_source['Description'] != info_new_source['Description']
                        #             or info_now_source['Label'] != info_new_source['Label']
                        #             or info_now_source['PlatformName'] != info_new_source['PlatformName']
                        #             or info_now_source['PlatformType'] != info_new_source['PlatformType']):
                        #
                        #         self.dbcommunitor.update_info_source(info=info_new_source)

                        inactive_metrics = copy.deepcopy(now_source['metrics'])

                        for new_metric in new_source['metrics']:
                            if 'MetricId' in new_metric:
                                for now_metric in now_source['metrics']:
                                    if now_metric["MetricId"] == new_metric["MetricId"]:
                                        # if (now_metric["MetricName"] != new_metric["MetricName"]
                                        #         or now_metric["MetricType"] != new_metric["MetricType"]
                                        #         or now_metric['Unit'] != new_metric['Unit']
                                        #         or now_metric['MetricDomain'] != new_metric['MetricDomain']):
                                        # temp_metric = copy.deepcopy(new_metric)
                                        new_metric['MetricStatus'] = 'active'
                                        new_metric['SourceId'] = info_new_source['SourceId']
                                        self.dbcommunitor.update_metric(info_metric=new_metric)
                                        inactive_metrics.remove(now_metric)
                                        break
                            else:
                                # New metric
                                #temp_metric = copy.deepcopy(new_metric)
                                new_metric['SourceId'] = info_new_source['SourceId']
                                new_metric['MetricStatus'] = 'active'
                                new_metric['MetricId'] = str(uuid.uuid4())
                                self.dbcommunitor.update_metric(info_metric=new_metric, new_metric=True)

                        if len(inactive_metrics) != 0:
                            # Inactive metrics
                            for metric in inactive_metrics:
                                metric['SourceId'] = info_new_source['SourceId']
                                metric['MetricStatus'] = 'inactive'
                                self.dbcommunitor.update_metric(info_metric=metric)

                        inactive_sources.remove(now_source)
                        break
            else:
                # New Source
                new_source_id = str(uuid.uuid4())
                new_source['information']['SourceId'] = new_source_id
                new_source['information']['SourceStatus'] = 'active'
                self.dbcommunitor.update_info_source(info=new_source['information'], new_source=True)
                for metric in new_source['metrics']:
                    metric['SourceId'] = new_source_id
                    metric['MetricStatus'] = 'active'
                    metric['MetricId'] = str(uuid.uuid4())
                    self.dbcommunitor.update_metric(info_metric=metric, new_metric=True)

        if len(inactive_sources) != 0:
            # Inactive sources
            for source in inactive_sources:
                source['information']['SourceStatus'] = 'inactive'
                self.dbcommunitor.update_info_source(info=source['information'])
                for metric in source['metrics']:
                    metric['MetricStatus'] = 'inactive'
                    metric['SourceId'] = source['information']['SourceId']
                    self.dbcommunitor.update_metric(info_metric=metric)

    def handle_configuration_changes(self, body, message):
        header = json.loads(body)['header']
        body = json.loads(body)['body']

        platform_id = header['PlatformId']

        platform = self.dbcommunitor.get_platforms(platform_id=platform_id)[0]
        if platform['PlatformStatus'] == 'active':
            if body['is_change'] is False:
                # print('Platform have Id: {} no changes'.format(platform_id))
                if header['mode'] == "PULL":
                    new_info = body['new_info']
                    self.update_changes_to_db(new_info, platform_id)

            else:
                self.logger.info('Platform have Id: {} changed sources configuration'.format(platform_id))
                new_info = body['new_info']
                self.update_changes_to_db(new_info, platform_id)

        message = {
            'header': {
                'PlatformId': platform_id
            },
            'body': {
                'active_sources': self.dbcommunitor.get_sources(platform_id=platform_id, source_status='active', metric_status='active')
            }

        }
        queue_name = 'driver.request.api_update_now_configuration'
        self.publish_messages(message, self.producer_connection, queue_name, self.exchange)
        # print("now config: {}".format(message))

    def api_get_list_platforms(self, body, message):
        self.logger.info("API get list platform with platform_status")
        header = json.loads(body)['header']
        platform_status = header['PlatformStatus']
        queue_name = header['reply_to']

        message_response = {
            'header':{},
            'body': {}
        }
        message_response['body']['list_platforms'] = self.dbcommunitor.get_platforms(platform_status=platform_status)
        self.publish_messages(message_response, self.producer_connection, queue_name, self.exchange)

    def api_add_platform(self, body, message):
        header = json.loads(body)['header']
        body = json.loads(body)['body']

        message_response = {
            'header': {},
            'body': {}
        }

        platform_id = ""
        if header['registered'] is True:
            platform_id = header['PlatformId']
            self.logger.info("Platform {} - {} come back to system".format(body['PlatformName'], platform_id))
            info_platform = {
                "PlatformId": platform_id,
                "PlatformName": body['PlatformName'],
                "PlatformType": body['PlatformType'],
                "PlatformHost": body['PlatformHost'],
                "PlatformPort": body['PlatformPort'],
                "PlatformStatus": "active",
                "LastResponse": time.time()
            }
            self.dbcommunitor.update_platform(info_platform)

        else:
            self.logger.info("Add new Platform to system")
            platform_id = str(uuid.uuid4())
            self.logger.info('Generate id for {} platform : {}'.format(body['PlatformName'], platform_id))

            info_platform = {
                "PlatformId": platform_id,
                "PlatformName": body['PlatformName'],
                "PlatformType": body['PlatformType'],
                "PlatformHost": body['PlatformHost'],
                "PlatformPort": body['PlatformPort'],
                "PlatformStatus": "active",
                "LastResponse": time.time()
            }
            self.dbcommunitor.update_platform(info_platform, new_platform=True)

        sources = self.dbcommunitor.get_sources(platform_id=platform_id)
        # print(sources)
        message_response['header']['PlatformId'] = platform_id
        message_response['header']['PlatformHost'] = body['PlatformHost']
        message_response['header']['PlatformPort'] = body['PlatformPort']
        message_response['body']['sources'] = sources

        # check connection and publish message
        # queue_response = Queue(name='registry.response.driver.api_add_platform', exchange=self.exchange,
        #                        routing_key='registry.response.driver.api_add_platform', message_ttl=20)
        routing_key = 'registry.response.driver.api_add_platform'
        self.publish_messages(message_response, self.producer_connection, routing_key, self.exchange)

        self.send_notification_to_collector()

    def api_get_sources(self, body, message):
        self.logger.info('API get sources')
        message_received = json.loads(body)

        reply_to = message_received['header']['reply_to']
        platform_id = message_received['body']['PlatformId']
        source_id = message_received['body']['SourceId']
        metric_status = message_received['body']['MetricStatus']
        source_status = message_received['body']['SourceStatus']
        # print(message_received)
        message_response = {
            'body': {
                "sources": self.dbcommunitor.get_sources(platform_id=platform_id, source_id=source_id, source_status=source_status, metric_status=metric_status)
            }
        }

        self.publish_messages(message_response, self.producer_connection, reply_to, self.exchange)

    def handle_check_platform_active(self, body, message):

        header = json.loads(body)['header']
        body = json.loads(body)['body']
        platform_id = header['PlatformId']
        self.logger.debug("Handle message when platform {} response message check active".format(platform_id))
        if body['active'] is True:
            platform = self.dbcommunitor.get_platforms(platform_id=platform_id)[0]
            if platform['PlatformStatus'] == 'inactive':
                platform['PlatformStatus'] = 'active'
                self.update_config_changes_by_platform_id(platform['PlatformId'])
            platform['LastResponse'] = time.time()
            self.dbcommunitor.update_platform(info_platform=platform)

    def send_notification_to_collector(self):
        self.logger.info('Send notification to Collector')
        message = {
            'notification': 'Have Platform_id change'
        }

        queue_name = 'collector.request.notification'
        self.publish_messages(message, self.producer_connection, queue_name, self.exchange)

    def publish_messages(self, message, conn, queue_name, exchange, routing_key=None, queue_routing_key=None):
        self.logger.debug("message: {}".format(message))
        if queue_routing_key is None:
            queue_routing_key = queue_name
        if routing_key is None:
            routing_key = queue_name

        # queue_publish = Queue(name=queue_name, exchange=exchange, routing_key=queue_routing_key, message_ttl=20)

        conn.ensure_connection()
        with Producer(conn) as producer:
            producer.publish(
                json.dumps(message),
                exchange=exchange.name,
                routing_key=routing_key,
                retry=True
            )

    def run(self):

        queue_get_sources = Queue(name='registry.request.api_get_sources', exchange=self.exchange,
                                 routing_key='registry.request.api_get_sources', message_ttl=20)
        queue_get_list_platforms = Queue(name='registry.request.api_get_list_platforms', exchange=self.exchange,
                                         routing_key='registry.request.api_get_list_platforms', message_ttl=20)
        queue_add_platform = Queue(name='registry.request.api_add_platform', exchange=self.exchange,
                                   routing_key='registry.request.api_add_platform', message_ttl=20)
        queue_check_config = Queue(name='driver.response.registry.api_check_configuration_changes', exchange=self.exchange,
                                   routing_key='driver.response.registry.api_check_configuration_changes', message_ttl=20)
        queue_check_platform_active = Queue(name='driver.response.registry.api_check_platform_active', exchange=self.exchange,
                                            routing_key='driver.response.registry.api_check_platform_active', message_ttl=20)

        thread_check_active = threading.Thread(target=self.check_platform_active)
        thread_check_active.setDaemon(True)
        thread_check_active.start()

        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with nested(Consumer(self.consumer_connection, queues=queue_add_platform, callbacks=[self.api_add_platform],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_sources, callbacks=[self.api_get_sources],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_list_platforms,
                                     callbacks=[self.api_get_list_platforms], no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_check_config,
                                     callbacks=[self.handle_configuration_changes], no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_check_platform_active,
                                     callbacks=[self.handle_check_platform_active], no_ack=True)
                            ):
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
        BROKER_CLOUD = 'localhost'  # rabbitmq
        MODE = 'PULL'  # or PUSH or PULL

        dbconfig = {
            "database": "Registry",
            "user": "root",
            "host": '0.0.0.0',
            "passwd": "root",
            "autocommit": "True"
        }

        TIME_INACTIVE_PLATFORM = 60     # Time when platform is marked inactive
        TIME_UPDATE_CONF = 5            # Time when registry send request update conf to Driver
        TIME_CHECK_PLATFORM_ACTIVE = 5  # Time when check active_platform in system
    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        MODE = sys.argv[2] # or PUSH or PULL

        dbconfig = {
          "database": "Registry",
          "user":     "root",
          "host":     sys.argv[3],
          "passwd":   "root",
          "autocommit": "True"
        }

        TIME_INACTIVE_PLATFORM = int(sys.argv[4])
        TIME_UPDATE_CONF = int(sys.argv[5])
        TIME_CHECK_PLATFORM_ACTIVE = int(sys.argv[6])

    registry = Registry(BROKER_CLOUD, MODE, TIME_INACTIVE_PLATFORM, TIME_UPDATE_CONF, TIME_CHECK_PLATFORM_ACTIVE)
    registry.run()
