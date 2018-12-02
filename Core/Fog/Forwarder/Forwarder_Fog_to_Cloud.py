import paho.mqtt.client as mqtt
import json
from kombu import Connection, Queue, Exchange, Producer
import sys
import logging


class ForwarderFogToCloud:
    def __init__(self, broker_cloud, broker_fog):

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

        self.client_fog = mqtt.Client()
        self.client_fog.connect(broker_fog)

        self.rabbitmq_connection = Connection(broker_cloud)
        self.exchange = Exchange('IoT', type='direct')

    def on_message_registry(self, client, userdata, msg):
        self.logger.info("Forward from Driver to Registry: api_check_configuration_changes")
        message = json.loads(msg.payload.decode("utf-8"))  # vd: data = {"have_change": False, "now_info": [{}], "platform_id": "", "reply_to": ""}
        queue_name = message['header']['reply_to']
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    def on_message_filter(self, client, userdata, msg):
        self.logger.info('Forward from Filter to Collector vs Rule Engine: api_get_states')
        message = json.loads(msg.payload.decode("utf-8"))

        # publish to collector
        queue_name = message['header']['reply_to']
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

        # publish to rule_engine
        queue_rule = 'rule.request.states'
        self.publish_messages(message, self.rabbitmq_connection, queue_rule, self.exchange)

    def on_message_add_platform(self, client, userdata, msg):
        self.logger.info('Forward from Driver to Registry: api_add_platform')
        message = json.loads(msg.payload.decode('utf-8'))
        queue_name = "registry.request.api_add_platform"
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    def on_message_check_platform_active(self, client, userdata, msg):
        self.logger.info('Forward from Driver to Registry: check_platform_active')
        message = json.loads(msg.payload.decode('utf-8'))
        queue_name = message['header']['reply_to']
        self.publish_messages(message, self.rabbitmq_connection, queue_name, self.exchange)

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info("Connected to Broker Fog")
        self.client_fog.message_callback_add("driver/response/forwarder/api_check_configuration_changes", self.on_message_registry)
        self.client_fog.message_callback_add("filter/response/forwarder/api_get_states", self.on_message_filter)
        self.client_fog.message_callback_add("registry/request/api_add_platform", self.on_message_add_platform)
        self.client_fog.message_callback_add("driver/response/forwarder/api_check_platform_active", self.on_message_check_platform_active)

        self.client_fog.subscribe("driver/response/forwarder/api_check_configuration_changes")
        self.client_fog.subscribe("filter/response/forwarder/api_get_states")
        self.client_fog.subscribe("registry/request/api_add_platform")
        self.client_fog.subscribe("driver/response/forwarder/api_check_platform_active")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.logger.warning("Disconnected to Broker Fog.")

    def publish_messages(self, message, conn, queue_name, exchange, routing_key=None, queue_routing_key=None):
        self.logger.debug("message: {}".format(message))
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
        self.client_fog.on_disconnect = self.on_disconnect
        self.client_fog.on_connect = self.on_connect
        self.client_fog.loop_forever()


if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_CLOUD = 'localhost'  # rabbitmq
        BROKER_FOG = 'localhost'  # mosquitto

    else:
        BROKER_CLOUD = sys.argv[1]  #rabbitmq
        BROKER_FOG = sys.argv[2]    #mosquitto

    forwarder = ForwarderFogToCloud(BROKER_CLOUD, BROKER_FOG)
    forwarder.run()
