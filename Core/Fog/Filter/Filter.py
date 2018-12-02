import json
import paho.mqtt.client as mqtt
import sys
import copy
import logging


class Filter:
    def __init__(self, broker_fog):
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

        self.client = mqtt.Client()
        self.client.connect(broker_fog)
        self.now_state = {}         # {global_id : value_state}

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info("Connected to Mosquitto")
        filter_topic_sub = 'driver/response/filter/api_get_states'
        self.client.subscribe(filter_topic_sub)

    def filter_message(self, client, userdata, msg):
        self.logger.info("Filter message before send to Collector")
        filter_topic_pub = 'filter/response/forwarder/api_get_states'
        message = json.loads(msg.payload.decode('utf-8'))
        self.logger.debug("Meassage before: {}".format(message))
        # self.client.publish(filter_topic_pub, json.dumps(message))

        received_state = message['body']['states']
        filter_states = copy.deepcopy(received_state)
        for state in received_state:
            if 'MetricId' in state:
                if state['MetricId'] in self.now_state:
                    if self.now_state[state['MetricId']] != state["DataPoint"]["Value"]:
                        # metric change state value
                        self.now_state[state['MetricId']] = state["DataPoint"]["Value"]
                    else:
                        # metric don't change state value
                        if message['header']['mode'] == 'PUSH':
                            filter_states.remove(state)
                else:
                    # metric has registered and this is the first time it is passed to filter
                    self.now_state[state['MetricId']] = state["DataPoint"]["Value"]
            else:
                # metric don't have MetricId =>> Metric hasn't registered
                filter_states.remove(state)
        if len(filter_states) > 0:
            message['body']['states'] = filter_states
            self.logger.debug("Message after: {}".format(message))
            self.client.publish(filter_topic_pub, json.dumps(message))
        else:
            self.logger.debug("Filter the messages and don't send to Collector ")

    def run(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.filter_message
        self.client.loop_forever()


if __name__ == '__main__':
    MODE_CODE = 'Develop'
    # MODE_CODE = 'Deploy'

    if MODE_CODE == 'Develop':
        BROKER_FOG = 'localhost'
    else:
        BROKER_FOG = sys.argv[1]

    filter_fog = Filter(BROKER_FOG)
    filter_fog.run()

