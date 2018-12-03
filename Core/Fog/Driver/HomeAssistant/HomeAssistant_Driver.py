import json
import requests
import hashlib
import time
from Fog.Driver.Driver_Base import Driver
import sys
import logging


class HomeAssistant(Driver):
    def __init__(self, config_path, time_push):

        # logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG, datefmt='%m-%d-%Y %H:%M:%S')
        Driver.__init__(self, config_path, time_push)

    def get_states(self):
        # self.logger.debug('Get state of all things')
        url = 'http://' + self.host + ':' + self.port + '/api/states'
        response = self.connect_platform(url)

        states = []

        for metric in response:

            thing_local_type = metric['entity_id'].split(".")[0]
            if thing_local_type != 'group' and thing_local_type != 'automation' and thing_local_type != 'updater':
                metric_local_id = metric['entity_id']
                detect_value = self.detect_data_type(metric['state'])
                value_detected = detect_value[1]
                data_type_detected = detect_value[0]
                if metric_local_id in self.now_metric_domain:
                    mapped = self.mapping_data_value(self.now_metric_domain[metric_local_id], value_detected, data_type_detected)
                    data_type_mapped = mapped[1]
                    value_mapped = mapped[0]

                    states.append({
                        "MetricLocalId": metric_local_id,
                        "DataPoint": {
                            "DataType": data_type_mapped,
                            "Value": value_mapped
                        }
                    })

        return states

    def check_configuration_changes(self):
        # self.logger.debug('Check for configuration')

        url = 'http://' + self.host + ':' + self.port + '/api/states'
        response = self.connect_platform(url)

        new_info = []

        for thing in response:

            thing_local_type = thing['entity_id'].split(".")[0]
            if thing_local_type != 'group' and thing_local_type != 'automation' and thing_local_type != 'updater':
                value = self.detect_data_type(thing['state'])[1]
                sentence = thing['attributes']['friendly_name'] + " " + thing_local_type
                metric_domain = self.detect_metric_domain(sentence, value)

                thing_temp = {
                    "information": {
                        "EndPoint": 'http://' + self.host + ':' + self.port + '/api/states',
                        "Description": "",
                        "SourceType": "Thing",

                        "Label": str({
                            "thing_local_type": thing_local_type
                        }),
                        "LocalId": "thing-" + thing['entity_id'],
                        "ThingName": thing['attributes']['friendly_name'],
                        "PlatformId": self.platform_id
                    },
                    "metrics": []
                }

                if 'unit_of_measurement' in thing['attributes']:
                    metric = {
                        "MetricName": thing['attributes']['friendly_name'],
                        "MetricType": self.metric_domain_file[metric_domain]['metric_type'],
                        "MetricLocalId": thing['entity_id'],
                        "Unit": thing['attributes']['unit_of_measurement'],
                        "MetricDomain": metric_domain
                    }
                else:
                    metric = {
                        "MetricName": thing['attributes']['friendly_name'],
                        "MetricType": self.metric_domain_file[metric_domain]['metric_type'],
                        "MetricLocalId": thing['entity_id'],
                        "Unit": "unknown",
                        "MetricDomain": metric_domain
                    }

                thing_temp['metrics'].append(metric)

                new_info.append(thing_temp)

        # print("new_info: {}".format(new_info))
        # print("now_info: {}".format(self.now_info))

        hash_now = hashlib.md5(str(self.ordered(new_info)).encode())
        hash_pre = hashlib.md5(str(self.ordered(self.now_info)).encode())

        # print("new_info: {}".format(new_info))
        # print("now_info: {}".format(self.now_info))

        if hash_now.hexdigest() == hash_pre.hexdigest():
            self.logger.debug("Configuration don't change")
            return {
                'is_change': False,
                'new_info': new_info,
            }

        else:
            self.logger.debug("Configuration have change")
            return {
                'is_change': True,
                'new_info': new_info
            }

    def connect_platform(self, url):
        while True:
            try:
                # url = 'http://' + host_homeAssistant + ':' + port_homeAssistant + '/api/states'
                response = requests.get(url).json()
                return response
            except:
                self.logger.error("Error connect to Platform")
                time.sleep(2)
                continue

    def set_state(self, metric_local_id, metric_name, metric_domain, new_value):
        if metric_domain == 'switch':
            if new_value == "on":
                url = 'http://' + self.host + ':' + self.port + '/api/services/light/turn_on'
                data = {"entity_id": metric_local_id}
                response = requests.post(url, json.dumps(data))
            else:
                url = 'http://' + self.host + ':' + self.port + '/api/services/light/turn_off'
                data = {"entity_id": metric_local_id}
                response = requests.post(url, json.dumps(data))
        else:
            self.logger.error("Don't support {} set new_value: {}".format(metric_name, new_value))


if __name__ == '__main__':
    CONFIG_PATH = "config/configuration.ini"
    # MODE = sys.argv[1]
    # TIME_PUSH = int(sys.argv[2])
    MODE = 'PULL'
    TIME_PUSH = 5
    home_assistant = HomeAssistant(CONFIG_PATH, TIME_PUSH)
    home_assistant.run()
