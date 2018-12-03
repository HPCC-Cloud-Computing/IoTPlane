####################################################################################
######## This part for the docs of this program ####################################
'''
.
. 	thing_global_id = platform_id + thing_local_id
. 	item_global_id  = platform_id + thing_local_id + item_local_id
. 	A item without things seemly as a thing
.
'''

import paho.mqtt.client as mqtt
import json
from requests import get
import hashlib
from openhab import openHAB
import urllib
import numpy as np

import hashlib
import time
from Fog.Driver.Driver_Base import Driver
# from Driver_Base import Driver


class OpenHAB(Driver):
    def __init__(self, config_path, time_push):
        Driver.__init__(self, config_path, time_push)

        base_url = "http://" + self.host + ":" + self.port + "/rest"
        self.openhab = openHAB(base_url)

    def get_things_from_openhab(self):
        while True:
            try:
                thing_url = "http://" + self.host + ":" + self.port + "/rest/things"
                things = get(thing_url).json()
                return things
            except:
                self.logger.error("Error connect to Platform")
                time.sleep(2)

    def get_states(self):
        things = self.get_things_from_openhab()
        # print("STATES: {}".format(things))
        states = []
        item_of_thing_list = []

        # Get all items in openHAB
        items = self.openhab.fetch_all_items()  # dict of openHAB items
        # items = items.items()                   # convert dict into list
        # items = np.asarray(items)

        # items_list = list(items[:, 0])
        items_list = list(items)
        # print (items_list)

        # Get all things and items of things in openHAB
        for thing in things:
            thing_type = thing["thingTypeUID"]
            thing_name = thing["label"]
            thing_local_id = thing["UID"]
            thing_global_id = self.platform_id + "-" + thing_local_id
            thing_location = thing["location"]
            linked_items = thing["channels"]
            items_state = []

            for linked_item in linked_items:
                item_type = linked_item["itemType"]
                item_name = linked_item["linkedItems"][0]
                item_of_thing_list.append(item_name)  # Get all item of things.
                item_local_id = item_name
                item_url = "http://" + self.host + ":" + self.port + "/rest/items?recursive=false"
                item = self.openhab.get_item(item_name)
                # item_state = item.state
                # if item_type == "Number":
                #     item_state = int(item['state'])
                # else:
                #     item_state = str(item['state'])

                metric_local_id = linked_item["itemType"]
                detect_value = self.detect_data_type(item['state'])
                value_detected = detect_value[1]
                data_type_detected = detect_value[0]

                if metric_local_id in self.now_metric_domain:
                    mapped = self.mapping_data_value(self.now_metric_domain[metric_local_id], value_detected, data_type_detected)
                    try:
                        data_type_mapped = mapped[1]
                        value_mapped = mapped[0]
                    except:
                        data_type_mapped = data_type_detected
                        value_mapped = value_detected

                    states.append({
                        "MetricLocalId": metric_local_id,
                        "DataPoint": {
                            "DataType": data_type_mapped,
                            "Value": value_mapped
                        }
                    })

        # Get items not belong thing
        item_of_thing_list = np.asarray(item_of_thing_list)
        # print (items_list)
        # print (item_of_thing_list)

        if (item_of_thing_list != []):
            remain_item_list = list(set(items_list) - set(item_of_thing_list))
        else:
            remain_item_list = list(set(items_list))

        # Those items above be converted to things
        for item_to_thing in remain_item_list:
            item = self.openhab.get_item_raw(item_to_thing)
            # print("STATES ITEM: {}".format(item))

            item_type = item['type']
            item_name = item['name']
            item_local_id = item_name
            thing_name = item_name
            # # item_state = item['state']
            # if (item_type == "Number"):
            #     item_state = int(item['state'])
            # else:
            #     item_state = str(item['state'])
            # # transfer type

            # thing_type = item_type
            # thing_local_id = thing_name
            # thing_global_id = self.platform_id + '-' + thing_local_id
            # item_global_id = self.platform_id + '-' + thing_local_id + '-' + item_local_id

            metric_local_id = item_local_id
            detect_value = self.detect_data_type(item['state'])
            value_detected = detect_value[1]
            data_type_detected = detect_value[0]

            if metric_local_id in self.now_metric_domain:
                mapped = self.mapping_data_value(self.now_metric_domain[metric_local_id], value_detected,
                                                 data_type_detected)
                try:
                    data_type_mapped = mapped[1]
                    value_mapped = mapped[0]
                except:
                    data_type_mapped = data_type_detected
                    value_mapped = value_detected

                states.append({
                    "MetricLocalId": metric_local_id,
                    "DataPoint": {
                        "DataType": data_type_mapped,
                        "Value": value_mapped
                    }
                })
        return states

    def check_configuration_changes(self):
        # print('Check for changes')
        item_of_thing_list = []
        new_info = []

        things = self.get_things_from_openhab()
        # print (things)


        # Get all items in openHAB
        items = self.openhab.fetch_all_items()  # dict of openHAB items
        #items = items.items()  # convert dict into list
        #items = np.asarray(items)
        #print (items)

        # items_list = list(items[:, 0])
        items_list = list(items)

        for thing in things:
            thing_type = thing["thingTypeUID"]
            thing_name = thing["label"]
            thing_local_id = thing["UID"]
            linked_items = thing["channels"]
            metrics = []

            for linked_item in linked_items:
                item_type = linked_item["itemType"]
                item_name = linked_item["linkedItems"][0]
                # item_local_id  = linked_item["uid"]
                item_of_thing_list.append(item_name)  # Get all item of things.
                item_local_id = item_name
                # item_global_id = self.platform_id + "-" + thing_local_id + "-" + item_local_id
                # item_url = "http://" + self.host + ":" + self.port + "/rest/items?recursive=false"
                item_state = item.state
                item = self.openhab.get_item(item_name)
                value = self.detect_data_type(item_state)[1]
                sentence = item_name + " " + item_type
                metric_domain = self.detect_metric_domain(sentence, value)
                metrics.append(
                    {
                        'MetricType': self.metric_domain_file[metric_domain]['metric_type'],
                        'MetricName': str(item_name),
                        'MetricLocalId': str(item_local_id),
                        'Unit': "unknown",
                        'MetricDomain': metric_domain
                    }
                )

            thing_temp = {
                'information':{
                    'SourceType': "Thing",
                    'ThingName': str(thing_name),
                    'LocalId': "thing-" + str(thing_local_id),
                    'PlatformId': str(self.platform_id),
                    "Label": str({
                        "thing_local_type": thing_type
                    }),
                    "Description": "",
                    "EndPoint": "http://" + self.host + ":" + self.port + "/rest"
                },
                'metrics': metrics
            }
            new_info.append(thing_temp)

        if len(item_of_thing_list) != 0:
            remain_item_list = list(set(items_list) - set(item_of_thing_list))
        else:
            remain_item_list = list(set(items_list))
        # print (remain_item_list)

        # Those items above be converted to things
        for item_to_thing in remain_item_list:
            item = self.openhab.get_item_raw(item_to_thing)
            print("Item: {}".format(item))
            item_type = item['type']
            item_name = item['name']
            thing_name = item_name
            item_state = item['state']
            item_local_id = item_name
            thing_local_id = thing_name
            thing_type = item_type
            value = self.detect_data_type(item_state)[1]
            sentence = item_name + " " + item_type
            metric_domain = self.detect_metric_domain(sentence, value)
            metrics = [
                {
                    'MetricType': self.metric_domain_file[metric_domain]['metric_type'],
                    'MetricName': str(item_name),
                    'MetricLocalId': str(item_local_id),
                    'Unit': "unknown",
                    'MetricDomain': metric_domain
                }
            ]

            thing_temp = {
                'information':{
                    'SourceType': "Thing",
                    'ThingName': str(thing_name),
                    'LocalId': "thing-" + str(thing_local_id),
                    'PlatformId': str(self.platform_id),
                    "Label": str({
                        "thing_local_type": thing_type
                    }),
                    "Description": "",
                    "EndPoint": "http://" + self.host + ":" + self.port + "/rest"
                },
                'metrics': metrics
            }

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

    def set_state(self, metric_local_id, metric_name, metric_domain, new_value):
        # print("SET STATE {} to {}".format(metric_local_id, new_value))
        try:
            item = self.openhab.get_item(metric_local_id)
            if isinstance(new_value, str):
                item.command(new_value.upper())
        except:
            self.logger.error("Don't support {} set new_value: {}".format(metric_name, new_value))




if __name__ == '__main__':
    CONFIG_PATH = "config/openhab.ini"
    MODE = 'PULL'
    TIME_PUSH = 5
    openHAB = OpenHAB(CONFIG_PATH, TIME_PUSH)
    openHAB.run()