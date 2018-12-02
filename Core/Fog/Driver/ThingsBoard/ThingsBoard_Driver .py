import http.client
import json
import hashlib
from Fog.Driver.Driver_Base import Driver
import time


class ThingsBoard(Driver):
    def __init__(self, config_path, time_push):
        Driver.__init__(self, config_path, time_push)

    def get_authorization(self):
        conn = http.client.HTTPConnection(self.host + ':' + self.port)
        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json"
        }

        body = '{"username":"tenant@thingsboard.org", "password":"tenant"}'

        conn.request("POST", "/api/auth/login", body=body, headers=headers)
        response_data = conn.getresponse().read()
        response_json = json.loads(response_data.decode("utf-8"))

        return response_json

    def connect(self):
        while True:
            try:
                conn = http.client.HTTPConnection(self.host + ':' + self.port)
                authorization = self.get_authorization()
                headers = {
                    'Accept': "application/json",
                    'X-Authorization': "Bearer " + authorization["token"],
                }
                return [conn, headers]
            except:
                self.logger.error("Error connect to Platform")
                time.sleep(2)
                continue

    def get_list_device_on_customes(self):
        # print("get list")
        result = self.connect()
        conn = result[0]
        headers = result[1]

        conn.request("GET", "/api/customer/3f4fd570-4ed4-11e8-a082-9dc4b7fcfa12/devices?limit=111", headers=headers)
        data = conn.getresponse().read()
        json_data = json.loads(data.decode("utf-8"))
        device_list = json_data['data']

        return device_list

    def get_access_token_device(self, thing_local_id):
        # print("Get Access token device: ")
        result = self.connect()
        conn = result[0]
        headers = result[1]

        conn.request("GET", "/api/device/" + thing_local_id + "/credentials", headers=headers)
        data = conn.getresponse().read()
        json_data = json.loads(data.decode("utf-8"))

        return json_data['credentialsId']

    def get_telemetry_keys(self, thing_local_id):
        telemetries = []

        result = self.connect()
        conn = result[0]
        headers = result[1]

        url = "/api/plugins/telemetry/DEVICE/" + thing_local_id + "/keys/timeseries"
        conn.request("GET", url, headers=headers)
        data = conn.getresponse().read()
        json_data = json.loads(data.decode("utf-8"))

        for i, telemetry in enumerate(json_data):
            telemetries.append(telemetry)

        return [json_data, ",".join(sorted(telemetries))]

    def get_dashboard_id_on_customes_id(self, customers_id):
        result = self.connect()
        conn = result[0]
        headers = result[1]

        conn.request("GET", "/api/customer/" + customers_id + "/dashboards?ascOrder=false&limit=111", headers=headers)
        data = conn.getresponse().read()
        json_data = json.loads(data.decode("utf-8"))

        return json_data['data'][0]['id']['id']

    def get_label_on_dashboard_id(self):
        result = self.connect()
        conn = result[0]
        headers = result[1]

        dashboard_id = self.get_dashboard_id_on_customes_id("3f4fd570-4ed4-11e8-a082-9dc4b7fcfa12")
        conn.request("GET", "/api/dashboard/" + dashboard_id, headers=headers)
        data = conn.getresponse().read()
        json_data = json.loads(data.decode("utf-8"))
        #print("JSON_DASHBOARD: {}".format(json_data))
        return json_data["configuration"]["widgets"]["0c6413aa-8860-50e4-6eb8-935d21a1eacc"]["config"]["settings"]["gpioList"]

    def get_states(self):
        # print("get states")
        states = []
        device_list = self.get_list_device_on_customes()

        result = self.connect()
        conn = result[0]
        headers = result[1]

        for device in device_list:
            result_telemetry_keys = self.get_telemetry_keys(device["id"]["id"])
            keys_telemetry_list = result_telemetry_keys[0]
            telemetries = result_telemetry_keys[1]

            url = "/api/plugins/telemetry/DEVICE/" + device["id"]["id"] + "/values/timeseries?keys=" + telemetries

            conn.request("GET", url, headers=headers)
            response_data = conn.getresponse().read()
            response_json = json.loads(response_data.decode("utf-8"))
            print(response_json)

            for telemetry in keys_telemetry_list:
                item_state = response_json[telemetry][0]["value"]
                item_name = telemetry

                metric_local_id = device["id"]["id"] + '-' + item_name
                detect_value = self.detect_data_type(item_state)
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
        new_info = []
        device_list = self.get_list_device_on_customes()
        print("list: {}".format(device_list))
        result = self.connect()
        conn = result[0]
        headers = result[1]
        gpio_list = self.get_label_on_dashboard_id()
        for device in device_list:
            result_telemetry_keys = self.get_telemetry_keys(device["id"]["id"])
            keys_telemetry_list = result_telemetry_keys[0]
            telemetries = result_telemetry_keys[1]

            url = "/api/plugins/telemetry/DEVICE/" + device["id"]["id"] + "/values/timeseries?keys=" + telemetries

            conn.request("GET", url, headers=headers)
            response_data = conn.getresponse().read()
            response_json = json.loads(response_data.decode("utf-8"))
            thing_temp = {
                'information':{
                    'ThingName': device["name"],
                    'PlatformId': str(self.platform_id),
                    'LocalId': "thing-" + device["id"]["id"],
                    'EndPoint': "/api/plugins/telemetry/DEVICE/" + device["id"]["id"],
                    'Label': str({
                        'thing_local_type': device["type"]
                    }),
                    'Description': "",
                    'SourceType': "Thing"
                },
                'metrics': []
            }
            metrics = []
            for telemetry in keys_telemetry_list:
                item_name = telemetry
                for gpio in gpio_list:
                    # print(type(gpio['pin']))
                    if str(gpio['pin']) == telemetry:
                        item_name = gpio['label']

                item_state = response_json[telemetry][0]["value"]
                value = self.detect_data_type(item_state)[1]
                sentence = item_name + " " + device["type"]
                metric_domain = self.detect_metric_domain(sentence, value)

                metric = {
                    "MetricType": self.metric_domain_file[metric_domain]['metric_type'],
                    'MetricName': item_name,
                    'MetricLocalId': device["id"]["id"] + '-' + telemetry,
                    "Unit": "unknown",
                    "MetricDomain": metric_domain
                }
                metrics.append(metric)

            thing_temp['metrics'] = metrics
            new_info.append(thing_temp)

        # print("new_info: {}".format(new_info))
        # print("now_info: {}".format(self.now_info))

        hash_now = hashlib.md5(str(self.ordered(new_info)).encode())
        hash_pre = hashlib.md5(str(self.ordered(self.now_info)).encode())
        #
        # print("new_info: {}".format(str(self.ordered(new_info))))
        # print("now_info: {}".format(str(self.ordered(self.now_info))))

        if hash_now.hexdigest() == hash_pre.hexdigest():
            self.logger.debug("Configuration don't change")
            return {
                'is_change': False,
                'new_info': new_info,
            }

        else:
            self.logger.debug("Configuration have change")
            # self.now_info = new_info
            return {
                'is_change': True,
                'new_info': new_info
            }

    def set_state(self, metric_local_id, metric_name, metric_domain, new_value):
        result = self.connect()
        conn = result[0]
        headers = result[1]

        if metric_domain == "switch":
            pin = metric_local_id.rsplit('-', 1)[1]
            device_id = metric_local_id.rsplit('-', 1)[0]
            # print("TACH : {}".format(metric_local_id.rsplit('-', 1)))
            # print("pin: {} device_id: {}".format(pin, device_id))
            if new_value == "on":
                body = '{"method":"setGpioStatus","params":{"pin":' + pin + ',"enabled":true}}'
                conn.request("POST", "/api/plugins/rpc/twoway/" + device_id, body=body, headers=headers)
            elif new_value == "off":
                body = '{"method":"setGpioStatus","params":{"pin":' + pin + ',"enabled":false}}'
                conn.request("POST", "/api/plugins/rpc/twoway/" + device_id, body=body, headers=headers)
            else:
                self.logger.error("Don't support type of new_value: {}".format(new_value))
        else:
            self.logger.error("Don't support {} set new_value: {}".format(metric_name, new_value))


if __name__ == '__main__':
    CONFIG_PATH = "config/thingsboard.ini"
    MODE = 'PULL'
    TIME_PUSH = 5
    things_board = ThingsBoard(CONFIG_PATH, TIME_PUSH)
    things_board.run()