from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
import json
import sys
import socket
import datetime
import os
from flask_cors import CORS
import paramiko


MODE_CODE = "Develop"
# MODE_CODE = "Deploy"

if MODE_CODE == "Deploy":
    BROKER_CLOUD = sys.argv[1]
else:
    BROKER_CLOUD = "0.0.0.0"


rabbitmq_connection = Connection(BROKER_CLOUD)
exchange = Exchange("IoT", type="direct")

my_path = os.path.dirname(__file__)
filename = os.path.join(my_path, '../../Semantic_Analysis/metric_domain.json')

with open(filename) as json_file:
    metric_domain_file = json.load(json_file)

#=====================================================
from flask import Flask, jsonify, request

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})


@app.route('/api/platforms', defaults={'platform_status': 'active'}, methods=['GET'])
@app.route('/api/platforms/<platform_status>', methods=['GET'])
def api_get_platforms(platform_status):
    return jsonify(get_list_platforms(platform_status))


@app.route('/api/sources', defaults={'source_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/sources/<source_status>/<metric_status>', methods=['GET'])
def api_get_sources(source_status, metric_status):
    return jsonify(get_sources(source_status=source_status, metric_status=metric_status))


@app.route('/api/source/<source_id>', methods=['GET'])
def api_get_sources_by_source_id(source_id):
    return jsonify(get_sources(source_id=source_id, source_status="all", metric_status="all"))


@app.route('/api/sources/platform_id/<platform_id>', defaults={'source_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/sources/platform_id/<platform_id>/<source_status>/<metric_status>', methods=['GET'])
def api_get_sources_platform_id(platform_id, source_status, metric_status):
    return jsonify(get_sources(platform_id=platform_id, source_status=source_status, metric_status=metric_status))


@app.route('/api/history/sources/<start_time>/<end_time>', defaults={'source_status': 'active', 'metric_status': 'active', 'scale': '0s'}, methods=['GET'])
@app.route('/api/history/sources/<start_time>/<end_time>/<scale>', defaults={'source_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/history/sources/<source_status>/<metric_status>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_sources_history(source_status, metric_status, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_sources_history(start_time=start_time, end_time=end_time, scale=scale, source_status=source_status, metric_status=metric_status))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/history/metric/<metric_id>/<start_time>/<end_time>', defaults={'scale': '0s'}, methods=['GET'])
@app.route('/api/history/metric/<metric_id>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_metric_history_by_id(metric_id, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        list_metric_id = [metric_id]
        return jsonify(get_metric_state_history(list_metric_id, start_time, end_time, scale))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S',
                        'start_time': start_time,
                        'end_time': end_time,
                        'metric_id': metric_id})


@app.route('/api/history/source/<source_id>/<start_time>/<end_time>', defaults={'scale': '0s'}, methods=['GET'])
@app.route('/api/history/source/<source_id>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_sources_history_by_source_id(source_id, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_sources_history(start_time=start_time, end_time=end_time, scale=scale, source_id=source_id, source_status="all", metric_status="all"))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/history/sources/platform_id/<platform_id>/<start_time>/<end_time>', defaults={'source_status': 'active', 'metric_status': 'active', 'scale': '0s'}, methods=['GET'])
@app.route('/api/history/sources/platform_id/<platform_id>/<start_time>/<end_time>/<scale>', defaults={'source_status': 'active', 'metric_status': 'active'}, methods=['GET'])
@app.route('/api/history/sources/platform_id/<platform_id>/<source_status>/<metric_status>/<start_time>/<end_time>/<scale>', methods=['GET'])
def api_get_sources_history_by_platform_id(platform_id, source_status, metric_status, start_time, end_time, scale):
    try:
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return jsonify(get_sources_history(platform_id=platform_id, source_status=source_status, metric_status=metric_status, start_time=start_time, end_time=end_time, scale=scale))
    except ValueError:
        return jsonify({'error': 'Incorrect data format, should be %Y-%m-%d %H:%M:%S'})


@app.route('/api/metric', methods=['POST'])
def api_set_state():
    request_message = request.json
    print("NHAN SET_STATE: {}".format(request_message))
    source_id = request_message['SourceId']
    metric_id = request_message['MetricId']
    new_state = request_message['new_value']
    return jsonify(set_state(source_id, metric_id, new_state))


@app.route('/api/platform', methods=['POST'])
def api_add_platform():
    print("Hahhahah")
    request_message = request.json
    ssh_host = request_message['ssh_host']
    user_name = request_message['user_name']
    password = request_message['password']

    platform_host = request_message['platform_host']
    platform_port = request_message['platform_port']
    platform_name = request_message['platform_name']
    platform_type = request_message['platform_type']

    broker_fog = request_message['broker_fog']
    return jsonify(add_platform(ssh_host, user_name, password, platform_host, platform_port, platform_name, platform_type, broker_fog))



# prevent cached responses
@app.after_request
def add_header(response):

    response.headers["Cache-Control"] = "no-cache, no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


def add_platform(ssh_host, user_name, password, platform_host, platform_port, platform_name, platform_type, broker_fog):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        s.connect(ssh_host, 22, username=user_name, password=password, timeout=4)

    except:
        return {
            'error': 'Cannot ssh to host ' + ssh_host
        }

    config = ['[PLATFORM]\n',
              'host = ' + platform_host + '\n',
              'port = ' + platform_port + '\n',
              'platform_name = ' + platform_name + '\n',
              'platform_type = ' + platform_type + '\n',
              '[BROKER]\n',
              'host = ' + broker_fog + '\n']

    file = open('add_platform/OpenHab/config/openhab.ini', 'w')
    file.write(''.join(config))
    file.close()
    # # return {"ddd":"adsad"}
    # s.exec_command("mkdir /home/" + user_name + "/driver_" + platform_name, get_pty=True)
    #
    # sftp = s.open_sftp()
    # sftp.put('configuration_new_platform.ini', "/home/" + user_name + "/driver_" + platform_name + "/configuration"
    #                                                                                                ".ini")
    # sftp.close()

    # stdin, stdout, stderr = s.exec_command("docker run haiquan5396/homeassistant", get_pty=True)
    #
    # while not stdout.channel.exit_status_ready():
    #     print("wait finish command")
    #
    # s.exec_command("mkdir /home/" + user_name + "/driver_", get_pty=True)


    #
    # stdin, stdout, stderr = s.exec_command("docker ", get_pty=True)
    # stdin.write('raspberry\n')
    # stdin.flush()
    # deeata = stderr.readlines()
    # for line in deeata:
    #     print(line)
    # data = stdout.readlines()
    # for line in data:
    #     print(line)
    s.exec_command("mkdir /home/" + user_name + "/driver_", get_pty=True)
    s.exec_command("mkdir /home/" + user_name + "/driver_/config", get_pty=True)
    sftp = s.open_sftp()
    sftp.put('add_platform/OpenHab/Driver_Base.py', "/home/" + user_name + "/driver_/Driver_Base.py")
    sftp.put('add_platform/OpenHab/metric_domain.json', "/home/" + user_name + "/driver_/metric_domain.json")
    sftp.put('add_platform/OpenHab/Openhab_Driver.py', "/home/" + user_name + "/driver_/Openhab_Driver.py")
    sftp.put('add_platform/OpenHab/requirements.txt', "/home/" + user_name + "/driver_/requirements.txt")
    sftp.put('run_platform.sh', "/home/" + user_name + "/driver_/run_platform.sh")
    sftp.put('add_platform/OpenHab/config/openhab.ini', "/home/" + user_name + "/driver_/config/openhab.ini")

    sftp.close()
    stdin, stdout, stderr = s.exec_command("cd "+"/home/" + user_name + "/driver_ && " + "python3 /home/" + user_name + "/driver_/Openhab_Driver.py &&\n \n \n \n \n \n exit")
    # stdin, stdout, stderr = s.exec_command("/home/" + user_name + "/driver_/run_platform.sh", get_pty=True)
    # stdin.write('\n')
    # stdin.flush()
    # stdin.write('\n')
    # stdin.flush()
    # stdin.write('\n')
    # stdin.flush()
    # print("LOI")
    # deeata = stderr.readlines()
    # for line in deeata:
    #     print(line)
    # print("OUT")
    # data = stdout.readlines()
    # for line in data:
    #     print(line)
    print("Command done, closing SSH connection")
    s.close()
    return {
        "add_platform": "done"
    }



# add_platform("192.168.60.199", "pi", "raspberry", "platform_host", "platform_port", "platform_name", "platform_type", "broker_fog")



def get_sources_history(start_time, end_time, scale, source_status=None, metric_status=None, platform_id=None, source_id=None):
    print("API get things state history")
    try:
        list_sources_info = get_sources_info(source_status=source_status, metric_status=metric_status, platform_id=platform_id, source_id=source_id)
        list_sources_state = get_sources_state_history_by_list_thing(list_sources_info, start_time, end_time, scale)
        return list_sources_state
    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }

        return error


def get_sources_state_history_by_list_thing(list_sources_info, start_time, end_time, scale):
    print("get_things_state_history_by_list_thing")
    list_metric_id = []
    for source_info in list_sources_info:
        for metric_info in source_info['metrics']:
            list_metric_id.append(metric_info['MetricId'])

    list_metric_state = get_metric_state_history(list_metric_id, start_time, end_time, scale)
    # print(list_item_state[0])
    for metric_state in list_metric_state:
        for source_info in list_sources_info:
            for metric_info in source_info['metrics']:
                if metric_state['MetricId'] == metric_info['MetricId']:
                    metric_info['history'] = metric_state['history']
                    if 'max_global' in metric_state:
                        metric_info['max_global'] = metric_state['max_global']
                    if 'min_global' in metric_state:
                        metric_info['min_global'] = metric_state['min_global']
                    if 'average_global' in metric_state:
                        metric_info['average_global'] = metric_state['average_global']
                    break

    for source_info in list_sources_info:
        for metric_info in source_info['metrics']:
            if 'history' in metric_info:
                continue
            else:
                metric_info['history'] = []
    list_things_state = list_sources_info
    return list_things_state


def get_metric_state_history(list_metric_id, start_time, end_time, scale):
    message_request = {
        'header': {},
        'body':{
            'list_metric_id': list_metric_id,
            'start_time': start_time,
            'end_time': end_time,
            'scale': scale
        }

    }

    # request to api_get_things of Registry
    request_routing_key = 'dbreader.request.api_get_metric_history'
    message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)

    # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
    return message_response['body']['metrics']


def get_list_platforms(platform_status):
    print("API list platforms from Registry")

    if platform_status in ['active', "inactive", "all"]:
        message_request = {
            'header':{
                'PlatformStatus': platform_status
            }
        }

        # request to api_get_list_platform of Registry
        request_routing_key = 'registry.request.api_get_list_platforms'
        message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
        # print(message_response)
        if 'list_platforms' in message_response['body']:
            return message_response['body']['list_platforms']
        else:
            # have error
            return message_response
    else:
        return None


def get_sources_info(source_status=None, metric_status=None, platform_id=None, source_id=None):
    print("API get source info")
    if ((source_status in ["active", "inactive", "all"]) and (metric_status in ["active", "inactive", "all"])) or platform_id is not None or source_id is not None:
        message_request = {
            'header': {},
            'body': {
                "PlatformId": None,
                "SourceStatus": None,
                "MetricStatus": None,
                "SourceId": None,
            }
        }

        if (source_status in ["active", "inactive", "all"]) and (metric_status in ["active", "inactive", "all"]):
            message_request['body']['SourceStatus'] = source_status
            message_request['body']['MetricStatus'] = metric_status

        if platform_id is not None:
            message_request['body']['PlatformId'] = platform_id

        if source_id is not None:
            message_request['body']['SourceId'] = source_id
        # print(message_request)
        # request to api_get_things of Registry
        request_routing_key = 'registry.request.api_get_sources'
        message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
        # print(message_response)
        return message_response['body']['sources']
    else:
        return None


def get_sources(source_status=None, metric_status=None, platform_id=None, source_id=None):
    try:
        list_sources_info = get_sources_info(source_status=source_status, metric_status=metric_status, platform_id=platform_id, source_id=source_id)
        list_sources_state = get_sources_state_by_list_source(list_sources_info)
        return list_sources_state

    except (KeyError, IndexError):
        error = {
            'error': "Can not connect to service"
        }
        return error


def get_metric_state(list_metric_id):
    message_request = {
        'header': {},
        'body': {
            'list_metric_id': list_metric_id
        }
    }
    # request to api_get_things of Registry
    request_routing_key = 'dbreader.request.api_get_metric'
    message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
    # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
    return message_response['body']["metrics"]


def get_sources_state_by_list_source(list_sources_info):
    list_metric_id = []
    for source_info in list_sources_info:
        for metric_info in source_info['metrics']:
            list_metric_id.append(metric_info['MetricId'])

    list_metric_state = get_metric_state(list_metric_id)
    # print(list_item_state[0])
    for metric_state in list_metric_state:
        for source_info in list_sources_info:
            for metric in source_info['metrics']:
                if metric['MetricId'] == metric_state['MetricId']:
                    metric['DataPoint'] = metric_state["DataPoint"]
                    break

    list_things_state = list_sources_info
    return list_things_state


def set_state(source_id, metric_id, new_value):
    try:
        source = get_sources_info(source_id=source_id)[0]
    except:
        print("Wrong source_id")
        return
    for metric in source['metrics']:
        if metric['MetricId'] == metric_id:
            domain_name = metric["MetricDomain"]
            if metric_domain_file[domain_name]['can_set_value'] is True:
                value_domain = metric_domain_file[domain_name]["value"]
                if isinstance(value_domain, list):
                    if new_value in value_domain:
                        value_mapped = new_value
                    elif 'mapping' in metric_domain_file[domain_name]:
                        if new_value in metric_domain_file[domain_name]['mapping']:
                            value_mapped = metric_domain_file[domain_name]['mapping'][new_value]
                        else:
                            return "Don't have mapping value with {}".format(new_value)
                    else:
                        return "ERROR typedata"

                elif value_domain == "number":
                    if isinstance(new_value, int) or isinstance(new_value, float):
                        value_mapped = new_value
                    else:
                        return "ERROR typedata"

                message_request ={
                    'header': {
                        "PlatformId": source['information']['PlatformId']
                    },
                    'body': {
                        'information': source['information'],
                        'metric': metric,
                        'new_value': value_mapped
                    }
                }
                print("SET STATE: {}".format(message_request))
                request_routing_key = 'driver.request.api_set_state'
                # message_response = request_service(rabbitmq_connection, message_request, exchange, request_routing_key)
                # # message_response = {"items": [{'item_global_id': "", 'item_state': "", 'last_changed': ""}]}
                # return message_response
                rabbitmq_connection.ensure_connection()
                with Producer(rabbitmq_connection) as producer:
                    producer.publish(
                        json.dumps(message_request),
                        exchange=exchange.name,
                        routing_key=request_routing_key,
                        retry=True
                    )
                return "Public set state"

            else:
                return {
                    'error': 'MetricDomain can not set state'
                }


def request_service(conn, message_request, exchange_request, request_routing_key):
    id_response = uuid()
    queue_response = Queue(name=id_response, exchange=exchange_request, routing_key=id_response, exclusive=True, auto_delete=True)
    message_request['header']['reply_to'] = id_response
    conn.ensure_connection()
    with Producer(conn) as producer:
        producer.publish(
            json.dumps(message_request),
            exchange=exchange_request.name,
            routing_key=request_routing_key,
            declare=[queue_response],
            retry=True
        )

    message_response = None

    def on_response(body, message):
        nonlocal message_response
        message_response = json.loads(body)
    try:

        with Consumer(conn, queues=queue_response, callbacks=[on_response], no_ack=True):
            try:
                while message_response is None:
                    conn.drain_events(timeout=10)
            except socket.timeout:
                return {
                    'error': 'Can not connect to service'
                }
    except Exception:
        print("cannot create Consumer: " + request_routing_key)
        return {
            'error': 'Cannot create Consumer'
        }

    return message_response


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000, threaded=True)
