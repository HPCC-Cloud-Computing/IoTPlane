import json
from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue
from kombu.utils.compat import nested
from influxdb import InfluxDBClient
import sys
import logging


class Dbreader:
    def __init__(self, broker_cloud, host_influxdb):
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

        self.clientDB = InfluxDBClient(host_influxdb, 8086, 'root', 'root', 'Collector_DB')
        self.clientDB.create_database('Collector_DB')

        self.producer_connection = Connection(broker_cloud)
        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")

    def api_get_metric(self, body, message):
        self.logger.info("API get metric")
        list_metric_id = json.loads(body)['body']["list_metric_id"]
        reply_to = json.loads(body)['header']['reply_to']
        metrics = self.get_data_metric(list_metric_id)
        message_response = {
            'header': {},
            'body': {
                "metrics": metrics
            }
        }
        self.publish_messages(message_response, self.producer_connection, reply_to, self.exchange)

    def api_get_metric_history(self, body, message):
        self.logger.info("API get metric history")
        list_metric_id = json.loads(body)['body']["list_metric_id"]
        reply_to = json.loads(body)['header']['reply_to']
        start_time = json.loads(body)['body']["start_time"]
        end_time = json.loads(body)['body']["end_time"]
        scale = json.loads(body)['body']["scale"]
        metrics = self.get_data_metric_history(list_metric_id, start_time, end_time, scale)
        message_response = {
            'header': {},
            'body':{
                "metrics": metrics
            }
        }

        self.publish_messages(message_response, self.producer_connection, reply_to, self.exchange)

    def get_data_metric(self, list_metric_id):
        metrics = []
        for metric_id in list_metric_id:
            query_statement = 'SELECT * FROM \"' + metric_id + '\" ORDER BY time DESC LIMIT 1 '
            query_result = self.clientDB.query(query_statement)

            for data_points in query_result:
                metric = {
                    'MetricId': metric_id,
                    'DataPoint': {
                        "DataType": data_points[0]['DataType'],
                        "Value": data_points[0]['Value'],
                        "TimeCollect": data_points[0]['time']
                    }
                }
                metrics.append(metric)
        # print("metrics: {}".format(metrics))
        return metrics

    def get_data_metric_history(self, list_metric_id, start_time, end_time, scale):
        metrics = []
        # print(scale)
        for metric_id in list_metric_id:
            query_statement_type_field = """SHOW FIELD KEYS ON \"Collector_DB\" FROM \"{}\"""".format(metric_id)
            query_result_type_field = self.clientDB.query(query_statement_type_field)
            type_field = list(query_result_type_field.get_points())[0]['fieldType']
            metric = {
                'MetricId': metric_id,
                'history': []
            }
            if scale == "0s":
                if type_field == 'integer' or type_field == 'float':
                    query_statement_history = """SELECT *  FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    query_statement_global = """SELECT MAX(\"Value\") AS max_state, MIN(\"Value\") AS min_state, MEAN(\"Value\") AS average_state FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_global = self.clientDB.query(query_statement_global)
                    query_result_global = list(query_result_global.get_points())
                    # print(query_result_global[0]['max_state'], query_result_global[0]['min_state'],
                    #       query_result_global[0]['average_state'])

                    if len(query_result_history) > 0 and len(query_result_global) > 0:

                        metric['max_global'] = query_result_global[0]['max_state']
                        metric['min_global'] = query_result_global[0]['min_state']
                        metric['average_global'] = query_result_global[0]['average_state']

                        for data_history in query_result_history:
                            point = {
                                'TimeCollect': data_history['time'],
                                'DataType': data_history['DataType'],
                                'Value': data_history['Value']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)
                else:

                    query_statement_history = """SELECT *  FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    if len(query_result_history) > 0:
                        for data_history in query_result_history:
                            point = {
                                'TimeCollect': data_history['time'],
                                'DataType': data_history['DataType'],
                                'Value': data_history['Value']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)
            else:
                if type_field == 'integer' or type_field == 'float':
                    query_statement_history = """SELECT MODE(\"Value\") AS mode_state, MAX(\"Value\") AS max_state, MIN(\"Value\") AS min_state, MEAN(\"Value\") AS average_state FROM \"{}\" WHERE time >= \'{}\' AND time <= \'{}\' GROUP BY time({})""".format(
                        metric_id, start_time, end_time, scale)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    query_statement_global = """SELECT MAX(\"Value\") AS max_state, MIN(\"Value\") AS min_state, MEAN(\"Value\") AS average_state FROM \"{}\" where time >= \'{}\' AND time <= \'{}\'""".format(
                        metric_id, start_time, end_time)
                    query_result_global = self.clientDB.query(query_statement_global)
                    query_result_global = list(query_result_global.get_points())

                    if len(query_result_history) > 0 and len(query_result_global) > 0:

                        metric['max_global'] = query_result_global[0]['max_state']
                        metric['min_global'] = query_result_global[0]['min_state']
                        metric['average_global'] = query_result_global[0]['average_state']

                        for data_history in query_result_history:
                            point = {
                                'min_value': data_history['min_state'],
                                'max_value': data_history['max_state'],
                                'average_value': data_history['average_state'],
                                'TimeCollect': data_history['time'],
                                'Value': data_history['mode_state']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)

                else:
                    query_statement_history = """SELECT MODE(\"Value\") AS mode_state FROM \"{}\" WHERE time >= \'{}\' AND time <= \'{}\' GROUP BY time({})""".format(
                        metric_id, start_time, end_time, scale)
                    query_result_history = self.clientDB.query(query_statement_history)
                    query_result_history = list(query_result_history.get_points())

                    if len(query_result_history) > 0:
                        for data_history in query_result_history:
                            point = {
                                'TimeCollect': data_history['time'],
                                'Value': data_history['mode_state']
                            }
                            metric['history'].append(point)
                    metrics.append(metric)

        return metrics

    def publish_messages(self, message, conn, queue_name, exchange, routing_key=None, queue_routing_key=None):
        self.logger.debug("Message publish to queue {}: {}".format(queue_name, message))
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

        queue_get_metric = Queue(name='dbreader.request.api_get_metric', exchange=self.exchange,
                                     routing_key='dbreader.request.api_get_metric', message_ttl=20)
        queue_get_metric_history = Queue(name='dbreader.request.api_get_metric_history', exchange=self.exchange,
                                             routing_key='dbreader.request.api_get_metric_history', message_ttl=20)
        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with nested(Consumer(self.consumer_connection, queues=queue_get_metric, callbacks=[self.api_get_metric],
                                     no_ack=True),
                            Consumer(self.consumer_connection, queues=queue_get_metric_history,
                                     callbacks=[self.api_get_metric_history], no_ack=True)):
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
        HOST_INFLUXDB = "localhost"
    else:
        BROKER_CLOUD = sys.argv[1]
        HOST_INFLUXDB = sys.argv[2]

    db_reader = Dbreader(BROKER_CLOUD, HOST_INFLUXDB)
    db_reader.run()