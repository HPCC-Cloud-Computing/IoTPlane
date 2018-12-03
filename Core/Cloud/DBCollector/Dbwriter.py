from influxdb import InfluxDBClient
import json
from kombu import Connection, Consumer, Exchange, Queue, exceptions
import sys
from datetime import datetime
import logging


class DBwriter:
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

        self.consumer_connection = Connection(broker_cloud)
        self.exchange = Exchange("IoT", type="direct")

    def write_db(self, data_points):

        self.logger.debug("Received data points: {}".format(data_points))
        for point in data_points:
            record = [{
                'measurement': point['MetricId'],
                'tags': {
                    'DataType': point['DataType'],
                },
                'fields': {
                    'Value': point['Value'],
                },
                'time': point['TimeCollect']
            }]
            try:
                self.clientDB.write_points(record)
                self.logger.info("Updated Database")
            except:
                self.logger.error("Can't write to database : {}".format(point['MetricId']))
                self.clientDB.drop_measurement(measurement=point['MetricId'])
                self.logger.warning("Delete mesurement: {}".format(point['MetricId']))

    def api_write_db(self, body, message):
        data_points = json.loads(body)['body']['data_points']
        self.write_db(data_points)

    def run(self):
        queue_write_db = Queue(name='dbwriter.request.api_write_db', exchange=self.exchange,
                               routing_key='dbwriter.request.api_write_db', message_ttl=20)
        while 1:
            try:
                self.consumer_connection.ensure_connection(max_retries=1)
                with Consumer(self.consumer_connection, queues=queue_write_db, callbacks=[self.api_write_db], no_ack=True):
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

    db_writer = DBwriter(BROKER_CLOUD, HOST_INFLUXDB)
    db_writer.run()