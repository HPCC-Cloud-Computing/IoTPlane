from mysql.connector.pooling import MySQLConnectionPool


class DbCommunicator:
    def __init__(self, db_name, db_user, password, host):
        db_config = {
          "database": db_name,
          "user":     db_user,
          "host":     host,
          "passwd":   password,
          "autocommit": "True"
        }

        # dbconfig = {
        #     "database": "Registry",
        #     "user": "root",
        #     "host": '0.0.0.0',
        #     "passwd": "root",
        #     "autocommit": "True"
        # }

        self.cnxpool = MySQLConnectionPool(pool_name="mypool", pool_size=32, **db_config)

    def get_connection_to_db(self):
        while True:
            try:
                # print("Get connection DB")
                connection = self.cnxpool.get_connection()
                return connection
            except:
                print("Can't get connection DB")
                pass

    def get_sources(self, platform_id=None, source_id=None, source_status=None, source_type=None, metric_status=None, get_metric=True, get_source_id_of_metric=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        have_where = False
        sources = []
        query_source = """SELECT SourceId, EndPoint, SourceStatus, Description, SourceType, Label, PlatformId, LocalId
                            FROM IoTSource"""

        if platform_id is not None:
            if have_where is False:
                query_source = query_source + """ WHERE PlatformId='{}'""".format(platform_id)
                have_where = True
            else:
                query_source = query_source + """ and PlatformId='{}'""".format(platform_id)

        if source_id is not None:
            if have_where is False:
                query_source = query_source + """ WHERE SourceId='{}'""".format(source_id)
                have_where = True
            else:
                query_source = query_source + """ and SourceId='{}'""".format(source_id)

        if source_type is not None:
            if have_where is False:
                query_source = query_source + """ WHERE SourceType='{}'""".format(source_type)
                have_where = True
            else:
                query_source = query_source + """ and SourceType='{}'""".format(source_type)

        if (source_status is not None) and (source_status in ['active', 'inactive']):
            if have_where is False:
                query_source = query_source + """ WHERE SourceStatus='{}'""".format(source_status)
                have_where = True
            else:
                query_source = query_source + """ and SourceStatus='{}'""".format(source_status)
        print(query_source)
        cursor_1.execute(query_source)
        rows_source = cursor_1.fetchall()
        for row_source in rows_source:
            source = {
                'information': {},
            }

            source['information']['SourceId'] = row_source[0]
            source['information']['EndPoint'] = row_source[1]

            if source_status is not None:
                source['information']['SourceStatus'] = row_source[2]

            source['information']['Description'] = row_source[3]
            source['information']['SourceType'] = row_source[4]
            source['information']['Label'] = row_source[5]
            source['information']['PlatformId'] = row_source[6]
            source['information']['LocalId'] = row_source[7]
            if source['information']['SourceType'] == "Thing":
                self.get_thing_info(source['information']['SourceId'], source)

            # elif source['information']['SourceType'] == "Platform":
            #     self.get_platform_info(source['information']['SourceId'], source)

            if get_metric is True:
                source['metrics'] = self.get_metrics(source_id=row_source[0], metric_status=metric_status, get_source_id=get_source_id_of_metric)

            sources.append(source)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        return sources

    def get_metrics(self, source_id=None, metric_status=None, get_source_id=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        have_where = False
        query_metric = """SELECT MetricId, MetricName, MetricType, Unit, MetricDomain, MetricStatus, SourceId, MetricLocalId
                            FROM Metric"""

        if source_id is not None:
            if have_where is False:
                query_metric = query_metric + """ WHERE SourceId='{}'""".format(source_id)
                have_where = True
            else:
                query_metric = query_metric + """ and SourceId='{}'""".format(source_id)

        if (metric_status is not None) and (metric_status in ['active', 'inactive']):
            if have_where is False:
                query_metric = query_metric + """ WHERE MetricStatus='{}'""".format(metric_status)
                have_where = True
            else:
                query_metric = query_metric + """ and MetricStatus='{}'""".format(metric_status)

        cursor_1.execute(query_metric)
        rows_metric = cursor_1.fetchall()
        metrics = []
        for row_metric in rows_metric:
            metric = {}
            metric['MetricId'] = row_metric[0]
            metric['MetricName'] = row_metric[1]
            metric['MetricType'] = row_metric[2]
            metric['Unit'] = row_metric[3]
            metric['MetricDomain'] = row_metric[4]
            if metric_status is not None:
                metric['MetricStatus'] = row_metric[5]
            if get_source_id is True:
                metric['SourceId'] = row_metric[6]

            metric['MetricLocalId'] = row_metric[7]

            metrics.append(metric)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        return metrics

    def get_thing_info(self, source_id, source_info):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        cursor_1.execute("""SELECT ThingName
                            FROM Thing  WHERE ThingGlobalId=%s""", (str(source_id),))
        thing_info = cursor_1.fetchone()
        source_info['information']['ThingName'] = thing_info[0]
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def update_info_source(self, info, new_source=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()

        source_id = info['SourceId']
        endpoint = info['EndPoint']
        source_status = info['SourceStatus']
        description = info['Description']
        source_type = info['SourceType']
        label = info['Label']
        platform_id = info['PlatformId']
        local_id = info['LocalId']

        if new_source is False:

            cursor_1.execute("""UPDATE IoTSource SET EndPoint=%s, SourceStatus=%s, Description=%s, Label=%s, LocalId=%s  WHERE SourceId=%s""",
                             (endpoint, source_status, description, str(label), local_id, str(source_id)))

            if source_type == "Thing":
                cursor_1.execute("""UPDATE Thing SET ThingName=%s  WHERE ThingGlobalId=%s""",
                    (info['ThingName'], str(source_id)))
            # elif source_type == "Platform":
            #     cursor_1.execute("""UPDATE Platform SET PlatformName=%s, PlatformType=%s WHERE PlatformId=%s""",
            #         (info['PlatformName'], info['PlatformType'], str(source_id)))

        else:
            cursor_1.execute("""INSERT INTO IoTSource VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                             (source_id, endpoint, source_status, description, source_type, str(label), str(platform_id), local_id))

            if source_type == "Thing":
                cursor_1.execute("""INSERT INTO Thing VALUES (%s,%s)""",
                                 (source_id, info['ThingName']))
            # elif source_type == "Platform":
            #     cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s)""",
            #                      (source_id, info['PlatformName'], info['PlatformType']))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def get_platforms(self, platform_id = None, platform_status = None):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        query_statement = """SELECT PlatformId, PlatformName, PlatformType, PlatformHost, PlatformPort, PlatformStatus, LastResponse FROM Platform"""

        have_where = False
        platforms = []

        if platform_id is not None:
            if have_where is False:
                query_statement = query_statement + """ WHERE PlatformId='{}'""".format(platform_id)
                have_where = True
            else:
                query_statement = query_statement + """ and PlatformId='{}'""".format(platform_id)

        if (platform_status is not None) and (platform_status in ['active', 'inactive']):
            if have_where is False:
                query_statement = query_statement + """ WHERE PlatformStatus='{}'""".format(platform_status)
                have_where = True
            else:
                query_statement = query_statement + """ and PlatformId='{}'""".format(platform_status)

        cursor_1.execute(query_statement)
        rows = cursor_1.fetchall()
        for row in rows:
            platform = {
                'PlatformId': row[0],
                'PlatformName': row[1],
                'PlatformType': row[2],
                'PlatformHost': row[3],
                'PlatformPort': row[4],
                'PlatformStatus': row[5],
                'LastResponse': row[6]
            }

            platforms.append(platform)
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()
        return platforms

    def update_platform(self, info_platform, new_platform=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        platform_id = info_platform['PlatformId']
        platform_name = info_platform['PlatformName']
        platform_type = info_platform['PlatformType']
        platform_host = info_platform['PlatformHost']
        platform_port = info_platform['PlatformPort']
        platform_status = info_platform['PlatformStatus']
        last_response = info_platform['LastResponse']

        if new_platform is False:
            cursor_1.execute("""UPDATE Platform SET PlatformName=%s, PlatformType=%s, PlatformHost=%s, PlatformPort=%s, PlatformStatus=%s, LastResponse=%s WHERE PlatformId=%s""",
                             (platform_name, platform_type, platform_host, platform_port, platform_status, last_response, platform_id))
        else:
            cursor_1.execute("""INSERT INTO Platform VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                             (platform_id, platform_name , platform_type, platform_host, platform_port, platform_status, last_response))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()

    def update_metric(self, info_metric, new_metric=False):
        cnx_1 = self.get_connection_to_db()
        cursor_1 = cnx_1.cursor()
        metric_id = info_metric['MetricId']
        source_id = info_metric['SourceId']
        metric_name = info_metric['MetricName']
        metric_type = info_metric['MetricType']
        unit = info_metric['Unit']
        metric_domain = info_metric['MetricDomain']
        metric_status = info_metric['MetricStatus']
        metric_local_id = info_metric['MetricLocalId']
        if new_metric is False:
            cursor_1.execute("""UPDATE Metric SET SourceId=%s, MetricName=%s, MetricType=%s, Unit=%s, MetricDomain=%s, MetricStatus=%s, MetricLocalId=%s
                                WHERE MetricId=%s""", (source_id, metric_name, metric_type, unit, metric_domain, metric_status, metric_local_id, metric_id))
        else:
            cursor_1.execute("""INSERT INTO Metric VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                             (metric_id, source_id, metric_name, metric_type, unit, metric_domain, metric_status, metric_local_id))
        cnx_1.commit()
        cursor_1.close()
        cnx_1.close()


# DbCommunicator("mysql", "root", "root", "0.0.0.0").test()
