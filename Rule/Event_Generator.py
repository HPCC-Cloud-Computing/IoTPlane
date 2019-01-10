'''
    Use case:
        When any sensor send it's temperature value greater/smaller/... than a threshold
        then create an event
'''


from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested
import json
import MySQLdb
from Event_Generator_Base import Event_Generator_Base
from Event import Event
from random import *
from datetime import timedelta, datetime
import requests
import logging
import time 

class Event_Generator_1(Event_Generator_Base):
    list_event_trigger = []         # list_event_trigger saved as a mysql table: |condition_id | condition_name | condition |

    def __init__(self, event_generator_name, event_generator_id,
                 description, event_dest_topic):
        Event_Generator_Base.__init__(self, event_generator_name, event_generator_id,
                                      description, event_dest_topic)

        BROKER_CLOUD = "192.168.0.102"
        self.producer_connection = Connection("192.168.0.102")
        self.consumer_connection = Connection("192.168.0.102")
        self.exchange = Exchange("IoT", type="direct")
        self.api_get_source = "http://192.168.0.102:5000/api/sources"
        # self.queue_get_states = Queue(name='data_source.to.' + str(self.event_generator_name), exchange=self.exchange,
        #                          routing_key='data_source.to.' + str(self.event_generator_name))#, message_ttl=20)
        self.queue_get_states = Queue(name='rule.request.states', exchange=self.exchange,
                                      routing_key='rule.request.states', message_ttl=20)

        # Config logger
        #logging.basicConfig(level=logging.DEBUG)
        self.handler = logging.StreamHandler()
        self.formatter = logging.Formatter("[ %(asctime)s - %(levelname)s - %(name)s - line: %(lineno)d ]- %(message)s")
        self.handler.setFormatter(self.formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.DEBUG)
        self.logger.addHandler(self.handler)

        try:
            self.db_trigger = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root", db="Trigger_DB")
            self.cursor_trigger = self.db_trigger.cursor()
        except Exception as e:
            self.logger.error("Error connect to database", exc_info=True)
       

    def read_event_trigger(self):
        """Read all trigger in the database
        Output: List of triggers in database
        """
        self.logger.info("reading event trigger ...")
        request = "Select trigger_id, trigger_type, trigger_content from TriggerTable"

        try:
            # Execute the SQL command
            self.cursor_trigger.execute(request)
            result = self.cursor_trigger.fetchall()
            self.list_event_trigger = result
            # Commit your changes in the database
            self.db_trigger.commit()

            return self.list_event_trigger
        except Exception as e:
            # Rollback in case there is any error
            self.db_trigger.rollback()
            self.logger.error("Read trigger error", exc_info=True)
            return None

        self.logger.info("Finish reading event trigger!")

    # def check_trigger_item_state_change(self, trigger_id, item_id):
    #     print ("checking trigger item state change ...")
    #     request = "Select item_state from ItemTable where item_id='" + str(item_id) + "' order by time desc limit 2"
    #     print (request)
    #     try:
    #         # Execute the SQL command
    #         self.cursor_trigger.execute(request)
    #         result = self.cursor_trigger.fetchall()
    #         print (result)
    #         current_state = result[0]
    #         previous_state = result[1]
    #
    #         self.db_trigger.commit()
    #
    #         if (current_state != previous_state):
    #             return True
    #         else:
    #             return False
    #
    #     except Exception as e:
    #         # Rollback in case there is any error
    #         self.db_trigger.rollback()
    #         print ("error checkTriggerItemStateChange")
    #         return None


    def check_trigger_item_state_update(self, trigger_content, item_id):
        pass


    def check_triger_fix_time_of_day(self, trigger_content, item_id):
        pass


    def check_trigger_item_has_given_state(self, trigger_content, item_id, item_state):
        """Check trigger state: Check if any trigger contain item has item_id and item_state
        
        Input: 
            trigger_content: contain one or many condition of one or many items
            item_id: identify item which is checking it's state
            item_state: state of item which just changed so we need to check this state if any trigger sastify

        Output: Result after check. Value: True or False

        Note: 
            Cannot run multi-thread becase of changing data frequently, then checking item condition will not correctly
        """
        self.logger.info("Checking trigger item_has_given_state ...\n")
        trigger_content = json.loads(trigger_content)
        # self.logger.debug(("trigger content: ", trigger_content))
        config = trigger_content['config']
        # self.logger.debug(("config: ", config))
        pre_result = False
        total_result = False


        """List all trigger_item_id in trigger (a trigger might have many item condition)
        Check if item_id in the list above. If no, break; if yes, check condition all of items in trigger
        """
        list_trigger_item_id = []
        for sub_config in config:
            trigger_item_id = sub_config['constraint']['item']['item_global_id']
            list_trigger_item_id.append(trigger_item_id)


            '''
            if (item_id not in list_trigger_item_id):
                return false
            else:
                for sub_config in config:
                    # result = False
                    # Get item state in trigger
                    timer = sub_config['constraint']['time']
                    trigger_item_id = sub_config['constraint']['item']['item_global_id']
                    trigger_item_name = sub_config['constraint']['item']['item_name']
                    operator = sub_config['constraint']['comparation']
                    trigger_item_value = sub_config['constraint']['value']
                    bitwise_operator = sub_config['bitwise_operator']

                    if (str(trigger_item_value).isdigit() == True):
                        trigger_item_value = float(trigger_item_value)

                    result = True
                    if (trigger_item_id == item_id):
                        self.logger.info("Found item_id on trigger, checking state!")
                        self.logger.debug(("trigger_item_id: ", trigger_item_id))
                        self.logger.debug(("check item id: ", item_id))
                        self.logger.debug(("trigger_item_name: ", trigger_item_name))
                        self.logger.debug(("item_state: ", item_state))
                        self.logger.debug(("trigger_item_value: ", trigger_item_value))
                        self.logger.debug(("operator: ", operator))

                        if (str(item_state).isdigit() == True):
                            item_state = float(item_state)
                        # elif (item_state == "on"):
                        #     item_state = 1
                        # elif (item_state == "off"):
                        #     item_state = 0

                        if (operator == "GT"):
                            if (item_state <= trigger_item_value):
                                result = False
                                # break
                        elif (operator == "GE"):
                            if (item_state < trigger_item_value):
                                result = False
                                # break
                        elif (operator == "LT"):
                            if (item_state >= trigger_item_value):
                                result = False
                                # break
                        elif (operator == "LE"):
                            if (item_state > trigger_item_value):
                                result = False
                                # break
                        elif (operator == "EQ"):
                            if (item_state != trigger_item_value):
                                result = False
                                # break
                        elif (operator == "NE"):
                            if (item_state == trigger_item_value):
                                result = False
                                # break
                        else:
                            self.logger.info("operator is not valid")
                            result = False
                            # break

                        if (bitwise_operator.upper() == "NONE"):
                            total_result = result
                            pre_result = result
                        elif (bitwise_operator.upper() == "AND"):
                            total_result = pre_result and result
                            pre_result = total_result
                        elif (bitwise_operator.upper() == "OR"):
                            total_result = pre_result or result
                            pre_result = total_result
                        else:
                            self.logger.info("bitwise operator is not pre-defined")

                    else:
                        self.logger.info("trigger_item_id is not item_id, check others item condition in trigger")
                        sql = """ select item_state from   """


                        
                    self.logger.debug("\n")


            # self.logger.info(("total result: ", total_result))
            self.logger.info("Finish check_trigger_item_has_given_state")
            '''


            # '''
            # result = False
            # Get item state in trigger
            timer = sub_config['constraint']['time']
            trigger_item_id = sub_config['constraint']['item']['item_global_id']
            trigger_item_name = sub_config['constraint']['item']['item_name']
            operator = sub_config['constraint']['comparation']
            trigger_item_value = sub_config['constraint']['value']
            bitwise_operator = sub_config['bitwise_operator']

            if (str(trigger_item_value).isdigit() == True):
                trigger_item_value = float(trigger_item_value)


            request = "select time from ItemTable where item_id = '%s' order by time asc limit 1" % (trigger_item_id)
            #self.logger.debug(("time request: ", request))
            last_insert_time = str(datetime.max)

            try:
                # Execute the SQL command
                self.cursor_trigger.execute(request)
                request_result = self.cursor_trigger.fetchall()
                #self.logger.debug(("request result: ", request_result))
                last_insert_time = request_result[0][0]
                # Commit your changes in the database
                self.db_trigger.commit()
            except Exception as e:
                self.logger.error("Item not found in database!", exc_info=True)
                result = False
                pre_result = result
                self.db_trigger.rollback()


            # #last_insert_time = datetime.strptime(last_insert_time, '%Y-%m-%d %H:%M:%S.%f')
            # #check_time = last_insert_time - timedelta(seconds=float(timer.split('s')[0]))
            # #request = "select item_state from ItemTable where item_id = '%s' and time >= '%s' order by time desc" % (trigger_item_id, check_time)

            request = "select item_state from ItemTable where item_id = '%s' and time >= '%s' order by time desc" % (trigger_item_id, last_insert_time)

            try:
                # Execute the SQL command
                self.cursor_trigger.execute(request)
                request_result = self.cursor_trigger.fetchall()
                item_state_list = request_result[0]
                # self.logger.debug(("item_state_list: ", item_state_list))
                # # Commit your changes in the database
                self.db_trigger.commit()

                for item_state in item_state_list:
                    self.logger.debug(("item_state: ", item_state))
                    self.logger.debug(("trigger_item_value: ", trigger_item_value))
                    self.logger.debug(("operator: ", operator))
                    
                    # time.sleep(10000)
                    result = True

                    if (item_state.isdigit() == True):
                        item_state = float(item_state)
                    # elif (item_state == "on"):
                    #     item_state = 1
                    # elif (item_state == "off"):
                    #     item_state = 0

                    if (operator == "GT"):
                        if (item_state <= trigger_item_value):
                            result = False
                            self.logger.info(("result: ", result))
                            self.logger.debug("\n")                            
                            break
                    elif (operator == "GE"):
                        if (item_state < trigger_item_value):
                            result = False
                            self.logger.info(("result: ", result))
                            self.logger.debug("\n")
                            break
                    elif (operator == "LT"):
                        if (item_state >= trigger_item_value):
                            result = False
                            self.logger.info(("result: ", result))
                            self.logger.debug("\n")
                            break
                    elif (operator == "LE"):
                        if (item_state > trigger_item_value):
                            result = False
                            self.logger.info(("result: ", result))
                            self.logger.debug("\n")
                            break
                    elif (operator == "EQ"):
                        if (item_state != trigger_item_value):
                            result = False
                            self.logger.info(("result: ", result))
                            self.logger.debug("\n")
                            break
                    elif (operator == "NE"):
                        if (item_state == trigger_item_value):
                            result = False
                            self.logger.info(("result: ", result))
                            self.logger.debug("\n")
                            break
                    else:
                        self.logger.info("operator is not valid")
                        result = False
                        self.logger.info(("result: ", result))
                        self.logger.debug("\n")
                        break

                    self.logger.info(("result: ", result))
                    self.logger.debug("\n")

                    

                if (bitwise_operator.upper() == "NONE"):
                    total_result = result
                    pre_result = result
                elif (bitwise_operator.upper() == "AND"):
                    total_result = pre_result and result
                    pre_result = total_result
                elif (bitwise_operator.upper() == "OR"):
                    total_result = pre_result or result
                    pre_result = total_result
                else:
                    self.logger.info("bitwise operator is not pre-defined")

            except Exception as e:
                # Rollback in case there is any error
                self.db_trigger.rollback()
                self.logger.error("Error read last item value in database", exc_info=True)
                # return None
                result = False
                pre_result = result

        # self.logger.info(("total result: ", total_result))
        self.logger.info("Finish check_trigger_item_has_given_state")
        # '''

        return total_result


    def check_trigger_condition(self, trigger_id, trigger_type, trigger_content, item_id, item_state):
        """Check trigger expression; Call to a specific function based on the trigger_type
        Default trigger_type: item_state_change; need to future implement more trigger types
        Output: 
            Result after check expression. Value: True or False
        """
        self.logger.info(("checking trigger condition ..."))
        result = False
        if (trigger_type == "item_state_change"):
            # result = self.check_trigger_item_state_change(trigger_content, item_id)
            pass
        elif (trigger_type == "item_state_update"):
            result = self.check_trigger_item_state_update(trigger_content, item_id)
        elif (trigger_type == "item_receive_command"):
            pass
        elif (trigger_type == "fix_time_of_day"):
            result = self.check_triger_fix_time_of_day(trigger_content, item_id)
        elif (trigger_type == "item_has_given_state"):
            result = self.check_trigger_item_has_given_state(trigger_content, item_id, item_state)
        else:
            self.logging.info("trigger_type is not pre-defined")

        #self.logger.info("Finish check one event_trigger!\n")

        return result


    def create_event(self, trigger_id):
        """Create event
        Send message to a specific topic using rabbitmq broker, MQTT protocol
        Output: None
        """
        self.logger.info("creating Event ...")
        request = "select trigger_content from TriggerTable where trigger_id = '%s'" % (trigger_id)

        try:
            # Execute the SQL command
            self.cursor_trigger.execute(request)
            request_result = self.cursor_trigger.fetchall()

            # self.logger.info("trigger content: " + str(request_result[0][0]))
            
            trigger_content = request_result[0][0]
            trigger_content = json.loads(trigger_content)
            # print (trigger_content)
            output_field = trigger_content['outputs']
            self.logger.info("event content: " + str(output_field))
            # Commit your changes in the database
            self.db_trigger.commit()

            for output in output_field:
                event_id = output['event_id']
                # event_name = output['event_name']
                event_source = output['event_source']
                description = output['description']

                message = {
                    'event_generator_id' : self.event_generator_id,
                    'event_id' : event_id,
                    # 'event_name' : event_name,
                    'event_source' : event_source,
                    'trigger_id' : trigger_id,
                    'description' : description,
                    'time' : str(datetime.now())
                }

                self.producer_connection.ensure_connection()
                with Producer(self.producer_connection) as producer:
                    producer.publish(
                        json.dumps(message),
                        exchange=self.exchange.name,
                        routing_key='event_generator.to.' + str(self.event_dest_topic),
                        retry=True
                    )
                self.logger.info(("Send event to Rule Engine: " + 'event_generator.to.' + str(self.event_dest_topic)))


        except Exception as e:
            self.db_trigger.rollback()
            self.logger.error("error read trigger_content", exc_info=True)

        self.logger.info("Finish create a event!")


    def write_item_to_database(self, item_id, item_name, item_type, item_state, time):
        """Write items receive from DataSource to database
        Default: Write to ItemTable 
        """
        self.logger.info("writting item to database ...")

        if ('Z' in time and 'T' in time): # Check format of time
            time_1 = time.split('T')
            time_2 = time_1[1].split('Z')
            time = time_1[0] + " " + time_2[0]

        request = """INSERT INTO ItemTable(item_id, item_name, item_type, item_state, time) VALUES ("%s", "%s", "%s", "%s", "%s")"""\
                    % (item_id, item_name, item_type, item_state, time)

        #self.logger.info(("request: " + str(request)))

        try:
            # Execute the SQL command
            self.cursor_trigger.execute(request)
            result = self.cursor_trigger.fetchall()
            self.logger.info("write item %s to database: %s" % (item_id, result))
            self.db_trigger.commit()
            return  True
        except Exception as e:
            # Rollback in case there is any error
            self.db_trigger.rollback()
            self.logger.error("error write item to database", exc_info=True)
            return False

        self.logger.info("Finish write item to database")

    def receive_states(self):
        """Wait message at a specific topic and callback when receive a message
        """
        def handle_notification(body, message):
            #time = datetime.now()
            #print ("time: " + str(datetime.now()))
            #time = str(time)

            self.logger.info("Receive state!\n")
            message = json.loads(body)["body"]["states"]
            # self.logger.debug(("message: ", message))

            for i in range(len(message)):
                item_id = json.loads(body)["body"]["states"][i]["MetricId"]
                item_name  = json.loads(body)["body"]["states"][i]["MetricLocalId"]
                item_type = json.loads(body)["body"]["states"][i]["DataPoint"]["DataType"]
                item_state = json.loads(body)["body"]["states"][i]["DataPoint"]["Value"]
                self.logger.info("item_name: " + str(item_name))
                self.logger.info("item_state: " + str(item_state))

                # time = json.loads(body)[""]
                

                # Write new item to the database
                write_result = self.write_item_to_database(item_id, item_name, item_type, item_state, str(datetime.now()))

                #if write new item success to the database, --> consider the trigger with that item
                if (write_result == 1):
                    list_event_trigger = self.read_event_trigger()
                    for event_trigger in list_event_trigger:
                        trigger_id = event_trigger[0]
                        trigger_type = event_trigger[1]
                        trigger_content = event_trigger[2]

                        # Check the item with each trigger condition
                        result = self.check_trigger_condition(trigger_id, trigger_type, trigger_content, item_id, item_state)
                        self.logger.info("check trigger : " + str(result))
                        
                        
                        if (result == None):
                            return None

                        # If checkTriggerCondition success, create an event
                        if (result == True):
                            # event_source = item_id
                            # event_name = str(item_name) + "_" + str(datetime.now())
                            # event_id   = randint(1, 1000000)
                            # my_event = Event(event_name, event_id, event_source)
                            self.create_event(trigger_id)

                        

            self.logger.info("Finish handle one notification! \n\n\n")

            # End handle_notification

        try:
            self.consumer_connection.ensure_connection(max_retries=1)
            with nested(Consumer(self.consumer_connection, queues=self.queue_get_states,
                                    callbacks=[handle_notification], no_ack=True)
                        ):
                while True:
                    self.consumer_connection.drain_events()
        except (ConnectionRefusedError, exceptions.OperationalError):
            self.logger.error('Connection lost', exc_info=True)
        except self.consumer_connection.connection_errors:
            self.logger.error('Connection error', exc_info=True)


    # Just for test with cross-platform system
    def crawl_init_data(self):
        """Crawl data from DataSource where send data to Event Generator
        """
        self.logger.info("Craw initial data from data source")

        try:
            request = requests.get(self.api_get_source)
            data = request.json()
        except Exception as e:
            self.logger.error("Crawl data failed!", exc_info=True)


        for thing in data:
            metrics = thing["metrics"]
            for metric in metrics:
                metric_id = metric["MetricId"]
                metric_name = metric["MetricName"]
                metric_type = metric["MetricType"]
                data_point = metric["DataPoint"]
                time_collect = data_point["TimeCollect"]
                state = data_point["Value"]

                self.write_item_to_database(metric_id, metric_name, metric_type, state, time_collect)

        self.logger.info("Finish crawl init data! \n")


    def run(self):
        self.cursor_trigger.execute("""CREATE TABLE IF NOT EXISTS ItemTable(
                        item_id VARCHAR(50),
                        item_name VARCHAR(50),
                        item_type VARCHAR(50),
                        item_state VARCHAR(50), 
                        time VARCHAR(50),
                        PRIMARY KEY (item_id, time))""")
        
        self.crawl_init_data()

        while 1:
            self.receive_states()



event_generator_1 = Event_Generator_1(event_generator_name="event_generator_1",
                                      event_generator_id="1", description="",
                                      event_dest_topic="rule_engine_1")

# print (event_generator_1.event_generator_id)
event_generator_1.run()
