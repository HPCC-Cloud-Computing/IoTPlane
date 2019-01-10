from kombu import Producer, Connection, Consumer, exceptions, Exchange, Queue, uuid
from kombu.utils.compat import nested
import json
import MySQLdb
from Rule_Engine_Base import Rule_Engine_Base
from Action import Action
from datetime import timedelta, datetime
import os
import time
import requests
import logging

class Rule_Engine_1(Rule_Engine_Base):
    def __init__(self, rule_engine_name, rule_engine_id,
                  description, input_topic, output_topic):

        Rule_Engine_Base.__init__(self, rule_engine_name, rule_engine_id,
                                  description, input_topic, output_topic)

        # Config broker
        BROKER_CLOUD = "192.168.0.102"
        self.host_api_set_state = "http://192.168.0.102:5000/api/metric"
        self.api_get_source = "http://192.168.0.102:5000/api/sources"
        self.producer_connection = Connection("192.168.0.102")
        self.consumer_connection = Connection("192.168.0.102")
        self.exchange = Exchange("IoT", type="direct")
        self.queue_get_states = Queue(name='event_generator.to.' + str(self.input_topic), exchange=self.exchange,
                                      routing_key='event_generator.to.' + str(self.input_topic), message_ttl=20)

        # Config database
        self.db_trigger = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root", db='Trigger_DB')
        self.cursor_trigger = self.db_trigger.cursor()

        # Config logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.DEBUG)
        self.handler = logging.StreamHandler()
        self.formatter = logging.Formatter("[ %(asctime)s - %(levelname)s - %(name)s - line: %(lineno)d ]- %(message)s")
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

    def mapping(self, trigger_id):
        """Mapping trigger and condition correspond with that trigger_id
        Input : trigger_id
        Output: condition if exist
        """
        self.logger.info("mapping ...")
        request = "select condition_id, condition_content, action_id, action_content from RuleTable where trigger_id = '" + str(trigger_id) + "'"
        self.logger.info("mapping request: " + str(request))

        try:
            # Execute the SQL command
            self.cursor_trigger.execute(request)
            #self.logger.info("execute request")
            result = self.cursor_trigger.fetchall()
            self.logger.debug("results: " + str(result))
        except Exception as e:
            # Rollback in case there is any error
            self.logger.error("error mapping trigger_id", exc_info=True)
            self.db_trigger.rollback()
            return None

        condition_id, condition_content, action_id, action_content = result[0]
        condition_content = json.loads(condition_content)
        condition_type = condition_content['condition_type']
        action_content = json.loads(action_content)

        return condition_id, condition_type, condition_content, action_id, action_content


    def check_condition(self, condition_id, condition_type, condition_content):
        """Check condition expression
        Input: 
            Condition_id
            Condition_type: default is item_has_given_state; but wait for extent other types of condition
            condition_content
        Output:
            Result after check condition expression. Receive value True or False

        Note: 
            Always return true for now
            Need to change
        """

        return True

        self.logger.info("checking condition ...")


        #self.logger.info(condition_type)
        result = False
        if (condition_type == "item_has_given_state"):
            result = self.check_condition_item_has_given_state(condition_id, condition_type, condition_content)
        elif (condition_type == "given_script_is_true"):
            result = self.check_condition_given_script_is_true(condition_id, condition_type, condition_content)
        elif (condition_type == "certain_day_of_week"):
            result = self.check_condition_certain_day_of_week(condition_id, condition_type, condition_content)
        else:
            self.logger.info("condition_type is not pre-defined!")

        return result


    # Just for test with cross-platform system
    def crawl_init_data(self):
        """Crawl data from DataSource where send data to Event Generator
        Currently not use in Rule Engine module
        """
        pass
        '''
        def write_item_to_database(item_id, item_name, item_type, item_state, time):
            self.logger.info("writting item to database ...")
            request = """INSERT INTO ItemTable(item_id, item_name, item_type, item_state, time)
                        VALUES ("%s", "%s", "%s", "%s", "%s")"""\
                        % (item_id, item_name, item_type, item_state, time)

            #self.logger.info("request: " + str(request))

            try:
                # Execute the SQL command
                self.logger.info(self.cursor_trigger.execute(request))
                result = self.cursor_trigger.fetchall()
                self.logger.info("write item %s to database: %s" % (item_id, result))
                self.db_trigger.commit()
                return  True

            except Exception as e:
                # Rollback in case there is any error
                self.db_trigger.rollback()
                self.logger.error("error write item to database", exc_info=True)
                return False
        

        # Crawl data
        request = requests.get(self.api_get_source)
        data = request.json()

        for thing in data:
            metrics = thing["metrics"]
            for metric in metrics:
                metric_id = metric["MetricId"]
                metric_name = metric["MetricName"]
                metric_type = metric["MetricType"]
                data_point = metric["DataPoint"]
                time_collect = data_point["TimeCollect"]
                state = data_point["Value"]

                write_item_to_database(metric_id, metric_name, metric_type, state, time_collect)

        self.logger.info("Finish crawl init data! \n")
        '''


    def check_condition_item_has_given_state(self, condition_id, condition_type, condition_content):
        """Check state in condition field
        Output: result after check condition. Value: True or False

        Note: 
            Always return true for now

        """

        return True

        self.logger.info("condition_id: " + str(condition_id))
        condition_content = json.dumps(condition_content)
        condition_content = json.loads(condition_content)
        config = condition_content['config']

        pre_result = False
        total_result = False

        for sub_config in config:
            result = False
            condition_item_id = sub_config['constraint']['item']['item_global_id']
            #self.logger.debug("condition_item_id: " + str(condition_item_id))
            timer = sub_config['constraint']['time']
            operator = sub_config['constraint']['comparation']
            value = sub_config['constraint']['value']

            if (value.isdigit() == True):
                value = float(value)

            bitwise_operator = sub_config['bitwise_operator']

            request = "select time from ItemTable where item_id = '%s' order by time asc limit 1" % (condition_item_id)
            #self.logger.debug(request)

            last_insert_time = str(datetime.max)

            try:
                # Execute the SQL command
                self.cursor_trigger.execute(request)
                request_result = self.cursor_trigger.fetchall()
                #self.logger.debug("request result: " + str(request_result))
                last_insert_time = request_result[0][0]
                # Commit your changes in the database
                self.db_trigger.commit()
            except:
                self.logger.error("Item in rule not found in database!", exc_info=True)
                result = False
                pre_result = result
                self.db_trigger.rollback()


            # last_insert_time = datetime.strptime(last_insert_time, '%Y-%m-%d %H:%M:%S.%f')
            # check_time = last_insert_time - timedelta(seconds=float(timer.split('s')[0]))
            # request = "select item_state from ItemTable where item_id = '%s' and time >= '%s'" % (condition_item_id, check_time)
            
            request = "select item_state from ItemTable where item_id = '%s' and time >= '%s'" % (condition_item_id, last_insert_time)
            # print (request)

            try:
                # Execute the SQL command
                self.cursor_trigger.execute(request)
                result = self.cursor_trigger.fetchall()
                item_state_list = result[0]
                # Commit your changes in the database
                self.db_trigger.commit()

                for item_state in item_state_list:
                    if (item_state.isdigit() == True):
                        item_state = float(item_state)
                    # elif (item_state == "on"):
                    #     item_state = 1
                    # elif (item_state == "off"):
                    #     item_state = 0

                    result = True

                    if (operator == "GT"):
                        if (item_state <= value):
                            result = False
                            break
                    elif (operator == "GE"):
                        if (item_state < value):
                            result = False
                            break
                    elif (operator == "LT"):
                        if (item_state >= value):
                            result = False
                            break
                    elif (operator == "LE"):
                        if (item_state > value):
                            result = False
                            break
                    elif (operator == "EQ"):
                        if (item_state != value):
                            result = False
                            break
                    elif (operator == "NE"):
                        if (item_state == value):
                            result = False
                            break
                    else:
                        print ("operator is not valid")
                        result = False
                        break

                #self.logger.info("result: " + str(result))

                if (bitwise_operator.upper() == "NONE"):
                    pre_result = result
                    total_result = result
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
                self.logger.error("error check_condition_item_has_given_state", exc_info=True)
                result = False
                pre_result = result


        #self.logger.info("total result: " + str(total_result))

        return total_result


    def check_condition_given_script_is_true(self, condition_id, condition_type, condition_content):
        pass


    def check_condition_certain_day_of_week(self, condition_id, condition_type, condition_content):
        pass


    # def call_to_action(self, action_type, action_id, action_content):
    #     message = {
    #         'rule_engine_name' : self.rule_engine_name,
    #         'rule_engine_id' : self.rule_engine_id,
    #         'action_id' : action_id,
    #         'action_type' : action_type,
    #         "action_content" : action_content
    #     }
    #
    #     self.producer_connection.ensure_connection()
    #     with Producer(self.producer_connection) as producer:
    #         producer.publish(
    #             json.dumps(message),
    #             exchange=self.exchange.name,
    #             routing_key='event_generator.to.' + str(self.output_topic),
    #             retry=True
    #         )
    #     print ("Send event to Actor: ", self.output_topic)


    def call_to_action(self, action_id, action_content):
        """Call Action 
        TODO: Currently only implement call_cross_platform_api, use REST API. 
                  Need to implement more action method
        """
        self.logger.info("call to action")
        self.logger.info("action id: " + str(action_content['action_id']))
        result = False

        for action in action_content['action']:
            action_type = action['action_type']

            if (action_type == "send_a_command"):
                result = self.send_a_command(action_content)
            elif (action_type == "enable_or_disable_rule"):
                result = self.enable_or_disable_rule(action_content)
            elif (action_type == "run_rule"):
                result = self.run_rule(action_content)
            elif (action_type == "exec_given_script"):
                result = self.exec_given_script(action_content)
            elif (action_type == "write_log"):
                result = self.write_log(action_content)
            elif (action_type == "call_cross_platform_api" or action_type == "update"):
                result = self.call_cross_platform_api(action_content)
            else:
                self.logger.info("action_type is not pre-defined")
                result = False      # error

            if (result == False):
                self.logger.error("Error execute action")
                break

        return result


    def send_a_command(self, action_content):
        pass

    def enable_or_disable_rule(self, action_content):
        pass


    def run_rule(self, action_content):
        pass


    def exec_given_script(self, action_content):
        pass


    def write_log(self, action_content):
        self.logger.info("writing log ...")
        action_content = json.dumps(action_content)
        action_content = json.loads(action_content)
        self.logger.info(action_content)

        for action in action_content["action"]:
            config = action["config"]
            self.logger.info(config)
            file_name = config["file_name"]
            file_format = config["format"]
            f = open(file_name + "" + file_format, "a")
            f.write(str(action_content))
            f.write("\n")


    def call_cross_platform_api(self, action_content):
        """Call Quan's API
        Before call API, check the current state of items. If no change, no call API. Otherwise, call API
        Because the limitation of API, it's won't gurantee that API react after called. So retry call API 3 time
        """
        self.logger.info("call_cross_platform_api ...\n")
        # print (action_content["action"][1])
        num_retry = 3

        for action in action_content['action']:
            try:
                # self.logger.debug("action: " + str(action))
                thing_global_id = action['config']['item']['thing_global_id']
                item_global_id  = action['config']['item']['item_global_id']
                new_value           = action['config']['value']
                item_name       = action['config']['item']['item_name']
                self.logger.info("item name: " + str(item_name))
                # self.logger.info("new value: "+ str(new_value))


                # Check current state of item. If new state == current state, won't call API
                try:
                    sql = """select item_state, time from ItemTable where item_id = "%s" order by time desc limit 1""" % (item_global_id)
                    # self.logger.debug("request current state command: " + str(sql))
                    request_result = self.cursor_trigger.execute(sql) 
                    current_state = self.cursor_trigger.fetchall()[0]
                    self.logger.info("read current item state result: " + str(current_state[0]))
                    self.logger.info("new value: " + str(new_value))
                    self.logger.info("time: " + str(current_state[1]))
                    # self.logger.debug("current vs new value is the same: " + str(new_value in current_state))
                    self.db_trigger.commit()

                    if (new_value == current_state[0]):
                        self.logger.info("State has no changes, not call API!\n")
                        continue


                    self.logger.info("Calling API")
                    # Send API message
                    message = {
                    "header" : {},
                    "body":{
                        "SourceId"  : thing_global_id,
                        "MetricId"  : item_global_id,
                        "new_value" : new_value
                        }
                    }

                    #self.logger.debug("message: " + str(json.dumps(message)))

                    request = "curl -H \"Content-type: application/json\" -X POST " + self.host_api_set_state + " -d " + "\'" + json.dumps(message) + "\'"

                    # logging.debug(request)
                    for i in range(num_retry):
                        os.system(request)
                        time.sleep(3)
                except:
                    self.db_trigger.rollback()
                    self.logger.error("Error read current item state", exc_info=True)


                
            except Exception as e:
                self.logger.error("error call_api_cross_platform", exc_info=True)


        # time.sleep(1)


###############################################################
###############################################################


###############################################################
############## RECEIVE EVENT ##################################

    def receive_event(self):
        def handle_notification(body, message):
            self.logger.info("Receive Event!")
            # event_name = json.loads(body)["event_name"]
            event_id  = json.loads(body)["event_id"]
            event_source = json.loads(body)["event_source"]
            trigger_id = json.loads(body)["trigger_id"]
            # event_generator_id = json.loads(body)["event_generator_id"]
            time = json.loads(body)["time"]

            # mapping trigger_id to condition_id and action_id
            condition_id, condition_type, condition_content, action_id, action_content = self.mapping(trigger_id)

            # Check if condition has condition_id is true
            is_condition_satisfice = self.check_condition(condition_id, condition_type, condition_content)

            self.logger.info("check condition result: " + str(is_condition_satisfice))

            if (is_condition_satisfice == True):
                    # Execute an action
                    self.call_to_action(action_id, action_content)

            self.logger.info("Finish receive one event!\n\n\n")
            # End handle_notification


        try:
            self.consumer_connection.ensure_connection(max_retries=1)
            with nested(Consumer(self.consumer_connection, queues=self.queue_get_states,
                                 callbacks=[handle_notification], no_ack=True)
                        ):
                while True:
                    self.consumer_connection.drain_events()
        except (ConnectionRefusedError, exceptions.OperationalError):
            self.logger.error("Connection lost", exc_info=True)
        except self.consumer_connection.connection_errors:
            self.logger.error("Connection error", exc_info=True)
        except Exception as e:
            self.logger.error("Error receive event", exc_info=True)
            # pass

        

    def run(self):
        # self.crawl_init_data()
        while 1:
            self.receive_event()
            '''
            try:
                self.receive_event()
            except (ConnectionRefusedError, exceptions.OperationalError):
                self.logger.error("Connection lost", exc_info=True)
            except self.consumer_connection.connection_errors:
                self.logger.error("Connection error", exc_info=True)
            except:
                self.logger.error("error", exc_info=True)
            '''




###########################################################
####################### MAIN ##############################

rule_engine_1 = Rule_Engine_1("rule_engine_1", "rule_engine_id_1", "", "rule_engine_1", "")
print (rule_engine_1.rule_engine_name)
rule_engine_1.run()
