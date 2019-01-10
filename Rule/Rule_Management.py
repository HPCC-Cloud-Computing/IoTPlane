from flask import Flask
from flask import json
from flask import jsonify
from flask import request
import json
import os
import MySQLdb
import time
import random
import logging

app = Flask(__name__)

#####################################
# Config database

#db = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root")
#cursor = db.cursor()

db_trigger = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root")
cursor_trigger = db_trigger.cursor()

#db_engine = MySQLdb.connect(host="0.0.0.0", user="root", passwd="root")
#cursor_engine = db_engine.cursor()

# End config database
######################################


#####################################
# Config logging
handler = logging.StreamHandler()
formatter = logging.Formatter("[ %(asctime)s - %(levelname)s - %(name)s - line: %(lineno)d ]- %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
logger.addHandler(handler)
# End config loggin
####################################

save_set_state = []
data_list = []

"""Receive rule message
"""
@app.route('/rule', methods = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE'])
def api():
    """API for: Add, remove, update rule
    """
    logger.info("\n")
    global data_list
    if request.method == 'GET':
        return jsonify(data_list)

    elif request.method == 'POST':
        data = request.json
        # logger.debug("rule: " + str(data))
        rule_status = data['rule_status']

        if (rule_status == "enable"):
            resutl = save_data(data)
            data_list = load_data_from_db()
            # logger.debug(data_list)
        elif (rule_status == "disable"):
            delete_data(data)
            data_list = load_data_from_db()
        elif (rule_status == "edit"):
            update_data(data)
            data_list = load_data_from_db()
        else:
            logger.error("Error: rule status not defined!")
            return -1

        return jsonify(data_list)



@app.route('/api_set_state', methods = ['GET', 'POST'])
def api_set_state():
    global save_set_state
    data = request.json
    save_set_state.append(data)

    return jsonify(save_set_state)


def load_data_from_db(n_record=None):
    """Load rule data each time refresh the UI pages
    """
    logger.info("Call load rules")
    list_rule = []

    if (n_record == None):
        query_statement = 'SELECT rule_id, rule_status, rule_name, trigger_id, trigger_content, condition_id, condition_content, action_id, action_content FROM RuleTable WHERE rule_status = "enable" ORDER BY insert_time DESC'
    else:
        query_statement = 'SELECT rule_id, rule_status, rule_name, trigger_id, trigger_content, condition_id, condition_content, action_id, action_content FROM RuleTable WHERE rule_status = "enable" ORDER BY insert_time DESC LIMIT' + str(n_record)

    #cursor_trigger.execute(query_statement)
    # Fetch all the rows in a list of lists.
    #results = cursor_trigger.fetchall()

    # logger.debug(query_statement)

    try:
        # Execute the SQL command
        cursor_trigger.execute(query_statement)
        # Fetch all the rows in a list of lists.
        results = cursor_trigger.fetchall()
        # logger.debug("load rule result: " + str(results))

        for row in results:
            # logger.debug("row: " + str(row))
            # print ("\n\n\n\n")
            rule = {
                'rule_id'           : row[0],
                'rule_status'       : row[1],
                'rule_name'         : row[2],
                'trigger_id'        : row[3],
                'trigger'           : json.loads(row[4]),
                'condition_id'      : row[5],
                'condition'         : json.loads(row[6]),
                'action_id'         : row[7],
                'action'            : json.loads(row[8])
            }


            list_rule.append(rule)
            # print (rule['rule_condition'])

        # print ("list rule: ", list_rule)

    except:
        logger.error("Error: unable to fecth data", exc_info=True)

    # print (list_rule)
    return list_rule



def save_data(data):
    """Called if create new rule request received
    """
    logger.info("Call add new rule")

    data = json.dumps(data)
    data = json.loads(data)
    # logger.debug("saving rule :" + str(data))

    rule_id        = data["rule_id"]
    rule_name      = data["rule_name"]

    trigger_id     = data["trigger_id"]
    trigger_content= data["trigger"]
    condition_id   = data["condition_id"]
    condition_content = data["condition"]
    action_id      = data["action_id"]
    action_content = data["action"]

    # logger.debug("\n\ntrigger_content: ", trigger_content)
    trigger_content = json.dumps(trigger_content)
    # trigger_content = json.loads(trigger_content)

    trigger_type = json.loads(trigger_content)["trigger_type"]
    # logger.debug("\ntrigger_type: ", trigger_type)

    condition_content = json.dumps(condition_content)
    # condition_content = json.loads(condition_content)
    # logger.debug("\ncondition_content: ", condition_content)

    action_content = json.dumps(action_content)
    # action_content = json.loads(action_content)
    # logger.debug("\naction_content: ", action_content)


    rule_status = "enable"
    insert_time = time.time()
    result = None


############################################################################################################



    sql = """INSERT INTO RuleTable(rule_id, rule_name,
         rule_status, trigger_id, trigger_content, 
         condition_id, condition_content,
         action_id, action_content, insert_time)
         VALUES ("%s", "%s", "%s", "%s", '%s', "%s", '%s', "%s", '%s', %f)""" % (rule_id, rule_name, rule_status, trigger_id, trigger_content, condition_id, condition_content, action_id, action_content, insert_time)

    # logger.debug("save rule to RuleTable: " + str(sql))

    try:
        # Execute the SQL command
        result = cursor_trigger.execute(sql)
        logger.info("save rule result: " + str(result))
        # Commit your changes in the database
        db_trigger.commit()
    except:
        # Rollback in case there is any error
        db_trigger.rollback()


############################################################################################################
    """Becase of trigger_id changes each time update rule
    Then we need to add rule_id to each trigger, to specifi what trigger should be update
    """

    sql = """INSERT INTO TriggerTable(trigger_id, trigger_type, trigger_content, rule_id)
         VALUES ("%s", "%s", '%s', "%s")""" % (trigger_id, trigger_type, trigger_content, rule_id)


    # logger.debug("saving trigger to TriggerTable: " + str(sql))

    try:
        # Execute the SQL command
        result = cursor_trigger.execute(sql)
        logger.info("save trigger result: " + str(result))
        # Commit your changes in the database
        db_trigger.commit()
    except:
        # Rollback in case there is any error
        logger.error("error save trigger database", exc_info=True)
        db_trigger.rollback()

    return result


def delete_data(data):
    """Delete rule
    Consider to COMPLETE DELETE rule in database or just MARK AS DISABLE
    Default: COMPLETE DELETE rule in database
    """
    logger.info("Call delete rule")
    data = json.dumps(data)
    data = json.loads(data)

    rule_id     = data['rule_id']
    rule_status = "disable"
    trigger_id  = data["trigger_id"]

#################################################
# DELETE RULE: JUST MARK AS DISABLE
    # sql = 'UPDATE RuleTable SET rule_status = "%s" WHERE rule_id = "%s"' % (rule_status, rule_id)
    # try:
    #     # Execute the SQL command
    #     cursor_trigger.execute(sql)
    #     # Commit your changes in the database
    #     db_trigger.commit()
    #     logger.info("Deleted data from RuleTable")
    # except:
    #     # Rollback in case there is any error
    #     logger.error("Error delete data from RuleTable", exc_info=True)
    #     db_trigger.rollback()


###################################################
# DELETE RULE: COMPLETE DELETE RULE IN DATABASES
    sql = 'delete from RuleTable WHERE rule_id = "%s"' % (rule_id)
    try:
        # Execute the SQL command
        cursor_trigger.execute(sql)
        # Commit your changes in the database
        db_trigger.commit()
        logger.info("Deleted data from RuleTable")
    except:
        # Rollback in case there is any error
        logger.error("Error delete data from RuleTable", exc_info=True)
        db_trigger.rollback()



#########################################################################################################3
# DELETE TRIGGER: COMPLETE TRIGGER IN DATABASE
    """Because of trigger_id changes each time update rule, we don't update trigger_id 
    and need to add rule_id to detemine which trigger will be changes.
    Since rule_id is primiry key, we need to delete use rule_id
    """
    sql = "delete from TriggerTable where rule_id='%s' " % (rule_id)
    try:
        # Execute the SQL command
        cursor_trigger.execute(sql)
        # Commit your changes in the database
        db_trigger.commit()
        logger.info("Deleted data from TriggerTable")
    except:
        # Rollback in case there is any error
        db_trigger.rollback()
        logger.error("Error delete data from TriggerTable", exc_info=True)



def update_data(data):
    """Called if Update rule send request
    """
    logger.info("Call update rule")

    data = json.dumps(data)
    data = json.loads(data)

    rule_id        = data["rule_id"]
    rule_name      = data["rule_name"]

    trigger_id     = data["trigger_id"]
    trigger_content= data["trigger"]
    condition_id   = data["condition_id"]
    condition_content = data["condition"]
    action_id      = data["action_id"]
    action_content = data["action"]

    trigger_content = json.dumps(trigger_content)
    condition_content = json.dumps(condition_content)
    action_content = json.dumps(action_content)

    # trigger_content = json.loads(trigger_content)
    # condition_content = json.loads(condition_content)
    # action_content = json.loads(action_content)
    # logger.info("trigger_content: " + str(trigger_content))
    trigger_type = json.loads(trigger_content)["trigger_type"]
    # logger.info("trigger_type: " + str(trigger_type))

    rule_status = "enable"

#####################################################
# Update rule in RuleTable
    """Because of trigger_id, condition_id and action_id change each time update rule, then we should not to update these fields
    """
    sql = """UPDATE RuleTable SET rule_status = "%s", 
             rule_name = "%s", trigger_content='%s',
             condition_content='%s', 
             action_content='%s' WHERE rule_id = "%s" """ \
          % (rule_status, rule_name, trigger_content, condition_content, action_content, rule_id)
    
    # logger.debug("update rule command: " + str(sql))

    try:
        # Execute the SQL command
        result = cursor_trigger.execute(sql)
        logger.info("update result to RuleTable: " + str(result))
        # Commit your changes in the database
        db_trigger.commit()
    except:
        # Rollback in case there is any error
        logger.error("update to RuleTable error", exc_info=True)
        db_trigger.rollback()


###################################################
# Update trigger in TriggerTable
    """Because of trigger_id automaticaly changes each time update rule, we need rule_id to determine which rule will be update
    and should not update trigger_id
    """
    sql = """UPDATE TriggerTable SET trigger_type="%s", trigger_content='%s' WHERE rule_id = "%s" """ \
          % (trigger_type, trigger_content, rule_id)

    # logger.debug("update trigger command: " + str(sql))

    try:
        # Execute the SQL command
        result = cursor_trigger.execute(sql)
        logger.info("update result to TriggerTable: " + str(result))
        # Commit your changes in the database
        db_trigger.commit()
    except:
        # Rollback in case there is any error
        logger.error("update to TriggerTable error", exc_info=True)
        db_trigger.rollback()




if __name__ == "__main__":

    #cursor.execute("""CREATE DATABASE IF NOT EXISTS RuleAPI_DB""")
    #cursor.execute("""USE RuleAPI_DB""")

    #cursor.execute("""CREATE TABLE IF NOT EXISTS RuleTable(
    #    rule_id VARCHAR(100) PRIMARY KEY,
    #    rule_name VARCHAR(100),
    #    rule_status VARCHAR(100),
    #    trigger_id VARCHAR(100),
    #    trigger_content VARCHAR(20000),
    #    condition_id VARCHAR(100),
    #    condition_content VARCHAR(20000),
    #    action_id VARCHAR(100),
    #    action_content VARCHAR(20000),
    #    insert_time VARCHAR(100))  """)




    #cursor_engine.execute("""CREATE DATABASE IF NOT EXISTS RuleEngine_DB""")
    #cursor_engine.execute("""USE RuleEngine_DB""")

    #cursor_engine.execute("""CREATE TABLE IF NOT EXISTS RuleTable(
    #    rule_id VARCHAR(100) PRIMARY KEY,
    #    rule_name VARCHAR(100),
    #    rule_status VARCHAR(100),
    #    trigger_id VARCHAR(100),
    #    trigger_content VARCHAR(20000),
    #    condition_id VARCHAR(100),
    #    condition_content VARCHAR(20000),
    #    action_id VARCHAR(100),
    #    action_content VARCHAR(20000),
    #    insert_time VARCHAR(100))  """)




    cursor_trigger.execute("""CREATE DATABASE IF NOT EXISTS Trigger_DB""")
    cursor_trigger.execute("""USE Trigger_DB""")

    cursor_trigger.execute("""CREATE TABLE IF NOT EXISTS TriggerTable(
        rule_id VARCHAR(100) PRIMARY KEY,
        trigger_id VARCHAR(100),
        trigger_type VARCHAR(100),
        trigger_content VARCHAR(20000))  """)

    cursor_trigger.execute("""CREATE TABLE IF NOT EXISTS RuleTable(
        rule_id VARCHAR(100) PRIMARY KEY,
        rule_name VARCHAR(100),
        rule_status VARCHAR(100),
        trigger_id VARCHAR(100),
        trigger_content VARCHAR(20000),
        condition_id VARCHAR(100),
        condition_content VARCHAR(20000),
        action_id VARCHAR(100),
        action_content VARCHAR(20000),
        insert_time VARCHAR(100))  """)



    # print (json.dumps(rule))
    # save_data(rule)
    # delete_data(rule)


    try:
        data_list = load_data_from_db()
        # print ("data_list: ", data_list)
        app.run(host='0.0.0.0', port=5001, debug=False)
    except:
        logger.error("Error run rule api", exc_info=True)
    finally:
        db_trigger.commit()
        cursor_trigger.close()


