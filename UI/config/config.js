//The API URLs

//the base URL to get the platform, things, items, history
//var sensor_API = "http://7938ee0f.ngrok.io";
var sensor_API = "http://192.168.0.102:5000";

// var api_namea = "http://7938ee0f.ngrok.io";
//the base URL to post and get the rules

// var rule_API = "http://25.14.206.65:5001";
var rule_API = "http://localhost:5000";


var operator_button_list = ["AND", "OR"];

var action_list = ["update"];
// var action_list = ["update", "send a command", "enable or disable rule", "run rule", "execute given script", "write log"];

var trigger_list= ["item_has_given_state", "item_state_change", "item_state_update", "item_receive_command", "fix_time_of_day"];

var condition_list = ["item_has_given_state", "given_script_is_true", "certain_day_of_week"]

var items_can_action = ["Green Light", "Yellow Light", "Red Light"];

