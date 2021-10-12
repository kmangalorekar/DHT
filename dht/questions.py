# Number of nodes validator
def node_validator(current):
    try:
        if int(current) < 0:
            return False
        return True
    except Exception:
        return False

# Data addition query and validator
def data_validator(current):
    try:
        num_array = [int(entry) for entry in current.split()]
    except Exception:
        return False
    return True

def setup_prompt():
    return [
        {
            "type": "list",
            "name": "algorithm",
            "message": "Select DHT algorithm:",
            "choices": [
                {
                    "name": "Cassandra",
                    "value": "cassandra"
                },
                {
                    "name": "Ceph",
                    "value": "ceph"
                }
            ]
        },
        {
            "type": "input",
            "name": "num_nodes",
            "message": "How many starting nodes should be provisioned?",
            "validate": node_validator
        },
        {
            "type": "input",
            "name": "hash_space_size",
            "message": "What is the hash space size?",
            "validate": lambda a: a.isnumeric() and int(a) >= 0
        },
        {
            "type": "input",
            "name": "replicas",
            "message": "How many replicas should each data entry have?",
            "validate": lambda a: a.isnumeric() and int(a) > 0
        },
    ]
    
def system_event_prompt(c):
    return [
        {
            "type": "list",
            "name": "action",
            "message": "Select action:",
            "choices": [
                {
                    "name": "Locate data",
                    "value": "locate_data"
                },
                {
                    "name": "Add new node",
                    "value": "add_node"
                },
                {
                    "name": "Remove node",
                    "value": "remove_node"
                },
                {
                    "name": "Load Balance",
                    "value": "load_balance"
                },
                {
                    "name": "Exit program",
                    "value": "exit"
                }
            ]
        },
        {
            "type": "input",
            "name": "node_id",
            "message": "Enter the node value that you would like to add (for ceph enter 0):",
            "validate": lambda a: a.isnumeric() and int(a) >= 0, # make sure input is a number >= 0,
            "when": lambda answers: answers.get("action") == "add_node"
        },
        {
            "type": "input",
            "name": "node_id",
            "message": "Enter the node you would like to remove:",
            "validate": lambda a: a.isnumeric() and int(a) >= 0, # make sure input is a number >= 0
            "when": lambda answers: answers.get("action") == "remove_node"
        },
        {
            "type": "input",
            "name": "hashed_data_locate",
            "message": "Enter the data you would like to locate:",
            "validate": lambda a: a.isnumeric() and int(a) >= 0, # make sure input is a number >= 0
            "when": lambda answers: answers.get("action") == "locate_data"
        },
        {
            "type": "confirm",
            "name": "ceph",
            "message": "Are you load balancing for Ceph?",
            "when": lambda answers: answers.get("action") == "load_balance"
        },
        {
            "type": "input",
            "name": "overloaded",
            "message": "Enter the overloaded node id then the weight (seperated by a space):",
            #"validate": lambda a: a.isnumeric() and int(a) >= 0, # make sure input is a number >= 0
            "when": lambda answers: answers.get("action") == "load_balance" and answers.get("ceph")
        },
        {
            "type": "input",
            "name": "underloaded",
            "message": "Enter the underloaded node id then the weight (seperated by a space):",
            #"validate": lambda a: a.isnumeric() and int(a) >= 0, # make sure input is a number >= 0
            "when": lambda answers: answers.get("action") == "load_balance" and answers.get("ceph")
        },
        {
            "type": "input",
            "name": "overloaded",
            "message": "Enter the overloaded node value:",
            "when": lambda answers: answers.get("action") == "load_balance" and not answers.get("ceph")
        },
        {
            "type": "input",
            "name": "underloaded",
            "message": "Enter the underloaded node value:",
            "when": lambda answers: answers.get("action") == "load_balance" and not answers.get("ceph")
        }
    ]
