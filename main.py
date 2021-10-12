from dht.ceph import RUSH
from dht.cassandra import Cassandra
from dht import questions as inq
import time
from PyInquirer import style_from_dict, Token, prompt


# Prompt style
style = style_from_dict({
    Token.Separator: '#cc5454',
    Token.QuestionMark: '#673ab7 bold',
    Token.Selected: '#cc5454',  # default
    Token.Pointer: '#673ab7 bold',
    Token.Instruction: '',  # default
    Token.Answer: '#f44336 bold',
    Token.Question: '',
})

# Setup prompt
answers = prompt(inq.setup_prompt(), style=style)
num_nodes = int(answers.get("num_nodes"))
algorithm = answers.get("algorithm")

# Initializations
replication_factor = int(answers.get("replicas"))
hash_size = 2 ** int(answers.get("hash_space_size"))

# Initialize algorithm
print(f"Provisioning {num_nodes} nodes...")
if algorithm == "ceph":
    c = RUSH(hash_size,replication_factor)
    for i in range(num_nodes):
        c.add_node(i)
    c.add_data() # add data to the nodes initially
else:
    c = Cassandra(num_nodes, hash_size, replication_factor)
    
# Print initial node structure
time.sleep(1)
print(c)
print("Setup complete!")

# Prompt until a program exit
while True:
    answers = prompt(inq.system_event_prompt(c), style=style)
    action = answers.get("action")
    if action == "exit":
        break
    elif action=="add_node":
        if algorithm != "ceph":
            node_id = int(answers.get("node_id"))
            c.add_node(node_id)
            print(c)
        else:
            c.add_new_node()
            print(c)
    elif action=="remove_node":
        node_id = int(answers.get("node_id"))
        c.remove_node(node_id)
        print(c)
    elif action=="locate_data":
        value = int(answers.get("hashed_data_locate"))
        c.search_data(value)
        #print(f"{value} Located on Node {node.id}")
    elif action=="load_balance":
        if type(c) == RUSH:
            print("Table before load balancing")
            print(c)
            over = answers.get("overloaded").split(' ')
            under = answers.get("underloaded").split(' ')
        elif type(c) == Cassandra:
            over = int(answers.get("overloaded"))
            under = int(answers.get("underloaded"))
        c.load_balance(over, under)
        print(c)

