from prettytable import PrettyTable
from collections import Counter

class RUSH():
    def __init__(self, hash_size,replication_factor):
        self.node_array = []
        self.replication_factor = replication_factor
        self.hash_size = hash_size
    
    def add_data(self):
        dataArray = [i for i in range(self.hash_size)]
        self.add_array(dataArray)

    def calc_hash(self,x,r,cid):
        hashed = hash((x,r,cid))    # get hash of tuple (x,r,cid)
        mod = abs(hashed) % 10000   # make hash between 0 and 10,000
        prob = mod / 10000.0        # make it a probability between 0 and 1
        return prob

    def search_data(self,value):
        tableHasData = False
        t = PrettyTable(["Value", "Replica ID", "Found at Node ID"])

        # check for the value with each replication id
        for i in range(1,self.replication_factor + 1):
            #print(f"Value {value} Replica {i}")
            node = self.locate_data(value,i)    # fetch the node this value and replica id hash to

            # if the node exists check if this node already has the value were searching for in it
            if node is not None and node.value_in_node(value):
                tableHasData = True
                t.add_row([value, i, node.id])
        
        # if the value was found then print it, if not then tell the user
        if tableHasData:
            print(str(t))
        else:
            print(f"Value {value} was not found!")

    def sum_weights(self,nodes):
        totalWeight = 0.0
        for node in nodes:
            totalWeight += node.weight
        return totalWeight

    def adjust_weights(self):
        pass

    def locate_data(self,value,replica_id): # Finds the node destionation
        nodeIndex = 0   # reset what node were on

        # copy node weights to new array 
        nodeWeights = []
        for node in self.node_array:
            nodeWeights.append(node.weight)
        #print(nodeWeights)

        # loop through every node weight to see if we should place the value there
        for weight in nodeWeights:    # check which node it should be added to
            values_hash = self.calc_hash(value,replica_id,self.node_array[nodeIndex].id)  # get hash
            node_weight = node.weight             # get node weight
            #print(f"Node: {self.node_array[nodeIndex].id} Hash: {values_hash} Weight: {node_weight}")

            # if prob <= weight then place the value in this node
            if values_hash <= nodeWeights[nodeIndex]:
                #print(f"Adding value {value} to node {self.node_array[nodeIndex]}")
                return self.node_array[nodeIndex] # return the node where it should be appended
            
            weightIndex = nodeIndex
            # chnage weights for current set of nodes were considering for value placement
            for curWeight in nodeWeights[nodeIndex+1:]:
                #print(f"curWeight before {curWeight}")
                #print(f"{curWeight} + ({weight} / ({len(nodeWeights)} - {nodeIndex+1}))")
                curWeight = curWeight + (weight/ (len(nodeWeights) - (nodeIndex+1)))
                try:
                    nodeWeights[weightIndex+1] = curWeight
                except:
                    pass
                #weight = curWeight
                #print(f"curWeight after {curWeight}")
                weightIndex += 1
            #print(nodeWeights)
            nodeIndex += 1

    # allocate data to nodes but dont create new replicas
    def add_array_no_replica(self):
        allValues = []
        for node in self.node_array:
            for value in node.data_array:
                allValues.append(value)
        self.clear_node_data()

        valDict = Counter(allValues)

        for value in valDict: # loop through every input value
            for replicaId in range(1,valDict[value]+1):
                #print(f"Value {value} Replica {i}")
                node = self.locate_data(value,replicaId) # what node were hashing to
                # if we didnt find a node to insert the value in put it in node with lowest node id
                if node is None:
                    self.node_array[len(self.node_array)-1].data_array.append(value)
                else:
                    node.data_array.append(value)    # append the value to this nodes data array

    # allocate data to nodes
    def add_array(self,data_array):
        arrayIndex = 0      # index of value were on in data_array

        for value in data_array: # loop through every input value
            for i in range(1,self.replication_factor + 1): # add every replica
                #print(f"Value {value} Replica {i}")
                node = self.locate_data(value,i) # what node were hashing to
                node.data_array.append(value)    # append the value to this nodes data array
                arrayIndex += 1

    # check if this node id already exists in list
    def node_id_exists(self,id):
        for node in self.node_array:
            if node.id == id:
                return True
        return False

    # erases all data in every node
    def clear_node_data(self):
        for node in self.node_array:
            node.data_array = []

    # called when user chooses "add node" as input
    def add_new_node(self,node_id=0):
        # check if we already have a node with that id
        if self.node_id_exists(node_id):
            #print("Node with that id already exists!")
            self.add_new_node(node_id+1)
        else:
            self.node_array.insert(len(self.node_array) - node_id,CephNode(node_id)) # insert node to front of array
            self.reset_Weights() # sum of all nodes (1 / node weight) should be = 1
            self.add_array_no_replica()

    # called when we initialize the nodes with data at start of program
    def add_node(self,node_val):
        self.node_array.insert(0,CephNode(node_val)) # insert node to front of array
        self.reset_Weights() # sum of all nodes (1 / node weight) should be = 1
        #self.redistribute_weights()
    
    def reset_Weights(self):
        # weight for every node is 1 / num of nodes
        for node in self.node_array:
            node.weight = 1 / len(self.node_array)

    def remove_node(self, node_id):
        try:
            self.node_index = 0

            # find the index of the node with node_id
            for node in self.node_array:
                if node.id == node_id:
                    break
                self.node_index += 1
            
            # remove that index from our list
            self.node_array.pop(self.node_index)
            self.reset_Weights()
            self.add_array_no_replica()
        except IndexError:
            print("No nodes exist with this ID! Returning to menu...")
    
    # For debugging only, prints total number of values in all nodes data arrays
    def total_num_values(self):
        total = 0
        for node in self.node_array:
            total += len(node.data_array)
        print('Total values in all nodes: ' + str(total))

    def load_balance(self, over, under):
        # get and cast values from input
        overId = int(over[0])
        overWeight = float(over[1])
        underId = int(under[0])
        underWeight = float(under[1])

        # change the weights for the specified nodes to the specified weights
        for node in self.node_array:
            if node.id == overId:
                node.weight = overWeight
            if node.id == underId:
                node.weight = underWeight
        
        totalWeight = self.sum_weights(self.node_array)
        difference = abs(totalWeight - 1)
        # change weights of remaining nodes so sum of weights is equal to 1
        if totalWeight > 1:
            for node in self.node_array:
                if node.id != overId and node.id != underId:
                    node.weight = node.weight - (difference / (len(self.node_array) - 2))
        if totalWeight < 1:
            for node in self.node_array:
                if node.id != overId and node.id != underId:
                    node.weight = node.weight + (difference / (len(self.node_array) - 2))
        # redistribute the data according to new node weights
        self.add_array_no_replica()

    def __repr__(self):
        if ((self.hash_size * self.replication_factor) / len(self.node_array)) < 30:
            t = PrettyTable(["Node ID", "Weight", "Values", "# of Values"])

            for cephNode in self.node_array:
                nodeWeight = "{:.4f}".format(cephNode.weight)
                numValues = len(cephNode.data_array)
                t.add_row([cephNode.id, nodeWeight, cephNode.values(), numValues])
            totalWeight = f"Total of all weights is {self.sum_weights(self.node_array)}"
        else:
            t = PrettyTable(["Node ID", "Weight", "# of Values"])

            for cephNode in self.node_array:
                nodeWeight = "{:.4f}".format(cephNode.weight)
                numValues = len(cephNode.data_array)
                t.add_row([cephNode.id, nodeWeight, numValues])
        #print(totalWeight)
        return str(t)



class CephNode():
    def __init__(self,id):
        self.id = id            
        self.threshold = 10     # max amount of data allowed in the node
        self.weight = 0.0       # represents the denominator of node weight
        self.data_array = []    # data this node contains
        #raise NotImplementedError

    def values(self):
        # returns the values this node contains in a string
        allHashes = ''
        for val in self.data_array:
            allHashes += str(val) + ' '
        return allHashes

    def value_in_node(self,x):
        # check if x is in this nodes data array
        if x in self.data_array:
            return True
        else:
            return False

    def __repr__(self):
        return f"Node ID {self.id} Weight {self.weight}"
        
