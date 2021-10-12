from prettytable import PrettyTable

class CassandraNode():
    def __init__(self, id, value):
        self.id = id
        self.value = value
        self.primary = []
        self.replicas = set()
        self.status = "RUNNING"
        self.new_value = 0
        
        # Links to other nodes
        self.next = None
        self.prev = None
    
    def is_correct_loc(self, hash):
        if self.prev.value > self.value:
            if hash > self.prev.value or hash <= self.value:
                return True
            return False
        else:
            if hash > self.prev.value and hash <= self.value:
                return True
            return False
        
    def shift_left(self, cutoff, replication_factor):
        # Find the indices in the data array to move
        cond = [e <= cutoff for e in self.primary]
        if cond[0]:
            index = cond.index(False)
        else:
            index = cond.index(False, cond.index(True))
            
        # Move the indices
        self.prev.primary += self.primary[:index]
        self.primary = self.primary[index:]
        
        # Print and move the replicas
        for i in range(1, replication_factor):
            array = self.prev.nth_left(replication_factor - i).primary
            self.prev.nth_right(i).replicas.difference_update(array)
            self.prev.replicas.update(array)

    def nth_right(self, n):
        cur_node = self
        for i in range(n):
            cur_node = cur_node.next
        return cur_node
    
    def nth_left(self, n):
        cur_node = self
        for i in range(n):
            cur_node = cur_node.prev
        return cur_node
        
    def space_used(self):
        return len(self.primary) + len(self.replicas)
        
    def __repr__(self):
        return f"NODE {self.id} VALUE {self.value}"

class Cassandra():
    def __init__(self, num_nodes, ring_length, replicas):
        self.nodes = []
        self.ring_length = ring_length
        self.replicas = replicas
        
        # Provisioning new nodes
        self.node_distance = int(ring_length / num_nodes)
        for i in range(num_nodes):
            self.nodes.append(CassandraNode(i, self.node_distance * i))
        self.next_id = len(self.nodes)
        
        # Link the nodes together
        for i in range(len(self.nodes)):
            self.nodes[i].next = self.nodes[i+1] if i != len(self.nodes) - 1 else self.nodes[0]
            self.nodes[i].prev = self.nodes[i-1] if i != 0 else self.nodes[len(self.nodes) - 1]
            
        # Initialize data
        self.init_data()

    def init_data(self):
        # Nodes 1 to n
        for i in range(1, len(self.nodes)):
            # Find the hash values that this node is a direct successor of
            self.nodes[i].primary = list(range(self.node_distance * (i - 1) + 1, self.node_distance * i + 1))
            
            # Apply replicas
            cur_node = self.nodes[i].next
            for r in range(self.replicas - 1):
                cur_node.replicas.update(self.nodes[i].primary)
                cur_node = cur_node.next
                
        # Special for node 0
        self.nodes[0].primary = list(range(self.node_distance * (len(self.nodes)-1) + 1, self.ring_length)) + [0]
        
        # Node 0 replicas
        cur_node = self.nodes[0].next
        for r in range(self.replicas - 1):
            cur_node.replicas.update(self.nodes[0].primary)
            cur_node = cur_node.next
                
    def add_node(self, node_value):
        # Creating the new node with new ID
        new_node = CassandraNode(self.next_id, node_value)
        self.next_id += 1
        
        # Finding the correct place to put the node in the DHT ring
        for i in range(len(self.nodes)):
            if self.nodes[i].is_correct_loc(node_value):
                
                # Change the node links
                self.nodes[i].prev.next = new_node
                new_node.prev = self.nodes[i].prev
                new_node.next = self.nodes[i]
                self.nodes[i].prev = new_node
                self.nodes.append(new_node)
                
                # Move data over
                self.nodes[i].shift_left(node_value, self.replicas)
                break
            
    def remove_node(self, node_value):
        # Find the index of the node in the DHT ring
        i = self.get_node_values().index(node_value)
        
        # Unlink the node
        self.nodes[i].prev.next = self.nodes[i].next
        self.nodes[i].next.prev = self.nodes[i].prev
        self.nodes = self.nodes[:i] + self.nodes[i+1:]
        
    def correct_node(self, hash_value):
        for node in self.nodes:
            if node.is_correct_loc(hash_value):
                return node
    
    def section_len(self, node):
        if node.prev.value > node.value:
            return node.value + self.ring_length - node.prev.value
        else:
            return node.value - node.prev.value
        
    def search_data(self, hash_value):
        status = ""
        replica_nodes = []
        node = self.correct_node(hash_value)
        
        # Check for hash value in direct successor
        if hash_value in node.primary:
            replica_nodes.append(node)
        
        # Check for hash value in replicas in distant successors
        cur_node = node
        for _ in range(self.replicas):
            if hash_value in cur_node.replicas:
                replica_nodes.append(cur_node)
            cur_node = cur_node.next
        
        # Print report
        if len(replica_nodes) > 0:
            status += f"Replicas can be found at {', '.join([f'node {node.id} (value {node.value})' for node in replica_nodes])}.\n"
        if status == "":
            print(f"\nNo replicas with value {hash_value} were found.\n")
        else:
            print(status)
        
    def get_node_values(self):
        return [node.value for node in self.nodes]
    
    def min_section_len(self):
        values = sorted([node.value for node in self.nodes])
        min_len = self.ring_length
        for i in range(len(values) - 1):
            diff = values[i+1] - values[i]
            if diff < min_len:
                min_len = diff
        return min(min_len, self.ring_length - values[len(values) - 1] + values[0])
        
        
    def load_balance(self, busy_node, idle_node):
        # Find where the under/overloaded nodes are
        node_values = self.get_node_values()
        o_index = node_values.index(busy_node)
        u_index = node_values.index(idle_node)

        # Initializations
        o_node = self.nodes[o_index]
        u_node = self.nodes[u_index]
        o_space = o_node.space_used()
        u_space = u_node.space_used()
        
        # Find out how much we can shorten the overloaded node's ring responsibility
        # or lengthening the underloaded node's without overbalancing other nodes
        # Start by shortening the node's right side
        record_array = []
        
        # Shrink the overloaded node's ring on the right
        threshold = self.section_len(o_node) - 1
        o_shrink_num = (o_space - o_node.nth_right(self.replicas).space_used()) // 2
        if o_shrink_num > 1 and threshold >= 2:
            record_array.append(self.shrink_right(o_node, min(o_shrink_num, threshold // 2)))
            
        # Shrink the overloaded node's ring on the left
        threshold = self.section_len(o_node) - 1
        o_shrink_num = (o_space - o_node.prev.space_used()) // 2
        if o_shrink_num > 1 and threshold >= 2:
            record_array.append(self.expand_right(o_node.prev, min(o_shrink_num, threshold // 2)))
        
        # Expand a node's section on the right so that the overloaded node can drop replicas
        threshold = self.section_len(o_node.nth_left(self.replicas - 1)) - 1
        o_expand_num = (o_space - o_node.nth_left(self.replicas).space_used()) // 2
        if o_expand_num > 1 and threshold >= 2:
            record_array.append(self.expand_right(o_node.nth_left(self.replicas), min(o_expand_num, threshold // 2)))
        
        # Expand the underloaded node's ring on the right
        threshold = self.section_len(u_node.next) - 1
        u_expand_num = (u_node.nth_right(self.replicas).space_used() - u_space) // 2
        if u_expand_num > 1 and threshold >= 2:
            record_array.append(self.expand_right(u_node, min(u_expand_num, threshold // 2)))
            
        # Expand the underloaded node's ring on the left (shrink the previous node on the right)
        threshold = self.section_len(u_node.prev) - 1
        u_expand_num = (u_node.prev.space_used() - u_space) // 2
        if u_expand_num > 1 and threshold >= 2:
            record_array.append(self.shrink_right(u_node.prev, min(u_expand_num, threshold // 2)))
        
        # Expand a node's section right so that the underloaded node can receive more replicas
        threshold = self.section_len(u_node.nth_left(self.replicas)) - 1
        u_shrink_num = (u_node.nth_left(self.replicas).space_used() - u_space) // 2
        if u_shrink_num > 1 and threshold >= 2:
            record_array.append(self.shrink_right(u_node.nth_left(self.replicas), min(u_shrink_num, threshold // 2)))
            
        # Print the record table
        t = PrettyTable(["Source ID", "Dest ID", "Values"])
        for r in record_array:
            t.add_row([r.get("source_id"), r.get("dest_id"), r.get("hash_values")])
        print(t)
        
    def create_move_record(self, source, dest, values):
        return {
            "source_id": source.id,
            "dest_id": dest.id,
            "hash_values": values
        }
    
    def shrink_right(self, node, num_values):
        move_values = node.primary[-num_values:]
        node.primary = node.primary[:-num_values]
        node.value = node.primary[-1]
        node.next.primary = move_values + node.next.primary
        
        # Duplicate replica deletion from direct successor
        node.next.replicas.difference_update(move_values)
        
        # Replica addition to nth successor
        node.nth_right(self.replicas).replicas.update(move_values)
        
        return self.create_move_record(node, node.nth_right(self.replicas), move_values)

    def expand_right(self, node, num_values):
        move_values = node.next.primary[:num_values]
        node.primary += move_values
        node.value = node.primary[-1]
        node.next.primary = node.next.primary[num_values:]

        # Duplicate replica deletion from nth successor
        node.nth_right(self.replicas).replicas.difference_update(move_values)
        
        # Replica addition to direct successor
        node.next.replicas.update(move_values)
        
        return self.create_move_record(node.nth_right(self.replicas), node, move_values)
        
            
    def __repr__(self):
        # Stats logging
        t = PrettyTable(["Node ID", "Value", "Space Used", "Status"])
        for node in sorted(self.nodes, key=lambda n: n.value):
            t.add_row([node.id, node.value, node.space_used(), node.status])

        # Detailed logging
        detail_str = ""
        for node in self.nodes:
            detail_str += f"Node {node.id} (Value {node.value}):\n  - Primary Replicas: {node.primary} \n  - Other Replicas: {node.replicas}\n\n"
        
        if self.ring_length * self.replicas / len(self.nodes) < 100:
            return detail_str + "\n" + str(t)
        else:
            return str(t)