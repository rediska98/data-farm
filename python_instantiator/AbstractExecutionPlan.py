import json
import networkx as nx
from python_instantiator.AbstractOperator import AbstractOperator

class AbstractExecutionPlan:
    def __init__(self, exec_plan=None):
        if not exec_plan:
            self.exec_plan = nx.DiGraph()
        else:
            self.exec_plan = exec_plan
            
    def get_nr_joins(self):
        def filter_joins(n):
            return self.exec_plan.nodes[n]["pact"] == "Join"
        
        return nx.number_of_nodes(nx.subgraph_view(self.exec_plan, filter_node=filter_joins))

    def get_operators(self):
        operators = []
        for (node, pact) in self.exec_plan.nodes.data('pact'):
            if pact not in operators:
                operators.append(pact)
        return operators
    
    def get_parents_from(self, node_id):
        new_abstract_graph = nx.DiGraph()
        possible_prev = [node_id]
        nodes_to_color = []
        edges_to_color = []
        source = ""
        source_idx = 0
        for (u, v, c) in self.exec_plan.edges.data('color'):
            if v in possible_prev and c != "red":
                if source == "":
                    nodes_to_color.insert(0,u)
                    edges_to_color.insert(0,[u,v])
                else:
                    nodes_to_color.append(u)
                    edges_to_color.append([u,v])
                if self.exec_plan.nodes[u]["pact"] == "Data Source" and source == "":
                    source = u
                    
                possible_prev.append(u)
        for n in nodes_to_color:
            new_abstract_graph.add_node(n, pact=self.exec_plan.nodes[n]["pact"], color="black")
            self.exec_plan.nodes[n]["color"] = "red"
        for e in edges_to_color:
            new_abstract_graph.add_edge(e[0],e[1], color = "black")
            self.exec_plan.edges[e[0],e[1]]["color"] = "red"            
        return new_abstract_graph, source
    
    def get_single_path(self):
        nodes = []
        prev = ""
        for (u, v) in self.exec_plan.edges():
            if prev == "":
                prev = v
                nodes.append(u)
            elif prev == u:
                nodes.append(u)
                prev = v
        nodes.append(prev)
        return nodes
                
    def all_nodes_to_black(self):
        for (u, v) in self.exec_plan.edges():
            self.exec_plan.edges[u,v]["color"] = "black"
            self.exec_plan.nodes[u]["color"] = "black"
            self.exec_plan.nodes[v]["color"] = "black"    

def parseExecPlan(path):
    abstract_plan = None
    with open(path) as f:
        data = json.load(f)
        abstract_plan = AbstractExecutionPlan()
        for node in data["nodes"]:
            # I put all nodes black which are not visited yet
            abstract_plan.exec_plan.add_node(node["id"], pact = node["pact"], color = "black")
        
        for edge in data["links"]:
            abstract_plan.exec_plan.add_edge(edge["source"], edge["target"], color = "black")

    return abstract_plan