class AbstractOperator:
    
    def __init__(self, op_id, table_operator_manager=None, parents=None, children=None):
        self.op_id = op_id
        self.table_operator_manager = table_operator_manager
        self.parents = parents
        self.children = children
        self.op_pact = op_id.split("_")[0]
        self.n_id = int(op_id.split("_")[1])
        
    def get_variable_name(self):
        match_dict = {
            "Data Source": f"source{self.n_id}",
            "Map": f"map{self.n_id}",
            "Filter": f"filter{self.n_id}",
            "Join": f"join{self.n_id}",
            "Reduce": f"reduce{self.n_id}",
            "Group by": f"group{self.n_id}",
            "Bulk iteration": f"iteration{self.n_id}",
            "Partition": f"partition{self.n_id}",
            "Sort-Partition": f"sort{self.n_id}",
            "Data Sink": f"sink{self.n_id}"
        }
        return match_dict[self.op_pact]
    
    def data_source_code(self):
        # Do something with the Job Info Recorder
        return self.table_operator_manager.data_source_code(self.get_variable_name())
    
    def map_code(self, in_var_name, has_group, has_order, map_seed, complexity = 0):
        return self.table_operator_manager.map_code(in_var_name, self.get_variable_name(), has_group, has_order, map_seed, complexity)
        
    def join_code(self, lx_var_name, rx_var_name, join_relation):
        return self.table_operator_manager.join_code(lx_var_name, rx_var_name, self.get_variable_name(), join_relation)
    
    def group_by_code(self, in_var_name, next_op=None, seed=0, used_groups = []):
        if next_op:
            c, field = self.table_operator_manager.group_by_code_2(in_var_name, self.get_variable_name(), next_op.get_variable_name(), seed, used_groups)
        else:
            c, field = self.table_operator_manager.group_by_code_1(in_var_name, self.get_variable_name(), seed, used_groups)
            
        return c, field
    
    def reduce_code(self, in_var_name, next_op=None, seed=0): 
        if next_op:
            c = self.table_operator_manager.reduce_code_2(in_var_name, self.get_variable_name(), next_op.get_variable_name(), seed)
        else:
            c = self.table_operator_manager.reduce_code_1(in_var_name, self.get_variable_name())
            
        return c
    
    def sink_code(self, in_var_name):
        return self.table_operator_manager.sink_code(in_var_name, self.get_variable_name())
    
    def filter_code(self, in_var_name, seed=0, used_filters = []):
        #return self.table_operator_manager.filter_code(in_var_name, self.get_variable_name(), seed, seed)
        filter_code, field = self.table_operator_manager.filter_code(in_var_name, self.get_variable_name(), seed, seed, used_filters)
        return filter_code, field
    
    def sort_partition_code(self, in_var_name, has_group, seed_1=0, seed_2=0):
        return self.table_operator_manager.sort_partition_code(in_var_name, self.get_variable_name(), has_group, seed_1, seed_2)
    
    def partition_code(self, in_var_name):
        return self.table_operator_manager.partition_code(in_var_name, self.get_variable_name())