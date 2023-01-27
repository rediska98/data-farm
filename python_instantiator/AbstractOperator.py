class AbstractOperator:

    def __init__(self, op_id, table_operator_manager=None, parents=None, children=None, job_info=None):
        self.op_id = op_id
        self.table_operator_manager = table_operator_manager
        self.parents = parents
        self.children = children
        self.op_pact = op_id.split("_")[0]
        self.n_id = int(op_id.split("_")[1])
        self.job_info = job_info

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
        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name],
                selectivity=1.0
            )
        # Do something with the Job Info Recorder
        return self.table_operator_manager.data_source_code(self.get_variable_name())

    def map_code(self, in_var_name, has_group, has_order, map_seed, complexity = 0):
        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name]
            )
        return self.table_operator_manager.map_code(in_var_name, self.get_variable_name(), has_group, has_order, map_seed, complexity)

    def join_code(self, lx_var_name, rx_var_name, join_relation):
        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [join_relation.lx.table_name, join_relation.rx.table_name]
            )
        return self.table_operator_manager.join_code(lx_var_name, rx_var_name, self.get_variable_name(), join_relation)

    def group_by_code(self, in_var_name, next_op=None, seed=0, used_groups = [], has_reduce = False):
        if next_op:
            c, field, selectivity = self.table_operator_manager.group_by_code_2(in_var_name, self.get_variable_name(), next_op.get_variable_name(), seed, used_groups)
        else:
            c, field, selectivity = self.table_operator_manager.group_by_code_1(in_var_name, self.get_variable_name(), seed, used_groups, has_reduce)

        if self.job_info and c:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name],
                out_cardinality=int(selectivity)
            )
        return c, field

    def reduce_code(self, in_var_name, next_op=None, seed=0):
        if next_op:
            c = self.table_operator_manager.reduce_code_2(in_var_name, self.get_variable_name(), next_op.get_variable_name(), seed)
        else:
            c = self.table_operator_manager.reduce_code_1(in_var_name, self.get_variable_name())

        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name],
                out_cardinality=1
            )
        return c

    def sink_code(self, in_var_name):
        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name]
            )
        return self.table_operator_manager.sink_code(in_var_name, self.get_variable_name())

    def filter_code(self, in_var_name, seed=0, used_filters = []):

        #return self.table_operator_manager.filter_code(in_var_name, self.get_variable_name(), seed, seed)
        filter_code, field, selectivity = self.table_operator_manager.filter_code(in_var_name, self.get_variable_name(), seed, seed, used_filters)
        if selectivity: selectivity = float(selectivity)
        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name],
                selectivity=selectivity
            )

        return filter_code, field

    def sort_partition_code(self, in_var_name, has_group, seed_1=0, seed_2=0):
        if self.job_info:
            self.job_info.record(
                self.n_id,
                self.get_variable_name(),
                self.op_pact,
                [self.table_operator_manager.table_name]
            )
        return self.table_operator_manager.sort_partition_code(in_var_name, self.get_variable_name(), has_group, seed_1, seed_2)

    def partition_code(self, in_var_name):
        # since partition_code always returns empty string - commented out
        # if self.job_info:
        #     self.job_info.record(
        #         self.n_id,
        #         self.get_variable_name(),
        #         self.op_pact,
        #         [self.table_operator_manager.table_name]
        #     )
        return self.table_operator_manager.partition_code(in_var_name, self.get_variable_name())