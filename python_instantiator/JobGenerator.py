import sys
import os
import numpy as np
import random
import argparse
from pathlib import Path

import python_instantiator.AbstractExecutionPlan as AbstractExecutionPlan
import python_instantiator.AbstractJob as AbstractJob
import python_instantiator.AbstractOperator as AbstractOperator
from python_instantiator.DatasetJob.tpc_h.TPCHDatasetOperatorManager import TPCHDatasetOperatorManager
from python_instantiator.SubqueryHandler import SubqueryHandler

MAX_INT = 100000 # Random max number for the random init

class JobGenerator:
    def __init__(self, abstract_plan, dataset_operator_manager, seed = 0):
        self.abstract_plan = abstract_plan
        self.dataset_operator_manager = dataset_operator_manager
        self.seed = seed
        random.seed(self.seed) 
        self.map_seed = random.randint(0,1)
        self.n_joins = self.abstract_plan.get_nr_joins() 
        self.table_sequence, self.join_sequence = self.dataset_operator_manager.get_source_table_sequence(self.n_joins, self.seed)
        
        # Again something is done with the JobInfoRecorder
        self._join_queue = [] # not sure if that is correct
        self._table_queue = []
        for table in self.table_sequence:
            self._table_queue.append(table)
        for join in self.join_sequence:
            self._join_queue.append(join)
        self._prev_vars = []
        self._map_count = 0
        self._last_operator = None
        self._colored = []
        self.operators = []
        self.used_tables = []
        self.used_filters = []
        self.used_groups = []
        
        #self.subquery_handler = SubqueryHandler("./subqueries")
        
        
    def get_op_code(self, op, next_op=None, second_op=None) -> str:
        if next_op and not second_op:
            out_var = next_op.get_variable_name()
        elif second_op and not next_op:
            out_var = op.get_variable_name()
        elif next_op and second_op:
            raise Exception("Impossible to manage nextOp and secondOp together")
        else: 
            out_var = op.get_variable_name()
            
        gen_code_dict = {
            "Data Source": self.handle_data_source,
            "Map": self.handle_map,
            "Filter": self.handle_filter,
            "Reduce": self.handle_reduce,
            "Join": self.handle_join,
            "Group by": self.handle_group_by,
            "Partition": self.handle_partition,
            "Sort-Partition": self.handle_sort_partition,
            "Data Sink": self.handle_data_sink,
        }
        
        self._prev_vars.append(out_var)
        print(f"|-> Queue status -> ${self._prev_vars}")
        return gen_code_dict[op.op_pact](op, next_op)
        
        
    def handle_data_source(self, op, next_op):
        return op.data_source_code()
    
    def handle_map(self, op, next_op):
        has_group = ("Group by" in self.operators)
        has_order = ("Sort-Partition" in self.operators)
        c = op.map_code(self._prev_vars.pop(-1), has_group, has_order, self.map_seed, complexity= self._map_count%3)
        self._map_count += 1
        return c
    
    def handle_filter(self, op, next_op):
        filt, field = op.filter_code(self._prev_vars.pop(-1), random.randint(0,MAX_INT), self.used_filters)
        if field:
            self.used_filters.append(field)
        return filt
    
    def handle_reduce(self, op, next_op):
        return op.reduce_code(self._prev_vars.pop(-1), next_op, random.randint(0,MAX_INT))
    
    def handle_join(self, op, next_op):
        join_relation = self._join_queue.pop(0)
        return op.join_code(join_relation.rx, join_relation.lx, join_relation)
    
    def handle_group_by(self, op, next_op):
        group_by, field = op.group_by_code(self._prev_vars.pop(-1), next_op, random.randint(0,MAX_INT), self.used_groups)
        if field:
            self.used_groups.append(field)
        return group_by
    
    def handle_partition(self, op, next_op):
        return op.partition_code(self._prev_vars.pop(-1))
    
    def handle_sort_partition(self, op, next_op):
        has_group = ("Group by" in self.operators)
        return op.sort_partition_code(self._prev_vars.pop(-1), has_group, random.randint(0, MAX_INT), random.randint(0, MAX_INT))
    
    def handle_data_sink(self, op, next_op):
        return op.sink_code(self._prev_vars.pop(-1))
    
    def color_edge(self, op_id_1, op_id_2):
        self._colored.append(f"{op_id_1};{op_id_2}")
        
    def check_colored(self, op_id_1, op_id_2):
        return f"{op_id_1};{op_id_2}" in self._colored
    
    def _generate(self, entry_op_id, abstract_plan=None):
        abstract_plan = abstract_plan or self.abstract_plan
        prev_op_id = "Start"
        temp_prev_op_id = ""
        skip_next = False
        
        table_op_manager = self._table_queue.pop(0)
        branch_operators = abstract_plan.get_single_path()
        operator_codes = []
        
        # It might happen that the code is a bit mixed up, that might be the case because of the different handling of the for-loop
        for op in branch_operators:
            operator = AbstractOperator.AbstractOperator(op, table_operator_manager=table_op_manager)            
            op_code = ""
            abstract_plan.exec_plan.nodes[op]["color"] = "red" # Node visited
            if prev_op_id != "Start":
                abstract_plan.exec_plan.edges[prev_op_id, operator.op_id]["color"] = "red"
            
            if skip_next:
                skip_next = False
                prev_op_id = operator.op_id
                continue
            
            if operator.op_pact == "Join":
                if op == branch_operators[-1]:
                    break
                
                new_exec_plan, source = abstract_plan.get_parents_from(operator.op_id)
                new_abstract_plan = AbstractExecutionPlan.AbstractExecutionPlan(new_exec_plan)
                
                before_join_code = self._generate(source, new_abstract_plan)
                rx_operator = self._last_operator
                table_op_manager = rx_operator.table_operator_manager
                op_join_code = self.get_op_code(operator)
                before_join_code.append(op_join_code)
                operator_codes.extend(before_join_code)
                #op_code = "\n".join(before_join_code)
                
            elif operator.op_pact == "Group by":
                idx = operator.n_id
                if idx + 1 < len(branch_operators) and branch_operators[idx+1].split("_")[0] == "Group by":
                    skip_next = True
                    next_op = AbstractOperator.AbstractOperator(branch_operators[idx+1]) # table_operator_manager seems to be needed, but is not inserted yet
                    op_code = self.get_op_code(operator, next_op)
                else:
                    op_code = self.get_op_code(operator)
                    
            elif operator.op_pact == "Reduce":
                prev_op_id = operator.op_id
                continue
                
            else:
                op_code = self.get_op_code(operator) 
                
            prev_op_id = operator.op_id
            self._last_operator = operator
            if op_code:
                operator_codes.append(op_code)
        abstract_plan.all_nodes_to_black()
        return operator_codes
        
        
    def generate(self, entry_op_id, job_name, save_execution_plan = False):
        self.operators = self.abstract_plan.get_operators()
        operator_codes = self._generate(entry_op_id) 
        #subquery_choice = random.randint(0,20)
        #if subquery_choice == 0:
        #    table = random.choice(self.table_sequence)
        #    table = table.table_name
        #    query = random.choice(self.subquery_handler.table_to_subquery[table])
        #    query["OPERATOR"] = "SUBQUERY"
        #    operator_codes.append({"WHERE": query})
            
        
        seed_1 = random.randint(0,9)
        seed_2 = random.randint(1,10)
        #return AbstractJob.AbstractJob("\n".join(operator_codes), job_name, save_plan=save_execution_plan) 
        return AbstractJob.AbstractJob(operator_codes, seed_1, seed_2)

def parse_args(args):
    params = {}
    if args.__len__() >= 5:
        params["nJobs"] = int(args[0])
        params["nVersions"] = int(args[1])
        params["dataManager"] = args[2]
        params["abstractPlansSource"] = args[3]
        params["genJobsDest"] = args[4]
        try:
            params["jobSeed"] = int(args[5])
        except:
            params["jobSeed"] = -1
    else:
        raise Exception("Expected arguments: <nJobs> <nVersions> <dataManager> <abstractPlansSource> <genJobsDest> [jobSeed]")
    return params

def main(args):
    params = parse_args(args)
    
    for root, dirs, files in os.walk(params["abstractPlansSource"], topdown=False):
        for name in files:
            # here, a JobInfoRecorder is instatiated, I will skip this for now
            job_nr = int(name.split(".")[0].split("_")[-1])
            p = Path(root) / name
            abstract_plan = AbstractExecutionPlan.parseExecPlan(p)
            tpch_dataset_op_mng = TPCHDatasetOperatorManager()
            
            for i in range(params["nVersions"]):
                print("\n---------------------------------------")
                print(f"----- GENERATING JOB {job_nr} v{i} --------------\n")
                    
                job_id = f"Job{job_nr}v{i}"
                # Something is now done for the JobInfoRecorder, this will be skipped again until it is needed 
                # or until I have finished the rest
                job_seed = params["jobSeed"]
                if job_seed < 0:
                    job_seed = job_nr
                try:
                    job_generator = JobGenerator(abstract_plan, tpch_dataset_op_mng, seed = 10*job_seed + i + job_nr)
                #generated_job = job_generator.generate(abstract_plan.node["Data Source_0"]., job_id, saveExecutionPlan=True)
                    generated_job = job_generator.generate("Data Source_0", job_id, save_execution_plan=True)
                # Again something is done with the JobInfoRecorder
                
                    generated_job.create_sbt_project(params["genJobsDest"], job_id)
                except:
                    continue


if __name__ == '__main__':
    main(sys.argv[1:])
    
                
    # JobInfoRecorder.persist
                    
                    
                    
                    
                    
                    
                    
                    
                    
            