import sys
import os
import random
import argparse
from pathlib import Path
import pickle

from python_instantiator.DatasetJob.utils import get_element_by_seed

import python_instantiator.AbstractOperator as AbstractOperator
from python_instantiator.DatasetJob.tpc_h.TPCHDatasetOperatorManager import TPCHDatasetOperatorManager


class SubqueryGenerator:
    types = {0: "EXISTS",
             1: "COMPARISON",
             2: "IN"
            }
    
    def __init__(self,dataset_operator_manager, max_joins, seed = 0):
        self.dataset_operator_manager = dataset_operator_manager
        self.seed = seed
        random.seed(self.seed) 
        self.max_joins = max_joins
        self.n_joins = random.randint(0, max_joins)
        self.used_filters = []
        
        self.subtype = self.types[random.randint(0,2)]
        print(self.subtype)
        self.table_sequence, self.join_sequence = self.dataset_operator_manager.get_source_table_sequence(self.n_joins, self.seed)
        
        # far relation has to be used for filter
        self.relation, self.far_relation = self.find_relations()

        # Again something is done with the JobInfoRecorder
        self._join_queue = [] # not sure if that is correct
        self._table_queue = []
        for table in self.table_sequence:
            self._table_queue.append(table)
        for join in self.join_sequence:
            self._join_queue.append(join)

        self.code = ""
        self.info = {"TYPE": self.subtype}
        self.pickle_dict = {}
        
    def find_relations(self):
        seed = random.randint(0,100)
        table_names = [item.table_name for item in self.table_sequence]
        joins = [(join.rx.table_name,join.lx.table_name) for join in self.join_sequence]
        poss_relation = None
        far_relation = None        
        # when there is no join, far is the relation in table_names 
        # and relation is the relation too for COMPARISON
        if len(joins) == 0:
            far_relation = self.table_sequence[0]
            if self.subtype == "COMPARISON":
                poss_relation = self.table_sequence[0].map_fields
            else:
                poss_relation = []
                for key in self.table_sequence[0].join_field_table.keys():
                    for subkey in self.table_sequence[0].join_field_table[key]:
                        poss_relation.append((self.table_sequence[0].join_field_table[key][subkey], key))
        
        outer_relations = {}
        # Get the relations occuring only once inside the join relations --> outer relations
        for join in self.join_sequence:
            if join.rx.table_name not in outer_relations.keys():
                outer_relations[join.rx.table_name] = {"Manager": join.rx, "Count": 0}
            else:
                outer_relations[join.rx.table_name]["Count"] = 1
            if join.lx.table_name not in outer_relations.keys():
                outer_relations[join.lx.table_name]  = {"Manager": join.lx, "Count": 0}
            else:
                outer_relations[join.lx.table_name]["Count"] = 1
        
        for rel in outer_relations.keys():
            if outer_relations[rel]["Count"] == 0:
                man = outer_relations[rel]["Manager"]
                if poss_relation:
                    far_relation = man
                else:
                    if self.subtype == "COMPARISON":
                        poss_relation = man.map_fields
                        if not poss_relation:
                            far_relation = man
                        continue
                    poss_relation = []
                    for key in man.join_field_table.keys():
                        for subkey in man.join_field_table[key]:
                            if subkey not in table_names:
                                poss_relation.append((man.join_field_table[key][subkey], key))
                    if len(poss_relation) == 0:
                        poss_relation = None
                        far_relation = man
        return poss_relation, far_relation
    
    def generate_comparison_code(self):
        self.generate_select_comparison()
        self.generate_from()
        self.generate_where()
        self.generate_joins()
        
    def generate_exists_code(self):
        self.code += "SELECT * "
        self.generate_from()
        self.generate_where()
        self.generate_joins()
        
        rel = random.choice(self.relation)
        
        self.code += "AND "+rel[0]+ " = "+rel[1]
        self.info["RELATION"] = rel[0]
        
    def generate_in_code(self):
        rel = random.choice(self.relation)
        
        self.code += "SELECT "+rel[1]+" "
        self.generate_from()
        self.generate_where()
        self.generate_joins()
        
        self.info["RELATION"] = rel[0]        
        

    
    def generate_select_comparison(self): 
        possible_operators = ["MIN", "MAX", "AVG_1", "AVG_2"]
        
        func_to_operators = {
            "MIN": "=",
            "MAX": "=",
            "AVG_1": "<",
            "AVG_2": ">="
        }
        
        op = random.choice(possible_operators)
        rel = random.choice(self.relation)
        
        self.info["RELATION"] = rel
        self.info["OPERATOR"] = func_to_operators[op]
        
        self.code += "SELECT "+op.split("_")[0]+"("+rel+") "
        self.pickle_dict["SELECT"] = {"FIELD": rel, "OPERATOR": op.split("_")[0]}
        
    
    def generate_from(self):
        temp_from = []
        self.code += "FROM "
        for table in self.table_sequence:
            self.code += table.full_table_name +", "
            temp_from.append(table.full_table_name)
        self.code = self.code[:-2]
        self.pickle_dict["FROM"] = temp_from
        
    def generate_where(self):
        temp_d = None
        att = 0
        while not temp_d and att < 10:
            att += 1
            temp_d, field = self.far_relation.filter_code("TEMP", "TEMP", random.randint(0,100), random.randint(0,100), self.used_filters)
        if att == 10:
            raise Exception("Not possible to do filter")
        self.used_filters.append(field)
        self.pickle_dict["WHERE"] = [temp_d["WHERE"]]
        temp_names = [temp_d["WHERE"]["FIELD"]]
            
        self.where_to_sql()
            
    def where_to_sql(self):
        s = " WHERE "
        for field in self.pickle_dict["WHERE"]:
            if field["OPERATOR"] == "BETWEEN":
                s += f"{field['FIELD']} BETWEEN {field['VALUE'][0]} AND {field['VALUE'][0]} AND "
            else:
                s += f'{field["FIELD"]} {field["OPERATOR"]} {field["VALUE"]} AND '
            
        self.code += s[:-4]        
    
    def generate_joins(self):
        self.pickle_dict["WHERE JOIN"] = []
        for join in self.join_sequence:
            temp_dict = {"FIELD": join.field, "RIGHT": join.rx.table_name, "LEFT": join.lx.table_name}
            self.pickle_dict["WHERE JOIN"].append(temp_dict)
            f = join.field.split("_")[1]
            left = join.lx.suffix+f
            right = join.rx.suffix+f
            self.code += "AND " + left +" = " + right + " "
    
    def generate(self):
        if self.subtype == "COMPARISON":
            self.generate_comparison_code()
        elif self.subtype == "EXISTS":
            self.generate_exists_code()
        else:
            self.generate_in_code()
        print(self.code)
        
    def create(self, path, job_id):
        path = path+"/"+str(job_id)
        with open(path+"_sql.txt","w") as f:
            f.write(self.code)
        with open(path+"_info.pickle", "wb") as f:
            pickle.dump(self.info, f)
        with open(path+"_dict.pickle", "wb") as f:
            pickle.dump(self.pickle_dict, f)

def parse_args(args):
    params = {}
    if args.__len__() >= 4:
        params["nJobs"] = int(args[0])
        params["dataManager"] = args[1]
        params["genJobsDest"] = args[2]
        params["maxJoins"] = args[3]
        try:
            params["jobSeed"] = int(args[4])
        except:
            params["jobSeed"] = -1
    else:
        raise Exception("Expected arguments: <nJobs> <nVersions> <dataManager> <genJobsDest> <maxJoins> [jobSeed]")
    return params

def main(args):
    params = parse_args(args)
    
    for i in range(params["nJobs"]):
        # here, a JobInfoRecorder is instatiated, I will skip this for now
        job_nr = i
        tpch_dataset_op_mng = TPCHDatasetOperatorManager()
        print("\n---------------------------------------")
        print(f"----- GENERATING SUBQUERY {i} --------------\n")
            
        job_id = f"Job{i}"
        # Something is now done for the JobInfoRecorder, this will be skipped again until it is needed 
        # or until I have finished the rest
        job_seed = params["jobSeed"]
        if job_seed < 0:
            job_seed = job_nr
        
        sub_generator = SubqueryGenerator(tpch_dataset_op_mng, params["maxJoins"], seed = 10*job_seed + i)
        #try:
        sub_generator.generate()  
        sub_generator.create(params["genJobsDest"], job_id)
        #except:
        

if __name__ == '__main__':
    main(sys.argv[1:])
               
                    
                    
                    
                    
                    
                    
                    
                    
            