import os
import pickle

class SubqueryHandler:
    suffix_to_table = {
        "C": "customer",
        "L": "lineitem",
        "N": "nation",
        "O": "orders",
        "P": "part",
        "PS": "partsupp",
        "R": "region",
        "S": "supplier"
    }
    
    def __init__(self, path):
        self.table_to_subquery = {
            "customer": [],
            "lineitem": [],
            "nation": [],
            "orders": [],
            "part": [],
            "partsupp": [],
            "region": [],
            "supplier": []
        }
        
        for file in os.listdir(path):
            if file.endswith("_info.pickle"):
                temp_d = {
                    "SQL": "",
                    "DICT": ""
                }
                table = ""
                
                job_id = file.split("_")[0]
                with open(path+"/"+job_id+"_dict.pickle", "rb") as f:
                    pickle_dict = pickle.load(f)
                    temp_d["DICT"] = pickle_dict
                with open(path+"/"+file, "rb") as f:
                    pickle_dict = pickle.load(f)
                    table = self.suffix_to_table[pickle_dict["RELATION"].split("_")[0]]
                    if pickle_dict["TYPE"] == "COMPARISON":
                        temp_d["SQL"] = pickle_dict["RELATION"] + " " + pickle_dict["OPERATOR"] + " ("
                    elif pickle_dict["TYPE"] == "IN":
                        temp_d["SQL"] = pickle_dict["RELATION"] + " IN ("
                    else: temp_d["SQL"] = " EXISTS ("
                with open(path+"/"+job_id+"_sql.txt", "r") as f:
                    for line in f:
                        temp_d["SQL"] += line
                temp_d["SQL"] += ")"
                self.table_to_subquery[table].append(temp_d)