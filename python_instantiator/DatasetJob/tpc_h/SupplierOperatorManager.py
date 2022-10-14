from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils
import random

class SupplierOperatorManager(AbstractTableOperatorManager):
    table_name = "supplier"
    full_table_name = "tcph.dbo.SUPPLIER"
    type_schema = "(int, str, str, int, str, float, str)"
    suffix = "S_"
    fields = {
        "S_SUPPKEY": "0",
        "S_NAME": "1",
        "S_ADDRESS": "2",
        "S_NATIONKEY": "3",
        "S_PHONE": "4",
        "S_ACCTBAL": "5",
        "S_COMMENT": "6"
    }
    
    join_field_table = {
        "S_SUPPKEY": {"partsupp": "PS_SUPPKEY", 
                      "lineitem": "L_SUPPKEY"},
        "S_NATIONKEY": {"nation": "N_NATIONKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "S_ACCTBAL": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["1770.7175", "4541.0650000000005", "7270.635"]
        },
        "S_NATIONKEY": {
            "selectivity": ["0.25", "0.5", "0.75", "0.25", "0.5", "0.75", "0.25", "0.5", "0.75"],
            "values": ["2.0", "3.0", "4.0", "5.0" , "6.0", "12.0", "18.0", "22.0", "24.0"]
        }
    }
    contradict_filter = {"S_NATIONKEY": ["C_NATIONKEY", "N_REGIONKEY", "R_NAME"]}
    
    group_fields = {
        "S_NATIONKEY": "25"
    }
    
    contradict_group = {"S_NATIONKEY": ["R_NAME", "N_REGIONKEY"]}
    
    map_fields = ["S_ACCTBAL"]
    
    def __init__(self):
        super().__init__()
    
    def data_source_code(self, out_var_name):
        return self.build_data_source_code(self.type_schema, self.full_table_name, out_var_name)
                 
    def filter_code(self, in_var_name, out_var_name, field_seed=0, value_seed=0, used_filters = []):
        field = self.filter_fields[field_seed % len(self.filter_fields)]
        filter_field = ""
        filter_value = ""
        filter_op = ""
        attempts = 0
        while attempts < 10:
            attempts += 1
            if field in self.contradict_filter.keys():
                if any([el in self.contradict_filter[field] for el in used_filters]) or field in used_filters:
                    field = self.filter_fields[(field_seed+attempts) % len(self.filter_fields)]
                else:
                    break                
                if attempts == 10:
                    return None, None, None
            else:
                break
                
        if field == "S_ACCTBAL":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "<="
        elif field == "S_NATIONKEY":
            if (value_seed * field_seed) % 5 == 0:
                s = random.randint(3,6)
                subset = random.sample(self.filter_field_value["S_NATIONKEY"]["values"], s)
                filter_value = "("+", ".join(subset)+")"
                filter_op = "IN"
            #filter_field = self.fields[field]
            else:
                filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
                filter_op = "="

        selectivity = utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)

        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity
            
        