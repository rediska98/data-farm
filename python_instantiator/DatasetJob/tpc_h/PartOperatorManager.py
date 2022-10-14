from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils
import random

class PartOperatorManager(AbstractTableOperatorManager):
    table_name = "part"
    full_table_name = "tcph.dbo.PART"
    type_schema = "(int, str, str, str, str, int, str, float, str)"
    suffix = "P_"
    fields = {
        "P_PARTKEY": "0",
        "P_NAME": "1",
        "P_MFGR": "2",
        "P_BRAND": "3",
        "P_TYPE": "4",
        "P_SIZE": "5",
        "P_CONTAINER": "6",
        "P_RETAILPRICE": "7",
        "P_COMMENT": "8"
    }
    
    join_field_table = {
        "P_PARTKEY": {"partsupp": "PS_PARTKEY", 
                      "lineitem": "L_PARTKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "P_RETAILPRICE": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["1249.2475", "1499.495", "1749.7425"]
        },
        "P_TYPE": {
            "selectivity": ["0.2", "0.2", "0.2", "0.2", "0.2"],
            "values": ["'%%BRASS'", "'%%NICKEL'", "'STANDARD%%'", "'MEDIUM%%'", "'%%PLATED%%'"]
        },
        "P_NAME": {
            "selectivity": ["0.2", "0.2", "0.2", "0.2", "0.2"],
            "values": ["'%green%'", "'%tomato%'", "'%almond%'", "'%steel%'", "'%yellow%'"]
        },
        "P_SIZE": {
            "selectivity": ["0.2", "0.2", "0.2", "0.2", "0.2", "0.2", "0.2", "0.2", "0.2"],
            "values": ["1", "11", "15", "18", "24", "26", "32", "36", "44" ]
        }
    }
    contradict_filter = {}
    
    group_fields = {
        "P_BRAND": "25",
        "P_TYPE": "150"
    }
    
    map_fields = ["P_RETAILPRICE"]
    
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
            if field in used_filters:
                field = self.filter_fields[(field_seed+attempts) % len(self.filter_fields)]
            else:
                break                
            if attempts == 10:
                return None, None, None

        selectivity = utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)

        if field == "P_RETAILPRICE":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "<="
        elif field == "P_TYPE":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            return {"WHERE": {"FIELD": "P_TYPE", "OPERATOR": "LIKE", "VALUE": filter_value}}, field, selectivity

        elif field == "P_NAME":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            return {"WHERE": {"FIELD": "P_NAME", "OPERATOR": "LIKE", "VALUE": filter_value}}, field, selectivity

        elif field == "P_SIZE":
            if (value_seed * field_seed) % 5 == 0:
                s = random.randint(3,6)
                subset = random.sample(self.filter_field_value["P_SIZE"]["values"], s)
                filter_value = "("+", ".join(subset)+")"
                filter_op = "IN"
            #filter_field = self.fields[field]
            else:
                filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
                filter_op = "="
            return {"WHERE": {"FIELD": "P_SIZE", "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity

        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity
            
        