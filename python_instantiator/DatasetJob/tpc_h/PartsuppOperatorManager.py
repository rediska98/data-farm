from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils

class PartsuppOperatorManager(AbstractTableOperatorManager):
    table_name = "partsupp"
    full_table_name = "PARTSUPP"
    type_schema = "(int, int, int, float, str)"
    suffix = "PS_"
    fields = {
        "PS_PARTKEY": "0",
        "PS_SUPPKEY": "1",
        "PS_AVAILQTY": "2",
        "PS_SUPPLYCOST": "3",
        "PS_COMMENT": "4"
    }
    
    join_field_table = {
        "PS_PARTKEY": {"part": "P_PARTKEY", 
                      "lineitem": "L_PARTKEY"},
        "PS_SUPPKEY": {"supplier": "S_SUPPKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "PS_AVAILQTY": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["2506.0", "5003.0", "7499.0"]
        },
        "PS_SUPPLYCOST": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["250.81", "500.41", "750.2425"]
        }
    }
    
    contradict_filter = {}
    
    group_fields = {
        "PS_PARTKEY": "200000",
        "PS_SUPPKEY": "10000"
    }
    
    contradict_group = {"PS_PARTKEY": ["L_PARTKEY"], "PS_SUPPKEY": ["L_SUPPKEY"]}
    
    map_fields = ["PS_SUPPLYCOST"]
    
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


        if field == "PS_AVAILQTY":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "<="
        elif field == "PS_SUPPLYCOST":
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "<="

        selectivity = utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)

        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity
            
        