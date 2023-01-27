from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils

class NationOperatorManager(AbstractTableOperatorManager):
    table_name = "nation"
    full_table_name = "NATION"
    type_schema = "(int, str, int, str)"
    suffix = "N_"
    fields = {
        "N_NATIONKEY": "0",
        "N_NAME": "1",
        "N_REGIONKEY": "2",
        "N_COMMENT": "3"
    }
    
    join_field_table = {
        "N_NATIONKEY": {"customer": "C_NATIONKEY", "supplier": "S_NATIONKEY"},
        "N_REGIONKEY": {"region": "R_REGIONKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "N_REGIONKEY": {
            'selectivity': ['0.2', '0.2', '0.2', '0.2', '0.2'],
            'values': ['0', '1', '2', '3', '4']
        }
    }
    
    contradict_filter = {"N_REGIONKEY": ["C_NATIONKEY", "R_NAME", "S_NATIONKEY"]}
    
    group_fields = {
        "N_REGIONKEY": "5"    
    }
    
    contradict_group = {"N_REGIONKEY": ["R_NAME", "S_NATIONKEY"]}
    
    def __init__(self):
        super().__init__()
    
    def data_source_code(self, out_var_name):
        return self.build_data_source_code(self.type_schema, self.full_table_name, out_var_name)
                 
    def filter_code(self, in_var_name, out_var_name, field_seed=0, value_seed=0, used_filters = []):
        field = self.filter_fields[field_seed % len(self.filter_fields)]
        filter_field = ""
        filter_value = ""
        filter_op = ""
        if field in self.contradict_filter.keys():
            if any([el in self.contradict_filter[field] for el in used_filters]) or field in used_filters:
                return None, None, None

        if field == "N_REGIONKEY":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"], value_seed))
            filter_op = "="

        selectivity = utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)

        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity
            
        
    
    
    