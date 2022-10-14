from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils

class RegionOperatorManager(AbstractTableOperatorManager):
    table_name = "region"
    full_table_name = "tcph.dbo.REGION"
    type_schema = "(int, str, str)"
    suffix = "R_"
    fields = {
        "R_REGIONKEY": "0",
        "R_NAME": "1",
        "R_COMMENT": "2"
    }
    
    join_field_table = {
        "R_REGIONKEY": {"nation": "N_REGIONKEY"}
    }
    
    filter_field_value = {
        "R_NAME": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["'AFRICA'", "'EUROPE'", "'ASIA'"]
        }
    }
    
    contradict_filter = {"R_NAME": ["C_NATIONKEY", "N_REGIONKEY", "S_NATIONKEY"]}
    
    group_fields = {
        "R_NAME": "5"
    }
    
    contradict_group = {"R_NAME": ["N_REGIONKEY", "S_NATIONKEY"]}

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
        if field == "R_NAME":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "="

        selectivity = utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)

        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity
        