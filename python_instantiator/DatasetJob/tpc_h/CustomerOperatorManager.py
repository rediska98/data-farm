from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils

class CustomerOperatorManager(AbstractTableOperatorManager):
    table_name = "customer"
    full_table_name = "CUSTOMER"
    suffix = "C_"
    type_schema = "(int, str, str, int, str, float, str, str)"
    fields = {
        "C_CUSTKEY": "0",
        "C_NAME": "1",
        "C_ADDRESS": "2",
        "C_NATIONKEY": "3",
        "C_PHONE": "4",
        "C_ACCTBAL": "5",
        "C_MKTSEGMENT": "6",
        "C_COMMENT": "7"
    }
    
    join_field_table = {
        "C_CUSTKEY": {"orders": "O_CUSTKEY"},
        "C_NATIONKEY": {"nation": "N_NATIONKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "C_ACCTBAL": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["1757.6200000000001", "4477.3", "7246.3150000000005"]
        },
        "C_MKTSEGMENT": {
            "selectivity": ["0.19834666666666667", "0.20094666666666666", "0.19978666666666667", "0.20126", "0.19966"],
            "values": ["'AUTOMOBILE'", "'BUILDING'", "'FURNITURE'", "'HOUSEHOLD'", "'MACHINERY'"]
        },
        "C_NATIONKEY": {
            'selectivity': ['0.0395', '0.03983333333333333', '0.03999333333333333', '0.04013333333333333', '0.039966666666666664', '0.03968', '0.04066666666666666', '0.03938666666666667',
                            '0.04028', '0.04107333333333334', '0.04006', '0.039753333333333335', '0.03965333333333333', '0.04022',
                            '0.039946666666666665', '0.03947333333333333', '0.03982666666666667', '0.03983333333333333',
                            '0.04016', '0.04066666666666666', '0.03936', '0.04005333333333334', '0.04052', '0.040073333333333336',
                            '0.03988666666666667'],
            'values': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
                       '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24']}
    }
    
    contradict_filter = {"C_NATIONKEY": ["N_REGIONKEY", "R_NAME","S_NATIONKEY"]}
    
    group_fields = {
        "C_MKTSEGMENT": "5"    
    }
    
    contradict_group = {}
    
    map_fields = ["C_ACCTBAL"]

    
    def __init__(self):
        super().__init__()
        # I have the feeling that this main is just for the lols
        #for i in range(3):
        #    for j in range(3):
        #        print(self.filter_code("a", "b", i, j)
    
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

        if field == "C_ACCTBAL":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "<="
        elif field == "C_MKTSEGMENT":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"], value_seed))
            filter_op = "="
        elif field == "C_NATIONKEY":
            # This is not correct, but I will first leave it as it is, so that I can see what is wrong
            #filter_field = self.fields[field]
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "="

        selectivity = utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)

        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity

