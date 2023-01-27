from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils

class LineitemOperatorManager(AbstractTableOperatorManager):
    table_name = "lineitem"
    full_table_name = "LINEITEM"
    suffix = "L_"
    type_schema = "(int, int, int, int, float, float, float, float, str, str, date, date, date, str, str, str)"
    fields = {
        "L_ORDERKEY": "0",
        "L_PARTKEY": "1",
        "L_SUPPKEY": "2",
        "L_LINENUMBER": "3",
        "L_QUANTITY": "4",
        "L_EXTENDEDPRICE": "5",
        "L_DISCOUNT": "6",
        "L_TAX": "7",
        "L_RETURNFLAG": "8",
        "L_LINESTATUS": "9",
        "L_SHIPDATE": "10",
        "L_COMMITDATE": "11",
        "L_RECEIPTDATE": "12",
        "L_SHIPINSTRUCT": "13",
        "L_SHIPMODE": "14",
        "L_COMMENT": "15"
    }
    
    join_field_table = {
        "L_ORDERKEY": {"orders": "L_ORDERKEY"},
        "L_PARTKEY": {"part": "P_PARTKEY", "partsupp": "PS_PARTKEY"},
        "L_SUPPKEY": {"supplier": "S_SUPPKEY", "partsupp": "PS_SUPPKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "L_QUANTITY": {
            "selectivity": ["0.25", "0.5", "0.75"],
            "values": ["13.0", "26.0", "38.0"]
        },
        "L_SHIPDATE": {
            'selectivity': ['0.25', '0.5', '0.75'],
            'values': ["'1993-10-26'", "'1995-06-19'", "'1997-02-09'"]
        },
        "L_RECEIPTDATE": {
            'selectivity': ['0.25', '0.5', '0.75'],
            'values': ["'1993-11-11'", "'1995-07-05'", "'1997-02-24'"]
        },
        "L_SHIPMODE": {
            "selectivity": ["0.1414", "0.1417", "0.1411", "0.1432", "0.1421", "0.1462"],
            "values": ["'AIR'", "'FOB'", "'MAIL'", "'RAIL'", "'REG AIR'", "'SHIP'", "'TRUCK'"]
        }
    }
    contradict_filter = {}
    
    group_fields = {
        "L_ORDERKEY": "1500000",
        "L_PARTKEY": "200000",
        "L_SUPPKEY": "10000",
        "L_SHIPMODE": "7",
        "L_RETURNFLAG": "3",
    }
    
    contradict_group = {"L_PARTKEY": ["PS_PARTKEY"], "L_SUPPKEY": ["PS_SUPPKEY"]}
    
    map_fields = ["L_QUANTITY", "L_DISCOUNT"]
    
    ship_date_choices = ["<="] #, ">", "BETWEEN"] it's too consuming to collect selectivities for all operators
    
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

        if field == "L_SHIPDATE":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"], value_seed))
            filter_op = "<="
            #return self.handle_shipdate(field_seed+value_seed, field_seed+1, value_seed+1), field, selectivity
        elif field == "L_RECEIPTDATE":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"], value_seed))
            filter_op = "<="
            #return self.handle_receiptdate(field_seed+value_seed, field_seed+1, value_seed+1), field, selectivity
        elif field == "L_QUANTITY":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"], value_seed))
            filter_op = "<="
        elif field == "L_SHIPMODE":
            filter_value = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            filter_op = "="


        return {"WHERE": {"FIELD": field, "OPERATOR": filter_op, "VALUE": filter_value}}, field, selectivity
            
        
    
    def handle_shipdate(self, op_seed, value_seed_1, value_seed_2):
        filter_op = self.ship_date_choices[op_seed % len(self.ship_date_choices)]
        
        if filter_op == "BETWEEN":
            filter_value_1 = str(utils.get_element_by_seed(self.filter_field_value["L_SHIPDATE"]["values"], value_seed_1))
            filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["L_SHIPDATE"]["values"], value_seed_2))
            i=1
            while filter_value_1 == filter_value_2:
                filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["L_SHIPDATE"]["values"], value_seed_2+i))
                i += 1
            
            if filter_value_1 > filter_value_2:
                return {"WHERE": {"FIELD": "L_SHIPDATE", "OPERATOR": filter_op, "VALUE": [filter_value_2, filter_value_1]}}
            return {"WHERE": {"FIELD": "L_SHIPDATE", "OPERATOR": filter_op, "VALUE": [filter_value_1, filter_value_2]}}
        elif filter_op == "<=":
            value = str(utils.get_element_by_seed(self.filter_field_value["L_SHIPDATE"]["values"], value_seed_2))
            if (value_seed_1 % 2) == 0:
                if (value_seed_2 % 2) == 0:
                    return {"WHERE": {"FIELD": "L_SHIPDATE", "OPERATOR": filter_op, "VALUE": value}}
                else:
                    return {"WHERE": {"FIELD": "L_SHIPDATE", "OPERATOR": filter_op, "VALUE": value}}
            else:
                return {"WHERE": {"FIELD": "L_SHIPDATE", "OPERATOR": filter_op, "VALUE": value}}
        else:
            value = str(utils.get_element_by_seed(self.filter_field_value["L_SHIPDATE"]["values"], value_seed_2))
            return {"WHERE": {"FIELD": "L_SHIPDATE", "OPERATOR": filter_op, "VALUE": value}}
        
        
    def handle_receiptdate(self, op_seed, value_seed_1, value_seed_2):
        filter_op = self.ship_date_choices[op_seed % len(self.ship_date_choices)]
        
        if filter_op == "BETWEEN":
            filter_value_1 = str(utils.get_element_by_seed(self.filter_field_value["L_RECEIPTDATE"]["values"], value_seed_1))
            filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["L_RECEIPTDATE"]["values"], value_seed_2))
            i=1
            while filter_value_1 == filter_value_2:
                filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["L_RECEIPTDATE"]["values"], value_seed_2+i))
                i += 1
            
            if filter_value_1 > filter_value_2:
                return {"WHERE": {"FIELD": "L_RECEIPTDATE", "OPERATOR": filter_op, "VALUE": [filter_value_2, filter_value_1]}}
            return {"WHERE": {"FIELD": "L_RECEIPTDATE", "OPERATOR": filter_op, "VALUE": [filter_value_1, filter_value_2]}}
        elif filter_op == "<=":
            value = str(utils.get_element_by_seed(self.filter_field_value["L_RECEIPTDATE"]["values"], value_seed_2))
            if (value_seed_1 % 2) == 0:
                if (value_seed_2 % 2) == 0:
                    return {"WHERE": {"FIELD": "L_RECEIPTDATE", "OPERATOR": filter_op, "VALUE": value}}
                else:
                    return {"WHERE": {"FIELD": "L_RECEIPTDATE", "OPERATOR": filter_op, "VALUE": value}}
            else:
                return {"WHERE": {"FIELD": "L_RECEIPTDATE", "OPERATOR": filter_op, "VALUE": value}}
        else:
            value = str(utils.get_element_by_seed(self.filter_field_value["L_RECEIPTDATE"]["values"], value_seed_2))
            return {"WHERE": {"FIELD": "L_RECEIPTDATE", "OPERATOR": filter_op, "VALUE": value}}