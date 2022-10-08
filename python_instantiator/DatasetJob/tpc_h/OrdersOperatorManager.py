from  python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager
import  python_instantiator.DatasetJob.utils as utils

class OrdersOperatorManager(AbstractTableOperatorManager):
    table_name = "orders"
    full_table_name = "tcph.dbo.ORDERS"
    type_schema = "(int, int, str, float, date, str, str, int, str)"
    suffix = "O_"
    fields = {
        "O_ORDERKEY": "0",
        "O_CUSTKEY": "1",
        "O_ORDERSTATUS": "2",
        "O_TOTALPRICE": "3",
        "O_ORDERDATE": "4",
        "O_ORDERPRIORITY": "5",
        "O_CLERK": "6",
        "O_SHIPPRIORITY": "7",
        "O_COMMENT": "8"
    }
    
    join_field_table = {
        "O_ORDERKEY": {"lineitem": "L_ORDERKEY"},
        "O_CUSTKEY": {"customer": "C_CUSTKEY"}
    }
    
    # filter_field_value for the selectivities, don't know if that is relevant for my case yet
    filter_field_value = {
        "O_ORDERSTATUS": {
            "selectivity": ["0.48627533333333334", "0.4880293333333333", "0.025695333333333334"],
            "values": ["'F'", "'O'", "'P'"]
        },
        "O_TOTALPRICE": {
            "selectivity": ["0.25", "0.5", "0.75","0.25", "0.5", "0.75"],# changed, to be regarded again if necessary
            "values": ["5662.01","34886.128966", "77894.7475", "144409.03999999998", "215500.225", "375992.3129"]
        },
        "O_ORDERDATE": {
            "selectivity": ["0.25","0.25", "0.25", "0.5", "0.75", "0.25"], # changed, to be regarded again if necessary
            "values": ["'1992-03-01'","'1993-08-27'", "'1994-05-01'", "'1995-04-20'", "'1996-12-10'", "'1998-02-25'"]
        },
        "O_ORDERPRIORITY": {
            "selectivity": ["0.2","0.2", "0.2", "0.2", "0.2"],
            "values": ["'1-URGENT'", "'2-HIGH'", "'3-MEDIUM'", "'4-NOT SPECIFIED'", "'5-LOW'"]
        }
    }
    contradict_filter = {}
    
    group_fields = {
        "O_CUSTKEY": "99996",
        "O_ORDERSTATUS": "3",
        "O_ORDERPRIORITY": "5"
    }
    
    map_fields = ["O_TOTALPRICE"]

    
    order_date_choices =  ["<=", ">", "BETWEEN"]
    
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
                return None, None
        if field == "O_ORDERDATE":
            return self.handle_orderdate(field_seed+value_seed, field_seed+1, value_seed+1), field
        elif field == "O_TOTALPRICE":
            return self.handle_price(field_seed+value_seed, field_seed+1, value_seed+1), field
        elif field == "O_ORDERSTATUS":
            #filter_field = self.fields[field]
            element = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            return {"WHERE": {"FIELD": "O_ORDERSTATUS", "OPERATOR": "=", "VALUE": element}}, field
            #filter_value = f' "{element}" '
            #filter_op = "=="
        elif field == "O_ORDERPRIORITY":
            element = str(utils.get_element_by_seed(self.filter_field_value[field]["values"],value_seed))
            return {"WHERE": {"FIELD": "O_ORDERPRIORITY", "OPERATOR": "=", "VALUE": element}}, field

            
        #return (self.build_filter_code(in_var_name, out_var_name, field, filter_value, filter_op),
        #        float(utils.get_element_by_seed(self.filter_field_value[field]["selectivity"], value_seed)))
            
    def handle_orderdate(self, op_seed, value_seed_1, value_seed_2):
        # Could be less hardcoded, but sufficient for the beginning
        filter_op = self.order_date_choices[op_seed % len(self.order_date_choices)]
        
        if filter_op == "BETWEEN":
            filter_value_1 = str(utils.get_element_by_seed(self.filter_field_value["O_ORDERDATE"]["values"], value_seed_1))
            filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["O_ORDERDATE"]["values"], value_seed_2))
            i=1
            while filter_value_1 == filter_value_2:
                filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["O_ORDERDATE"]["values"], value_seed_2+i))
                i += 1
            
            if filter_value_1 > filter_value_2:
                return {"WHERE": {"FIELD": "O_ORDERDATE", "OPERATOR": filter_op, "VALUE": [filter_value_2, filter_value_1]}}
                
                #return f" O_ORDERDATE BETWEEN '{filter_value_2}' AND '{filter_value_1}'",0
            return {"WHERE": {"FIELD": "O_ORDERDATE", "OPERATOR": filter_op, "VALUE": [filter_value_1, filter_value_2]}}
        elif filter_op == "<=":
            value = str(utils.get_element_by_seed(self.filter_field_value["O_ORDERDATE"]["values"], value_seed_2))
            if (value_seed_1 % 2) == 0:
                if (value_seed_2 % 2) == 0:
                    return {"WHERE": {"FIELD": "O_ORDERDATE", "OPERATOR": filter_op, "VALUE": f"dateadd(yy, 1, cast({value} as date))"}}
                else:
                    value = str(utils.get_element_by_seed(self.filter_field_value["O_ORDERDATE"]["values"], value_seed_2))
                    return {"WHERE": {"FIELD": "O_ORDERDATE", "OPERATOR": filter_op, "VALUE": f"dateadd(mm, 1, cast({value} as date))"}}
            else:
                return {"WHERE": {"FIELD": "O_ORDERDATE", "OPERATOR": filter_op, "VALUE": value}}
        else:
            value = str(utils.get_element_by_seed(self.filter_field_value["O_ORDERDATE"]["values"], value_seed_2))
            return {"WHERE": {"FIELD": "O_ORDERDATE", "OPERATOR": filter_op, "VALUE": value}}
        
    def handle_price(self, op_seed, value_seed_1, value_seed_2):
        # Could be less hardcoded, but sufficient for the beginning
        filter_op = self.order_date_choices[op_seed % len(self.order_date_choices)]
        
        if filter_op == "BETWEEN":
            filter_value_1 = str(utils.get_element_by_seed(self.filter_field_value["O_TOTALPRICE"]["values"], value_seed_1))
            filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["O_TOTALPRICE"]["values"], value_seed_2))
            i=1
            while filter_value_1 == filter_value_2:
                filter_value_2 = str(utils.get_element_by_seed(self.filter_field_value["O_TOTALPRICE"]["values"], value_seed_2+i))
                i += 1
            
            if filter_value_1 > filter_value_2:
                return {"WHERE": {"FIELD": "O_TOTALPRICE", "OPERATOR": filter_op, "VALUE": [filter_value_2, filter_value_1]}}

            return {"WHERE": {"FIELD": "O_TOTALPRICE", "OPERATOR": filter_op, "VALUE": [filter_value_1, filter_value_2]}}

        elif filter_op == "<=":
            value = str(utils.get_element_by_seed(self.filter_field_value["O_TOTALPRICE"]["values"], value_seed_2))
            return {"WHERE": {"FIELD": "O_TOTALPRICE", "OPERATOR": filter_op, "VALUE": value}}

        else:
            value = str(utils.get_element_by_seed(self.filter_field_value["O_TOTALPRICE"]["values"], value_seed_2))
            return {"WHERE": {"FIELD": "O_TOTALPRICE", "OPERATOR": filter_op, "VALUE": value}}       

    
    