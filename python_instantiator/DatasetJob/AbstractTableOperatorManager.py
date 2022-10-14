from python_instantiator.DatasetJob.utils import get_element_by_seed

class AbstractTableOperatorManager:
    table_name = ""
    full_table_name = ""
    type_schema = ""
    fields = {}
    join_field_table = {}
    filter_field_value = {}
    group_fields = {}
    map_options = ["COUNT", "AVG", "SUM"]
    map_fields = []
    contradict_group = {}
    contradict_filter = {}
    
    def __init__(self):
        self.join_tables = []
        self.join_fields = []
        
        for key in self.join_field_table.keys():
            self.join_tables.extend(list(self.join_field_table[key].keys()))
            self.join_fields.append(key)
        self.filter_fields = list(self.filter_field_value.keys())
        
    def filter_code(self, in_var_name, out_var_name, field_seed, value_seed, used_filters):
        return "", [], None


    def build_data_source_code(self, schema, table_name, out_var_name, delimiter="|"):
        return {"FROM": table_name}

    def build_filter_code(self, in_var_name, out_var_name, filter_field, filter_value, filter_op, used_filters):
        return {"WHERE": {"FIELD": filter_field, "OPERATOR": filter_op, "VALUE": filter_value}}, filter_field

    def build_group_by_code_1(self, in_var_name, out_var_name, group_by_field, used_groups):
        if group_by_field in self.contradict_group.keys():
            if any([el in self.contradict_group[group_by_field] for el in used_groups]):
                return None, None

        return {"GROUP BY": group_by_field}, group_by_field
        #return f"GROUP BY {group_by_field}"

    def build_group_by_code_2(self, in_var_name, out_var_name_1, out_var_name_2, group_by_field, used_groups):
        return None, None

    def build_sort_partition_code(self, in_var_name, out_var_name, sort_field, order="ASC"):
        return {"ORDER BY": {"FIELD": sort_field, "ORDER": order}}
        #return f"ORDER BY {sort_field} {order}"

    def build_join_code(self, lx_var_name, rx_var_name, out_var_name, join_relation):
        return {"WHERE_JOIN": {"LEFT": lx_var_name, "RIGHT": rx_var_name, "FIELD": join_relation.field}}
        #return f"JOIN {lx_var_name} {join_relation.field} {rx_var_name}"

    def reduce_code_1(self, in_var_name, out_var_name):
        return ""

    def map_code(self, in_var_name, out_var_name, has_group, has_order, map_seed, seed=0):
        map_operator = get_element_by_seed(self.map_options, seed+11)
        if (map_seed % 2 == 0 or has_order) and (not has_group):
            map_field = get_element_by_seed(list(self.fields.keys()), seed+7)
            return {"SELECT": {"FIELD": map_field}}

        if map_operator != "COUNT" and len(self.map_fields) > 0:
            map_field = get_element_by_seed(self.map_fields, seed+7)
        else:
            map_field = "*"
            map_operator = "COUNT"
        return {"SELECT": {"FIELD": map_field, "OPERATOR": map_operator}}


    def join_code(self, lx_var_name, rx_var_name, out_var_name, join_relation):
        return self.build_join_code(lx_var_name, rx_var_name, out_var_name, join_relation)

    def partition_code(self, in_var_name, out_var_name):
        return ""

    def data_source_code(self, out_var_name):
        return self.build_data_source_code(self.type_schema, self.table_name, self.out_var_name)

    def group_by_code_1(self, in_var_name, out_var_name, seed, used_groups):
        g_field = str(get_element_by_seed(list(self.group_fields.keys()), seed))
        g_field_id = self.fields[g_field]
        code, field = self.build_group_by_code_1(in_var_name, out_var_name, g_field, used_groups)
        selectivity = float(self.group_fields[g_field])
        return code, field, selectivity

    def group_by_code_2(self, in_var_name, out_var_name_1, out_var_name_2, seed, used_groups):
        g_field = str(get_element_by_seed(list(self.group_fields.keys()), seed))
        g_field_id = self.fields[g_field]
        code, field = self.build_group_by_code_2(in_var_name, out_var_name_1, out_var_name_2, g_field, used_groups)
        selectivity = float(self.group_fields[g_field])
        return code, field, selectivity
    
    def reduce_code_2(self, in_var_name, out_var_name_1, out_var_name_2, seed):
        r_field = str(get_element_by_seed(list(self.group_fields.keys()), seed))
        r_field_id = self.fields[r_field]
        return self.build_reduce_code_2(in_var_name, out_var_name_1, out_var_name_2, r_field_id)
        
    
    def sort_partition_code(self, in_var_name, out_var_name, has_group, seed_1, seed_2):
        if has_group:
            if seed_1 % 2 == 0:
                map_operator = get_element_by_seed(self.map_options, seed_1+seed_2)
                if map_operator != "COUNT" and len(self.map_fields) > 0:
                    s_field = map_operator+"("+get_element_by_seed(self.map_fields, seed_1+seed_2)+")"            
                else:
                    s_field = "COUNT(*)"
            else:
                s_field = "GROUP BY"
                
                
        else:
            s_field = str(get_element_by_seed(list(self.group_fields.keys()), seed_1))
        #s_field_id = self.fields[s_field]
        if seed_2 % 2 == 0:
            order = "ASC"
        else:
            order = "DESC"
        
        return self.build_sort_partition_code(in_var_name, out_var_name, s_field, order)
    
    def sink_code(self, in_var_name, out_var_name):
        return ""
    
    def equals(self, obj):
        if type(obj) == AbstractTableOperatorManager:
            return obj.table_name == self.table_name
        return false
    
    def to_string(self):
        return self.table_name
        