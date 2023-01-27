from python_instantiator.DatasetHelper import DatasetHelper
from python_instantiator.DatasetJob.utils import get_element_by_seed
from python_instantiator.DatasetJob.AbstractTableOperatorManager import AbstractTableOperatorManager

import random

class JoinRelation:
    def __init__(self, lx, rx, field):
        self.lx = lx
        self.rx = rx
        self.field = field
        
    def equals(self, join_relation):
        return join_relation.field == self.field and ((join_relation.rx == self.rx and join_relation.lx == self.lx) or
                                                     (join_relation.rx == self.lx and join_relation.lx == self.rx))
    
    def to_string(self):
        return f"{self.lx} - {self.rx} : {self.field}"
    
class AbstractDatasetOperatorManager:
    tables = []
    tables_to_class = {}
    
    max_attempts = 100
    
    def get_table_operator_manager(self, s):
        return AbstractTableOperatorManager()
    
    
    def get_next_join_relation(self, table_sequence, table_names):
        attempts = 1
        successfull = False
        while attempts <= self.max_attempts and not successfull:
            t0_op_man = get_element_by_seed(table_sequence, attempts)
            t0_j_field = str(get_element_by_seed(t0_op_man.join_fields, random.randint(0,20)))
            t0_j_table = str(get_element_by_seed(list(t0_op_man.join_field_table[t0_j_field].keys()), random.randint(0,20)))
            t0_j_table_field = t0_op_man.join_field_table[t0_j_field][t0_j_table]
            t1_op_man = self.get_table_operator_manager(t0_j_table)
            attempts += 1
            if not t1_op_man.table_name in table_names:
                successfull = True
        
        return JoinRelation(t0_op_man, t1_op_man, t0_j_table_field)
    
    def get_source_table_sequence(self, n_joins, seed=0):
        success = False
        tmp_seed = seed
        attempt = 0
        tS = None
        jS = None
        
        while not success and attempt <= 10:
            try:
                resTS, resJS = self._get_source_table_sequence(n_joins, tmp_seed)
                tS = resTS
                jS = resJS
                success = True
            except:
                attempt += 1
                tmp_seed += 1
        return tS, jS
        
    def _get_source_table_sequence(self, n_joins, seed=0):
        random.seed(seed)
        table_sequence = []
        table_names = []
        join_sequence = []
        
        t0 = str(get_element_by_seed(self.tables, seed))
        t0_op_man = self.get_table_operator_manager(t0)
        table_sequence.append(t0_op_man)
        table_names.append(t0)
        
        for i in range(n_joins): # I guess it should be +1 because we have one table more than joins
            next_join_relation = None
            attempt = 1
            next_join_relation = self.get_next_join_relation(table_sequence, table_names)
            while next_join_relation in join_sequence and attempt <= self.max_attempts:
                attempt += 1
                next_join_relation = self.get_next_join_relation(table_sequence, table_names)
                
            
            if attempt >= self.max_attempts:
                raise Exception(f"Impossible to generate '{n_joins}' join sequence for {table_sequence}.")
        
            join_sequence.append(next_join_relation)
            t0_op_man = next_join_relation.rx
            table_sequence.append(t0_op_man)
            table_names.append(t0_op_man.table_name)

        return table_sequence, join_sequence
        
    def apply(self):
        return TPCHDatasetOperatorManager
            
    