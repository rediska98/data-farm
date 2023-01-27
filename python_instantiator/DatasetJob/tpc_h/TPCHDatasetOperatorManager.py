from python_instantiator.DatasetJob.AbstractDatasetOperatorManager import AbstractDatasetOperatorManager
from python_instantiator.DatasetJob.tpc_h.RegionOperatorManager import RegionOperatorManager
from python_instantiator.DatasetJob.tpc_h.NationOperatorManager import NationOperatorManager
from python_instantiator.DatasetJob.tpc_h.PartOperatorManager import PartOperatorManager
from python_instantiator.DatasetJob.tpc_h.SupplierOperatorManager import SupplierOperatorManager
from python_instantiator.DatasetJob.tpc_h.PartsuppOperatorManager import PartsuppOperatorManager
from python_instantiator.DatasetJob.tpc_h.OrdersOperatorManager import OrdersOperatorManager
from python_instantiator.DatasetJob.tpc_h.CustomerOperatorManager import CustomerOperatorManager
from python_instantiator.DatasetJob.tpc_h.LineitemOperatorManager import LineitemOperatorManager

class TPCHDatasetOperatorManager(AbstractDatasetOperatorManager):
    tables = ["lineitem", "orders", "customer", "partsupp", "part", "supplier", "nation"]
    tables_to_class = {
        "lineitem": LineitemOperatorManager,
        "nation": NationOperatorManager,
        "part": PartOperatorManager,
        "supplier": SupplierOperatorManager,
        "customer": CustomerOperatorManager,
        "orders": OrdersOperatorManager,
        "partsupp": PartsuppOperatorManager
    }
    
    def __init__(self):
        super().__init__() # Don't know if there are no arguments
        #self.start_routine()
        
    def get_table_operator_manager(self, s):
        return self.tables_to_class[s]()
    
    def start_routine(self):
        for i in range(100):
            try:
                table_sequence, join_sequence = self.get_source_table_sequence(5,i)
                for join in join_sequence:
                    print(f"> {join}\n")
                    print(f"Table sequence: {table_sequence}")
            except:
                print("Exception thrown")
            print("-------------------------------")
    
# here is a main function which I will first use as class method
        