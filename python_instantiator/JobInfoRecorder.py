from typing import Optional
import json

class JobInfoRecorder:   
    
    def __init__(self, job_id):
        self.job_id = job_id
        self.operators_info = []
        self.table_sequence = None
        self.join_relations_sequence = None
        self.current_job_info = None
        self.jobs_info = []
        
    def record_table_sequence(self, table_sequence):
        self.table_sequence = table_sequence
    
    def record_join_relations_sequence(self, join_relations_sequence)
        self.join_relations_sequence = join_relations_sequence
        
    def record(self, idx, var_name, tables, selectivity, out_cardinality, complexity):
        data_info = self.DataInfoRecorder(idx, var_name, tables, selectivity, out_cardinality, complexity)
        self.operators_info.append(data_info)
    
    def __str__(self):
        s = f"----------\n{self.job_id}\n----------\n"
        for op in self.operators_info:
            s += str(op)
        return s
    
    def apply(self, job_id):
        return JobDataInfoRecorder(job_id)
    
    def open_job_recorder(self, job_id):
        self.current_job_info = JobDataInfoRecorder(job_id)
        return self.current_job_info
    
    def close_job_recorder(self):
        if self.current_job_info:
            self.jobs_info.append(self.current_job_info)
            self.current_job_info = None
            return True
        else:
            return False
        
    def persist(self, path):
        file = Path(path) / "generated_jobs_info.txt"
        with open(file, "w") as f:
            for c in current_job_info:
                f.write(str(c))

    class DataInfoRecorder:
        def __init__(
            idx: int,
            var_name: str,
            pact: str,
            tables: list,
            selectivity: Optional[float] = None,
            out_cardinality: Optional[int] = None,
            complexity: Optional[int] = None
        ):
            self.idx = idx
            self.var_name = var_name
            self.pact = pact
            self.tables = tables
            self.selectivity = selectivity
            self.out_cardinality = out_cardinality
            self.complexity = complexity
            
        def __str__(self):
            selectivity = self.selectivity or "None"
            out_c = self.out_cardinality or "None"
            tables = ", ".join(self.tables)
            
            return f"{self.idx} : {self.pact} : {self.var_name} -> s = {selectivity} c = {out_c} | Tables: {tables}"
        
    