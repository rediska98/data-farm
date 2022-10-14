from typing import Optional
import json
import pathlib

class JobInfoRecorder:

    def __init__(self, job_id):
        self.job_id = job_id
        self.operators_info = []
        self.table_sequence = None
        self.join_relations_sequence = None

    def record_table_sequence(self, table_sequence):
        self.table_sequence = table_sequence

    def record_join_relations_sequence(self, join_relations_sequence):
        self.join_relations_sequence = join_relations_sequence

    def record(self, idx, var_name, pact, tables, selectivity=None, out_cardinality=None, complexity=None):
        data_info = self.DataInfoRecorder(idx, var_name, pact, tables, selectivity, out_cardinality, complexity)
        self.operators_info.append(data_info)

    def __str__(self):
        s = f"----------\n{self.job_id}\n----------\n"
        for op in self.operators_info:
            s += str(op)
        print(self.table_sequence)
        print(self.join_relations_sequence)
        return s

    def apply(self, job_id):
        return JobInfoRecorder(job_id)

    def open_job_recorder(self, job_id):
        self.current_job_info = JobInfoRecorder(job_id)
        return self.current_job_info

    def close_job_recorder(self):
        if self.current_job_info:
            self.jobs_info.append(self.current_job_info)
            self.current_job_info = None
            return True
        else:
            return False

    # I am using JsonUtils to convert to json and write to file

    # def persist(self, path):
    #     file = Path(path) / "generated_jobs_info.txt"
    #     with open(file, "w") as f:
    #         for c in current_job_info:
    #             f.write(str(c))

    class DataInfoRecorder:
        def __init__(
            self,
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
