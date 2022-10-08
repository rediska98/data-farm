from pathlib import Path

class JobGeneratorHelper:
    LINEITEM = "lineitem.tbl"
    
    def get_input_data_path(self, working_dir):
        path = Path(working_dir)
        path = path / self.LINEITEM
        return str(path.resolve())
    
    def save_to_file(path, code):
        with open(path, r) as f:
            f.write(code)