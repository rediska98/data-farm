# JsonUtil.py

import json
import pathlib


def to_json(jobs_info):
    output = json.dumps(jobs_info, default=vars,
                        sort_keys=True, indent=4)

    # Changing some field cases for DataFarm's LabelForecaster compatibility
    char_to_replace = {'job_id': 'JobId', 'operators_info': 'operatorsInfo',
                       'idx': 'id', 'out_cardinality': 'outCardinality', 'var_name': 'varName',
                       'join_relations_sequence': 'joinRelationsSequence', 'table_sequence': 'tableSequence'}

    for key, value in char_to_replace.items():
        # Replace key character with value character in string
        output = output.replace(key, value)

    return output

def persist(json_string, path):
    file = pathlib.Path(path) / "generated_jobs_info.json"
    with open(file, "w") as f:
        f.write(json_string)


