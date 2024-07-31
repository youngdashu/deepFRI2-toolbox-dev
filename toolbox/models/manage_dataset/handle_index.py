import json
from pathlib import Path
from typing import Dict, Union, List


def read_index(index_file_path: Path) -> Dict[str, str]:
    with index_file_path.open('r') as f:
        index = json.load(f)
        return index


def create_index(index_file_path: Path, values: Union[Dict[str, str], List[str]]):
    with index_file_path.open('w') as f:
        if isinstance(values, list):
            values = {str(i): v for i, v in enumerate(values)}
        json.dump(values, f)

    if index_file_path.name == "dataset.idx":
        values = groupby_dict_by_values(values)
        with (index_file_path.parent / 'pdbs_files.idx').open('w') as f:
            json.dump(values, f)


def add_new_files_to_index(dataset_index_file_path, new_files_index):
    try:
        current_index = read_index(dataset_index_file_path)
        current_index.update(new_files_index)
        create_index(dataset_index_file_path, current_index)
    except Exception as e:
        print(e)


def groupby_dict_by_values(d):
    v = {}

    for key, value in sorted(d.items()):
        v.setdefault(value, []).append(key)

    return v
