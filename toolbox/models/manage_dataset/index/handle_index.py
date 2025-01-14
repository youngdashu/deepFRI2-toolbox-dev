import json
import traceback
from pathlib import Path
from typing import Dict, Union, List

from toolbox.models.manage_dataset.utils import groupby_dict_by_values


def read_index(index_file_path: Path) -> Dict[str, str]:
    try:
        with index_file_path.open("r") as f:
            index = json.load(f)
            return index
    except FileNotFoundError:
        return {}


def create_index(index_file_path: Path, values: Union[Dict[str, str], List[str]]):
    with index_file_path.open("w") as f:
        if isinstance(values, list):
            values = {str(i): v for i, v in enumerate(values)}
        json.dump(values, f)

    file_name = index_file_path.stem

    values = groupby_dict_by_values(values)
    with (index_file_path.parent / f"{file_name}_reversed.idx").open("w") as f:
        json.dump(values, f)


def add_new_files_to_index(dataset_index_file_path, new_files_index):
    try:
        current_index = read_index(dataset_index_file_path)
        current_index.update(new_files_index)
        create_index(dataset_index_file_path, current_index)
    except Exception as e:
        print("Exception in add_new_files_to_index")
        print(e)
