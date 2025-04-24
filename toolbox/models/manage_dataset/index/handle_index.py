import json

import traceback
from pathlib import Path
from typing import Dict, Union, List

from toolbox.models.manage_dataset.utils import groupby_dict_by_values

from toolbox.models.manage_dataset.paths import get_pdir

from toolbox.utlis.logging import logger


def read_index(index_file_path: Path) -> Dict[str, str]:
    try:
        with index_file_path.open('r') as f:
            index = json.load(f)
            DATA_PATH = get_pdir()
            if "reversed" in index_file_path.stem:
                index = {DATA_PATH + "/" + k: v for k, v in index.items()}
            else:
                index = {k: DATA_PATH + "/" + v for k, v in index.items()}
            return index
    except Exception as e:
        logger.error("Exception in read_index", e)
        return {}


def create_index(index_file_path: Path, values: Union[Dict[str, str], List[str]]):
    with index_file_path.open("w") as f:
        if isinstance(values, list):
            values = {str(i): v for i, v in enumerate(values)}
        # Map over values to ensure they are strings
        DATA_PATH = get_pdir()
        values = {k: v.removeprefix(DATA_PATH + "/") for k, v in values.items()}
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
        logger.error("Exception in add_new_files_to_index", e)
