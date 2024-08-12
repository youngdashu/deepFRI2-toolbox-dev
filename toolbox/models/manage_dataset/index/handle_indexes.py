import json
import os
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import dotenv

import dask.bag as db

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.paths import datasets_path

dotenv.load_dotenv()
SEPARATOR = os.getenv("SEPARATOR")


class HandleIndexes:
    structures_dataset: "StructuresDataset"
    file_paths_storage: Dict[str, Dict[str, str]]

    def __init__(self, structures_dataset: "StructuresDataset"):
        self.structures_dataset = structures_dataset
        self.file_paths_storage = {}

    def file_paths(self, index_type) -> Dict[str, str]:
        return self.file_paths_storage.get(index_type, {})

    def read_indexes(self, index_type: str):

        db_type = self.structures_dataset.db_type

        datasets_path_obj = Path(datasets_path)
        print(f"Globbing {datasets_path}")
        if db_type == DatabaseType.other:
            path = datasets_path_obj / '*' / f"{index_type}.idx"
        else:
            path = datasets_path_obj / f"{db_type.name}{SEPARATOR}*" / f"{index_type}.idx"

        try:
            file_paths_jsons_list = db.read_text(path).map(json.loads).compute()
        except Exception as e:
            file_paths_jsons_list = []

        file_paths = {k.removesuffix('.pdb'): v for d in file_paths_jsons_list for k, v in d.items()}

        self.file_paths_storage[index_type] = file_paths
        print(f"Found {len(file_paths)} files")

    def find_present_and_missing_ids(self, index_type, requested_ids) -> Tuple[Dict[str, str], List[str]]:
        file_paths = self.file_paths(index_type)

        ids_present = file_paths.keys()

        if self.structures_dataset.db_type == DatabaseType.PDB:
            special_pdb_file_paths = {key.split('_')[0]: value for key, value in file_paths.items()}

            def process_pdb_id(id_):
                if '_' in id_:
                    if id_ in ids_present:
                        return True, (id_, file_paths[id_])
                    else:
                        return False, id_
                else:
                    if id_ in special_pdb_file_paths:
                        return True, (id_, special_pdb_file_paths[id_])
                    else:
                        return False, id_

            process_id_func = process_pdb_id
        else:
            def process_id(id_):
                if id_ in ids_present:
                    return True, (id_, file_paths[id_])
                else:
                    return False, id_

            process_id_func = process_id

        result_bag = db.from_sequence(requested_ids, partition_size=self.structures_dataset.batch_size).map(
            process_id_func
        )

        def partition_results(accumulator, item):
            is_present, value = item
            if is_present:
                accumulator['present'][value[0]] = value[1]
            else:
                accumulator['missing'].append(value)
            return accumulator

        initial = {'present': {}, 'missing': []}

        aggregated = result_bag.foldby(
            key=lambda x: True,  # We're using a single partition
            binop=partition_results,
            combine=lambda x, y: {
                'present': {**x['present'], **y['present']},
                'missing': x['missing'] + y['missing']
            },
            initial=initial
        )

        # Compute the result
        result = aggregated.compute()

        # Extract the results
        present_file_paths = result[0][1]['present']
        missing_ids = result[0][1]['missing']

        print(f"Found {len(present_file_paths)} present protein files")
        print(f"Found {len(missing_ids)} missing protein ids")

        return present_file_paths, missing_ids
