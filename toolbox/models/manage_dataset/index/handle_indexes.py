import json
import os
from functools import reduce
from pathlib import Path
from typing import Dict, List, Tuple, Iterable

import dask.bag as db
import dotenv
from pydantic import BaseModel

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.paths import datasets_path
from toolbox.models.manage_dataset.utils import groupby_dict_by_values

dotenv.load_dotenv()
SEPARATOR = os.getenv("SEPARATOR")


class SearchIndexResult(BaseModel):
    present: Dict[str, str]
    missing_protein_files: Dict[str, str]
    grouped_missing_proteins: Dict[str, List[str]]


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
            # For "other" db_type, the current implementation yields a single path pattern.
            # Wrap it in a list if needed.
            path = [str(p) for p in datasets_path_obj.glob(f"*{os.sep}{index_type}.idx")]
        else:
            base_path = datasets_path_obj / f"{db_type.name}{SEPARATOR}*"
            # Get all matching directories
            dataset_dirs = list(base_path.parent.glob(base_path.name))
            
            # Read creation dates from dataset.json files and sort directories (newest first)
            dir_dates = []
            for dir_path in dataset_dirs:
                dataset_json = dir_path / "dataset.json"
                idx_path = dir_path / f"{index_type}.idx"
                if dataset_json.exists() and idx_path.exists():
                    try:
                        with open(dataset_json) as f:
                            data = json.load(f)
                            created_at = data.get("created_at", "")
                            dir_dates.append((dir_path, int(created_at)))
                    except Exception as e:
                        print(f"Error reading {dataset_json}: {e}")
                
            # Sort by creation date, newest first
            dir_dates.sort(key=lambda x: x[1], reverse=True)
            
            # Create list of paths from sorted directories
            paths = [str(dir_path / f"{index_type}.idx") for dir_path, _ in dir_dates]
            path = paths
        
        file_paths_jsons_list = []
        for p in path:
            try:
                with open(p) as f:
                    file_paths_jsons_list.append(json.load(f))
            except Exception as e:
                print(f"Error reading {p}: {e}")
        
        # Merge dictionaries ensuring that if a key is found in a newer entry,
        # it won't be overwritten by an older one.
        file_paths = {}
        for d in file_paths_jsons_list:
            for k, v in d.items():
                if k not in file_paths:
                    file_paths[k] = v

        self.file_paths_storage[index_type] = file_paths
        print(f"Found {len(file_paths)} files")

    def find_present_and_missing_ids(
        self, index_type, requested_ids: Iterable[str]
    ) -> Tuple[Dict[str, str], Iterable[str]]:
        file_paths = self.file_paths(index_type)

        ids_present = file_paths.keys()

        if self.structures_dataset.db_type == DatabaseType.PDB:
            chain_codes_to_short = {key: key.split("_")[0] for key in file_paths.keys()}
            pdb_code_to_pdb_with_chain_codes = groupby_dict_by_values(
                chain_codes_to_short
            )

            def process_pdb_id(id_):
                if "_" in id_:
                    if id_ in ids_present:
                        return True, {id_: file_paths[id_]}
                    else:
                        return False, id_
                else:
                    if id_ in pdb_code_to_pdb_with_chain_codes:
                        codes_with_chains: List[str] = pdb_code_to_pdb_with_chain_codes[
                            id_
                        ]
                        return True, {
                            code_with_chain: file_paths[code_with_chain]
                            for code_with_chain in codes_with_chains
                        }
                    else:
                        return False, id_

            process_id_func = process_pdb_id
        else:

            def process_id(id_):
                if id_ in ids_present:
                    return True, {id_: file_paths[id_]}
                else:
                    return False, id_

            process_id_func = process_id

        result_bag = (
            db.from_sequence(
                requested_ids, partition_size=self.structures_dataset.batch_size
            )
            .map(process_id_func)
            .compute()
        )

        present, missing_ids = __splitter__(result_bag)

        def merge_dicts(d1, d2):
            d1.update(d2)
            return d1

        present_file_paths = reduce(merge_dicts, present, {})

        print(f"Found {len(present_file_paths)} present {index_type} files")
        print(f"Found {len(missing_ids)} missing {index_type} ids")

        return present_file_paths, missing_ids

    def find_missing_protein_files(
        self, protein_index: Dict[str, str], missing: Iterable[str]
    ):

        missing_items: Dict[str, str] = {
            missing_protein_name: protein_index[missing_protein_name]
            for missing_protein_name in missing
        }

        reversed_missings: dict[str, List[str]] = groupby_dict_by_values(missing_items)

        return missing_items, reversed_missings

    def full_handle(
        self, index_type: str, protein_index: Dict[str, str], overwrite: bool = False
    ) -> SearchIndexResult:

        self.read_indexes(index_type)

        requested_ids = protein_index.keys()

        if overwrite:
            return SearchIndexResult(
                present={},
                missing_protein_files=protein_index,
                grouped_missing_proteins=groupby_dict_by_values(protein_index),
            )

        present, missing_ids = self.find_present_and_missing_ids(
            index_type, requested_ids
        )

        missing_protein, grouped_missing_proteins = self.find_missing_protein_files(
            protein_index, missing_ids
        )

        return SearchIndexResult(
            present=present,
            missing_protein_files=missing_protein,
            grouped_missing_proteins=grouped_missing_proteins,
        )


def __splitter__(data):
    yes, no = [], []
    for d in data:
        if d[0]:
            yes.append(d[1])
        else:
            no.append(d[1])
    return tuple(yes), tuple(no)
