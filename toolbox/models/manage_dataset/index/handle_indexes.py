import json
import os
from functools import reduce
from pathlib import Path
from typing import Dict, List, Tuple, Iterable

import dask.bag as db
import dotenv
from pydantic import BaseModel

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.utils import groupby_dict_by_values

from toolbox.utlis.logging import logger

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
        logger.debug(f"Initializing HandleIndexes with dataset type: {structures_dataset.db_type}")
        self.structures_dataset = structures_dataset
        self.file_paths_storage = {}

    def file_paths(self, index_type) -> Dict[str, str]:
        logger.debug(f"Retrieving file paths for index type: {index_type}")
        paths = self.file_paths_storage.get(index_type, {})
        logger.debug(f"Found {len(paths)} paths for index type {index_type}")
        return paths

    def read_indexes(self, index_type: str):
        logger.debug(f"Reading indexes for type: {index_type}")
        db_type = self.structures_dataset.db_type
        datasets_path_obj = Path(self.structures_dataset.config.data_path) / "datasets"
        logger.debug(f"Using database type: {db_type}, datasets path: {datasets_path_obj}")
        
        if db_type == DatabaseType.other:
            logger.debug("Processing 'other' database type")
            path = [str(p) for p in datasets_path_obj.glob(f"*{os.sep}{index_type}.idx")]
            logger.debug(f"Found {len(path)} index files for 'other' type")
        else:
            logger.debug(f"Processing {db_type.name} database type")
            if self.structures_dataset.config:
                sep = self.structures_dataset.config.separator
            else:
                raise ValueError("No config found")
            base_path = datasets_path_obj / f"{db_type.name}{sep}*"
            dataset_dirs = list(base_path.parent.glob(base_path.name))
            logger.debug(f"Found {len(dataset_dirs)} dataset directories")
            
            dir_dates = []
            for dir_path in dataset_dirs:
                dataset_json = dir_path / "dataset.json"
                idx_path = dir_path / f"{index_type}.idx"
                logger.debug(f"Checking {dataset_json} and {idx_path}")
                if dataset_json.exists() and idx_path.exists():
                    try:
                        with open(dataset_json) as f:
                            data = json.load(f)
                            created_at = data.get("created_at", "-inf")
                            dir_dates.append((dir_path, float(created_at)))
                            logger.debug(f"Added directory {dir_path} with creation date {created_at}")
                    except Exception as e:
                        logger.error(f"Error reading {dataset_json}: {e}")
                else:
                    logger.debug(f"Skipping directory {dir_path} - missing required files")
                
            dir_dates.sort(key=lambda x: x[1], reverse=True)
            logger.debug("Sorted directories by creation date (newest first)")
            
            paths = [str(dir_path / f"{index_type}.idx") for dir_path, _ in dir_dates]
            path = paths
            logger.debug(f"Created {len(paths)} index file paths")
        
        file_paths_jsons_list = []
        for p in path:
            try:
                with open(p) as f:
                    data = json.load(f)
                    file_paths_jsons_list.append(data)
                    logger.debug(f"Successfully loaded index file: {p} with {len(data)} entries")
            except Exception as e:
                logger.error(f"Error reading {p}: {e}")
        
        file_paths = {}
        for d in file_paths_jsons_list:
            for k, v in d.items():
                if k not in file_paths:
                    file_paths[k] = v
        
        self.file_paths_storage[index_type] = file_paths
        logger.debug(f"Sample of file paths (up to 5): {list(file_paths.items())[:5]}")

    def find_present_and_missing_ids(
        self, index_type, requested_ids: Iterable[str]
    ) -> Tuple[Dict[str, str], Iterable[str]]:
        logger.debug(f"Finding present and missing IDs for index type: {index_type}")
        file_paths = self.file_paths(index_type)
        requested_ids_list = list(requested_ids)
        logger.debug(f"Processing {len(requested_ids_list)} requested IDs")

        ids_present = file_paths.keys()
        logger.debug(f"Have {len(ids_present)} IDs in storage")

        if self.structures_dataset.db_type == DatabaseType.PDB:
            logger.debug("Processing PDB database type")
            chain_codes_to_short = {key: key.split("_")[0] for key in file_paths.keys()}
            pdb_code_to_pdb_with_chain_codes = groupby_dict_by_values(chain_codes_to_short)
            logger.debug(f"Created mapping for {len(pdb_code_to_pdb_with_chain_codes)} PDB codes")

            def process_pdb_id(id_):
                logger.debug(f"Processing PDB ID: {id_}")
                if "_" in id_:
                    if id_ in ids_present:
                        logger.debug(f"Found exact match for chain ID: {id_}")
                        return True, {id_: file_paths[id_]}
                    else:
                        logger.debug(f"Missing chain ID: {id_}")
                        return False, id_
                else:
                    if id_ in pdb_code_to_pdb_with_chain_codes:
                        codes_with_chains = pdb_code_to_pdb_with_chain_codes[id_]
                        logger.debug(f"Found {len(codes_with_chains)} chain codes for PDB: {id_}")
                        return True, {
                            code_with_chain: file_paths[code_with_chain]
                            for code_with_chain in codes_with_chains
                        }
                    else:
                        logger.debug(f"Missing PDB ID: {id_}")
                        return False, id_

            process_id_func = process_pdb_id
        else:
            logger.debug(f"Processing non-PDB database type: {self.structures_dataset.db_type}")
            def process_id(id_):
                if id_ in ids_present:
                    logger.debug(f"Found ID: {id_}")
                    return True, {id_: file_paths[id_]}
                else:
                    logger.debug(f"Missing ID: {id_}")
                    return False, id_

            process_id_func = process_id

        logger.debug("Starting parallel processing of IDs")
        result_bag = (
            db.from_sequence(
                requested_ids_list, partition_size=self.structures_dataset.batch_size
            )
            .map(process_id_func)
            .compute()
        )
        logger.debug("Completed parallel processing")

        present, missing_ids = __splitter__(result_bag)
        logger.debug(f"Split results - Present: {len(present)}, Missing: {len(missing_ids)}")

        def merge_dicts(d1, d2):
            d1.update(d2)
            return d1

        present_file_paths = reduce(merge_dicts, present, {})

        logger.info("Present files: %s", len(present_file_paths))
        logger.info("Missing files: %s", len(missing_ids))
        logger.debug(f"Sample of present files (up to 5): {list(present_file_paths.items())[:5]}")
        logger.debug(f"Sample of missing IDs (up to 5): {list(missing_ids)[:5]}")

        return present_file_paths, missing_ids

    def find_missing_protein_files(
        self, protein_index: Dict[str, str], missing: Iterable[str]
    ):
        logger.debug("Finding missing protein files")
        logger.debug(f"Protein index size: {len(protein_index)}")
        missing_list = list(missing)
        logger.debug(f"Processing {len(missing_list)} missing entries")

        missing_items: Dict[str, str] = {
            missing_protein_name: protein_index[missing_protein_name]
            for missing_protein_name in missing_list
        }
        logger.debug(f"Created missing items dictionary with {len(missing_items)} entries")

        reversed_missings: dict[str, List[str]] = groupby_dict_by_values(missing_items)
        logger.debug(f"Grouped missing proteins into {len(reversed_missings)} categories")
        logger.debug(f"Sample of missing items (up to 5): {list(missing_items.items())[:5]}")

        return missing_items, reversed_missings

    def full_handle(
        self, index_type: str, protein_index: Dict[str, str], overwrite: bool = False
    ) -> SearchIndexResult:
        logger.info(f"{index_type.capitalize()} dataset size: {len(protein_index)}")
        logger.debug(f"Overwrite mode: {overwrite}")

        self.read_indexes(index_type)

        requested_ids = protein_index.keys()
        logger.debug(f"Processing {len(requested_ids)} requested IDs")

        if overwrite:
            logger.debug("Overwrite mode enabled - returning all items as missing")
            result = SearchIndexResult(
                present={},
                missing_protein_files=protein_index,
                grouped_missing_proteins=groupby_dict_by_values(protein_index),
            )
            logger.debug("Created SearchIndexResult with all items marked as missing")
            return result

        present, missing_ids = self.find_present_and_missing_ids(
            index_type, requested_ids
        )

        missing_protein, grouped_missing_proteins = self.find_missing_protein_files(
            protein_index, missing_ids
        )

        result = SearchIndexResult(
            present=present,
            missing_protein_files=missing_protein,
            grouped_missing_proteins=grouped_missing_proteins,
        )
        logger.debug(f"Created SearchIndexResult with {len(present)} present and {len(missing_protein)} missing items")
        return result


def __splitter__(data):
    yes, no = [], []
    for d in data:
        if d[0]:
            yes.append(d[1])
        else:
            no.append(d[1])
    return tuple(yes), tuple(no)
