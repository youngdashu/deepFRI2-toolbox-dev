from argparse import Namespace
from datetime import datetime
import json
import concurrent.futures

from pathlib import Path

from toolbox.models.chains.verify_chains import verify_chains
from toolbox.models.embedding.embedding import Embedding
from toolbox.models.manage_dataset.distograms.generate_distograms import (
    generate_distograms,
    read_distograms_from_file,
)
from toolbox.models.manage_dataset.structures_dataset import FatalDatasetError, StructuresDataset
from toolbox.models.manage_dataset.utils import (read_pdbs_from_h5)
from toolbox.models.utils.create_client import create_client
from toolbox.scripts.archive import create_archive

from toolbox.utlis.logging import logger


class CommandParser:
    def __init__(self, args: Namespace):
        self.structures_dataset = None
        self.args = args

    def _create_dataset_from_path_(self) -> StructuresDataset:
        path = self.args.file_path
        if path.is_dir() and (path / "dataset.json").exists():
            self.structures_dataset = StructuresDataset.model_validate_json(
                (path / "dataset.json").read_text()
            )
        elif path.is_file():
            self.structures_dataset = StructuresDataset.model_validate_json(
                path.read_text()
            )
        else:
            logger.error("Dataset path is not valid")
            raise FileNotFoundError

        self.structures_dataset._client = create_client(
            True if self.args.slurm else self.structures_dataset.is_hpc_cluster
        )
        return self.structures_dataset

    def dataset(self):
        dataset = StructuresDataset(
            db_type=self.args.db,
            collection_type=self.args.collection,
            type_str=self.args.type,
            version=self.args.version,
            ids_file=self.args.ids,
            seqres_file=self.args.seqres,
            archive_path=self.args.archive,
            overwrite=self.args.overwrite,
            batch_size=(
                None if self.args.batch_size is None else int(self.args.batch_size)
            ),
            binary_data_download=self.args.binary,
            is_hpc_cluster=self.args.slurm,
            input_path=self.args.input_path,
            verbose=self.args.verbose if hasattr(self.args, 'verbose') else False,
        )
        self.structures_dataset = dataset
        dataset.create_dataset()
        return dataset

    def embedding(self):
        logger.info("Not implemented")
        return


    def load(self):
        dataset = self._create_dataset_from_path_()
        logger.info(dataset)

    def generate_sequence(self):
        self._create_dataset_from_path_()
        self.structures_dataset.generate_sequence(
            self.args.ca_mask, self.args.no_substitution
        )

    def generate_distograms(self):
        self._create_dataset_from_path_()
        generate_distograms(self.structures_dataset)

    def read_distograms(self):
        logger.info(read_distograms_from_file(self.args.file_path))

    def read_pdbs(self):
        read_pdbs(self.args.file_path, self.args.ids, self.args.to_directory, self.args.print)

    def verify_chains(self):
        self._create_dataset_from_path_()
        verify_chains(self.structures_dataset, "./toolbox/pdb_seqres.txt")

    def create_archive(self):
        self._create_dataset_from_path_()
        create_archive(self.structures_dataset)

    def input_generation(self):
        try:
            self.dataset()
        except FatalDatasetError as e:
            logger.error("Fatal error! Exiting...")
            logger.error(e)
            return
        except Exception as e:
            print_exc(e)
        try:
            self.structures_dataset.generate_sequence()
        except Exception as e:
            print_exc(e)
        try:
            generate_distograms(self.structures_dataset)
        except Exception as e:
            print_exc(e)
        try:
            embedding = Embedding(self.structures_dataset)
            embedding.run()
        except Exception as e:
            print_exc(e)

    def run(self):
        command_method = getattr(self, self.args.command)
        if command_method:
            command_method()
        else:
            raise ValueError(f"Unknown command - {self.args.command}")

def print_exc(e):
    logger.error(f"Error ({type(e)}): {str(e)}")

def read_pdbs(file_path, ids, to_directory, is_print):
    if ids.exists():
        ids = ids.read_text().splitlines()

    pdbs_dict = read_pdbs_from_h5(file_path, ids)

    if is_print:
        logger.info(json.dumps(pdbs_dict))
    
    if to_directory:
        extract_dir: Path = to_directory
        if not extract_dir.exists() and not extract_dir.is_dir():
            logger.error("ERROR: Provided output path doesn't exist")
            return
    else:
        extract_dir = file_path.parent / f"extracted_{file_path.stem}"
        extract_dir.mkdir(exist_ok=True, parents=True)

    def save_pdb(pdb_code, pdb_file):
        file_name = f"{pdb_code}" if pdb_code.endswith(".pdb") else f"{pdb_code}.pdb"
        logger.info(f"Saving {file_name}")
        with open(extract_dir / file_name, "w") as f:
            f.write(pdb_file)

    logger.info("Extracting PDB files")
    for pdb_code, pdb_file in pdbs_dict.items():
        save_pdb(pdb_code, pdb_file)
    logger.info("Extraction complete")