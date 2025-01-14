import os
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Any

import dask.bag as db
import dotenv
import requests
from Bio.PDB import PDBList
from dask.distributed import Client, as_completed
from pydantic import BaseModel, field_validator

from toolbox.models.manage_dataset.collection_type import CollectionType
from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.extract_archive import (
    extract_archive,
    save_extracted_files,
)
from toolbox.models.manage_dataset.index.handle_index import (
    create_index,
    add_new_files_to_index,
)
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.manage_dataset.paths import datasets_path, repo_path
from toolbox.models.manage_dataset.sequences.sequence_retriever import SequenceRetriever
from toolbox.models.manage_dataset.utils import chunk
from toolbox.models.manage_dataset.utils import (
    foldcomp_download,
    mkdir_for_batches,
    retrieve_pdb_chunk_to_h5,
    alphafold_chunk_to_h5,
)
from toolbox.models.utils.create_client import create_client, total_workers
from toolbox.utlis.filter_pdb_codes import filter_pdb_codes

dotenv.load_dotenv()
SEPARATOR = os.getenv("SEPARATOR")


class StructuresDataset(BaseModel):
    db_type: DatabaseType
    collection_type: CollectionType
    type_str: str = ""
    version: str = datetime.now().strftime("%Y%m%d_%H%M")
    ids_file: Optional[Path] = None
    seqres_file: Optional[Path] = None  # to delete
    overwrite: bool = False
    batch_size: int = 1000
    binary_data_download: bool = False
    is_hpc_cluster: bool = False
    input_path: Optional[Path] = None
    _client: Optional[Client] = None
    _handle_indexes: Optional[HandleIndexes] = None
    _sequence_retriever: Optional[SequenceRetriever] = None

    @field_validator("version", mode="before")
    def set_version(cls, v):
        return v or datetime.now().strftime("%Y%m%d_%H%M")

    @field_validator("batch_size", mode="before")
    def set_batch_size(cls, size):
        return size or 1000

    def __init__(self, **data: Any):
        super().__init__(**data)
        self._handle_indexes = HandleIndexes(self)
        self._sequence_retriever = SequenceRetriever(self)

    def dataset_repo_path(self):
        return (
            Path(repo_path)
            / self.db_type.name
            / f"{self.collection_type.name}_{self.type_str}"
            / self.version
        )

    def dataset_path(self):
        return Path(f"{datasets_path}/{self.dataset_dir_name()}")

    def structures_path(self):
        return self.dataset_repo_path() / "structures"

    def dataset_dir_name(self):
        return f"{self.db_type.name}{SEPARATOR}{self.collection_type.name}{SEPARATOR}{self.type_str}{SEPARATOR}{self.version}"

    def dataset_index_file_path(self) -> Path:
        return self.dataset_path() / "dataset.idx"

    def sequences_index_path(self):
        return self.dataset_path() / "sequences.idx"

    def distograms_index_path(self):
        return self.dataset_path() / "distograms.idx"

    def distograms_file_path(self):
        return self.dataset_path() / "distograms.hdf5"

    def batches_count(self) -> int:
        return sum(1 for item in self.structures_path().iterdir() if item.is_dir())

    def add_client(self):
        if self._client is None:
            self._client = create_client(self.is_hpc_cluster)

    def requested_ids(self) -> List[str]:
        if self.collection_type is CollectionType.subset:
            with open(self.ids_file, "r") as f:
                return f.read().splitlines()
        else:
            return self.get_all_ids()

    def create_dataset(self):

        self.add_client()
        print(str(datetime.now()))

        if self.collection_type == CollectionType.subset:
            assert self.ids_file is not None and self.ids_file.exists()

        self.dataset_repo_path().mkdir(exist_ok=True, parents=True)
        self.dataset_path().mkdir(exist_ok=True, parents=True)

        if (
            self.collection_type is CollectionType.subset
            or self.collection_type is CollectionType.all
        ):

            if self.overwrite:
                present_file_paths = {}
                missing_ids = self.requested_ids()
            else:
                self._handle_indexes.read_indexes("dataset")
                requested_ids = self.requested_ids()

                present_file_paths, missing_ids = (
                    self._handle_indexes.find_present_and_missing_ids(
                        "dataset", requested_ids
                    )
                )

            create_index(self.dataset_index_file_path(), present_file_paths)

            if (
                self.db_type == DatabaseType.other
                and self.collection_type == CollectionType.subset
                and len(missing_ids) > 0
            ):
                raise RuntimeError(
                    "Missing ids are not allowed when subsetting all DBs!"
                )

            if len(missing_ids) > 0:
                self.download_ids(missing_ids)
        else:
            self.download_ids(None)

        self.save_dataset_metadata()

    def generate_sequence(
        self, ca_mask: bool = False, substitute_non_standard_aminoacids: bool = True
    ):
        self._sequence_retriever.retrieve(ca_mask, substitute_non_standard_aminoacids)

    def get_all_ids(self):
        match self.db_type:
            case DatabaseType.PDB:
                start_time = time.time()
                all_pdbs = PDBList().get_all_entries()
                all_pdbs = map(lambda x: x.lower(), all_pdbs)
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"PDBList().get_all_entries time: {elapsed_time} seconds")
                url = "http://files.wwpdb.org/pub/pdb/derived_data/pdb_entry_type.txt"

                response = requests.get(url)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
                res = filter_pdb_codes(response.text, all_pdbs)
                print(f"After removing non protein codes {len(res)}")
            case DatabaseType.AFDB:
                res = []
            case DatabaseType.ESMatlas:
                res = []
            case _:
                res = open(self.ids_file).readlines()
        return res

    def download_ids(self, ids):
        if self.input_path is not None:
            extracted_path = extract_archive(self.input_path, self)
            if extracted_path is not None:
                save_extracted_files(self, extracted_path, ids)
            return

        print("Downloading ids")
        match self.db_type:
            case DatabaseType.PDB:
                self.handle_pdb(ids)
            case DatabaseType.AFDB:
                self.handle_afdb()
            case DatabaseType.ESMatlas:
                self.handle_esma()
            case DatabaseType.other:
                self

    def handle_pdb(self, ids: List[str]):
        match self.collection_type:
            case CollectionType.all:
                self._download_pdb_(ids)
            case CollectionType.part:
                pass
            case CollectionType.clust:
                pass
            case CollectionType.subset:
                self._download_pdb_(ids)

    def find_add_new_files_to_index(self):
        self.dataset_path().mkdir(exist_ok=True, parents=True)

        structures_path = self.structures_path()

        def process_directory(dir_path):
            return [str(f) for f in Path(dir_path).glob("*.*") if f.is_file()]

        numbered_dirs = [
            d for d in structures_path.iterdir() if d.is_dir() and d.name.isdigit()
        ]

        if len(numbered_dirs) == 0:
            new_files = []
        else:
            dir_bag = db.from_sequence(numbered_dirs, npartitions=len(numbered_dirs))
            new_files = dir_bag.map(process_directory).flatten().compute()

        new_files_index = {str(i): v for i, v in enumerate(new_files)}

        self.add_new_files_to_index(new_files_index)

    def add_new_files_to_index(self, new_files_index):
        print("Getting len ")
        print(len(new_files_index))
        add_new_files_to_index(self.dataset_index_file_path(), new_files_index)

    def _download_pdb_(self, ids: List[str]):
        Path(self.structures_path()).mkdir(exist_ok=True, parents=True)
        pdb_repo_path = self.structures_path()
        chunks = list(self.chunk(ids))

        mkdir_for_batches(pdb_repo_path, len(chunks))

        print(f"Downloading PDBs into {len(chunks)} chunks")

        new_files_index = {}

        def run(input_data, machine):
            return self._client.submit(
                retrieve_pdb_chunk_to_h5,
                *input_data,
                self.binary_data_download,
                [machine],
                workers=[machine],
            )

        def collect(result):
            downloaded_pdbs, file_path = result
            print("Updating new_files_index", len(downloaded_pdbs))
            new_files_index.update({k: file_path for k in downloaded_pdbs})

        compute_batches = ComputeBatches(
            self._client,
            run,
            collect,
            "pdb" + "_b" if self.binary_data_download else "",
        )

        inputs = (
            (pdb_repo_path / f"{i}", ids_chunk) for i, ids_chunk in enumerate(chunks)
        )

        factor = 10
        factor = 15 if total_workers() > 1500 else factor
        factor = 20 if total_workers() > 2000 else factor
        compute_batches.compute(inputs, factor=factor)

        print("Adding new files to index")

        try:
            self.add_new_files_to_index(new_files_index)
        except Exception as e:
            print("Failed to update index")
            print(e)

    def foldcomp_decompress(self):

        db_path = str(self.dataset_repo_path() / self.type_str)

        ids = (
            db.read_text(f"{self.dataset_repo_path()}/{self.type_str}.lookup")
            .filter(
                lambda line: (
                    ".pdb" in line if (".pdb" in line or ".cif" in line) else True
                )
            )
            .map(lambda line: line.split()[1])
            .compute()
        )

        if self.ids_file:
            subset_ids = self.ids_file.read_text().splitlines()
            # Check if subset_id is included in any of the full ids
            ids = [id for id in ids if any(subset_id in id for subset_id in subset_ids)]

        structures_path = self.structures_path()
        structures_path.mkdir(exist_ok=True, parents=True)

        batches = list(self.chunk(ids))
        mkdir_for_batches(structures_path, len(batches))

        futures = [
            self._client.submit(
                alphafold_chunk_to_h5,
                db_path,
                str(structures_path / f"{number}"),
                list(batch),
            )
            for number, batch in enumerate(batches)
        ]

        result_index = {}

        i = 0
        total = len(futures)

        for batch in as_completed(futures, with_results=True).batches():
            for _, single_batch_index in batch:
                result_index.update(single_batch_index)
                print(f"{i}/{total}")
                i += 1

        create_index(self.dataset_index_file_path(), result_index)

    def handle_afdb(self):

        name = f"report_AFDB_{self.version}_{total_workers()}_{1}"
        # with performance_report(filename=f"{name}.html"):

        match self.collection_type:
            case CollectionType.all:
                pass
            case CollectionType.part:

                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.clust:
                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.subset:
                # TODO handle file from disk
                pass

    def handle_esma(self):
        match self.collection_type:
            case CollectionType.all:
                pass
            case CollectionType.part:
                pass
            case CollectionType.clust:
                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.subset:
                pass

    def chunk(self, it):
        return list(chunk(it, self.batch_size))

    def save_dataset_metadata(self):
        with (self.dataset_path() / "dataset.json").open("w+") as json_dataset_file:
            json_dataset_file.write(self.model_dump_json())


def create_subset():
    StructuresDataset(
        db_type=DatabaseType.other,
        collection_type=CollectionType.subset,
        ids_file=Path("./mix_ids1.txt"),
    ).create_dataset()


def create_e_coli():
    StructuresDataset(
        db_type=DatabaseType.AFDB,
        collection_type=CollectionType.part,
        type_str="e_coli",
    ).create_dataset()


def create_swissprot():
    StructuresDataset(
        db_type=DatabaseType.AFDB,
        collection_type=CollectionType.part,
        type_str="afdb_swissprot_v4",
    ).create_dataset()


if __name__ == "__main__":
    # create_swissprot()
    # create_e_coli()

    dataset = StructuresDataset(
        db_type=DatabaseType.PDB,
        collection_type=CollectionType.all,
        # ids_file=Path("/Users/youngdashu/sano/deepFRI2-toolbox-dev/ids_test.txt")
    )

    dataset.create_dataset()

    # create_e_coli()

    pass
