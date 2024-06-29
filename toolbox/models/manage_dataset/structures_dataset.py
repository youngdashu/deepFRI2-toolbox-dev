import os
import pickle
from datetime import datetime
from enum import Enum
from itertools import islice
from pathlib import Path
from types import NoneType
from typing import List, Union

import dask.bag as db
import dotenv
import foldcomp
from Bio.PDB import PDBList
from dask import delayed
from dask.bag import Bag
from distributed import Client, progress
from pydantic import BaseModel, field_validator

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.dataset_origin import datasets_path, repo_path, foldcomp_download
from toolbox.models.manage_dataset.handle_index import create_index, read_index
from toolbox.models.manage_dataset.sequences.from_pdb import get_sequence_from_pdbs
from toolbox.models.manage_dataset.sequences.load_fasta import extract_sequences_from_fasta
from toolbox.models.manage_dataset.sequences.sequence_indexes import search_sequence_indexes

dotenv.load_dotenv()
SEPARATOR = os.getenv("SEPARATOR")


def chunk(data, size):
    # if len(data) < size:
    #     return [data]

    it = iter(data)
    while True:
        chunk_data = tuple(islice(it, size))
        if not chunk_data:
            break
        yield chunk_data


class CollectionType(Enum):
    all = "all"
    clust = "clust"
    part = "part"
    subset = "subset"


def mkdir_for_batches(base_path: Path, batch_count: int):
    for i in range(batch_count):
        (base_path / f"{i}").mkdir(exist_ok=True, parents=True)


class StructuresDataset(BaseModel):
    db_type: DatabaseType
    collection_type: CollectionType
    type_str: str = ""
    version: str = datetime.now().strftime('%Y%m%d')
    ids_file: Union[Path, NoneType] = None
    seqres_file: Union[Path, NoneType] = None
    overwrite: bool = False
    batch_size: int = 5_000

    @field_validator('version', mode='before')
    def set_version(cls, v):
        return v or datetime.now().strftime('%Y%m%d')

    def dataset_repo_path(self):
        return Path(repo_path) / self.db_type.name / f"{self.collection_type.name}_{self.type_str}" / self.version

    def dataset_path(self):
        return Path(f"{datasets_path}/{self.dataset_index_file_name()}")

    def structures_path(self):
        return self.dataset_repo_path() / "structures"

    def dataset_index_file_name(self):
        return f"{self.db_type.name}{SEPARATOR}{self.collection_type.name}{SEPARATOR}{self.type_str}{SEPARATOR}{self.version}"

    def dataset_index_file_path(self) -> Path:
        return self.dataset_path() / "dataset.idx"

    def sequences_index_path(self):
        return self.dataset_path() / "sequences.idx"

    def batches_count(self) -> int:
        return sum(1 for item in self.structures_path().iterdir() if item.is_dir())

    def create_dataset(self) -> "Dataset":

        self.dataset_repo_path().mkdir(exist_ok=True, parents=True)

        if self.collection_type == CollectionType.subset:
            assert self.ids_file is not None

        if self.collection_type is CollectionType.subset or self.collection_type is CollectionType.all:
            dataset_index_file_name = self.dataset_index_file_name()

            # find existing files of the same DB
            if self.db_type == DatabaseType.other:
                all_files = Path(repo_path).rglob("*.*")
            else:
                db_path = Path(repo_path) / self.db_type.name
                all_files = db_path.rglob("*.*")

            file_paths = {file_path.stem: file_path for file_path in all_files if
                          file_path.is_file() and file_path.suffix in {'.cif', '.pdb', '.ent'}}

            missing_ids = []
            present_file_paths = {}

            ids = None
            if self.collection_type is CollectionType.subset:
                with open(self.ids_file, 'r') as f:
                    ids = f.read().splitlines()
            else:
                ids = self.get_all_ids()

            Path(f"{datasets_path}/{dataset_index_file_name}").mkdir(exist_ok=True, parents=True)

            for id_ in ids:
                present_file_paths[id_] = file_paths[id_] if id_ in file_paths else missing_ids.append(id_)

            create_index(self.dataset_index_file_path(), present_file_paths)

            print(len(missing_ids))

            if self.db_type == DatabaseType.other and self.collection_type == CollectionType.subset and len(
                    missing_ids) > 0:
                print(missing_ids)
                raise RuntimeError("Missing ids are not allowed when subsetting all DBs!")

            self.download_ids(missing_ids)
        else:
            if self.overwrite:
                print("Overwriting ")
                # TODO
                # find previous version of the same db_type and type
                # remove previous
                # download new one
            else:
                self.download_ids(None)

        self.save_dataset_metadata()

        self.generate_sequence()

    def generate_sequence(self):
        print("Generating sequences")
        index = read_index(self.dataset_index_file_path())
        print(len(index))
        batched_ids = self.chunk(index.values())

        print("Searching indexes")
        index, missing_sequences = search_sequence_indexes(
            self.db_type,
            Path(datasets_path),
            batched_ids
        )

        print(len(index))
        print(f"missing seqs: {len(missing_sequences)}")

        missing_ids = db.from_sequence(missing_sequences,
                                       partition_size=self.batch_size)  # self.chunk(missing_sequences)

        if self.seqres_file is not None:
            print("Getting sequences from provided fasta")
            tasks = extract_sequences_from_fasta(self.seqres_file, missing_ids)
        else:
            print("Getting sequences from stored PDBs")
            tasks = missing_ids.map(get_sequence_from_pdbs)

        def parallel_reduce_dicts_with_bag(bag: Bag):
            # Use foldby to combine all dictionaries
            # The key function returns a constant so all items are grouped together
            combined = bag.foldby(
                key=lambda x: 'all',
                binop=lambda acc, x: {**acc, **x},
                initial={}
            )

            # Compute the result and extract the combined dictionary
            with Client() as client:
                final_result = combined.compute()[0][1]

            return final_result

        # Parallel reduce for dictionary merging
        print("\tGetting result")
        results_dict = parallel_reduce_dicts_with_bag(tasks)
        sequences_file_path = self.dataset_path() / "pdb_sequence.pickle"
        with open(sequences_file_path, 'wb') as f:
            print("Saving sequences to dict")
            pickle.dump(results_dict, f, pickle.HIGHEST_PROTOCOL)

        print("Save new index with all proteins")
        for id_ in missing_sequences:
            index[id_] = str(sequences_file_path)
        create_index(self.sequences_index_path(), index)

    def get_all_ids(self):
        res = None
        match self.db_type:
            case DatabaseType.PDB:
                res = PDBList().get_all_entries()
            case DatabaseType.AFDB:
                res = []
            case DatabaseType.ESMatlas:
                res = []
            case _:
                res = open(self.ids_file).readlines()
        return res

    def download_ids(self, ids):
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

    def retrieve_pdb_file(self, pdb: str, pdb_repo_path: str):
        pdb_list = PDBList()
        # Ensure the directory string is properly passed

        # PDB ids from PDBList() are upper case without 'pdb' prefix
        pdb = pdb.removeprefix("pdb")
        pdb = pdb.upper()

        pdb_list.retrieve_pdb_file(
            pdb,
            pdir=pdb_repo_path,
            file_format="pdb"
        )

    def save_new_files_to_index(self):
        self.dataset_path().mkdir(exist_ok=True, parents=True)

        structures_path = self.structures_path()
        files = [str(f) for f in structures_path.rglob('*.*') if f.is_file()]

        create_index(self.dataset_index_file_path(), files)

    def _download_pdb_(self, ids: List[str]):
        Path(self.structures_path()).mkdir(exist_ok=True, parents=True)
        pdb_repo_path = self.structures_path()
        chunks = list(self.chunk(ids))

        mkdir_for_batches(pdb_repo_path, len(chunks))

        tasks = [
            delayed(self.retrieve_pdb_file)(pdb, pdb_repo_path / f"{batch_number}")
            for batch_number, pdb_ids_chunk in enumerate(chunks)
            for pdb in pdb_ids_chunk
        ]
        # Dask distributed client
        with Client() as client:
            futures = client.compute(tasks)
            progress(futures)
            results = client.gather(futures)

        self.save_new_files_to_index()

    def foldcomp_decompress(self):

        db_path = self.dataset_repo_path() / self.type_str

        lookup_path = self.dataset_repo_path() / f"{self.type_str}.lookup"

        ids_lines = lookup_path.open().readlines()
        ids_lines = filter(
            lambda line: ".pdb" in line if (".pdb" in line or ".cif" in line) else True, ids_lines
        )
        ids = list(map(lambda line: line.split()[1], ids_lines))

        structures_path = self.structures_path()
        structures_path.mkdir(exist_ok=True, parents=True)

        def process_chunk(batch_number: int, ids: List[str]):
            # Assuming the capability to open and handle slices of `db_type`
            # `db_type` must support slicing or an equivalent method to fetch a range of items
            with foldcomp.open(db_path, ids=ids) as db:
                for (_, pdb), file_name in zip(db, ids):
                    if ".pdb" not in file_name:
                        file_name = f"{file_name}.pdb"
                    file_path = structures_path / f"{batch_number}" / f"{file_name}"
                    with open(file_path, 'w') as f:
                        f.write(pdb)

        with Client() as client:
            cpu_cores = len(client.ncores())
            structures_path = self.structures_path()
            structures_path.mkdir(exist_ok=True, parents=True)

            batches = list(self.chunk(ids))
            mkdir_for_batches(structures_path, len(batches))

            futures = []
            for number, batch in enumerate(batches):
                future = client.submit(process_chunk, number, list(batch))
                futures.append(future)

            progress(futures)
            results = client.gather(futures)

        self.save_new_files_to_index()

    def handle_afdb(self):

        import shutil
        def copy_files(src_dir, dst_dir):
            # Create destination directory if it doesn't exist
            os.makedirs(dst_dir, exist_ok=True)

            # Iterate through files in source directory
            for filename in os.listdir(src_dir):
                src_file = os.path.join(src_dir, filename)
                dst_file = os.path.join(dst_dir, filename)

                # Copy the file
                shutil.copy2(src_file, dst_file)
                print(f"Copied: {filename}")

        match self.collection_type:
            case CollectionType.all:
                pass
            case CollectionType.part:
                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                # copy_files("/Users/youngdashu/sano/offline_data", str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.clust:
                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.subset:
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
        ids_file=Path("./mix_ids1.txt")
    ).create_dataset()


def create_e_coli():
    StructuresDataset(
        db_type=DatabaseType.AFDB,
        collection_type=CollectionType.part,
        type_str="e_coli"
    ).create_dataset()


def create_swissprot():
    StructuresDataset(
        db_type=DatabaseType.AFDB,
        collection_type=CollectionType.part,
        type_str="afdb_swissprot_v4"
    ).create_dataset()


if __name__ == '__main__':
    # test()
    # create_swissprot()
    # create_e_coli()
    # test2()
    pass
