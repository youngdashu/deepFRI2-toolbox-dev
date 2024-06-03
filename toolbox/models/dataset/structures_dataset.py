from datetime import datetime
from enum import Enum
from itertools import islice
from pathlib import Path
from typing import List

import foldcomp
from Bio.PDB import PDBList
from dask import delayed, compute
from distributed import Client, progress
from pydantic import BaseModel

from toolbox.models.dataset.database_type import DatabaseType
from toolbox.models.dataset.dataset import Dataset
from toolbox.models.dataset.dataset_origin import dataset_path, repo_path, foldcomp_download

SEPARATOR = "-"


# class Subset(BaseModel):
#     ids: List[str]


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
    ids_file: Path = None
    overwrite: bool = False
    batch_size: int = 10_000

    def dataset_repo_path(self):
        return Path(repo_path) / self.db_type.name / f"{self.type.name}_{self.type_str}" / self.version

    def dataset_path(self):
        return Path(f"{dataset_path}/{self.dataset_index_file_name()}")

    def structures_path(self):
        return self.dataset_repo_path() / "structures"

    def dataset_index_file_name(self):
        return f"{self.db_type.name}{SEPARATOR}{self.type.name}{SEPARATOR}{self.type_str}{SEPARATOR}{self.version}"

    def create_dataset(self) -> Dataset:

        self.dataset_repo_path().mkdir(exist_ok=True, parents=True)

        if self.type == CollectionType.subset:
            assert self.ids_file is not None

        print(self.type)

        if self.type is CollectionType.subset or self.type is CollectionType.all:
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

            ids = None
            if self.type is CollectionType.subset:
                with open(self.ids_file, 'r') as f:
                    ids = f.read().splitlines()
            else:
                ids = self.get_all_ids()

            Path(f"{dataset_path}/{dataset_index_file_name}").mkdir(exist_ok=True, parents=True)
            with open(f"{dataset_path}/{dataset_index_file_name}/dataset.idx", "a") as index_file:
                for id_ in ids:
                    if id_ in file_paths:
                        index_file.write(str(file_paths[id_]) + '\n')
                    else:
                        missing_ids.append(id_)

            print(len(missing_ids))

            if self.db_type == DatabaseType.other and self.type == CollectionType.subset and len(missing_ids) > 0:
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
        pass

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
        match self.type:
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

        with (self.dataset_path() / "dataset.idx").open("a") as index_file:
            structures_path = self.structures_path()
            files = [f for f in structures_path.rglob('*.*') if f.is_file()]
            for file_path in files:
                index_file.write(f"{file_path}\n")

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
            # Submit tasks to the client
            futures = client.compute(tasks)
            # Display progress of tasks
            progress(futures)
            # Wait for all tasks to complete
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
        match self.type:
            case CollectionType.all:
                pass
            case CollectionType.part:
                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.clust:
                foldcomp_download(self.type_str, str(self.dataset_repo_path()))
                self.foldcomp_decompress()
            case CollectionType.subset:
                pass

    def handle_esma(self):
        match self.type:
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
        it = iter(it)
        return iter(lambda: tuple(islice(it, self.batch_size)), ())

    def save_dataset_metadata(self):
        with (self.dataset_path() / "dataset.json").open("w+") as json_dataset_file:
            json_dataset_file.write(self.model_dump_json())

def create_subset():
    StructuresDataset(
        db_type=DatabaseType.other,
        type=CollectionType.subset,
        ids_file=Path("./mix_ids1.txt")
    ).create_dataset()

def create_e_coli():
    StructuresDataset(
        db_type=DatabaseType.AFDB,
        type=CollectionType.part,
        type_str="e_coli"
    ).create_dataset()

if __name__ == '__main__':
    # create_e_coli()
    pass
