import contextlib
import json
import logging
import os
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union, Dict, Optional
from urllib.request import urlopen

import dask.bag as db
import dotenv
import foldcomp
from Bio.PDB import PDBList
from dask import delayed
from dask.bag import Bag
from distributed import Client, progress, wait
from pydantic import BaseModel, field_validator

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.paths import datasets_path, repo_path
from toolbox.models.manage_dataset.utils import foldcomp_download, mkdir_for_batches, retrieve_pdb_chunk_to_h5, \
    retrieve_pdb_file_h5, alphafold_chunk_to_h5
from toolbox.models.manage_dataset.distograms.generate_distograms import generate_distograms
from toolbox.models.manage_dataset.handle_index import create_index, read_index
from toolbox.models.manage_dataset.sequences.from_pdb import get_sequence_from_pdbs
from toolbox.models.manage_dataset.sequences.load_fasta import extract_sequences_from_fasta
from toolbox.models.manage_dataset.utils import chunk, retrieve_pdb_file
from toolbox.utlis.filter_pdb_codes import filter_pdb_codes
from toolbox.utlis.search_indexes import search_indexes

dotenv.load_dotenv()
SEPARATOR = os.getenv("SEPARATOR")


class CollectionType(Enum):
    all = "all"
    clust = "clust"
    part = "part"
    subset = "subset"


class StructuresDataset(BaseModel):
    db_type: DatabaseType
    collection_type: CollectionType
    type_str: str = ""
    version: str = datetime.now().strftime('%Y%m%d_%H%M')
    ids_file: Optional[Path] = None
    seqres_file: Optional[Path] = None
    overwrite: bool = False
    batch_size: int = 10
    _client: Optional[Client] = None

    @field_validator('version', mode='before')
    def set_version(cls, v):
        return v or datetime.now().strftime('%Y%m%d_%H%M')

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

    def distograms_index_path(self):
        return self.dataset_path() / "distograms.idx"

    def distograms_file_path(self):
        return self.dataset_path() / "distograms.hdf5"

    def batches_count(self) -> int:
        return sum(1 for item in self.structures_path().iterdir() if item.is_dir())

    def create_dataset(self) -> "Dataset":

        self._client = Client(silence_logs=logging.ERROR)

        self.dataset_repo_path().mkdir(exist_ok=True, parents=True)

        if self.collection_type == CollectionType.subset:
            assert self.ids_file is not None

        if self.collection_type is CollectionType.subset or self.collection_type is CollectionType.all:
            dataset_index_file_name = self.dataset_index_file_name()

            # find existing files of the same DB
            if self.db_type == DatabaseType.other:
                print(f"Globbing {repo_path}")
                all_files = Path(repo_path).rglob("*.*")
            else:
                db_path = Path(repo_path) / self.db_type.name
                print(f"Globbing {db_path}")
                all_files = db_path.rglob("*.*")

            def process_file(file_path):
                if file_path.is_file() and file_path.suffix in {'.cif', '.pdb', '.ent'}:
                    return [(file_path.stem, str(file_path))]
                return []

            file_paths = (db.from_sequence(all_files, partition_size=self.batch_size)
                          .map(process_file)
                          .flatten()
                          .compute())
            file_paths = dict(file_paths)

            print(f"Found {len(file_paths)} files")

            ids = None
            if self.collection_type is CollectionType.subset:
                with open(self.ids_file, 'r') as f:
                    ids = f.read().splitlines()
            else:
                ids = self.get_all_ids()

            Path(f"{datasets_path}/{dataset_index_file_name}").mkdir(exist_ok=True, parents=True)

            ids_present = file_paths.keys()

            def process_id(id_):
                if id_ in ids_present:
                    return True, (id_, file_paths[id_])
                else:
                    return False, id_

            result_bag = db.from_sequence(ids, partition_size=self.batch_size).map(process_id)

            present_bag = result_bag.filter(lambda x: x[0]).map(lambda x: x[1])
            missing_bag = result_bag.filter(lambda x: not x[0]).map(lambda x: x[1])
            present_file_paths = dict(present_bag.compute())
            missing_ids = list(missing_bag.compute())

            create_index(self.dataset_index_file_path(), present_file_paths)
            print(f"Found {len(present_file_paths)} present protein files")
            print(f"Found {len(missing_ids)} missing protein ids")

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

    def generate_sequence(self):
        print("Generating sequences")
        index = read_index(self.dataset_index_file_path())
        print(len(index))
        batched_ids = self.chunk(index.keys())

        print("Searching indexes")
        index, missing_sequences = search_indexes(
            self.db_type,
            Path(datasets_path),
            batched_ids,
            'sequences'
        )

        print(len(index))
        print(f"missing seqs: {len(missing_sequences)}")

        missing_ids = db.from_sequence(missing_sequences,
                                       partition_size=self.batch_size)  # self.chunk(missing_sequences)

        missing_items: Dict[str, str] = dict.fromkeys(missing_sequences) & index

        if self.seqres_file is not None:
            print("Getting sequences from provided fasta")
            tasks = extract_sequences_from_fasta(self.seqres_file, missing_ids)
        else:
            print("Getting sequences from stored PDBs")
            db.from_sequence(missing_items).groupby(lambda code_file_path: code_file_path[1])
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
            final_result = combined.compute()[0][1]

            return final_result

        # Parallel reduce for dictionary merging
        print("\tGetting result")
        results_dict = parallel_reduce_dicts_with_bag(tasks)
        sequences_file_path = self.dataset_path() / "pdb_sequence.json"
        with open(sequences_file_path, 'w') as f:
            print("Saving sequences to dict")
            json.dump(results_dict, f)

        print("Save new index with all proteins")
        for id_ in missing_sequences:
            index[id_] = str(sequences_file_path)
        create_index(self.sequences_index_path(), index)

    def get_all_ids(self):
        match self.db_type:
            case DatabaseType.PDB:
                start_time = time.time()
                all_pdbs = PDBList().get_all_entries()
                all_pdbs = map(lambda x: x.lower(), all_pdbs)
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"PDBList().get_all_entries time: {elapsed_time} seconds")
                url = "ftp://ftp.wwpdb.org/pub/pdb/derived_data/pdb_entry_type.txt"
                start_time = time.time()
                with contextlib.closing(urlopen(url)) as handle:
                    res = list(filter_pdb_codes(handle, all_pdbs))
                    print(f"After removing non protein codes {len(res)}")
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"filter_pdb_codes time: {elapsed_time} seconds")
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

    def find_add_new_files_to_index(self):
        self.dataset_path().mkdir(exist_ok=True, parents=True)

        structures_path = self.structures_path()

        def process_directory(dir_path):
            return [str(f) for f in Path(dir_path).glob('*.*') if f.is_file()]

        numbered_dirs = [d for d in structures_path.iterdir() if d.is_dir() and d.name.isdigit()]

        if len(numbered_dirs) == 0:
            new_files = []
        else:
            dir_bag = db.from_sequence(numbered_dirs, npartitions=len(numbered_dirs))
            new_files = dir_bag.map(process_directory).flatten().compute()

        new_files_index = {str(i): v for i, v in enumerate(new_files)}

        self.add_new_files_to_index(new_files_index)

    def add_new_files_to_index(self, new_files_index):
        current_index = {}
        try:
            current_index = read_index(self.dataset_index_file_path())
        except Exception:
            pass

        current_index.update(new_files_index)

        create_index(self.dataset_index_file_path(), current_index)

    # def save_new_files_to_index(self):
    #     self.dataset_path().mkdir(exist_ok=True, parents=True)
    #
    #     structures_path = self.structures_path()
    #     files = [str(f) for f in structures_path.rglob('*.*') if f.is_file()]
    #
    #     create_index(self.dataset_index_file_path(), files)

    def _download_pdb_(self, ids: List[str]):
        Path(self.structures_path()).mkdir(exist_ok=True, parents=True)
        pdb_repo_path = self.structures_path()
        chunks = list(self.chunk(ids))

        mkdir_for_batches(pdb_repo_path, len(chunks))

        print(f"Downloading PDBs into {len(chunks)} chunks")

        new_files_index = {}

        for batch_number, pdb_ids_chunk in enumerate(chunks):
            start_time = time.time()
            futures = self._client.map(retrieve_pdb_file_h5, pdb_ids_chunk)
            downloaded_pdbs, file_path = retrieve_pdb_chunk_to_h5(pdb_repo_path / f"{batch_number}", futures)
            end_time = time.time()

            elapsed_time = end_time - start_time
            print(f"Chunk to h5 time: {elapsed_time} seconds")

            new_files_index.update(
                {
                    k: file_path for k in downloaded_pdbs
                }
            )

        # tasks = [
        #     delayed(retrieve_pdb_file)(pdb, pdb_repo_path / f"{batch_number}")
        #     for batch_number, pdb_ids_chunk in enumerate(chunks)
        #     for pdb in pdb_ids_chunk
        # ]
        # futures = self._client.compute(tasks)
        # progress(futures)
        # results = self._client.gather(futures)

        self.add_new_files_to_index(new_files_index)

    def foldcomp_decompress(self):

        db_path = str(self.dataset_repo_path() / self.type_str)

        lookup_path = self.dataset_repo_path() / f"{self.type_str}.lookup"

        ids_lines = lookup_path.open().readlines()
        ids_lines = filter(
            lambda line: ".pdb" in line if (".pdb" in line or ".cif" in line) else True, ids_lines
        )
        ids = list(map(lambda line: line.split()[1], ids_lines))

        structures_path = self.structures_path()
        structures_path.mkdir(exist_ok=True, parents=True)

        batches = list(self.chunk(ids))
        mkdir_for_batches(structures_path, len(batches))

        futures = [
            self._client.submit(
                alphafold_chunk_to_h5,
                db_path,
                structures_path / f"{number}",
                list(batch)
            )
            for number, batch in enumerate(batches)
        ]

        progress(futures)
        wait(futures)

        self.find_add_new_files_to_index()

    def handle_afdb(self):
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

    dataset = StructuresDataset(
        db_type=DatabaseType.PDB,
        collection_type=CollectionType.all,
    ).create_dataset()

    generate_distograms(dataset)

    pass
