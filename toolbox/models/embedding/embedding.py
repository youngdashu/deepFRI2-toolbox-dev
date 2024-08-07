import json
from datetime import datetime

from pathlib import Path
from typing import Dict, Tuple, ClassVar

import dask.bag as db

from toolbox.models.manage_dataset.paths import datasets_path, EMBEDDINGS_PATH
from toolbox.models.manage_dataset.handle_index import read_index

import subprocess
import time

import shutil


class Embedding:
    datasets_file_path: Path
    embeddings_path: Path

    Fasta_file: ClassVar[str] = "output.fasta"
    Db_file: ClassVar[str] = "output.db"
    Embedding_output_file: ClassVar[str] = "output.embedding"

    def __init__(self, datasets_file_path: Path):
        self.datasets_file_path = datasets_file_path

        embeddings_path_obj = Path(EMBEDDINGS_PATH)
        if not embeddings_path_obj.exists():
            embeddings_path_obj.mkdir(exist_ok=True, parents=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        embeddings_dir = embeddings_path_obj / timestamp
        embeddings_dir.mkdir(exist_ok=True, parents=True)

        self.embeddings_path = embeddings_dir

        # Copy the datasets_file_path to the embeddings_dir
        shutil.copy(self.datasets_file_path, self.embeddings_path)

    def sequences_to_single_fasta(self):
        """
        Process datasets of protein sequences, and save them into a FASTA file.
        """
        start_time = time.time()
        datasets = self.datasets_file_path.read_text().splitlines()

        all_proteins = []
        all_sequence_files = []

        for dataset_name in datasets:
            index_file = Path(datasets_path) / dataset_name / "sequences.idx"
            if not index_file.exists():
                print(f"{index_file} missing")
                continue
            index = read_index(index_file)
            proteins, files = index.keys(), index.values()
            files = list(set(files))

            all_proteins.extend(proteins)
            all_sequence_files.extend(files)

        if len(all_proteins) == 0 or len(all_sequence_files) == 0:
            return

        all_sequence_files = db.from_sequence(set(all_sequence_files), partition_size=1)
        merged_fasta = all_sequence_files.map(lambda file: Path(file).read_text()).compute()

        with open(self.embeddings_path / Embedding.Fasta_file, "w") as f:
            f.writelines(merged_fasta)

        end_time = time.time()
        print(f"Execution time for sequences_to_single_fasta: {end_time - start_time} seconds.")

    def build_db(self):
        start_time = time.time()
        cmd = [
            "tmvec", "build-db",
            "--input-fasta", f"{self.embeddings_path}/{self.Fasta_file}",
            "--output", f"{self.embeddings_path}/dbs/{self.Db_file}",
            "--cache-dir", f"{EMBEDDINGS_PATH}/cache"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        end_time = time.time()
        print(f"Execution time for create_db: {end_time - start_time} seconds.")

    def create_embeddings(self):
        start_time = time.time()

        cmd = [
            "tmvec", "embed",
            "--input-fasta", f"{self.embeddings_path}/{self.Fasta_file}",
            "--output-file", f"{self.embeddings_path}/outputs/{self.Embedding_output_file}",
            "--model-type", "ankh",
            "--database", f"{self.embeddings_path}/dbs/{self.Db_file}.npz",
            "--cache-dir", f"{EMBEDDINGS_PATH}/cache"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        end_time = time.time()
        print(f"Execution time for create_embeddings: {end_time - start_time} seconds.")
