import json
import math
from datetime import datetime

from pathlib import Path
from typing import Dict, Tuple, ClassVar, re

import dask.bag as db

from toolbox.models.embedding import esm_embedding
from toolbox.models.manage_dataset.paths import datasets_path, EMBEDDINGS_PATH
from toolbox.models.manage_dataset.index.handle_index import read_index

import subprocess
import time

import shutil


class Embedding:
    datasets_file_path: Path
    embeddings_index_path: Path
    embeddings_path: Path
    outputs_dir: Path

    Fasta_file: ClassVar[str] = "output.fasta"

    def __init__(self, output_dir_name: str | None, datasets_file_path: Path | None, embeddings_index_path: Path | None):
        self.datasets_file_path = datasets_file_path
        self.embeddings_index_path = embeddings_index_path

        embeddings_path_obj = Path(EMBEDDINGS_PATH)
        if not embeddings_path_obj.exists():
            embeddings_path_obj.mkdir(exist_ok=True, parents=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M") if output_dir_name is None else output_dir_name
        embeddings_dir = embeddings_path_obj / timestamp
        embeddings_dir.mkdir(exist_ok=True, parents=True)

        self.embeddings_path = embeddings_dir

        self.outputs_dir = self.embeddings_path
        self.outputs_dir.mkdir(exist_ok=True, parents=True)

    def run(self):
        # Read fasta file
        sequences = {}
        current_id = None
        current_seq = []

        # Determine which fasta file to read
        fasta_file = self.datasets_file_path
        if self.datasets_file_path.is_dir():
            # Look for a .fasta file with same stem in same directory
            for file in self.datasets_file_path.glob(f"*.fasta"):
                fasta_file = file
                break
            if not fasta_file or not fasta_file.exists():
                raise ValueError(f"No .fasta file found for {self.datasets_file_path.stem}")
            if self.embeddings_index_path is None:
                self.embeddings_index_path = self.datasets_file_path
        elif self.datasets_file_path.suffix == ".fasta":
            if self.embeddings_index_path is None:
                self.embeddings_index_path = self.datasets_file_path.parent



        # Read and parse the fasta file
        with open(fasta_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    if current_id:
                        sequences[current_id] = ''.join(current_seq)
                        current_seq = []
                    current_id = line[1:]
                else:
                    current_seq.append(line)

        if current_id:
            sequences[current_id] = ''.join(current_seq)

        esm_embedding.embed(sequences, self.outputs_dir, self.embeddings_index_path)




    # def sequences_to_multiple_fasta(self, num_files: int = 1):
    #     """
    #     Process datasets of protein sequences, transform them into a single entity,
    #     and then save them into multiple FASTA files of approximately equal size.

    #     :param num_files: Number of FASTA files to split the sequences into. Default is 1.
    #     """
    #     if datasets_path is None:
    #         raise ValueError("Please provide a dataset path")
    #     start_time = time.time()
    #     datasets = self.datasets_file_path.read_text().splitlines()

    #     all_sequence_files = []

    #     for dataset_name in datasets:
    #         index_file = Path(datasets_path) / dataset_name / "sequences.idx"
    #         if not index_file.exists():
    #             print(f"{index_file} missing")
    #             continue
    #         index = read_index(index_file)
    #         files = set(index.values())
    #         all_sequence_files.extend(files)

    #     if len(all_sequence_files) == 0:
    #         print("No sequence files found.")
    #         return

    #     all_sequence_files = db.from_sequence(set(all_sequence_files), partition_size=1)
    #     merged_fasta = all_sequence_files.map(
    #         lambda file: Path(file).read_text()
    #     ).compute()

    #     # Combine all sequences into a single string
    #     single_fasta_entity = "".join(merged_fasta)

    #     # Split the single entity into individual sequences
    #     sequences = re.findall(r"(>.+?\n(?:[^>]+\n)+)", single_fasta_entity, re.DOTALL)

    #     total_size = sum(len(seq) for seq in sequences)
    #     target_size_per_file = math.ceil(total_size / num_files)

    #     # Write the sequences to separate files
    #     current_file_index = 0
    #     current_file_size = 0
    #     current_file = open(
    #         self.embeddings_path / f"output_{current_file_index + 1}.fasta", "w"
    #     )

    #     for sequence in sequences:
    #         sequence_size = len(sequence)
    #         if (
    #             current_file_size + sequence_size > target_size_per_file
    #             and current_file_index < num_files - 1
    #         ):
    #             current_file.close()
    #             current_file_index += 1
    #             current_file = open(
    #                 self.embeddings_path / f"output_{current_file_index + 1}.fasta", "w"
    #             )
    #             current_file_size = 0

    #         current_file.write(sequence)
    #         current_file_size += sequence_size

    #     current_file.close()

    #     end_time = time.time()
    #     print(
    #         f"Execution time for sequences_to_multiple_fasta: {end_time - start_time} seconds."
    #     )
    #     print(
    #         f"Created {current_file_index + 1} FASTA files of approximately {target_size_per_file} bytes each."
    #     )

    # def create_embeddings(self):
    #     start_time = time.time()

    #     # Find all FASTA files in the embeddings directory
    #     fasta_files = glob.glob(str(self.embeddings_path / f"output_*.fasta"))

    #     if not fasta_files:
    #         print("No FASTA files found.")
    #         return

    #     # Create outputs directory if it doesn't exist

    #     total_embedding_time = 0

    #     for i, fasta_file in enumerate(fasta_files, 1):
    #         print(f"Processing file {i}/{len(fasta_files)}: {fasta_file}")
    #         output_file = f"output_{i}.embedding"

    #         file_execution_time = self.create_embedding_from_file(
    #             fasta_file, output_file
    #         )
    #         total_embedding_time += file_execution_time

    #     end_time = time.time()
    #     total_execution_time = end_time - start_time

    #     print(f"Total embedding time: {total_embedding_time:.2f} seconds")
    #     print(
    #         f"Total execution time (including overhead): {total_execution_time:.2f} seconds"
    #     )

    # def create_embedding_from_file(self, fasta_file_path: str, output_file_name: str):
    #     file_start_time = time.time()

    #     cmd = [
    #         "tmvec",
    #         "embed",
    #         "--input-fasta",
    #         str(fasta_file_path),
    #         "--output-file",
    #         str(self.outputs_dir / output_file_name),
    #         "--model-type",
    #         "ankh",
    #         "--cache-dir",
    #         str(Path(EMBEDDINGS_PATH) / "cache"),
    #     ]

    #     result = subprocess.run(cmd, capture_output=True, text=True)

    #     if result.returncode != 0:
    #         print(f"Error processing {fasta_file_path}:")
    #         print(result.stderr)
    #     else:
    #         print(f"Successfully processed {fasta_file_path}")

    #     file_end_time = time.time()
    #     file_execution_time = file_end_time - file_start_time

    #     print(f"Execution time for file: {file_execution_time:.2f} seconds")

    #     return file_execution_time

    # def build_db(self):
    #     start_time = time.time()
    #     cmd = [
    #         "tmvec",
    #         "build-db",
    #         "--input-fasta",
    #         f"{self.embeddings_path}/{self.Fasta_file}",
    #         "--output",
    #         f"{self.embeddings_path}/dbs/{self.Db_file}",
    #         "--cache-dir",
    #         f"{EMBEDDINGS_PATH}/cache",
    #     ]

    #     result = subprocess.run(cmd, capture_output=True, text=True)

    #     end_time = time.time()
    #     print(f"Execution time for create_db: {end_time - start_time} seconds.")
