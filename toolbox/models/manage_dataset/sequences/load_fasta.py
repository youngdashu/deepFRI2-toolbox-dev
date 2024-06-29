from pathlib import Path
from typing import List, Dict, Iterable

from Bio import SeqIO
from dask.bag import Bag
from dask.delayed import Delayed, delayed


def extract_sequences_from_fasta(file_path: Path, ids: Bag) -> Bag:
    sequences_dict = SeqIO.to_dict(SeqIO.parse(file_path, "fasta"))

    def handle_batch(batch_ids: List[str]):
        return [
            sequences_dict[protein_id].seq
            for protein_id in batch_ids
        ]

    return ids.map(handle_batch)
