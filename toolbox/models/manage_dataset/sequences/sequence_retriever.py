from pathlib import Path
from typing import Dict, List

import dask.bag as db
from distributed import progress

from toolbox.models.manage_dataset.index.handle_index import read_index, create_index
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.manage_dataset.paths import datasets_path
from toolbox.models.manage_dataset.sequences.load_fasta import extract_sequences_from_fasta
from toolbox.models.manage_dataset.utils import groupby_dict_by_values
from toolbox.models.utils.get_sequences import get_sequences_from_batch
from toolbox.utlis.search_indexes import search_indexes


class SequenceRetriever:

    def __init__(self, structures_dataset):
        self.structures_dataset = structures_dataset
        self.handle_indexes: HandleIndexes = self.structures_dataset._handle_indexes

    def retrieve(self):

        structures_dataset = self.structures_dataset

        print("Generating sequences")
        protein_index = read_index(structures_dataset.dataset_index_file_path())
        print(len(protein_index))

        self.handle_indexes.read_indexes('sequences')
        sequences_index, missing_sequences = self.handle_indexes.find_present_and_missing_ids(
            'sequences',
            protein_index.keys()
        )

        missing_items: Dict[str, str] = {
            missing_protein_name: protein_index[missing_protein_name] for missing_protein_name in missing_sequences
        }

        reversed: dict[str, List[str]] = groupby_dict_by_values(missing_items)

        sequences_file_path = structures_dataset.dataset_path() / "sequences.fasta"

        # if structures_dataset.seqres_file is not None:
        #     print("Getting sequences from provided fasta")
        #     missing_ids = db.from_sequence(missing_sequences,
        #                                    partition_size=structures_dataset.batch_size)
        #     tasks = extract_sequences_from_fasta(structures_dataset.seqres_file, missing_ids)
        # else:

        print("Getting sequences from stored PDBs")

        futures = []
        for proteins_file, codes in reversed.items():
            future = structures_dataset._client.submit(get_sequences_from_batch, proteins_file, codes)
            futures.append(future)
        progress(futures)
        all_sequences: List[List[str]] = structures_dataset._client.gather(futures)

        all_codes: List[List[str]] = reversed.values()

        with open(sequences_file_path, 'w') as f:
            print("Saving sequences to fasta")
            for sequences, codes in zip(all_sequences, all_codes):
                for sequence, code in zip(sequences, codes):
                    transformed_code = str(code).removesuffix(".pdb")
                    f.write(
                        f">{transformed_code}\n{sequence}\n"
                    )

        print("Save new index with all proteins")
        for id_ in missing_sequences:
            sequences_index[id_] = str(sequences_file_path)
        create_index(structures_dataset.sequences_index_path(), sequences_index)
