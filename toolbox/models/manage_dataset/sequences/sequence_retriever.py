from typing import Dict, List, Iterable

from distributed import progress, Client

from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.index.handle_index import read_index, create_index
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.utils.get_sequences import get_sequences_from_batch


class SequenceRetriever:

    def __init__(self, structures_dataset):
        self.structures_dataset = structures_dataset
        self.handle_indexes: HandleIndexes = self.structures_dataset._handle_indexes

    def retrieve(
        self, ca_mask: bool = False, substitute_non_standard_aminoacids: bool = True
    ):

        structures_dataset = self.structures_dataset

        protein_index = read_index(structures_dataset.dataset_index_file_path())
        print(len(protein_index))

        search_index_result = self.handle_indexes.full_handle(
            "sequences", protein_index
        )

        h5_file_to_codes = search_index_result.grouped_missing_proteins
        missing_sequences = search_index_result.missing_protein_files.keys()
        sequences_index = search_index_result.present

        print("Getting sequences from stored PDBs")

        client: Client = self.structures_dataset._client

        def run(input_data, workers):
            return client.submit(get_sequences_from_batch, *input_data, workers=workers)

        sequences_file_path = structures_dataset.dataset_path() / (
            "sequences_ca.fasta" if ca_mask else "sequences.fasta"
        )

        with open(sequences_file_path, "w") as f:

            def collect(result):
                f.writelines(result)

            compute = ComputeBatches(client, run, collect, "sequences")

            inputs = (
                (file, codes, ca_mask, substitute_non_standard_aminoacids)
                for file, codes in h5_file_to_codes.items()
            )

            compute.compute(inputs)

        print("Save new index with all proteins")
        for id_ in missing_sequences:
            sequences_index[id_] = str(sequences_file_path)
        create_index(structures_dataset.sequences_index_path(), sequences_index)
