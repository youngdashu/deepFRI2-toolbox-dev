from pathlib import Path
import time
from distributed import Client


from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.index.handle_index import read_index, create_index
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.manage_dataset.utils import format_time
from toolbox.models.utils.get_sequences import get_sequences_from_batch
from toolbox.utlis.logging import log_title

from toolbox.utlis.logging import logger

class SequenceRetriever:

    def __init__(self, structures_dataset):
        self.structures_dataset = structures_dataset
        self.handle_indexes: HandleIndexes = self.structures_dataset._handle_indexes

    def retrieve(
        self, ca_mask: bool = False, substitute_non_standard_aminoacids: bool = True
    ):
        
        log_title("Retrieving sequences")

        start = time.time()

        structures_dataset = self.structures_dataset

        protein_index = read_index(structures_dataset.dataset_index_file_path(), structures_dataset.config.data_path)
        
        search_index_result = self.handle_indexes.full_handle(
            "sequences", protein_index, structures_dataset.overwrite
        )

        h5_file_to_codes = search_index_result.grouped_missing_proteins
        missing_sequences = search_index_result.missing_protein_files.keys()
        sequences_index = search_index_result.present

        logger.info("Getting sequences from stored PDBs")

        client: Client = self.structures_dataset._client

        def run(input_data, workers):
            return client.submit(get_sequences_from_batch, *input_data, workers=workers)
        
        sequences_path_obj = Path(self.structures_dataset.config.data_path) / "sequences"
        if not sequences_path_obj.exists():
            sequences_path_obj.mkdir(exist_ok=True, parents=True)

        sequences_file_base_name = f"{structures_dataset.dataset_dir_name()}{('_ca' if ca_mask else '')}.fasta"
        sequences_file_path = sequences_path_obj / sequences_file_base_name

        with open(sequences_file_path, "w") as f:

            def collect(result):
                f.writelines(result)

            compute = ComputeBatches(client, run, collect, "sequences")

            inputs = (
                (file, codes, ca_mask, substitute_non_standard_aminoacids)
                for file, codes in h5_file_to_codes.items()
            )

            compute.compute(inputs)

        logger.info("Saving new sequences index")
        for id_ in missing_sequences:
            sequences_index[id_] = str(sequences_file_path)
        create_index(structures_dataset.sequences_index_path(), sequences_index, structures_dataset.config.data_path)

        end = time.time()
        logger.info(f"Total time: {format_time(end - start)}")
