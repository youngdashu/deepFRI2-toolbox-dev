from argparse import Namespace
from datetime import datetime

from toolbox.models.chains.verify_chains import verify_chains
from toolbox.models.embedding.embedding import Embedding
from toolbox.models.manage_dataset.distograms.generate_distograms import generate_distograms, read_distograms_from_file
from toolbox.models.manage_dataset.structures_dataset import StructuresDataset
from toolbox.models.utils.create_client import create_client
from toolbox.scripts.archive import create_archive


class CommandParser:
    def __init__(self, args: Namespace):
        self.structures_dataset = None
        self.args = args

    def _create_dataset_from_path_(self) -> StructuresDataset:
        path = self.args.file_path
        if path.is_dir() and (path / "dataset.json").exists():
            self.structures_dataset = StructuresDataset.model_validate_json((path / "dataset.json").read_text())
        elif path.is_file():
            self.structures_dataset = StructuresDataset.model_validate_json(path.read_text())
        else:
            print("Dataset path is not valid")
            raise FileNotFoundError

        self.structures_dataset._client = create_client(
            True if self.args.slurm else self.structures_dataset.is_hpc_cluster
        )
        return self.structures_dataset

    def dataset(self):
        dataset = StructuresDataset(
            db_type=self.args.db,
            collection_type=self.args.collection,
            type_str=self.args.type,
            version=self.args.version,
            ids_file=self.args.ids,
            seqres_file=self.args.seqres,
            overwrite=self.args.overwrite,
            batch_size=None if self.args.batch_size is None else int(self.args.batch_size),
            binary_data_download=self.args.binary,
            is_hpc_cluster=self.args.slurm
        )
        self.structures_dataset = dataset
        dataset.create_dataset()
        return dataset

    def embedding(self):
        embedding = Embedding(datasets_file_path=self.args.file_path)
        embedding.sequences_to_multiple_fasta()
        embedding.create_embeddings()

    def embedding_single_file(self):
        embedding = Embedding(datasets_file_path=None)
        time_now = datetime.now().strftime('%Y%m%d_%H%M%S')
        embedding.create_embedding_from_file(str(self.args.file_path), f"output_{time_now}.embedding")

    def load(self):
        dataset = self._create_dataset_from_path_()
        print(dataset)

    def generate_sequence(self):
        self._create_dataset_from_path_()
        self.structures_dataset.generate_sequence(self.args.ca_mask, self.args.no_substitution)

    def generate_distograms(self):
        self._create_dataset_from_path_()
        generate_distograms(self.structures_dataset)

    def read_distograms(self):
        print(read_distograms_from_file(self.args.file_path))

    def verify_chains(self):
        self._create_dataset_from_path_()
        verify_chains(self.structures_dataset, "./toolbox/pdb_seqres.txt")

    def create_archive(self):
        self._create_dataset_from_path_()
        create_archive(self.structures_dataset)

    def input_generation(self):
        try:
            print("Creating dataset")
            self.dataset()
        except Exception as e:
            print(f"Error: {e}")
        try:
            print("Generating sequences")
            self.structures_dataset.generate_sequence()
        except Exception as e:
            print(f"Error: {e}")
        try:
            print("Generating distograms")
            generate_distograms(self.structures_dataset)
        except Exception as e:
            print(f"Error: {e}")


    def run(self):
        command_method = getattr(self, self.args.command)
        if command_method:
            command_method()
        else:
            raise ValueError(f"Unknown command - {self.args.command}")
