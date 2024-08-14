import pathlib
from argparse import Namespace

from toolbox.models.chains.verify_chains import verify_chains
from toolbox.models.embedding.embedding import Embedding
from toolbox.models.manage_dataset.distograms.generate_distograms import generate_distograms, read_distograms_from_file
from toolbox.models.manage_dataset.structures_dataset import StructuresDataset
from toolbox.models.utils.create_client import create_client


class CommandParser:
    def __init__(self, args: Namespace):
        self.dataset = None
        self.args = args

    def _create_dataset_from_path_(self) -> StructuresDataset:
        res = None
        path = self.args.file_path
        if path.is_dir() and (path / "dataset.json").exists():
            res = StructuresDataset.model_validate_json((path / "dataset.json").read_text())
        elif path.is_file():
            res = StructuresDataset.model_validate_json(path.read_text())
        else:
            print("Dataset path is not valid")
            raise FileNotFoundError

        res._client = create_client()
        self.dataset = res
        return res

    def dataset(self):
        dataset = StructuresDataset(
            db_type=self.args.db,
            collection_type=self.args.collection,
            type_str=self.args.type,
            version=self.args.version,
            ids_file=self.args.ids,
            seqres_file=self.args.seqres,
            overwrite=self.args.overwrite,
            batch_size=self.args.batch_size
        )
        self.dataset = dataset
        dataset.create_dataset()

    def embedding(self):
        embedding = Embedding(datasets_file_path=self.args.file_path)
        embedding.sequences_to_single_fasta()
        embedding.build_db()
        embedding.create_embeddings()

    def load(self):
        dataset = self._create_dataset_from_path_()
        print(dataset)

    def generate_sequence(self):
        self._create_dataset_from_path_()
        self.dataset.generate_sequence()

    def generate_distograms(self):
        self._create_dataset_from_path_()
        generate_distograms(self.dataset)

    def read_distograms(self):
        print(read_distograms_from_file(self.args.file_path))

    def verify_chains(self):
        self._create_dataset_from_path_()
        verify_chains(self.dataset, "./toolbox/pdb_seqres.txt")

    def run(self):
        command_method = getattr(self, self.args.command)
        if command_method:
            command_method()
        else:
            raise ValueError(f"Unknown command - {self.args.command}")

        if self.dataset is not None:
            self.dataset.close()
