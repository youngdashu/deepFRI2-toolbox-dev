import logging
import pathlib
from argparse import Namespace

from distributed import Client

from toolbox.models.chains.verify_chains import verify_chains
from toolbox.models.embedding.embedding import Embedding
from toolbox.models.manage_dataset.distograms.generate_distograms import generate_distograms, read_distograms_from_file
from toolbox.models.manage_dataset.structures_dataset import CollectionType, StructuresDataset


def _create_dataset_from_path_(path: pathlib.Path) -> StructuresDataset:
    res = None
    if path.is_dir() and (path / "dataset.json").exists():
        res = StructuresDataset.model_validate_json((path / "dataset.json").read_text())
    elif path.is_file():
        res = StructuresDataset.model_validate_json(path.read_text())
    else:
        print("Dataset path is not valid")
        raise FileNotFoundError

    res._client = Client(silence_logs=logging.ERROR)
    return res


class CommandParser:
    def __init__(self, args: Namespace):
        self.args = args

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
        dataset.create_dataset()

    def embedding(self):
        embedding = Embedding(datasets_file_path=self.args.file_path)
        embedding.sequences_to_single_fasta()
        embedding.build_db()
        embedding.create_embeddings()

    def load(self):
        dataset = _create_dataset_from_path_(self.args.file_path)
        print(dataset)

    def generate_sequence(self):
        dataset = _create_dataset_from_path_(self.args.file_path)
        dataset.generate_sequence()

    def generate_distograms(self):
        dataset = _create_dataset_from_path_(self.args.file_path)
        generate_distograms(dataset)

    def read_distograms(self):
        print(read_distograms_from_file(self.args.file_path))

    def verify_chains(self):
        dataset = _create_dataset_from_path_(self.args.file_path)
        verify_chains(dataset, "./toolbox/pdb_seqres.txt")

    def run(self):
        command_method = getattr(self, self.args.command)
        if command_method:
            command_method()
        else:
            raise ValueError(f"Unknown command - {self.args.command}")
