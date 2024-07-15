import argparse
import pathlib

from toolbox.models.embedding.embedding import Embedding
from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.distograms.generate_distograms import generate_distograms, read_distograms_from_file
from toolbox.models.manage_dataset.structures_dataset import CollectionType, StructuresDataset


def _create_dataset_from_path_(path: pathlib.Path) -> StructuresDataset:
    if path.is_dir() and (path / "dataset.json").exists():
        return StructuresDataset.model_validate_json((path / "dataset.json").read_text())
    elif path.is_file():
        return StructuresDataset.model_validate_json(path.read_text())
    else:
        raise FileNotFoundError


def create_parser():
    db_types = DatabaseType._member_names_
    collection_types = CollectionType._member_names_

    parser = argparse.ArgumentParser(description="Create protein dataset")

    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    # Subparser for creating dataset
    parser_dataset = subparsers.add_parser("dataset", help="Create protein dataset")
    parser_dataset.add_argument("-d", "--db", required=True, choices=db_types, metavar="name",
                                help=f"Database Types: {' '.join(db_types)}")
    parser_dataset.add_argument("-c", "--collection", required=True, choices=collection_types, metavar="name",
                                help=f"Collection Types: {' '.join(collection_types)}")
    parser_dataset.add_argument("-t", "--type", required=False, default="", metavar="name",
                                help="Precise type described",
                                nargs='?')
    parser_dataset.add_argument("-v", "--version", required=False,
                                help="String to differentiate datasets; default: current date")
    parser_dataset.add_argument("-i", "--ids", required=False, type=pathlib.Path, help="File with ids to create subset")
    parser_dataset.add_argument("-s", "--seqres", required=False, type=pathlib.Path,
                                help="fasta file to use as sequence source")
    parser_dataset.add_argument('-o', '--overwrite', action='store_true',
                                help="Should overwrite existing files? Default - false")
    parser_dataset.add_argument('-b', '--batch-size', type=int, default=5_000)

    embedding_parser = subparsers.add_parser("embedding", help="Create embeddings from datasets")
    embedding_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path,
                                  help="Path to the datasets file")

    load_dataset_parser = subparsers.add_parser("load", help="Load a dataset from json")
    load_dataset_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    generate_sequence_parser = subparsers.add_parser("generate-sequence", help="Generate sequences for ")
    generate_sequence_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    generate_distograms_parser = subparsers.add_parser("generate-distograms", help="Generate distograms for ")
    generate_distograms_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    read_distograms_parser = subparsers.add_parser("read-distograms", help="Read distograms for ")
    read_distograms_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "dataset":
        dataset = StructuresDataset(
            db_type=args.db,
            collection_type=args.collection,
            type_str=args.type,
            version=args.version,
            ids_file=args.ids,
            seqres_file=args.seqres,
            overwrite=args.overwrite,
            batch_size=args.batch_size
        )

        dataset.create_dataset()
    elif args.command == "embedding":
        embedding = Embedding(datasets_file_path=args.file_path)
        embedding.create_embeddings()
    elif args.command == "load":
        dataset = _create_dataset_from_path_(args.file_path)
        print(dataset)
    elif args.command == "generate-sequence":
        dataset = _create_dataset_from_path_(args.file_path)
        dataset.generate_sequence()
    elif args.command == "generate-distograms":
        dataset = _create_dataset_from_path_(args.file_path)
        generate_distograms(dataset)
    elif args.command == "read-distograms":
        print(
            read_distograms_from_file(args.file_path)
        )


if __name__ == "__main__":
    main()
