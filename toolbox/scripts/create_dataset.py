import argparse
import pathlib

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.structures_dataset import CollectionType
from toolbox.scripts.command_parser import CommandParser


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
    parser_dataset.add_argument('-b', '--batch-size', type=int, default=None)

    embedding_parser = subparsers.add_parser("embedding", help="Create embeddings from datasets")
    embedding_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path,
                                  help="Path to the datasets file")

    load_dataset_parser = subparsers.add_parser("load", help="Load a dataset from json")
    load_dataset_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    generate_sequence_parser = subparsers.add_parser("generate_sequence", help="Generate sequences for ")
    generate_sequence_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    generate_distograms_parser = subparsers.add_parser("generate-_distograms", help="Generate distograms for ")
    generate_distograms_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    read_distograms_parser = subparsers.add_parser("read_distograms", help="Read distograms for ")
    read_distograms_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    verify_chains_parser = subparsers.add_parser("verify_chains", help="Verify chains for ")
    verify_chains_parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    CommandParser(args).run()


if __name__ == "__main__":
    main()
