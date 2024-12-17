import argparse
import pathlib

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.collection_type import CollectionType
from toolbox.scripts.command_parser import CommandParser

db_types = DatabaseType._member_names_
collection_types = CollectionType._member_names_

def add_common_arguments(parser):
    parser.add_argument('--slurm', action='store_true', help="Use SLURM job scheduler")
    parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)

def add_dataset_parser_arguments(parser):
    parser.add_argument("-d", "--db", required=True, choices=db_types, metavar="name",
                                help=f"Database Types: {' '.join(db_types)}")
    parser.add_argument("-c", "--collection", required=True, choices=collection_types, metavar="name",
                                help=f"Collection Types: {' '.join(collection_types)}")
    parser.add_argument("-t", "--type", required=False, default="", metavar="name",
                                help="Precise type described",
                                nargs='?')
    parser.add_argument("-v", "--version", required=False,
                                help="String to differentiate datasets; default: current date")
    parser.add_argument("-i", "--ids", required=False, type=pathlib.Path, help="File with ids to create subset")
    parser.add_argument("-s", "--seqres", required=False, type=pathlib.Path,
                                help="fasta file to use as sequence source")
    parser.add_argument('-o', '--overwrite', action='store_true',
                                help="Should overwrite existing files? Default - false")
    parser.add_argument('-b', '--batch-size', type=str, default=None)
    parser.add_argument('--binary', action='store_true', help='Download binary CIF in PDB db')


def create_parser():

    parser = argparse.ArgumentParser(description="Create protein dataset")

    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    parser_dataset = subparsers.add_parser("dataset", help="Create protein dataset")
    parser_dataset.add_argument('--slurm', action='store_true', help="Use SLURM job scheduler")
    add_dataset_parser_arguments(parser_dataset)

    embedding_parser = subparsers.add_parser("embedding", help="Create embeddings from datasets")
    add_common_arguments(embedding_parser)

    embedding_parser = subparsers.add_parser("embedding_single_file", help="Create embeddings from datasets")
    add_common_arguments(embedding_parser)

    load_dataset_parser = subparsers.add_parser("load", help="Load a dataset from json")
    add_common_arguments(load_dataset_parser)

    generate_sequence_parser = subparsers.add_parser("generate_sequence", help="Generate sequences for ")
    add_common_arguments(generate_sequence_parser)
    generate_sequence_parser.add_argument("--ca_mask", action='store_true', help="Require a carbon alpha atom to include an amino acid in a sequence")
    generate_sequence_parser.add_argument("--no_substitution", action='store_false', help="Don't substitute non standard amino acids")

    generate_distograms_parser = subparsers.add_parser("generate_distograms", help="Generate distograms for ")
    add_common_arguments(generate_distograms_parser)

    read_distograms_parser = subparsers.add_parser("read_distograms", help="Read distograms for ")
    add_common_arguments(read_distograms_parser)

    verify_chains_parser = subparsers.add_parser("verify_chains", help="Verify chains for ")
    add_common_arguments(verify_chains_parser)

    create_archive_parser = subparsers.add_parser("create_archive", help="Create PDB compressed archive")
    add_common_arguments(create_archive_parser)

    input_generation_parser = subparsers.add_parser("input_generation", help="Create dataset, generate sequences and distograms")
    input_generation_parser.add_argument('--slurm', action='store_true', help="Use SLURM job scheduler")
    add_dataset_parser_arguments(input_generation_parser)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    CommandParser(args).run()


if __name__ == "__main__":
    main()
