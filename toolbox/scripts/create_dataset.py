import argparse
import pathlib


from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.collection_type import CollectionType
from toolbox.scripts.command_parser import CommandParser

db_types = DatabaseType._member_names_
collection_types = CollectionType._member_names_

import logging
from toolbox.utlis.logging import logger, setup_colored_logging

def add_common_arguments(parser):
    parser.add_argument("--slurm", action="store_true", help="Use SLURM job scheduler")
    parser.add_argument("-p", "--file-path", required=True, type=pathlib.Path)
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")


def add_dataset_parser_arguments(parser):
    parser.add_argument(
        "-d",
        "--db",
        required=True,
        choices=db_types,
        metavar="name",
        help=f"Database Types: {' '.join(db_types)}",
    )
    parser.add_argument(
        "-c",
        "--collection",
        required=True,
        choices=collection_types,
        metavar="name",
        help=f"Collection Types: {' '.join(collection_types)}",
    )
    parser.add_argument(
        "-t",
        "--type",
        required=False,
        default="",
        metavar="name",
        help="Precise type described",
        nargs="?",
    )
    parser.add_argument(
        "--version",
        required=False,
        help="String to differentiate datasets; default: current date",
    )
    parser.add_argument(
        "-i",
        "--ids",
        required=False,
        type=pathlib.Path,
        help="File with ids to create subset",
    )
    parser.add_argument(
        "-s",
        "--seqres",
        required=False,
        type=pathlib.Path,
        help="fasta file to use as sequence source",
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="Should overwrite existing files? Default - false",
    )
    parser.add_argument("-b", "--batch-size", type=str, default=None)
    parser.add_argument(
        "--binary", action="store_true", help="Download binary CIF in PDB db"
    )
    parser.add_argument(
        "--input-path",
        type=pathlib.Path,
        default=None,
        help="Path to input directory or archive (zip/tar.gz) with protein files (pdb/cif)",
    )
    parser.add_argument(
        "--archive",
          type=pathlib.Path,
            help='Path to tar.gz archive containing structure files'
    )
    parser.add_argument(
        "-v", "--verbose", 
        action="store_true", 
        help="Enable verbose logging mode"
    )


def configure_logging(verbose):
    """Configure logging based on verbose flag"""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s %(levelname)s %(message)s'
    
    # Set up colored logging with the specified level and format
    setup_colored_logging(level=log_level, fmt=log_format)
    
    # When verbose is false, filter out logs with (V) prefix unless they're errors
    if not verbose:
        class VerboseFilter(logging.Filter):
            def filter(self, record):
                # Still show ERROR or higher regardless of (V) tag
                if record.levelno >= logging.ERROR:
                    return True
                # Filter out messages with (V) prefix in non-verbose mode
                return "(V)" not in record.getMessage()
                
        logger.addFilter(VerboseFilter())


def create_parser():
    parser = argparse.ArgumentParser(description="Create protein dataset")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    parser_dataset = subparsers.add_parser("dataset", help="Create protein dataset")
    parser_dataset.add_argument(
        "--slurm", action="store_true", help="Use SLURM job scheduler"
    )
    add_dataset_parser_arguments(parser_dataset)

    embedding_parser = subparsers.add_parser(
        "embedding", help="Create embeddings from datasets"
    )
    add_common_arguments(embedding_parser)

    load_dataset_parser = subparsers.add_parser("load", help="Load a dataset from json")
    add_common_arguments(load_dataset_parser)

    generate_sequence_parser = subparsers.add_parser(
        "generate_sequence", help="Generate sequences for "
    )
    add_common_arguments(generate_sequence_parser)
    generate_sequence_parser.add_argument(
        "--ca_mask",
        action="store_true",
        help="Require a carbon alpha atom to include an amino acid in a sequence",
    )
    generate_sequence_parser.add_argument(
        "--no_substitution",
        action="store_false",
        help="Don't substitute non standard amino acids",
    )

    generate_distograms_parser = subparsers.add_parser(
        "generate_distograms", help="Generate distograms for "
    )
    add_common_arguments(generate_distograms_parser)

    read_distograms_parser = subparsers.add_parser(
        "read_distograms", help="Read distograms for "
    )
    add_common_arguments(read_distograms_parser)

    read_pdbs_parser = subparsers.add_parser(
        "read_pdbs", help="Read pdbs for "
    )
    read_pdbs_parser.add_argument(
        "--print", action="store_true", help="Print PDB files to the terminal"
    )
    read_pdbs_parser.add_argument(
        "--to_directory", type=pathlib.Path, help="Extract PDB files to the provided directory"
    )
    read_pdbs_parser.add_argument(
        "-i",
        "--ids",
        required=False,
        type=pathlib.Path,
        help="File with ids to extract",
    )
    add_common_arguments(read_pdbs_parser)

    verify_chains_parser = subparsers.add_parser(
        "verify_chains", help="Verify chains for "
    )
    add_common_arguments(verify_chains_parser)

    create_archive_parser = subparsers.add_parser(
        "create_archive", help="Create PDB compressed archive"
    )
    add_common_arguments(create_archive_parser)

    input_generation_parser = subparsers.add_parser(
        "input_generation", help="Create dataset, generate sequences and distograms"
    )
    input_generation_parser.add_argument(
        "--slurm", action="store_true", help="Use SLURM job scheduler"
    )
    add_dataset_parser_arguments(input_generation_parser)

    create_archive_parser = subparsers.add_parser(
        "create_archive", help="Create PDB archive "
    )
    create_archive_parser.add_argument(
        "--slurm", action="store_true", help="Use SLURM job scheduler"
    )
    create_archive_parser.add_argument(
        "-p", "--file-path", required=True, type=pathlib.Path
    )
    create_archive_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()
    
    # Configure logging based on verbose flag
    configure_logging(args.verbose)
    
    # Log start of command execution
    logger.info(f"Running command: {args.command}")
    
    CommandParser(args).run()


if __name__ == "__main__":
    main()
