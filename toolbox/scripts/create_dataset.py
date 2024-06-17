import argparse
import pathlib

from toolbox.models.manage_dataset.database_type import DatabaseType
from toolbox.models.manage_dataset.structures_dataset import CollectionType, StructuresDataset


def create_parser():
    db_types = DatabaseType._member_names_
    collection_types = CollectionType._member_names_

    parser = argparse.ArgumentParser(description="Create protein dataset")
    parser.add_argument("-d", "--db", required=True, choices=db_types, metavar="name",
                        help=f"Database Types: {' '.join(db_types)}")
    parser.add_argument("-c", "--collection", required=True, choices=collection_types, metavar="name",
                        help=f"Collection Types: {' '.join(collection_types)}")
    parser.add_argument("-t", "--type", required=False, default="", metavar="name", help="Precise type described",
                        nargs='?')
    parser.add_argument("-v", "--version", required=False,
                        help="String to differentiate datasets; default: current date")
    parser.add_argument("-i", "--ids", required=False, type=pathlib.Path, help="File with ids to create subset")
    parser.add_argument("-s", "--seqres", required=False, type=pathlib.Path,
                        help="fasta file to use as sequence source")
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help="Should overwrite existing files? Default - false")
    parser.add_argument('-b', '--batch_size', type=int, default=10_000)
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

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


if __name__ == "__main__":
    main()
