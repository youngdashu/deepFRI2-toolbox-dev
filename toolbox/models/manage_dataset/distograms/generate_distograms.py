import csv
import os
import pathlib
from io import StringIO
from pathlib import Path
from typing import List, Tuple, Any, Union

import h5py
import numpy as np
from Bio.PDB import PDBParser
from numpy import ndarray, dtype

import toolbox.models.manage_dataset.utils
from toolbox.models.manage_dataset.paths import datasets_path
from toolbox.models.manage_dataset.handle_index import read_index, create_index
from toolbox.utlis.search_indexes import search_indexes
import dask.bag as db

from scipy.spatial.distance import pdist, squareform

import time


def __extract_coordinates__(file_path: Path) -> ndarray[Any, dtype[Any]]:
    parser = PDBParser(QUIET=True)
    structure = parser.get_structure("", file_path)

    coords = np.array([
        residue["CA"].get_coord()
        for residue in structure.get_residues()
        if "CA" in residue.child_dict
    ],
        dtype=np.dtype(np.float16)
    )

    return coords


def generate_distograms(structures_dataset: "StructuresDataset"):
    print("Generating distograms")
    index = read_index(structures_dataset.dataset_index_file_path())
    print(f"Index len {len(index)}")
    batched_ids = toolbox.models.manage_dataset.utils.chunk(index.values())

    present_distograms, missing = search_indexes(
        structures_dataset.db_type,
        Path(datasets_path),
        batched_ids,
        'distograms'
    )

    def __process_pdb__(pdb_path: str):
        path = Path(pdb_path)
        coords = __extract_coordinates__(path)
        distances = squareform(pdist(coords).astype(np.float16))
        return pdb_path.split("/")[-1], distances

    start = time.time()
    distograms = db.from_sequence(missing, partition_size=structures_dataset.batch_size).map(__process_pdb__).compute()
    end = time.time()

    print(f"Time taken (calculate distograms): {end - start} seconds")

    distograms_file = structures_dataset.dataset_path() / 'distograms.hdf5'

    hf = h5py.File(distograms_file, 'w')

    # write pdb_path and corresponding distogram to hdf5 file
    start = time.time()
    for pdb_path, distogram in distograms:
        protein_grp = hf.create_group(pdb_path)
        protein_grp.create_dataset('distogram', data=distogram, compression='szip')
    hf.close()
    end = time.time()
    print(f"Time taken (save to h5): {end - start} seconds")

    for id_ in missing:
        index[id_] = str(distograms_file)
    create_index(structures_dataset.distograms_index_path(), index)


def read_distograms_from_file(distograms_file: Union[str, pathlib.Path]) -> dict:
    """
    Reads distograms from a h5py file.

    :param distograms_file: Path to the h5py file.
    :return: distograms: A dictionary mapping pdb_path to the corresponding distogram.
    """
    distograms = {}
    try:
        with h5py.File(distograms_file, 'r') as hf:
            for pdb_path in hf.keys():
                print(pdb_path)
                distogram = hf[pdb_path]['distogram'][:]
                distograms[pdb_path] = distogram
    except IOError:
        print(f"Error while reading the file {distograms_file}")
    return distograms


def compression_test(distograms: List[Tuple[str, np.ndarray]], compression=None, compression_opts=None, shuffle=True):
    # Path and filename

    test_path = Path(datasets_path).parent / "tests"

    filename = f"distograms_{compression if compression else 'none'}_opts{compression_opts}_shuffle{shuffle}.hdf5"
    hf = h5py.File(test_path / filename, 'w')

    # write pdb_path and corresponding distogram to hdf5 file
    start = time.time()
    for pdb_path, distogram in distograms:
        protein_grp = hf.create_group(pdb_path)
        protein_grp.create_dataset('distogram', data=distogram,
                                   compression=compression,
                                   compression_opts=compression_opts,
                                   shuffle=shuffle)
    hf.close()
    end = time.time()

    # Print the info
    print(f"Compression: {compression}, Compression options: {compression_opts}")
    print(f"Time taken (save to h5): {end - start} seconds")

    # Get size in megabytes
    size_mb = os.path.getsize(test_path / filename) / (1024 * 1024)
    print(f"Size of the file: {size_mb} megabytes")

    return end - start, size_mb


def run_compression_tests(distograms: List[Tuple[str, np.ndarray]]):
    opts: List[Tuple[str, Any, bool]] = [
        ('gzip', 1, False),
        ('gzip', 4, False),
        ('gzip', 9, False),
        ('szip', None, False),
        ('lzf', None, False),
    ]

    test_path = Path(datasets_path).parent / "tests"

    # File opening mode is chosen to be 'w' meaning write, you may choose 'a' for append if the file already exists
    with open(test_path / 'test_compression_results.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Test Type", "Time(s)", "Size(MB)"])  # Column names

        for op in opts:
            time_s, size = compression_test(distograms, op[0], op[1], op[2])
            writer.writerow([op, time_s, size])  # Write row with values in each column

        for op in opts:
            op = (op[0], op[1], not op[2])
            time_s, size = compression_test(distograms, op[0], op[1], op[2])
            writer.writerow([op, time_s, size])  # Write row with values in each column
