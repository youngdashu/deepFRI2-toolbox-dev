import csv
import os
import pathlib
import time
from pathlib import Path
from typing import List, Tuple, Any, Union, Iterable, Dict, Optional

import dask.distributed
import h5py
import numpy as np
from dask.distributed import Client
from scipy.spatial.distance import pdist, squareform

from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.index.handle_index import read_index, create_index
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.manage_dataset.paths import datasets_path
from toolbox.models.manage_dataset.utils import read_pdbs_from_h5


def __extract_coordinates__(file: str) -> tuple[tuple[float, float, float], ...]:
    ca_coords = []

    for line in file.splitlines():
        if line.startswith('ATOM'):
            atom_type = line[12:16].strip()
            if atom_type == 'CA':
                x = float(line[30:38])
                y = float(line[38:46])
                z = float(line[46:54])
                ca_coords.append((x, y, z))

    return tuple(ca_coords)


# @njit
# def pdist_numba(X):
#     n = X.shape[0]
#     m = X.shape[1]
#     k = n * (n - 1) // 2
#     D = np.empty(k, dtype=np.float16)
#     l = 0
#     for i in range(n - 1):
#         for j in range(i + 1, n):
#             d = 0.0
#             for x in range(m):
#                 tmp = X[i, x] - X[j, x]
#                 d += tmp * tmp
#             D[l] = np.sqrt(d)
#             l += 1
#     return D


# @njit
# def squareform_numba(distances_1d):
#     # Calculate the size of the square matrix
#     n = int(np.ceil(np.sqrt(distances_1d.size * 2)))
#
#     # Initialize the square matrix
#     square_matrix = np.zeros((n, n), dtype=distances_1d.dtype)
#
#     # Fill the upper triangle of the matrix
#     k = 0
#     for i in range(n - 1):
#         for j in range(i + 1, n):
#             square_matrix[i, j] = distances_1d[k]
#             square_matrix[j, i] = distances_1d[k]  # Mirror the value to lower triangle
#             k += 1
#
#     return square_matrix

def __process_pdbs__(h5_file_path: str, codes: List[str]):
    pdbs = read_pdbs_from_h5(h5_file_path, codes)

    res = []

    for code, content in pdbs.items():
        coords = __extract_coordinates__(content)
        if len(coords) == 0:
            dask.distributed.print("No CA found in " + code)
            continue
        distances_1d = pdist(coords).astype(np.float16)
        distances = squareform(distances_1d)
        res.append(
            (code, distances)
        )
    return res


def __save_result_batch_to_h5__(hf: h5py.File, results: Iterable[Tuple[str, np.ndarray]]):
    distogram_pdbs_saved = []
    for pdb_path, distogram in results:

        if distogram.shape == (1, 1):
            print(f"Only one CA in {pdb_path}.")
        else:
            protein_grp = hf.create_group(pdb_path)

            if distogram.shape[0] < 3:  # no compression for small dataset
                protein_grp.create_dataset('distogram', data=distogram)
            else:
                protein_grp.create_dataset('distogram', data=distogram, compression='lzf', shuffle=True)
            distogram_pdbs_saved.append(pdb_path)
    return distogram_pdbs_saved


def generate_distograms(structures_dataset: "StructuresDataset"):
    print("Generating distograms")
    protein_index = read_index(structures_dataset.dataset_index_file_path())
    print(f"Index len {len(protein_index)}")

    handle_indexes: HandleIndexes = structures_dataset._handle_indexes

    search_index_result = handle_indexes.full_handle(
        'distograms',
        protein_index
    )

    distogram_index = search_index_result.present

    print("Missing distograms")
    print(len(search_index_result.missing_protein_files))

    distogram_pdbs_saved = []

    client: Client = structures_dataset._client
    distograms_file = structures_dataset.dataset_path() / 'distograms.hdf5'
    hf = h5py.File(distograms_file, 'w')

    def run(input_data):
        return client.submit(__process_pdbs__, *input_data)

    def collect(result):
        partial = __save_result_batch_to_h5__(hf, result)
        distogram_pdbs_saved.extend(partial)

    compute_batches = ComputeBatches(structures_dataset._client, run, collect)

    inputs = (item for item in search_index_result.grouped_missing_proteins.items())

    start = time.time()
    compute_batches.compute(inputs, 1)
    hf.close()
    end = time.time()
    print(f"Time taken (save to h5): {end - start} seconds")

    for id_ in distogram_pdbs_saved:
        distogram_index[id_] = str(distograms_file)
    create_index(structures_dataset.distograms_index_path(), distogram_index)


def read_distograms_from_file(distograms_file: Union[str, pathlib.Path], limit_keys: Optional[int] = None) -> Dict[
    str, np.ndarray]:
    """
    Reads distograms from a h5py file.

    :param limit_keys:
    :param distograms_file: Path to the h5py file.
    :return: distograms: A dictionary mapping pdb_path to the corresponding distogram.
    """
    distograms = {}

    try:
        with h5py.File(distograms_file, 'r') as hf:
            keys = hf.keys()
            if limit_keys is not None:
                keys = list(keys)[:limit_keys]
            for pdb_path in keys:
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
        protein_grp.create_dataset(
            'distogram',
            data=distogram,
            compression=compression,
            compression_opts=compression_opts,
            shuffle=shuffle
        )
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
