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
from distributed import Queue
from scipy.spatial.distance import pdist, squareform

from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.index.handle_index import read_index, create_index
from toolbox.models.manage_dataset.index.handle_indexes import HandleIndexes
from toolbox.models.manage_dataset.paths import datasets_path
from toolbox.models.manage_dataset.utils import read_pdbs_from_h5


def __extract_coordinates__(
    file: str,
) -> tuple[tuple[float, float, float], tuple[float | None, float | None, float | None]]:
    ca_coords = []
    coords_with_breaks = []
    previous_residue_sequence_number = None
    max_residue_number = 0  # Track the highest residue number encountered in the file

    for line in file.splitlines():
        if line.startswith("ATOM"):
            atom_type = line[12:16].strip()
            # Adjust indices for your specific PDB format if needed
            current_residue_sequence_number = int(line[22:26].strip())
            # Track the highest residue number encountered
            if current_residue_sequence_number > max_residue_number:
                max_residue_number = current_residue_sequence_number

            if atom_type == "CA":
                # If we have a previous residue and the current residue is not the immediate next,
                # fill the gap with None
                if (
                    previous_residue_sequence_number is not None
                    and current_residue_sequence_number
                    > previous_residue_sequence_number + 1
                ):
                    for _ in range(
                        previous_residue_sequence_number + 1,
                        current_residue_sequence_number,
                    ):
                        coords_with_breaks.append((None, None, None))

                x = float(line[30:38])
                y = float(line[38:46])
                z = float(line[46:54])

                ca_coords.append((x, y, z))
                coords_with_breaks.append((x, y, z))
                previous_residue_sequence_number = current_residue_sequence_number

    # After processing all lines, use the last encountered residue number as final_residue_number
    # If the last CA residue number is not equal to the max_residue_number, fill with None
    if (
        previous_residue_sequence_number is not None
        and max_residue_number > previous_residue_sequence_number
    ):
        for _ in range(previous_residue_sequence_number + 1, max_residue_number + 1):
            coords_with_breaks.append((None, None, None))

    return tuple(ca_coords), tuple(coords_with_breaks)


def improved_distances_calculation(coords, coords_with_breaks):
    # Calculate base distances from coords without breaks
    distances_1d = pdist(coords).astype(np.float16)
    distances_base = squareform(distances_1d)  # 2d array

    n = distances_base.shape[0]

    # 2. Wyliczyć medianę z diagonali +1 (wartości distances_base[i, i+1])
    # Sprawdzamy tylko elementy powyżej głównej przekątnej (pierwsza nadprzekątna).
    if n > 1:
        diag_plus_one = distances_base[np.arange(n - 1), np.arange(1, n)]
        median_diag = np.nanmedian(diag_plus_one)
    else:
        # Jeżeli jest tylko jeden punkt, medianę ustawiamy np. na 0 lub np.nan
        median_diag = 0.0

    # Create array with NaNs for missing coordinates
    n_full = len(coords_with_breaks)
    distances_with_nans = np.full((n_full, n_full), np.nan, dtype=np.float16)

    # Create mask of valid coordinates
    valid_indices = [
        i for i, coord in enumerate(coords_with_breaks) if coord[0] is not None
    ]

    # Fill in distances for valid coordinates
    for i, vi in enumerate(valid_indices):
        for j, vj in enumerate(valid_indices):
            distances_with_nans[vi, vj] = distances_base[i, j]

    # Fill in median_diag values around NaN diagonal elements
    for i in range(n_full):
        if np.isnan(distances_with_nans[i, i]):
            distances_with_nans[i, i] = 0.0  # Set diagonal to 0
            # Set adjacent cells to median_diag if they exist
            if i > 0:  # Left
                distances_with_nans[i, i - 1] = median_diag
                distances_with_nans[i - 1, i] = median_diag  # Symmetric
            if i < n_full - 1:  # Right
                distances_with_nans[i, i + 1] = median_diag
                distances_with_nans[i + 1, i] = median_diag  # Symmetric

    return distances_with_nans


def __process_pdbs__(h5_file_path: str, codes: List[str]):
    pdbs = read_pdbs_from_h5(h5_file_path, codes)

    res = []

    for code, content in pdbs.items():
        try:
            coords, coords_with_breaks = __extract_coordinates__(content)
            if len(coords) == 0:
                dask.distributed.print("No CA found in " + code)
                continue
            distances = improved_distances_calculation(coords, coords_with_breaks)
            res.append((code, distances))
        except Exception as e:
            # TODO logger
            print(f"Exception extracting coordinates in {code}: {e}")

    return res


def __save_result_batch_to_h5__(
    hf: h5py.File, results: Iterable[Tuple[str, np.ndarray]]
):
    distogram_pdbs_saved = []
    for pdb_path, distogram in results:

        if distogram.shape == (1, 1):
            print(f"Only one CA in {pdb_path}.")
        else:
            protein_grp = hf.create_group(pdb_path)

            if distogram.shape[0] < 3:  # no compression for small dataset
                protein_grp.create_dataset("distogram", data=distogram)
            else:
                protein_grp.create_dataset(
                    "distogram", data=distogram, compression="lzf", shuffle=True
                )
            distogram_pdbs_saved.append(pdb_path)
    return distogram_pdbs_saved


def generate_distograms(structures_dataset: "StructuresDataset"):
    print("Generating distograms")
    protein_index = read_index(structures_dataset.dataset_index_file_path())
    print(f"Index len {len(protein_index)}")

    handle_indexes: HandleIndexes = structures_dataset._handle_indexes

    search_index_result = handle_indexes.full_handle("distograms", protein_index)

    distogram_index = search_index_result.present

    print("Missing distograms")
    print(len(search_index_result.missing_protein_files))

    client: Client = structures_dataset._client

    path = structures_dataset.dataset_path() / "distograms"
    path.mkdir(exist_ok=True, parents=True)

    partial_result_data_q = Queue(name="partial_result_data_q")

    def run(input_data, machine):
        data_f = client.submit(__process_pdbs__, *input_data, workers=machine)
        return client.submit(collect_parallel, data_f, workers=machine)

    def collect(result):
        partial_result_data_q.put(result)

    def collect_parallel(result):
        if len(result) == 0:
            return {}
        hsh = hash(result[0][0])
        distograms_file = path / f"distograms_{hsh}.hdf5"
        hf = h5py.File(distograms_file, "w")
        partial = __save_result_batch_to_h5__(hf, result)
        hf.close()
        return {id_: str(distograms_file) for id_ in partial}

    compute_batches = ComputeBatches(
        structures_dataset._client,
        run,
        collect,
        f"distograms_{structures_dataset.db_type}",
    )

    inputs = (item for item in search_index_result.grouped_missing_proteins.items())

    start = time.time()
    compute_batches.compute(inputs, 5)

    end = time.time()
    print(f"Time taken (save to h5): {end - start} seconds")

    distogram_pdbs_saved_futures = partial_result_data_q.get(batch=True)

    distogram_pdbs_saved = client.gather(distogram_pdbs_saved_futures)

    for dict_id_to_h5_file in distogram_pdbs_saved:
        distogram_index.update(dict_id_to_h5_file)
    create_index(structures_dataset.distograms_index_path(), distogram_index)


def read_distograms_from_file(
    distograms_file: Union[str, pathlib.Path], limit_keys: Optional[int] = None
) -> Dict[str, np.ndarray]:
    """
    Reads distograms from a h5py file.

    :param limit_keys:
    :param distograms_file: Path to the h5py file.
    :return: distograms: A dictionary mapping pdb_path to the corresponding distogram.
    """
    distograms = {}

    try:
        with h5py.File(distograms_file, "r") as hf:
            keys = hf.keys()
            if limit_keys is not None:
                keys = list(keys)[:limit_keys]
            for pdb_path in keys:
                distogram = hf[pdb_path]["distogram"][:]
                distograms[pdb_path] = distogram
    except IOError:
        print(f"Error while reading the file {distograms_file}")
    return distograms


if __name__ == "__main__":
    distograms_file = "/Users/youngdashu/Downloads/distograms_-178652890355521777.hdf5"
    limit_keys = 1
    dists = read_distograms_from_file(
        distograms_file, limit_keys
    )

    np.savetxt('dist_1.csv', list(dists.items())[0][1], delimiter=",")
