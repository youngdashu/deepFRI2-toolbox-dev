import pathlib
from io import StringIO
from pathlib import Path
from typing import List, Tuple, Any, Union

import h5py
import numpy as np
from Bio.PDB import PDBParser
from Bio.PDB.Structure import Structure
from dask.array import Array
from numpy import ndarray, dtype

from toolbox.models.manage_dataset.dataset_origin import datasets_path
from toolbox.models.manage_dataset.handle_index import read_index, create_index
from toolbox.models.manage_dataset.structures_dataset import StructuresDataset
from toolbox.utlis.search_indexes import search_indexes
import dask.bag as db
import dask.array as da

from scipy.spatial.distance import pdist, squareform


def __extract_coordinates__(file_path: Path) -> ndarray[Any, dtype[Any]]:
    parser = PDBParser()
    structure = parser.get_structure("", file_path)

    coords = np.array([
        residue["CA"].get_coord()
        for residue in structure.get_residues()
        if "CA" in residue.child_dict
    ])

    return coords


def generate_distograms(structures_dataset: StructuresDataset):
    print("Generating distograms")
    index = read_index(structures_dataset.dataset_index_file_path())
    print(f"Index len {len(index)}")
    batched_ids = structures_dataset.chunk(index.values())

    present_distograms, missing = search_indexes(
        structures_dataset.db_type,
        Path(datasets_path),
        batched_ids,
        'distograms'
    )

    def __process_pdb__(pdb_path: str):
        path = Path(pdb_path)
        coords = __extract_coordinates__(path)
        distances = squareform(pdist(coords))
        return pdb_path.split("/")[-1], distances

    distograms = db.from_sequence(missing, partition_size=structures_dataset.batch_size).map(__process_pdb__).compute()

    distograms_file = structures_dataset.dataset_path() / 'distograms.hdf5'

    hf = h5py.File(distograms_file, 'w')

    # write pdb_path and corresponding distogram to hdf5 file
    for pdb_path, distogram in distograms:
        protein_grp = hf.create_group(pdb_path)
        protein_grp.create_dataset('distogram', data=distogram)

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
