from io import StringIO
from pathlib import Path
from typing import List, Tuple, Any

import numpy as np
from Bio.PDB import PDBParser
from Bio.PDB.Structure import Structure
from numpy import ndarray, dtype

from toolbox.models.manage_dataset.dataset_origin import datasets_path
from toolbox.models.manage_dataset.handle_index import read_index, create_index
from toolbox.models.manage_dataset.structures_dataset import StructuresDataset
from toolbox.utlis.search_indexes import search_indexes
import dask.bag as db

from scipy.spatial.distance import pdist, squareform


def __extract_coordinates__(file_path: Path) -> ndarray[Any, dtype[Any]]:
    parser = PDBParser()
    structure = parser.get_structure("", file_path)

    print(file_path)

    for residue in structure.get_residues():
        print(residue)
        print(residue.child_dict)

    coords = np.array([
        residue["CA"].get_coord()
        for residue in structure.get_residues()
        if "CA" in residue.child_dict
    ])

    return coords


def generate_distograms(structures_dataset: StructuresDataset):
    print("Generating sequences")
    index = read_index(structures_dataset.dataset_index_file_path())
    print(len(index))
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
        print(coords)
        distances = squareform(pdist(coords))
        return pdb_path, distances

    df = db.from_sequence(missing, partition_size=structures_dataset.batch_size).map(__process_pdb__).to_dataframe(
        columns=['pdb', 'distogram']
    )

    distograms_file = structures_dataset.dataset_path() / 'distograms.h5'

    df.to_hdf(distograms_file, key='df', mode='w')

    # df.to_csv(distograms_file, index=False)

    for id_ in missing:
        index[id_] = str(distograms_file)
    create_index(structures_dataset.distograms_index_path(), index)
