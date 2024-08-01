from typing import List, Dict

import dask.bag as db

from toolbox.models.manage_dataset.sequences.from_pdb import extract_sequence_from_pdb_string
from toolbox.models.manage_dataset.utils import read_all_pdbs_from_h5


def get_sequences_from_batch(hdf_file_path: str, codes: List[str]) -> List[str]:

    proteins: Dict[str, str] = read_all_pdbs_from_h5(hdf_file_path)

    filtered_proteins = db.from_sequence([proteins[k] for k in codes])

    sequences = filtered_proteins.map(extract_sequence_from_pdb_string).compute()

    return sequences
