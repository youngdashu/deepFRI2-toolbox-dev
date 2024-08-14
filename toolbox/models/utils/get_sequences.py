from typing import List, Dict

from toolbox.models.manage_dataset.sequences.from_pdb import extract_sequence_from_pdb_string
from toolbox.models.manage_dataset.utils import read_pdbs_from_h5


def get_sequences_from_batch(hdf_file_path: str, codes: List[str]) -> List[str]:

    proteins: Dict[str, str] = read_pdbs_from_h5(hdf_file_path, codes)

    return [
        extract_sequence_from_pdb_string(pdb) for pdb in proteins.values()
    ]
