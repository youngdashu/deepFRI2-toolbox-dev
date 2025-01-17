from typing import List, Dict, Tuple, Iterable

from toolbox.models.manage_dataset.sequences.from_pdb import (
    extract_sequence_from_pdb_string,
)
from toolbox.models.manage_dataset.utils import read_pdbs_from_h5


def get_sequences_from_batch(
    hdf_file_path: str,
    codes: List[str],
    ca_mask: bool,
    substitute_non_standard_aminoacids=True,
) -> Iterable[str]:
    proteins: Dict[str, str] = read_pdbs_from_h5(hdf_file_path, codes)

    return tuple(
        [
            f">{code.removesuffix('.pdb')}\n{extract_sequence_from_pdb_string(pdb, code, ca_mask, substitute_non_standard_aminoacids)}\n"
            for code, pdb in proteins.items()
        ]
    )
