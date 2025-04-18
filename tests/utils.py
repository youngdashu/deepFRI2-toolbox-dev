from pathlib import Path
import re

from filecmp import cmp
import tempfile
from typing import List, Optional, Union

import h5py
import numpy as np
from Bio import SeqIO


def split_pdb_atom_lines(lines):
    """
    Extract PDB atom properties
    for a list of strings and
    return a list of dictionaries.

    Parameters
    ----------
    lines : list of str
        list of PDB atom strings

    Returns
    -------
    list of dict
        list of PDB atom properties
    """
    return [{"atype": line[:6].strip(),
             "index": line[6:11].strip(),
             "atom": line[12:16].strip(),
             "resid": line[17:20].strip(),
             "chain": line[21:22].strip(),
             "resseq": int(line[22:26].strip()),
             "icode": line[26:27].strip(),
             "pos_x": line[30:38].strip(),
             "pos_y": line[38:46].strip(),
             "pos_z": line[46:54].strip(),
             "occ": line[54:60].strip(),
             "tfactor": line[60:66].strip(),
             "symbol": line[76:78].strip(),
             "charge": line[78:79].strip()}
            for line in lines]


def compare_pdb_files(file_1, file_2):
    """
    Compare two PDB files.

    Parameters
    ----------
    file_1 : str
        path to the first file
    file_2 : str
        path to the second file
    """
    # Load files
    with open(file_1, 'r') as f:
        atoms_1 = f.readlines()
    with open(file_2, 'r') as f:
        atoms_2 = f.readlines()

    compare_pdb_contents(atoms_1, atoms_2)


def compare_pdb_contents(content_1: Union[str, List[str]], content_2: Union[str, List[str]]):
    """
    Compare two PDB contents.
    """
    if isinstance(content_1, str):
        atoms_1 = content_1.split("\n")
    else:
        atoms_1 = content_1

    if isinstance(content_2, str):
        atoms_2 = content_2.split("\n")
    else:
        atoms_2 = content_2

    # Consider only ATOM or HETATM lines
    r = re.compile("^(ATOM|HETATM)")
    atoms_1 = list(filter(r.match, atoms_1))
    atoms_2 = list(filter(r.match, atoms_2))

    assert len(atoms_1) == len(atoms_2)

    atoms_1 = split_pdb_atom_lines(atoms_1)
    atoms_2 = split_pdb_atom_lines(atoms_2)

    for atom_1, atom_2 in zip(atoms_1, atoms_2):
        for k in ('atype', 'atom', 'resid',
                  'icode', 'pos_x', 'pos_y', 'pos_z',
                  'occ', 'tfactor', 'symbol'):
            assert atom_1[k] == atom_2[k], f' ERROR for key "{k}", ' \
                                           f'id_1 {atom_1["index"]}: ' \
                                           f'id_2 {atom_2["index"]}: ' \
                                           f'{atom_1[k]} different from {atom_2[k]}'


def create_temp_txt_file(lines):
    """Creates a temporary text file from a list of strings and returns its Path object."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
    temp_file.write("\n".join(lines))
    temp_file.close()  # Close so it can be used elsewhere
    return Path(temp_file.name)

class FileComparator:
    @staticmethod
    def compare_h5_files(path1, path2, abstraction_name: Optional[str], rtol, atol):
        """
        Compare two HDF5 (.h5) files by checking that:
          - They have the same set of keys (datasets)
          - Their individual datasets (arrays) are equal within specified tolerances.

        Parameters:
            path1 (str or Path): The file path to the first HDF5 file.
            path2 (str or Path): The file path to the second HDF5 file.
            abstraction_name (str): Name of the dataset to compare within the H5 files.
            rtol (float): Relative tolerance for numpy comparison (default: 1e-3)
            atol (float): Absolute tolerance for numpy comparison (default: 1e-8)

        Raises:
            AssertionError: If any differences are detected.
        """
        with h5py.File(path1, 'r') as file1, h5py.File(path2, 'r') as file2:
            keys1 = list(file1.keys())
            keys2 = list(file2.keys())
            
            assert set(keys1) == set(keys2), f"Mismatch in H5 file keys: {keys1} vs {keys2}"
            
            for key in keys1:
                if abstraction_name is not None:
                    data1 = file1[key][abstraction_name][:]
                    data2 = file2[key][abstraction_name][:]
                else:
                    data1 = file1[key][:]
                    data2 = file2[key][:]
                if not np.allclose(data1, data2, rtol=rtol, atol=atol):
                    # Find indices where data differs
                    diff_mask = ~np.isclose(data1, data2, rtol=rtol, atol=atol)
                    diff_indices = np.where(diff_mask)
                    print(f"\nData differences found at indices:")
                    for idx in zip(*diff_indices):
                        print(f"Index {idx}: value1={data1[idx]}, value2={data2[idx]}")
                    raise AssertionError(
                        f"Data mismatch in dataset '{key}' in files '{path1}' and '{path2}'"
                    )
        return True

    @staticmethod
    def compare_fasta_files(path1, path2):
        """
        Compare two FASTA files by checking that:
          - They contain the same number of records.
          - Each record has the same identifier.
          - Each sequence is identical.

        Parameters:
            path1 (str or Path): The file path to the first FASTA file.
            path2 (str or Path): The file path to the second FASTA file.

        Raises:
            AssertionError: If any differences are detected.
        """
        records1 = list(SeqIO.parse(path1, "fasta"))
        records2 = list(SeqIO.parse(path2, "fasta"))

        assert len(records1) == len(records2), (
            f"Different number of sequences: {len(records1)} vs {len(records2)}"
        )

        for rec1, rec2 in zip(records1, records2):
            assert rec1.id == rec2.id, f"Sequence ID mismatch: {rec1.id} vs {rec2.id}"
            assert str(rec1.seq) == str(rec2.seq), f"Sequence content mismatch for record {rec1.id}"
        return True

    @staticmethod
    def compare_h5_files_with_tolerance(path1, path2, abstraction_name, tol=1e-5):
        """
        Compare two HDF5 (.h5) files by allowing small floating point discrepancies
        using np.allclose instead of np.array_equal.
        """
        with h5py.File(path1, 'r') as file1, h5py.File(path2, 'r') as file2:
            keys1 = list(file1.keys())
            keys2 = list(file2.keys())
            
            assert set(keys1) == set(keys2), f"Mismatch in H5 file keys: {keys1} vs {keys2}"
            
            for key in keys1:
                data1 = file1[key][abstraction_name][:]
                data2 = file2[key][abstraction_name][:]
                print(data1)
                print(data2)
                if not np.allclose(data1, data2, atol=tol):
                    raise AssertionError(
                        f"Data mismatch in dataset '{key}' in files '{path1}' and '{path2}'"
                    )
        return True
    
def compare_dicts(dict1, expected_dict):
    """
    Compare two dictionaries by checking that:
      - They have the same set of keys.
      - Their individual values are equal.
    """
    assert set(dict1.keys()) == set(expected_dict.keys()), f"Mismatch in dictionary keys (d1 - expected_dict): {dict1.keys() - expected_dict.keys()} vs (expected_dict - d1): {expected_dict.keys() - dict1.keys()}"
    for key in dict1:
        assert dict1[key] == expected_dict[key], f"Mismatch in dictionary value for key {key}: {dict1[key]} vs (expected) {expected_dict[key]}"
    return True