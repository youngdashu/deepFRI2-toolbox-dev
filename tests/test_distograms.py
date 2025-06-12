import os
import glob
import pytest
import textwrap 
import numpy as np

import h5py

from os.path import join

from toolbox.models.manage_dataset.distograms.generate_distograms import __process_pdbs__
from tests.utils import compare_pdb_files
from pathlib import Path


# ===============================================================
# Testing behaviour of the distograms.__process_pdbs__() function
# ===============================================================

# Default paths
INPATH = Path(__file__).parent / "data" / "distograms_pdbs"
EXPPATH = Path(__file__).parent / "data" / "distograms_expected"

RTOL = 1e-3  # TODO: small precision
ATOL = 1e-8

def test_full():
    # All residues present in the PDB

    name = '2fjh_L'
    result = __process_pdbs__(INPATH / f"pdbs.hdf5", [f'{name}.pdb'])
    expected = np.load(EXPPATH / f"{name}.npy")

    assert np.allclose(expected, result[0][1], rtol=RTOL, atol=ATOL)


# TODO keep NaNs at the beggining of the distogram

def test_cropped():
    # Missing residudes at the beginning (1-4)
    # Should be skipped in the distogram generation

    name = '1aa6_Acropped'
    result = __process_pdbs__(INPATH / f"pdbs.hdf5", [f'{name}.pdb'])
    expected = np.load(EXPPATH / f"{name}.npy")

    assert np.allclose(expected, result[0][1], rtol=RTOL, atol=ATOL)

# TODO: albo CB (disto type)  !!!!!!
def test_missing_CA():
    # Missing CA atom in residue (3)
    # 1aa6_AmisssingCA
    pass


def test_missing_one():
    # Missing residue in the middle (7)
    # 5uyl_32oneX
    pass


def test_missing_two():
    # Missing residues in the middle in two places (8-10, 41)
    # 1aa6_Acropped
    pass

