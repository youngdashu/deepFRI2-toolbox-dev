import os
import glob
import shutil
import json
import tempfile
import pytest
import textwrap 

from os.path import join
from tests.utils import compare_dicts, compare_pdb_contents, FileComparator
from pathlib import Path
from _pytest.monkeypatch import MonkeyPatch

from toolbox.models.utils.create_client import create_client

import dotenv

# give 666 permissions to new files
os.umask(0o002)

# =====================================
# Testing behaviour of dataset creation
# =====================================

# Default input and output paths
OUTPATH = Path(__file__).parent / "data" / "dataset_generated"
EXPPATH = Path(__file__).parent / "data" / "dataset_expected"


@pytest.fixture(scope="session", autouse=True)
def clean_generated_files(tmp_path_factory):
    # Create output directory if it doesn't exist
    OUTPATH.mkdir(parents=True, exist_ok=True)
    # Clean existing files
    for f in OUTPATH.glob('*'):
        if f.is_file():
            f.unlink()
        elif f.is_dir():
            shutil.rmtree(f)
    # Verify directory is empty
    assert not list(OUTPATH.iterdir())
    yield
    # Cleanup after tests (optional)
    # for f in OUTPATH.glob('*'):
    #     if f.is_file():
    #         f.unlink()
    #     elif f.is_dir():
    #         shutil.rmtree(f)


@pytest.fixture(scope="session", autouse=True)
def setup_main_path():
    dotenv.load_dotenv()
    # Create a manual MonkeyPatch instance
    mp = MonkeyPatch()
    mp.setenv("DATA_PATH", str(OUTPATH))

    yield
    # Undo environment variable changes after the session finishes
    mp.undo()
    

# PDB

# Logic:
#   dataset        expected PDBs
# - PDB_5               5 (i.e. create new dataset with 5 PDBs) 
# - PDB_5 overwrite     5 (i.e. create new dataset with 5 PDBs) 
# - PDB_7               2 (i.e. take 5 from latest PDB_5 and add 2 new)
# - PDB_7 overwrite     7 (i.e. create new dataset with 7 PDBs) 
# - PDB_7               0 (i.e. take 7 from latest PDB_7 and add 0 new)

def test1_initial_5():
    """Create initial (new) dataset with 5 PDB structures."""

    dataset_name = "initial_5"

    create_dataset(
        dataset_name, 
        EXPPATH / "input_lists" / f"pdb_5.txt",
        overwrite=False,
    )
    compare_generated_abstraction_with_expected(dataset_name)
    compare_index_files(dataset_name)

def test2_second_5_with_overwrite():
    """Create second (new) dataset with 5 PDB structures."""

    dataset_name = "second_5"

    create_dataset(
        dataset_name, 
        EXPPATH / "input_lists" / f"pdb_5.txt",
        overwrite=True,
    )
    compare_index_files(dataset_name)

def test3_third_7():
    """Create third dataset with 7 PDB structures (only 2 should be new, rest from the second)."""

    dataset_name = "third_7"

    create_dataset(
        dataset_name, 
        EXPPATH / "input_lists" / f"pdb_7.txt",
        overwrite=False,
    )
    compare_index_files(dataset_name)

def test4_fourth_7():
    """Create fourth (new) dataset with 7 PDB structures."""

    dataset_name = "fourth_7"

    create_dataset(
        dataset_name, 
        EXPPATH / "input_lists" / f"pdb_7.txt",
        overwrite=True,
    )
    compare_index_files(dataset_name)

def test5_fifth_7():
    """Create fifth dataset with 7 PDB structures (all found in the fourth dataset)."""

    dataset_name = "fifth_7"

    create_dataset(
        dataset_name, 
        EXPPATH / "input_lists" / f"pdb_7.txt",
        overwrite=False,
    )
    compare_index_files(dataset_name)

# TODO @Paweł:
# AFDB


def create_dataset(dataset_name, ids_file_path, overwrite=False):
    from toolbox.models.manage_dataset.structures_dataset import StructuresDataset
    from toolbox.models.manage_dataset.collection_type import CollectionType
    from toolbox.models.manage_dataset.database_type import DatabaseType
    from toolbox.models.manage_dataset.distograms.generate_distograms import generate_distograms
    from toolbox.models.embedding.embedding import Embedding


    dataset = StructuresDataset(
        db_type=DatabaseType.PDB,
        collection_type=CollectionType.subset,
        version=dataset_name,
        ids_file=ids_file_path,
        overwrite=overwrite,
    )

    dataset.create_dataset()     # TODO: create_dataset(dataset)
    dataset.generate_sequence()  # TODO: generate_sequence(dataset)
    generate_distograms(dataset)

    embedding = Embedding(dataset)  # TODO: generate_embeddings(dataset, embedding_type="ESM-2")
    embedding.run()


def compare_index_files(dataset_name):
    dataset_path = OUTPATH / "datasets" / f"PDB-subset--{dataset_name}"
    expected_path = EXPPATH / "datasets" / f"PDB-subset--{dataset_name}"

    dataset_files = list(dataset_path.glob("**/*"))
    dataset_files = [file.name for file in dataset_files]

    expected_files = list(expected_path.glob("**/*"))
    expected_files = [file.name for file in expected_files]

    for file in dataset_files:
        assert file in expected_files, f"File {file} is not in expected files."

    for file in expected_files:
        assert file in dataset_files, f"File {file} is not in dataset files."

    for file_name in dataset_files:
        file_path = dataset_path / file_name
        if file_name.endswith(".idx") or file_name.endswith(".json"):
            with open(file_path) as f, open(expected_path / file_name) as f_expected:
                dataset_idx = json.load(f)
                expected_idx = json.load(f_expected)

                if dataset_idx.get("created_at"):
                    del dataset_idx["created_at"]
                if expected_idx.get("created_at"):
                    del expected_idx["created_at"]

                # Check that the keys in each index file match the expected keys.
                compare_dicts(dataset_idx, expected_idx)
        else:
            raise ValueError(f"Unknown file type: {file_name}")


def compare_generated_abstraction_with_expected(dataset_name):
    from toolbox.models.manage_dataset.utils import read_pdbs_from_h5
    
    # Compare distogram files
    # distogram_expected = EXPPATH / "distograms" / f"PDB-subset--{dataset_name}" / "batch_0.h5"
    # distogram_generated = OUTPATH / "distograms" / f"PDB-subset--{dataset_name}" / "batch_0.h5"
    # assert distogram_generated.exists(), f"Generated distogram file does not exist: {distogram_generated}"
    # FileComparator.compare_h5_files(distogram_expected, distogram_generated, "distogram", rtol=1e-3, atol=1e-5)

    # Compare embedding files
    embedding_expected = EXPPATH / "embeddings" / f"PDB-subset--{dataset_name}" / "batch_0.h5"
    embedding_generated = OUTPATH / "embeddings" / f"PDB-subset--{dataset_name}" / "batch_0.h5"
    assert embedding_generated.exists(), f"Generated embedding file does not exist: {embedding_generated}"
    FileComparator.compare_h5_files(embedding_expected, embedding_generated, None, rtol=1e-5, atol=1e-8)

    # Compare FASTA files
    fasta_expected = EXPPATH / "sequences" / f"PDB-subset--{dataset_name}.fasta"
    fasta_generated = OUTPATH / "sequences" / f"PDB-subset--{dataset_name}.fasta"
    assert fasta_generated.exists(), f"Generated FASTA file does not exist: {fasta_generated}"
    FileComparator.compare_fasta_files(fasta_expected, fasta_generated)

    pdb_files_expected = [EXPPATH / "structures" / "PDB" / "subset_" / f"{dataset_name}" / "structures" / "0" / "pdbs.h5"]
    pdb_files_generated = [OUTPATH / "structures" / "PDB" / "subset_" / f"{dataset_name}" / "structures" / "0" / "pdbs.h5"]

    for expected_file, generated_file in zip(pdb_files_expected, pdb_files_generated):
        assert generated_file.exists(), f"Generated file does not exist: {generated_file}"
        generated_pdbs = read_pdbs_from_h5(generated_file, None)
        expected_pdbs = read_pdbs_from_h5(expected_file, None)

        # Compare dictionary keys
        assert set(generated_pdbs.keys()) == set(expected_pdbs.keys()), \
            f"PDB dictionaries have different keys: {set(generated_pdbs.keys())} != {set(expected_pdbs.keys())}"

        # Compare dictionary values for each key
        for key in generated_pdbs: # TODO use utils/ comparepdbfiles
            compare_pdb_contents(generated_pdbs[key], expected_pdbs[key])