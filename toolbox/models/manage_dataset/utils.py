import asyncio
import traceback
from io import StringIO
from itertools import islice
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import biotite.database
import biotite.database.rcsb
import h5py
import numpy as np
from dask.distributed import as_completed, Future
from foldcomp import foldcomp
from foldcomp.setup import download

from toolbox.models.utils.cif2pdb import cif_to_pdb


def foldcomp_download(db: str, output_dir: str):
    print(f"Foldcomp downloading db: {db} to {output_dir}")
    download_chunks = 16
    for i in ["", ".index", ".dbtype", ".lookup", ".source"]:
        asyncio.run(
            download(
                f"https://foldcomp.steineggerlab.workers.dev/{db}{i}",
                f"{output_dir}/{db}{i}",
                chunks=download_chunks,
            )
        )


def chunk(data, size):
    it = iter(data)
    while True:
        chunk_data = tuple(islice(it, size))
        if not chunk_data:
            break
        yield chunk_data


def retrieve_pdb_file(pdb: str, pdb_repo_path_str: str, retry_num: int = 0):
    if retry_num > 2:
        print(f"Failed retrying {pdb}")
        return

    if retry_num > 0:
        print(f"Retrying downloading {pdb} {retry_num}")

    cif_file = biotite.database.rcsb.fetch(pdb, "cif")

    try:
        pdbs = convert_cif_to_pdb_multi_chain(cif_file, pdb)
        for pdb_name, pdb_file in pdbs.items():
            pdb_file_path = Path(pdb_repo_path_str) / pdb_name

            with open(pdb_file_path, 'w') as file:
                file.write(pdb_file)
    except Exception:
        print("Error converting CIF to PDB " + pdb)


def retrieve_pdb_chunk_to_h5(
        path_for_batch: Path,
        pdb_futures: List[Future]
) -> Tuple[List[str], str]:
    pdbs_file = path_for_batch / 'pdbs.hdf5'

    with h5py.File(pdbs_file, 'w') as hf:
        res_pdbs = []
        files_group = hf.create_group("files")

        # write pdb_path and corresponding distogram to hdf5 file
        for future, result in as_completed(pdb_futures, with_results=True):

            chains: Dict[str, str] = result
            for pdb_file_name, content in chains.items():
                pdb_content = np.frombuffer(content.encode('utf-8'), dtype=np.uint8)
                files_group.create_dataset(pdb_file_name, data=pdb_content, compression='szip')
                res_pdbs.append(pdb_file_name)

    return res_pdbs, str(pdbs_file)


def convert_cif_to_pdb_multi_chain(cif_stringio: StringIO, pdb_code: str) -> Dict[str, str]:
    """
    Convert a CIF file to multiple PDB files, one for each chain.

    Args:
        cif_stringio (TextIO): A file-like object containing the CIF data.
        pdb_code (str): The PDB code to use in the output filenames.

    Returns:
        Dict[str, str]: A dictionary where keys are filenames (str) and values are PDB content (str).
    """
    cif_stringio.seek(0)
    cif_str = cif_stringio.read()
    cif_stringio.close()

    return cif_to_pdb(cif_str, pdb_code)


def retrieve_pdb_file_h5(pdb: str) -> Dict[str, str]:
    retry_num: int = 0
    cif_file = None

    while retry_num <= 3 and cif_file is None:

        if retry_num > 0:
            print(f"Retrying downloading {pdb} {retry_num}")

        try:
            cif_file = biotite.database.rcsb.fetch(pdb, "cif")
        except Exception:
            cif_file = None

        if not cif_file:
            retry_num += 1

    if retry_num > 3:
        print(f"Failed retrying {pdb}")
        return {}

    converted = {}
    try:
        converted = convert_cif_to_pdb_multi_chain(cif_file, pdb)
    except Exception as e:
        traceback.print_exc()
        print(e)
        print("Error in converting cif " + pdb)

    return converted


def mkdir_for_batches(base_path: Path, batch_count: int):
    for i in range(batch_count):
        (base_path / f"{i}").mkdir(exist_ok=True, parents=True)


def alphafold_chunk_to_h5(db_path: str, structures_path_for_batch: str, ids: List[str]):
    pdbs_file = f"{structures_path_for_batch}/pdbs.hdf5"

    with h5py.File(pdbs_file, 'w') as hf:
        res_pdbs = []
        files_group = hf.create_group("files")

        with foldcomp.open(db_path, ids=ids) as db:
            for (_, pdb), file_name in zip(db, ids):
                if ".pdb" not in file_name:
                    file_name = f"{file_name}.pdb"

                pdb_content = np.frombuffer(pdb.encode('utf-8'), dtype=np.uint8)
                files_group.create_dataset(file_name, data=pdb_content, compression='szip')
                res_pdbs.append(file_name)


def read_all_pdbs_from_h5(h5_file_path: str) -> Optional[Dict[str, str]]:
    """
    Read all PDB contents from an HDF5 file.

    Args:
    h5_file_path (str): Path to the HDF5 file.

    Returns:
    Optional[Dict[str, str]]: A dictionary with PDB codes as keys and their contents as values,
                              or None if an error occurs.
    """
    h5_file_path = Path(h5_file_path)

    if not h5_file_path.exists():
        print(f"Error: File {h5_file_path} does not exist.")
        return None

    try:
        with h5py.File(h5_file_path, 'r') as hf:
            pdb_dict = {}
            pdb_files = hf["files"]

            for pdb_code in pdb_files:
                pdb_content = pdb_files[pdb_code][:].tobytes().decode('utf-8')
                pdb_dict[pdb_code] = pdb_content

            return pdb_dict
    except Exception as e:
        print(f"An error occurred while reading the HDF5 file: {e}")
        return None


def write_file(path, pdb_file_name, content):
    with open(path / pdb_file_name, 'w') as f:
        f.write(content)


def pdbs_h5_to_files(h5_file_path: str):
    path = Path("./pdbs")
    path.mkdir(exist_ok=True, parents=True)

    pdbs_dict = read_all_pdbs_from_h5(h5_file_path)

    if pdbs_dict is None:
        return []

    print(len(pdbs_dict.keys()))

    for i, (name, content) in enumerate(pdbs_dict.items()):
        write_file(path, f"{i}_{name}", content)


if __name__ == '__main__':

    pdbs_h5_to_files(
        "/Users/youngdashu/sano/deepFRI2-toolbox-dev/data/repo/PDB/all_/20240731_1058/structures/0/pdbs.hdf5"
    )

    # xd = read_all_pdbs_from_h5(
    #     "/Users/youngdashu/sano/deepFRI2-toolbox-dev/data/repo/PDB/all_/20240730_1747/structures/0/pdbs.hdf5"
    # )
    #
    # print(
    #     len(xd.keys())
    # )

