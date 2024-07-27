import asyncio
import shutil
from io import StringIO
from itertools import islice
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Literal, Union

import biotite.database
import biotite.database.rcsb
import dask.bag
import h5py
import numpy as np
from Bio import PDB
from Bio.PDB import PDBList
from dask.distributed import Client, as_completed, Future, fire_and_forget, wait
from foldcomp import foldcomp

from foldcomp.setup import download


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
        pdbs = convert_cif_to_pdb_multi_chain(cif_file, pdb, "strIO")
        for pdb_name, pdb_file in pdbs.items():
            pdb_file_path = Path(pdb_repo_path_str) / pdb_name

            with open(pdb_file_path, 'w') as file:
                pdb_file.seek(0)
                shutil.copyfileobj(pdb_file, file)
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


def remove_hetatoms_from_model(model):
    residue_to_remove = []
    chain_to_remove = []

    for chain in model:
        for residue in chain:
            if residue.id[0] != ' ':
                residue_to_remove.append((chain.id, residue.id))
        if len(chain) == 0:
            chain_to_remove.append(chain.id)

    for residue in residue_to_remove:
        model[residue[0]].detach_child(residue[1])

    for chain in chain_to_remove:
        model.detach_child(chain)

    return model


def convert_cif_to_pdb_multi_chain(cif_stringio: StringIO, pdb_code: str, out_type: Literal["str", "strIO"]) -> Dict[
    str, Union[str, StringIO]]:
    """
    Convert a CIF file to multiple PDB files, one for each chain.

    Args:
        cif_stringio (TextIO): A file-like object containing the CIF data.
        pdb_code (str): The PDB code to use in the output filenames.

    Returns:
        Dict[str, Union[str, StringIO]]: A dictionary where keys are filenames (str) and values are PDB content (str).
    """
    # Create a structure object from the CIF file
    parser = PDB.MMCIFParser(QUIET=True)
    structure = parser.get_structure("structure", cif_stringio)

    # Create a PDB file writer
    io = PDB.PDBIO()

    # Dictionary to store PDB content for each chain
    chain_pdbs: Dict[str, str] = {}

    # Iterate through models and chains
    for model in map(remove_hetatoms_from_model, structure):

        for chain in model:
            original_chain_id = chain.id
            chain.id = chain.id[-1]

            # Select this chain only
            io.set_structure(chain)

            # Write the chain to a new StringIO object in PDB format
            pdb_stringio = StringIO()
            io.save(pdb_stringio)

            if out_type == "strIO":
                pdb_content = pdb_stringio
            else:
                pdb_content = pdb_stringio.getvalue()

            chain_pdbs[original_chain_id] = pdb_content

    # Prepare the results
    results: Dict[str, str] = {}
    for original_chain_id, pdb_content in chain_pdbs.items():
        filename: str = f"{pdb_code}_{original_chain_id}.pdb"
        results[filename] = pdb_content

    return results


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
        converted = convert_cif_to_pdb_multi_chain(cif_file, pdb, "str")
    except Exception as e:
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


from dask import delayed, compute


def write_file(path, pdb_file_name, content, ):
    with open(path / pdb_file_name, 'w') as f:
        f.write(content)


def pdbs_h5_to_files(h5_file_path: str):
    path = Path("./pdbs")
    path.mkdir(exist_ok=True, parents=True)

    with Client(processes=False) as client:
        futures = client.compute([
            delayed(write_file, pure=False)(path, name, content)
            for name, content in read_all_pdbs_from_h5(h5_file_path).items()
        ])
        return [future.result() for future in as_completed(futures)]

if __name__ == '__main__':
    xd = pdbs_h5_to_files(
        "/Users/youngdashu/sano/deepFRI2-toolbox-dev/data/repo/PDB/all_/20240727_1540/structures/0/pdbs.hdf5"
    )
