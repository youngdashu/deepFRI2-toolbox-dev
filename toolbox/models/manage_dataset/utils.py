import asyncio
import os
import time
import traceback
import zlib
from io import BytesIO, StringIO
from itertools import islice
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Iterable

import biotite.database
import biotite.database.rcsb
import dask
import h5py
import numpy as np
from dask.distributed import as_completed, worker_client
from foldcomp import foldcomp
from foldcomp.setup import download

from toolbox.models.manage_dataset.compress_experiment.exp import compress_and_save_h5_combined_lzf_shuffle, compress_and_save_h5_individual, \
    compress_and_save_h5_individual_lzf, compress_and_save_h5_combined, compress_and_save_h5_combined_lzf, compress_and_save_h5_individual_lzf_shuffle
from toolbox.models.utils.cif2pdb import cif_to_pdb, binary_cif_to_pdb


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


def retrieve_cif(pdb: str) -> Tuple[Optional[str], str]:
    retry_num: int = 0
    cif_file: Optional[str] = None

    while retry_num <= 3 and cif_file is None:
        if retry_num > 0:
            print(f"Retrying downloading {pdb} {retry_num}")

        try:
            cif_file_io: StringIO = biotite.database.rcsb.fetch(pdb, "cif")
            cif_file: str = cif_file_io.getvalue()
        except Exception:
            cif_file = None

        if not cif_file:
            retry_num += 1

    if retry_num > 3:
        print(f"Failed retrying {pdb}")
        return None, pdb

    return cif_file, pdb


def cif_to_pdbs(input_data) -> Tuple[Dict[str, str], Tuple[str, Optional[str]]]:
    cif_file, pdb = input_data
    if cif_file is None:
        return {}, (pdb, None)

    converted = {}
    try:
        converted = cif_to_pdb(cif_file, pdb)
    except Exception as e:
        traceback.print_exc()
        print(e)
        print("Error in converting cif " + pdb)
        return {}, (pdb, None)

    if converted is None:
        return {}, (pdb, None)

    return converted, (pdb, cif_file)


def binary_cif_to_pdbs(input_data) -> Tuple[Dict[str, str], Tuple[str, BytesIO | None]]:
    binary_file_bytes_io, pdb = input_data
    if binary_file_bytes_io is None:
        return {}, (pdb, None)
    converted = {}
    try:
        converted = binary_cif_to_pdb(binary_file_bytes_io, pdb)
    except Exception as e:
        traceback.print_exc()
        print(e)
        print("Error in converting cif " + pdb)

    return converted, (pdb, binary_file_bytes_io)


def retrieve_binary_cif(pdb: str) -> tuple[BytesIO | None, str]:
    retry_num: int = 0
    binary_file_bytes_io: Optional[BytesIO] = None

    while retry_num <= 3 and binary_file_bytes_io is None:

        if retry_num > 0:
            print(f"Retrying downloading {pdb} {retry_num}")

        try:
            binary_file_bytes_io: BytesIO = biotite.database.rcsb.fetch(pdb, "bcif")
        except Exception:
            binary_file_bytes_io = None

        if not binary_file_bytes_io:
            retry_num += 1

    if retry_num > 3:
        print(f"Failed retrying {pdb}")
        return None, pdb

    return binary_file_bytes_io, pdb


def process_future(future: Tuple[Dict[str, str], Tuple[str, str]]):
    # Directly use future since it's a tuple containing the results
    chains: Dict[str, str] = future[0]
    cif_file: Tuple[str, str] = future[1]

    return chains.keys(), chains.values(), cif_file


def aggregate_results(protein_pdbs_with_cif: List[Tuple[Dict[str, str], Tuple[str, str]]],
                      download_start_time: float) -> Tuple[
    List[str], List[str], List[Tuple[str, str]]]:
    end_time = time.time()

    print(f"Download time: {end_time - download_start_time}")

    all_res_pdbs = []
    all_contents = []
    cif_files = []

    for prot, cif_file in protein_pdbs_with_cif:
        all_res_pdbs.extend(prot.keys())
        all_contents.extend(prot.values())
        cif_files.append(cif_file)

    return all_res_pdbs, all_contents, cif_files


# def create_zip_archive(path_for_batch: Path, results):
#     zip_path = path_for_batch / 'cif_files.zip'
#     cif_files = results[2]
#     with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#         for cif_file_name, cif_str in cif_files:
#             cif_file_name = cif_file_name if cif_file_name.endswith('.cif') else cif_file_name + '.cif'
#             zipf.writestr(cif_file_name, cif_str)
#     return str(zip_path)
#
#
# def create_pdb_zip_archive(path_for_batch: Path, results):
#     zip_path = path_for_batch / 'pdb_files.zip'
#     all_res_pdbs = results[0]
#     all_contents = results[1]
#     with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#         for pdb_file_name, pdb_str in zip(all_res_pdbs, all_contents):
#             pdb_file_name = pdb_file_name if pdb_file_name.endswith('.pdb') else pdb_file_name + '.pdb'
#             zipf.writestr(pdb_file_name, pdb_str)
#     return str(zip_path)


def compress_and_save_h5(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_res_pdbs):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        files_group = hf.create_group("files")
        files_together = zlib.compress("|".join(all_contents).encode('utf-8'))
        pdbs_content = np.frombuffer(files_together, dtype=np.uint8)
        files_group.create_dataset(
            name=";".join(all_res_pdbs),
            data=pdbs_content
        )
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time: {total_time}")
    return str(pdbs_file)


def compress_and_save_experiment(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    fs = [
        compress_and_save_h5_individual,
        compress_and_save_h5_individual_lzf,
        compress_and_save_h5_individual_lzf_shuffle,
        compress_and_save_h5_combined,
        compress_and_save_h5_combined_lzf,
        compress_and_save_h5_combined_lzf_shuffle,
        compress_and_save_h5
    ]

    descriptions = [
        'individual gzip',
        'individual lzf',
        'individual lzf shuffle',
        'combined gzip',
        'combined lzf',
        'combined lzf shuffle',
        'combined zlib'
    ]

    inputs = list(results)

    def get_file_size_mb(file_path):
        try:
            size_in_bytes = os.path.getsize(file_path)
            size_in_mb = size_in_bytes / (1024 * 1024)  # Convert bytes to megabytes
            return round(size_in_mb, 2)
        except Exception:
            return None

    for f, desc in zip(fs, descriptions):
        print(desc)
        path = f(path_for_batch, inputs)
        print(path, get_file_size_mb(path))



def retrieve_pdb_chunk_to_h5(
        path_for_batch: Path,
        pdb_ids: Iterable[str],
        is_binary: bool,
        workers: List[str] = None
) -> Tuple[List[str], str]:
    with worker_client() as client:
        start_time = time.time()

        pdb_futures = client.map(retrieve_binary_cif if is_binary else retrieve_cif, pdb_ids, workers=workers)
        converted_pdb_futures = client.map(binary_cif_to_pdbs if is_binary else cif_to_pdbs, pdb_futures,
                                           workers=workers)
        download_start_time = time.time()
        aggregated = client.submit(aggregate_results, converted_pdb_futures, download_start_time, workers=workers)

        # Create delayed tasks for H5 and ZIP creation
        h5_task = client.submit(compress_and_save_h5, path_for_batch, aggregated, pure=False, workers=workers)
        get_ids_task = client.submit(lambda results: results[0], aggregated, workers=workers)
        # zip_task = client.submit(create_cif_files_zip_archive, path_for_batch, aggregated, pure=False)
        # pdb_zip_task = client.submit(create_pdb_zip_archive, path_for_batch, aggregated, pure=False)

        # Compute the tasks
        pdb_ids, h5_file_path = client.gather(
            [get_ids_task, h5_task]
        )

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Total processing time {path_for_batch.stem}: {total_time}")

        return pdb_ids, h5_file_path


def mkdir_for_batches(base_path: Path, batch_count: int):
    for i in range(batch_count):
        (base_path / f"{i}").mkdir(exist_ok=True, parents=True)


def alphafold_chunk_to_h5(db_path: str, structures_path_for_batch: str, ids: List[str]):
    protein_codes = []
    contents = []

    with foldcomp.open(db_path, ids=ids) as db:
        for (_, content), file_name in zip(db, ids):
            if ".pdb" not in file_name:
                file_name = f"{file_name}.pdb"
            protein_id = file_name.removesuffix("-F1-model_v4.pdb").removeprefix("AF-")

            contents.append(content)
            protein_codes.append(protein_id)

    h5_file = compress_and_save_h5(
        Path(structures_path_for_batch),
        (protein_codes, contents, [])
    )

    if h5_file is None:
        return {}

    return {
        protein_code: h5_file for protein_code in protein_codes
    }


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
            pdb_files = hf["files"]

            for pdb_file_names in pdb_files:
                pdb_contents_bytes = pdb_files[pdb_file_names][:].tobytes()
                decompressed = zlib.decompress(pdb_contents_bytes).decode('utf-8')
                all_pdbs = decompressed.split("|")

                all_file_names_split = pdb_file_names.split(";")

            return dict(zip(all_file_names_split, all_pdbs))
    except Exception as e:
        print(f"An error occurred while reading the HDF5 file: {e}")
        return None


def read_pdbs_from_h5(h5_file_path: str, codes: Optional[List[str]]) -> Optional[Dict[str, str]]:
    h5_file_path_obj = Path(h5_file_path)
    if not h5_file_path_obj.exists():
        dask.distributed.print(f"Error: File {h5_file_path_obj} does not exist.")
        return None

    codes = None if not codes else set(codes)

    try:
        with h5py.File(h5_file_path_obj, 'r') as hf:
            pdb_files = hf["files"]
            result = {}

            for pdb_file_names in pdb_files:
                pdb_contents_bytes = pdb_files[pdb_file_names][:].tobytes()
                decompressed = zlib.decompress(pdb_contents_bytes).decode('utf-8')
                all_pdbs = decompressed.split("|")
                all_file_names_split = pdb_file_names.split(";")

                if codes:
                # Only include the codes that are in the 'codes' list
                    for code, pdb in zip(all_file_names_split, all_pdbs):
                        if code in codes:
                            result[code] = pdb
                else:
                    for code, pdb in zip(all_file_names_split, all_pdbs):
                        result[code] = pdb

            return result

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


def groupby_dict_by_values(d):
    v = {}

    for key, value in d.items():
        v.setdefault(value, []).append(key)

    return v


if __name__ == '__main__':
    # pdbs_h5_to_files(
    #     "/Users/youngdashu/sano/deepFRI2-toolbox-dev/data/repo/PDB/subset_/20240731_1535/structures/0/pdbs.hdf5"
    # )

    code = '1j6t'

    # retrieve_binary_cifs_to_pdbs(code)
    #
    # retrieve_cifs_to_pdbs(code)

    # d = read_all_pdbs_from_h5(
    #     "/Users/youngdashu/sano/deepFRI2-toolbox-dev/data/repo/PDB/all_/20240813_0238/structures/1/pdbs.hdf5")
    #
    # for k in d.keys():
    #     if k.startswith('5dat'):
    #         print(k)
    #
    #         print(d[k])

    # print(d['1hhz_F.pdb'])
