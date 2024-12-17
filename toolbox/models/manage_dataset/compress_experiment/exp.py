import h5py
import numpy as np
import zlib
import time
from pathlib import Path
from typing import List, Tuple

# Approach 1: Creating an HDF5 dataset for each protein structure
def compress_and_save_h5_individual(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs_individual_gzip.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_contents):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        for pdb_name, pdb_content in zip(all_res_pdbs, all_contents):
            hf.create_dataset(pdb_name, data=np.frombuffer(pdb_content.encode('utf-8'), dtype=np.uint8), compression="gzip")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time (individual): {total_time}")
    return str(pdbs_file)

def compress_and_save_h5_individual_lzf(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs_individual_lzf.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_contents):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        for pdb_name, pdb_content in zip(all_res_pdbs, all_contents):
            hf.create_dataset(pdb_name, data=np.frombuffer(pdb_content.encode('utf-8'), dtype=np.uint8), compression="lzf")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time (individual): {total_time}")
    return str(pdbs_file)

def compress_and_save_h5_individual_lzf_shuffle(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs_individual_lzf_shuffle.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_contents):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        for pdb_name, pdb_content in zip(all_res_pdbs, all_contents):
            hf.create_dataset(pdb_name, data=np.frombuffer(pdb_content.encode('utf-8'), dtype=np.uint8), compression="lzf", shuffle=True)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time (shuffle_individual): {total_time}")
    return str(pdbs_file)

# Approach 2: Creating an HDF5 dataset for all protein structures
def compress_and_save_h5_combined(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs_combined_gzip.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_contents):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        combined_content = "|".join(all_contents)
        compressed_content = np.frombuffer(combined_content.encode('utf-8'), dtype=np.uint8)
        hf.create_dataset(";".join(all_res_pdbs), data=compressed_content, compression="gzip")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time (combined): {total_time}")
    return str(pdbs_file)

def compress_and_save_h5_combined_lzf(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs_combined_lzf.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_contents):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        combined_content = "|".join(all_contents)
        compressed_content = np.frombuffer(combined_content.encode('utf-8'), dtype=np.uint8)
        hf.create_dataset(";".join(all_res_pdbs), data=compressed_content, compression="lzf")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time (combined): {total_time}")
    return str(pdbs_file)

def compress_and_save_h5_combined_lzf_shuffle(path_for_batch: Path, results: Tuple[List[str], List[str], List[str]]):
    start_time = time.time()
    pdbs_file = path_for_batch / 'pdbs_combined_lzf_shuffle.hdf5'
    all_res_pdbs = results[0]
    all_contents = results[1]
    if len(all_contents) == 0 or len(all_res_pdbs) == 0:
        print("No files to save")
        return None
    if len(all_res_pdbs) != len(all_contents):
        print("Wrong length of names and pdb contents")
        return None
    with h5py.File(pdbs_file, 'w') as hf:
        combined_content = "|".join(all_contents)
        compressed_content = np.frombuffer(combined_content.encode('utf-8'), dtype=np.uint8)
        hf.create_dataset(";".join(all_res_pdbs), data=compressed_content, compression="lzf", shuffle=True)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Compress time (shuffle_combined): {total_time}")
    return str(pdbs_file)

# Approach 3: Compressing data before storing (existing implementation)
