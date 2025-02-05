import os
import shutil
import time
from typing import Iterable, List, Optional, Tuple, Dict
import zipfile
import tarfile
from pathlib import Path

from tqdm import tqdm

from dask.distributed import worker_client
from toolbox.models.manage_dataset.index.handle_index import create_index
from toolbox.models.utils.create_client import total_workers

from toolbox.models.manage_dataset.compute_batches import ComputeBatches
from toolbox.models.manage_dataset.utils import (
    cif_to_pdbs,
    compress_and_save_h5,
    mkdir_for_batches,
)
from toolbox.models.utils.cif2pdb import cif_to_pdb


def extract_archive(
    input_path, structures_dataset: "StructureDataset"
) -> Optional[Path]:

    if not input_path.exists():
        print(f"Error: The provided path {input_path} does not exist.")
        return None

    if is_archive(input_path):
        print(f"Processing archive to extract protein files: {input_path}")
        extracted_path = structures_dataset.dataset_repo_path() / "extracted_files"
        os.makedirs(extracted_path, exist_ok=True)

        if zipfile.is_zipfile(input_path):
            print("Extracting zip file")
            with zipfile.ZipFile(input_path, "r") as zip_ref:
                zip_ref.extractall(extracted_path)
        elif tarfile.is_tarfile(input_path):
            print("Extracting tar/tar.gz file")
            with tarfile.open(
                input_path, "r:*"
            ) as tar_ref:  # 'r:*' auto-detects compression
                tar_ref.extractall(extracted_path)
        else:
            print("Provided path is neither a directory nor a supported archive.")
            return None

        return extracted_path

    return input_path  # Return original path if it's not an archive


def is_archive(path):
    if os.path.isdir(path):
        return False
    # Check if the file is a zip or tar/tar.gz archive
    return zipfile.is_zipfile(path) or tarfile.is_tarfile(path)


def save_extracted_files(
    structures_dataset: "StructuresDataset",
    extracted_path,
    ids: Optional[List[str]] = None,
):

    Path(structures_dataset.structures_path()).mkdir(exist_ok=True, parents=True)
    pdb_repo_path = structures_dataset.structures_path()

    files = list(extracted_path.glob("*.pdb")) + list(extracted_path.glob("*.cif"))
    print("extracted files from directory:", len(files))

    files_name_to_dir = {
        file.name.replace(".pdb", "").replace(".cif", ""): str(file) for file in files
    }
    present_files_set = set(files_name_to_dir.keys())

    ids_set = set(ids)

    if ids is None:
        files = list(files_name_to_dir.values())
        chunks = list(structures_dataset.chunk(files))
    else:
        print(f"Searching for requested files {len(ids)} in extracted files {len(files)}")

        wanted_files = present_files_set & ids_set
        wanted_files = [files_name_to_dir[file] for file in wanted_files] 
        missing_files = list(ids_set - present_files_set)

        print(f"\tFound {len(wanted_files)}, missing {len(missing_files)} out of {len(ids)} requested files")
        with open(structures_dataset.dataset_path() / "missing_ids_files.txt", "w") as f:
            for file in missing_files:
                f.write(file + "\n")
            print(f"\t\tMissing files saved to {structures_dataset.dataset_path() / 'missing_ids_files.txt'}")
        ids = wanted_files

    chunks = list(structures_dataset.chunk(ids))

    mkdir_for_batches(pdb_repo_path, len(chunks))

    new_files_index = {}

    def run(input_data, machine):
        return structures_dataset._client.submit(
            retrieve_protein_file_to_h5, *input_data, [machine], workers=[machine]
        )

    def collect(result):
        downloaded_pdbs, file_path = result
        print("Updating new_files_index", len(downloaded_pdbs))
        new_files_index.update({k: file_path for k in downloaded_pdbs})

    compute_batches = ComputeBatches(
        structures_dataset._client, run, collect, "pdb_extracted_from_archive"
    )

    inputs = ((pdb_repo_path / f"{i}", ids_chunk) for i, ids_chunk in enumerate(chunks))

    factor = 10
    factor = 15 if total_workers() > 1500 else factor
    factor = 20 if total_workers() > 2000 else factor
    compute_batches.compute(inputs, factor=factor)

    print("Adding new files to index")

    try:
        create_index(structures_dataset.dataset_index_file_path(), new_files_index)
    except Exception as e:
        print("Failed to update index")
        print(e)


def retrieve_protein_file_to_h5(
    path_for_batch: Path, pdb_ids: Iterable[str], workers: List[str] = None
) -> Tuple[List[str], str]:
    with worker_client() as client:
        start_time = time.time()

        pdb_futures = client.map(retrieve_single_file, pdb_ids, workers=workers)
        converted_pdb_futures = client.map(file_to_pdb, pdb_futures, workers=workers)
        download_start_time = time.time()
        aggregated = client.submit(
            aggregate_results,
            converted_pdb_futures,
            download_start_time,
            workers=workers,
        )

        # Create delayed tasks for H5 and ZIP creation
        h5_task = client.submit(
            compress_and_save_h5,
            path_for_batch,
            aggregated,
            pure=False,
            workers=workers,
        )
        get_ids_task = client.submit(
            lambda results: results[0], aggregated, workers=workers
        )

        # Compute the tasks
        pdb_ids, h5_file_path = client.gather([get_ids_task, h5_task])

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Total processing time {path_for_batch.stem}: {total_time}")

        return pdb_ids, h5_file_path


def retrieve_single_file(file_path):
    file_path = Path(file_path)
    file_name = file_path.stem
    file_extension = file_path.suffix
    with open(file_path, "r") as file:
        return file.read(), file_name, file_extension


def file_to_pdb(input_data):
    file_data, file_name, file_extension = input_data
    if file_extension == ".cif":
        return cif_to_pdb(file_data, file_name)
    elif file_extension == ".pdb":
        return {f"{file_name}.pdb": str(file_data)}
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")


def aggregate_results(
    protein_pdbs_with_cif: List[Dict[str, str]], download_start_time: float
) -> Tuple[List[str], List[str]]:
    end_time = time.time()

    print(f"Download time: {end_time - download_start_time}")

    all_res_pdbs = []
    all_contents = []

    for prot in protein_pdbs_with_cif:
        all_res_pdbs.extend(prot.keys())
        all_contents.extend(prot.values())

    return all_res_pdbs, all_contents
