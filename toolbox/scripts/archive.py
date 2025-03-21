from toolbox.models.manage_dataset.index.handle_index import read_index
import datetime
import os
import zipfile
from pathlib import Path
from toolbox.models.manage_dataset.utils import read_all_pdbs_from_h5


def process_h5_file(h5_file, dataset_path, output_dir):
    full_h5_file_path = Path(dataset_path) / h5_file
    prots = read_all_pdbs_from_h5(full_h5_file_path)
    archive_name = os.path.basename(h5_file).replace(".h5", ".zip")
    archive_path = Path(output_dir) / archive_name

    with zipfile.ZipFile(archive_path, "w") as zipf:
        for p, pdb_file_content in prots.items():
            code = p.removesuffix(".pdb")
            zipf.writestr(f"{code}.pdb", pdb_file_content)

            with open(archive_path / f"{code}.pdb", "w") as f:
                f.write(pdb_file_content)

    os.system(f"tar -czf {str(archive_path)}.tgz {str(archive_path)}")

    return str(archive_path)


def create_archive(structures_dataset: "StructuresDataset"):
    dataset_path = structures_dataset.dataset_path()
    proteins_index = read_index(Path(dataset_path) / "dataset_reversed.idx")
    output_dir = Path(dataset_path) / "archives"
    output_dir.mkdir(exist_ok=True)

    client = structures_dataset._client

    futures = []
    for h5_file in proteins_index.keys():
        future = client.submit(process_h5_file, h5_file, dataset_path, output_dir)
        futures.append(future)

    archive_paths = client.gather(futures)

    # Combine the archives into one archive
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    final_archive_name = f"archive_pdb_{current_time}.zip"
    final_archive_path = Path.cwd() / final_archive_name

    with zipfile.ZipFile(final_archive_path, "w") as final_zip:
        for idx, archive_path in enumerate(archive_paths):
            archive_name_in_final = f"{idx}.zip"
            # Read the archive file and write it into the final archive
            with open(archive_path, "rb") as f:
                archive_data = f.read()
                final_zip.writestr(archive_name_in_final, archive_data)
