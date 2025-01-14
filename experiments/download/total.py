from experiments.plot_from_htmls import plot_from_htmls

if __name__ == "__main__":
    html_files_dir = "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download/reports"  # replace with actual path
    task_names = [
        "retrieve_binary_cif",
        "binary_cif_to_pdbs",
        "aggregate_results",
        # "create_pdb_zip_archive",
        "compress_and_save_h5",
        "retrieve_pdb_chunk_to_h5",
    ]
    plot_from_htmls(html_files_dir=html_files_dir, task_names=task_names)
