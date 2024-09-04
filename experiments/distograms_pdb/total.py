from experiments.plot_from_htmls import plot_from_htmls

if __name__ == "__main__":
    html_files_dir = "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/distograms_pdb/reports"  # replace with actual path
    task_names = [
        "process_pdbs",
        "collect_parallel"
    ]
    plot_from_htmls(
        html_files_dir, task_names
    )
