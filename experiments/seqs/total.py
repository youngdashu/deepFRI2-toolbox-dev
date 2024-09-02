from experiments.plot_from_htmls import plot_from_htmls


if __name__ == "__main__":
    html_files_dir = "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/seqs/reports_seq"  # replace with actual path
    task_names = [
        "get_sequences_from_batch",

    ]
    plot_from_htmls(html_files_dir=html_files_dir, task_names=task_names)
