from pathlib import Path
from sys import argv
import os
import numpy as np
import h5py
from matplotlib import pyplot as plt
import dask
from dask.distributed import Client, progress
from toolbox.models.utils.create_client import create_client
from toolbox.models.manage_dataset.index.handle_index import read_index

MAX_COUNT = 100


def process_distogram_file(file_path, keys_to_get):
    histograms_dict = {}
    with h5py.File(file_path, "r") as f:
        for key in keys_to_get:
            distogram = f[key]["distogram"][:]
            # Extract upper triangle (excluding diagonal)
            upper_triangle = np.triu(distogram, k=1)
            # Convert to 1D array
            upper_triangle_1d = upper_triangle[
                np.triu_indices_from(upper_triangle, k=1)
            ]
            # Calculate histogram
            hist, bin_edges = np.histogram(upper_triangle_1d, bins=100)
            histograms_dict[key] = {
                "hist": hist,
                "bin_edges": bin_edges,
                "data": upper_triangle_1d,
                "max_distance": np.max(upper_triangle_1d),  # Store the maximum distance
            }
    return histograms_dict


def create_single_data(hist_data, i):
    hist = hist_data["hist"]
    bin_edges = hist_data["bin_edges"]
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    # Create the 3D bar plot
    dx = dy = (bin_edges[1] - bin_edges[0]) * 0.8  # Adjust bar width
    xpos, ypos = np.meshgrid(bin_centers, [i])
    xpos = xpos.flatten()
    ypos = ypos.flatten()
    zpos = np.zeros_like(xpos)
    dz = hist
    return xpos, ypos, zpos, dx, dy, dz


def process_file(file_path, start_index, keys_to_get):
    histograms_dict = process_distogram_file(file_path, keys_to_get)
    return [
        (
            key,
            create_single_data(hist_data, start_index + i),
            hist_data["data"],
            hist_data["max_distance"],
        )
        for i, (key, hist_data) in enumerate(histograms_dict.items())
    ]


def load_data(distograms_index_file, data_path):
    client = create_client(False)  # Start a Dask client

    index = read_index(Path(distograms_index_file), data_path)
    print("Found {} files".format(len(index)))

    # Count total number of keys across all files
    total_keys = 0
    keys_per_file = []
    for file_h5, pdbs in index.items():
        num_keys = len(pdbs)
        keys_per_file.append(pdbs)
        total_keys += num_keys
        print(num_keys)
        if MAX_COUNT is not None and total_keys > MAX_COUNT:
            exceeded_keys = total_keys - MAX_COUNT

            new_last_keys = keys_per_file[-1][:exceeded_keys]

            if len(new_last_keys) == 0:
                del keys_per_file[-1]
            else:
                keys_per_file[-1] = new_last_keys

            break
    if MAX_COUNT is not None:
        total_keys = min(MAX_COUNT, total_keys)
    print("Total keys:", total_keys)

    # Create Dask futures for parallel processing
    futures = []
    start_index = 0
    for file, keys in zip(index.keys(), keys_per_file):
        print("Appending delayed")
        futures.append(dask.delayed(process_file)(file, start_index, keys))
        start_index += len(keys)

    # Compute the results
    results = dask.compute(*futures)

    # Flatten the results
    inputs = [item for sublist in results for item in sublist]
    print("Get inputs", len(inputs))

    inputs = inputs[:MAX_COUNT]

    # Sort inputs by key
    inputs.sort(key=lambda x: x[3])

    client.close()  # Close the Dask client

    return inputs, total_keys


def generate_histogram(inputs):
    # Create the 3D plot
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection="3d")
    for i, (_, data, _, _) in enumerate(inputs):
        print(i)
        xpos, ypos, zpos, dx, dy, dz = data
        ypos = np.full_like(ypos, i)  # Update y-position based on sorted order
        ax.bar3d(xpos, ypos, zpos, dx, dy, dz, shade=True, alpha=0.8)

    # Customize the plot
    ax.set_xlabel("Distance")
    ax.set_ylabel("Protein Index (sorted by max distance)")
    ax.set_zlabel("Frequency")
    ax.set_title(
        f"3D Visualization of {len(inputs)} Distograms (Sorted by Max Distance)"
    )

    plt.savefig("3d_distogram_visualization_sorted.pdf")
    plt.close()
    print("Histogram saved as 3d_distogram_visualization_sorted.pdf")


def create_boxplot_data(all_data, num_bins=10):
    all_data_flat = np.concatenate(all_data)
    min_val = np.min(all_data_flat)
    max_val = np.max(all_data_flat)

    bin_edges = np.linspace(min_val, max_val, num_bins + 1)
    binned_data = [[] for _ in range(num_bins)]

    for data in all_data:
        for i, (low, high) in enumerate(zip(bin_edges[:-1], bin_edges[1:])):
            binned_data[i].extend(data[(data >= low) & (data < high)])

    return binned_data, bin_edges


def generate_boxplot(inputs):
    # Create boxplot
    all_data = [data for _, _, data in inputs]
    binned_data, bin_edges = create_boxplot_data(all_data)

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.boxplot(
        binned_data,
        positions=bin_edges[:-1],
        widths=(bin_edges[1] - bin_edges[0]) * 0.8,
    )
    ax.set_xlabel("Distance")
    ax.set_ylabel("Distribution")
    ax.set_title(
        f"Boxplot Visualization of Distances (Range: {bin_edges[0]:.2f} to {bin_edges[-1]:.2f})"
    )
    ax.set_xticks(bin_edges)
    ax.set_xticklabels([f"{edge:.2f}" for edge in bin_edges])
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig("boxplot_distogram_visualization.pdf")
    plt.close()

    print(f"Boxplot saved as boxplot_distogram_visualization.pdf")
    print(f"Data range: {bin_edges[0]:.2f} to {bin_edges[-1]:.2f}")


def main(distograms_index_file, generate_hist=True, generate_box=True):
    inputs, total_keys = load_data(distograms_index_file)

    if generate_hist:
        generate_histogram(inputs)

    if generate_box:
        generate_boxplot(inputs)


if __name__ == "__main__":
    if len(argv) < 2:
        print("Usage: python script.py <distograms_index_file> [histogram] [boxplot]")
        print("Options:")
        print("  histogram: Generate 3D histogram (default: yes)")
        print("  boxplot: Generate boxplot (default: yes)")
        exit(1)

    distograms_index_file = argv[1]
    generate_hist = argv[2].lower() == "histogram"
    generate_box = argv[3].lower() == "boxplot"

    main(distograms_index_file, generate_hist, generate_box)
