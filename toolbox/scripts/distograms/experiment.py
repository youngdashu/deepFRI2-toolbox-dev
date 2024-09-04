from sys import argv
import os

import numpy as np
import h5py
from matplotlib import pyplot as plt
import dask
from dask.distributed import Client, progress

from toolbox.models.utils.create_client import create_client


def process_distogram_file(file_path):
    histograms_dict = {}
    with h5py.File(file_path, 'r') as f:
        for key in f.keys():
            distogram = f[key][:]
            # Extract upper triangle (excluding diagonal)
            upper_triangle = np.triu(distogram, k=1)

            # Convert to 1D array
            upper_triangle_1d = upper_triangle[np.triu_indices_from(upper_triangle, k=1)]

            # Calculate histogram
            hist, bin_edges = np.histogram(upper_triangle_1d, bins=100)
            histograms_dict[key] = {'hist': hist, 'bin_edges': bin_edges}

    return histograms_dict


def create_single_data(hist_data, i):
    hist = hist_data['hist']
    bin_edges = hist_data['bin_edges']
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    # Create the 3D bar plot
    dx = dy = (bin_edges[1] - bin_edges[0]) * 0.8  # Adjust bar width
    xpos, ypos = np.meshgrid(bin_centers, [i])
    xpos = xpos.flatten()
    ypos = ypos.flatten()
    zpos = np.zeros_like(xpos)
    dz = hist

    return xpos, ypos, zpos, dx, dy, dz


def process_file(file_path, start_index):
    histograms_dict = process_distogram_file(file_path)
    return [(key, create_single_data(hist_data, start_index + i))
            for i, (key, hist_data) in enumerate(histograms_dict.items())]


def histograms(path_with_files):
    client = create_client(True)  # Start a Dask client

    # Get all HDF5 files in the directory
    hdf5_files = [os.path.join(path_with_files, f) for f in os.listdir(path_with_files) if f.endswith('.hdf5')]

    # Count total number of keys across all files
    total_keys = 0
    keys_per_file = []
    for file in hdf5_files:
        with h5py.File(file, 'r') as f:
            num_keys = len(f.keys())
            keys_per_file.append(num_keys)
            total_keys += num_keys

    # Create Dask futures for parallel processing
    futures = []
    start_index = 0
    for file, num_keys in zip(hdf5_files, keys_per_file):
        futures.append(dask.delayed(process_file)(file, start_index))
        start_index += num_keys

    # Compute the results
    results = dask.compute(*futures)

    # Flatten the results
    inputs = [item for sublist in results for item in sublist]

    # Sort inputs by key
    inputs.sort(key=lambda x: x[0])

    # Create the 3D plot
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')

    for _, data in inputs:
        ax.bar3d(*data, shade=True, alpha=0.8)

    # Customize the plot
    ax.set_xlabel('Distance')
    ax.set_ylabel('Distogram Index')
    ax.set_zlabel('Frequency')
    ax.set_title(f'3D Visualization of {total_keys} Distograms')

    plt.savefig("3d_distogram_visualization.png")
    plt.show()

    client.close()  # Close the Dask client


if __name__ == '__main__':
    histograms(argv[1])