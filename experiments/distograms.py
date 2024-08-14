import pickle

from typing import Optional

import matplotlib.pyplot as plt
import numpy as np


def plot_first_histogram(histograms_path: str):
    # Load the histograms from the pickle file
    with open(histograms_path, 'rb') as f:
        histograms_dict = pickle.load(f)

    # Get the first key in the dictionary
    first_key = next(iter(histograms_dict))

    # Extract the histogram data for the first distogram
    hist_data = histograms_dict[first_key]
    hist = hist_data['hist']
    bin_edges = hist_data['bin_edges']

    # Plot the histogram
    plt.figure(figsize=(10, 6))
    plt.bar(bin_edges[:-1], hist, width=np.diff(bin_edges), align="edge")
    plt.title(f"Histogram of Distogram: {first_key}")
    plt.xlabel("Distance")
    plt.ylabel("Frequency")
    plt.savefig("first_distogram_histogram.png")
    plt.show()


def visualize_distograms_3d(histograms_path, num_distograms: Optional[int] = 5):
    # Load the histograms from the pickle file
    with open(histograms_path, 'rb') as f:
        histograms_dict = pickle.load(f)

    # Select a subset of distograms
    keys = list(histograms_dict.keys())[:num_distograms] if num_distograms is not None else list(histograms_dict.keys())

    print(len(keys))

    exit(0)

    # Create the 3D plot
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')

    # Plot each histogram
    for i, key in enumerate(keys):
        hist_data = histograms_dict[key]
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

        ax.bar3d(xpos, ypos, zpos, dx, dy, dz, shade=True, alpha=0.8)

    # Customize the plot
    ax.set_xlabel('Distance')
    ax.set_ylabel('Distogram Index')
    ax.set_zlabel('Frequency')
    ax.set_title(f'3D Visualization of {len(keys)} Distograms')

    # Set y-axis ticks to distogram indices
    ax.set_yticks(range(len(keys)))
    ax.set_yticklabels(range(1, len(keys) + 1))

    plt.savefig("3d_distogram_visualization.png")
    plt.show()


if __name__ == "__main__":
    # plot_first_histogram('../distograms_hist_exp/histograms.pkl')
    visualize_distograms_3d('../distograms_hist_exp/histograms.pkl', None)
