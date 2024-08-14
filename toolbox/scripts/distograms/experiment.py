import pickle
from pathlib import Path
from sys import argv
from typing import Dict

import numpy as np

from toolbox.models.manage_dataset.distograms.generate_distograms import read_distograms_from_file


def histograms(distograms_h5_file):
    distograms: Dict[str, np.ndarray] = read_distograms_from_file(distograms_h5_file, 10_000)

    upper_triangles_1d = {}
    histograms_dict = {}
    for key, distogram in distograms.items():
        # Extract upper triangle (excluding diagonal)
        upper_triangle = np.triu(distogram, k=1)

        # Convert to 1D array
        upper_triangle_1d = upper_triangle[np.triu_indices_from(upper_triangle, k=1)]

        upper_triangles_1d[key] = upper_triangle_1d

        # Calculate histogram
        hist, bin_edges = np.histogram(upper_triangle_1d, bins=100)
        histograms_dict[key] = {'hist': hist, 'bin_edges': bin_edges}

    Path('./distograms_hist_exp').mkdir(exist_ok=True, parents=True)

    with open('./distograms_hist_exp/distances.pkl', 'wb') as f:
        pickle.dump(upper_triangles_1d, f)

    with open('./distograms_hist_exp/histograms.pkl', 'wb') as f:
        pickle.dump(histograms_dict, f)

if __name__ == '__main__':
    histograms(argv[1])