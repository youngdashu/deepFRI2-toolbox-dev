import pickle
import matplotlib.pyplot as plt
import numpy as np


def plot_first_histogram():
    # Load the histograms from the pickle file
    with open('histograms.pkl', 'rb') as f:
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

if __name__ == "__main__":
    plot_first_histogram()