import matplotlib.pyplot as plt
import numpy as np

# Font size variables
SMALL_FONT = 21
MEDIUM_FONT = 23
LARGE_FONT = 24
TITLE_FONT = 25

raw_size = 1224.63

# Data with added numbers
data = [
    (1, "gzip, 1, False", 17.84, 969.85),
    (2, "gzip, 4, False", 26.62, 966.50),
    (3, "gzip, 9, False", 28.75, 965.61),
    (4, "szip, None, False", 4.84, 756.35),
    (5, "lzf, None, False", 4.93, 1221.79),
    (6, "gzip, 1, True", 14.45, 830.28),
    (7, "gzip, 4, True", 20.86, 797.66),
    (8, "gzip, 9, True", 110.96, 781.90),
    (9, "szip, None, True", 5.47, 1049.11),
    (10, "lzf, None, True", 5.77, 923.48),
]

# Sort data by time
sorted_data = sorted(data, key=lambda x: x[2])

# Extract sorted values
numbers = [item[0] for item in sorted_data]
times = [item[2] for item in sorted_data]
sizes = [item[3] for item in sorted_data]

# Set the default font size
plt.rcParams.update({"font.size": SMALL_FONT})

# Create the plot
fig, ax1 = plt.subplots(figsize=(16, 8))

# Plot time as a line with dots
line = ax1.plot(
    range(len(data)), times, "bo-", linewidth=2, markersize=8, label="Time (s)"
)[0]
ax1.set_xlabel("Compression Options (original numbering)", fontsize=MEDIUM_FONT)
ax1.set_ylabel("Time (s)", color="b", fontsize=MEDIUM_FONT)
ax1.tick_params(axis="y", labelcolor="b", labelsize=SMALL_FONT)

# Create a second y-axis for size
ax2 = ax1.twinx()
bars = ax2.bar(range(len(data)), sizes, alpha=0.3, color="r", label="Size (MB)")
ax2.set_ylabel("Size (MB)", color="r", fontsize=MEDIUM_FONT)
ax2.tick_params(axis="y", labelcolor="r", labelsize=SMALL_FONT)

ax2.axhline(y=raw_size, color="g", linestyle="-", alpha=0.3, label="Raw Size (MB)")

# Set x-axis ticks and labels
plt.xticks(range(len(data)), numbers, fontsize=SMALL_FONT)

# Add a title
plt.title("HDF5 Compression: Time vs Size (Sorted by Time)", fontsize=TITLE_FONT)

# Add legend
lines = [line, bars]
labels = [l.get_label() for l in lines]

# Adjust layout
plt.tight_layout()

# Save the plot as a PDF file
plt.savefig("hdf5_compression_plot.pdf", format="pdf", bbox_inches="tight")

print("Plot saved as 'hdf5_compression_plot.pdf'")

# If you also want to display the plot, uncomment the following line:
plt.show()
