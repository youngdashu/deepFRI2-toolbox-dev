import matplotlib.pyplot as plt
import numpy as np

# Data
categories = {
    "Experimental Method": {
        "X-RAY DIFFRACTION": 187265,
        "ELECTRON MICROSCOPY": 21723,
        "SOLUTION NMR": 14229,
        "Other": 561 + 238  # Sum of the remaining methods
    },
    "Refinement Resolution (Å)": {
        # "< 1.0": 1057,
        "< 1.5": 19400 + 1057,
        "1.5 - 2.0": 64375,
        "2.0 - 2.5": 57899,
        "2.5 - 3.0": 35447,
        "3.0 - 3.5": 17694,
        "3.5 - 4.0": 7169,
        "> 4.0": 2585 + 3775,
        # "> 4.5": 3775
    },
    "Release Date": {
        # "earlier": 190 + 122 + 53,
        "earlier": 190 + 122 + 53 + 2506,
        "1995 - 1999": 8088,
        "2000 - 2004": 17726,
        "2005 - 2009": 33064,
        "2010 - 2014": 43311,
        "2015 - 2019": 53737,
        "2020 - 2024": 64993
    },
    "Polymer Entity Type": {
        "Protein": 219181,
        "DNA": 11244,
        "RNA": 7970,
        "Other": 277 + 8
    }
}

def fix_labels(mylabels, tooclose=0.1, sepfactor=2):
    vecs = np.zeros((len(mylabels), len(mylabels), 2))
    dists = np.zeros((len(mylabels), len(mylabels)))
    for i in range(0, len(mylabels)-1):
        for j in range(i+1, len(mylabels)):
            a = np.array(mylabels[i].get_position())
            b = np.array(mylabels[j].get_position())
            dists[i,j] = np.linalg.norm(a-b)
            vecs[i,j,:] = a-b
            if dists[i,j] < tooclose:
                mylabels[i].set_x(a[0] + sepfactor*vecs[i,j,0])
                mylabels[i].set_y(a[1] + sepfactor*vecs[i,j,1])
                mylabels[j].set_x(b[0] - sepfactor*vecs[i,j,0])
                mylabels[j].set_y(b[1] - sepfactor*vecs[i,j,1])

# Create subplots
fig, axs = plt.subplots(2, 2, figsize=(20, 20))

# Flatten the axs array for easier iteration
axs = axs.flatten()

# Colors for the pie charts
color_maps = [plt.cm.Set3, plt.cm.Set1, plt.cm.Set2, plt.cm.Pastel1]

# Create visualizations for each category
for i, (category, data) in enumerate(categories.items()):
    ax = axs[i]
    labels = list(data.keys())
    sizes = list(data.values())

    colors = color_maps[i](np.linspace(0, 1, len(labels)))

    ind = np.arange(len(labels))  # the x locations for the groups
    ax.bar(ind, sizes, color=colors)
    ax.set_xticks(ind)
    ax.set_xticklabels(labels, fontsize=16, rotation=30)  # Adjust rotation here
    ax.set_title(category, fontsize=18)

    # Set both ticks and labels to be fontsize 14
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(14)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(14)

# plt.suptitle('RSCB PDB structures metadata', fontsize=24)
plt.tight_layout()

# Save the figure as SVG
plt.savefig('pdb_structures_metadata.pdf', dpi=1200, bbox_inches='tight')

print("The visualization has been saved as 'data_visualization.svg'")

plt.show()

