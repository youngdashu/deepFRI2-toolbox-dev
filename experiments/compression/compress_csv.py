import re
from collections import defaultdict
import csv

def parse_compression_data(data):
    compression_info = defaultdict(lambda: defaultdict(list))
    current_method = ""

    for line in data.split('\n'):
        line = line.strip()
        if line.startswith(('individual', 'combined')):
            current_method = line
        elif "Compress time" in line:
            time_match = re.search(r'Compress time.*?: ([\d.]+)', line)
            if time_match:
                time = float(time_match.group(1))
                compression_info[current_method]['time'].append(time)
        elif '.hdf5' in line:
            parts = line.split()
            if len(parts) >= 2:
                try:
                    size = float(parts[-1])
                    compression_info[current_method]['size'].append(size)
                except ValueError:
                    print(f"Warning: Could not parse size from line: {line}")

    return compression_info

def calculate_averages(compression_info):
    averages = {}
    for method, data in compression_info.items():
        avg_time = sum(data['time']) / len(data['time']) if data['time'] else 0
        avg_size = sum(data['size']) / len(data['size']) if data['size'] else 0
        averages[method] = {'avg_time': avg_time, 'avg_size': avg_size}
    return averages

def write_csv(averages, filename='compression_results.csv'):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Compression Method', 'Average Time (s)', 'Average Size (MB)'])
        for method, data in averages.items():
            writer.writerow([method, f"{data['avg_time']:.2f}", f"{data['avg_size']:.2f}"])

# Main execution
data = """
individual gzip
Compress time (individual): 8.960439920425415
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs_individual_gzip.hdf5 175.14
individual lzf
Compress time (individual): 2.7194790840148926
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs_individual_lzf.hdf5 263.8
individual lzf shuffle
Compress time (shuffle_individual): 2.708061695098877
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs_individual_lzf_shuffle.hdf5 263.8
combined gzip
Compress time (combined): 8.428448915481567
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs_combined_gzip.hdf5 158.46
combined lzf
Compress time (combined): 1.545198917388916
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs_combined_lzf.hdf5 240.88
combined lzf shuffle
Compress time (shuffle_combined): 1.5805041790008545
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs_combined_lzf_shuffle.hdf5 240.88
combined zlib
Compress time: 26.11584734916687
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/8/pdbs.hdf5 145.9
Download time: 54.976277589797974
individual gzip
Compress time (individual): 10.46397614479065
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs_individual_gzip.hdf5 192.46
individual lzf
Compress time (individual): 3.113642930984497
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs_individual_lzf.hdf5 290.4
individual lzf shuffle
Compress time (shuffle_individual): 3.0937726497650146
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs_individual_lzf_shuffle.hdf5 290.4
combined gzip
Compress time (combined): 9.483084440231323
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs_combined_gzip.hdf5 173.85
combined lzf
Compress time (combined): 1.8779964447021484
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs_combined_lzf.hdf5 264.75
combined lzf shuffle
Compress time (shuffle_combined): 1.904184341430664
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs_combined_lzf_shuffle.hdf5 264.75
combined zlib
Compress time: 28.72207021713257
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/6/pdbs.hdf5 160.13
Retrying downloading 1n50 1
Retrying downloading 3n25 1
Download time: 155.74999403953552
individual gzip
Compress time (individual): 8.921097993850708
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs_individual_gzip.hdf5 168.3
individual lzf
Compress time (individual): 2.7093045711517334
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs_individual_lzf.hdf5 253.92
individual lzf shuffle
Compress time (shuffle_individual): 2.7249491214752197
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs_individual_lzf_shuffle.hdf5 253.92
combined gzip
Compress time (combined): 8.13132381439209
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs_combined_gzip.hdf5 152.31
combined lzf
Compress time (combined): 1.4809176921844482
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs_combined_lzf.hdf5 231.85
combined lzf shuffle
Compress time (shuffle_combined): 1.4856204986572266
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs_combined_lzf_shuffle.hdf5 231.85
combined zlib
Compress time: 24.98205327987671
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/9/pdbs.hdf5 140.05
Download time: 179.54293203353882
individual gzip
Compress time (individual): 9.52885913848877
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs_individual_gzip.hdf5 183.71
individual lzf
Compress time (individual): 2.8948311805725098
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs_individual_lzf.hdf5 277.01
individual lzf shuffle
Compress time (shuffle_individual): 2.916029691696167
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs_individual_lzf_shuffle.hdf5 277.01
combined gzip
Compress time (combined): 9.085760116577148
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs_combined_gzip.hdf5 166.45
combined lzf
Compress time (combined): 1.7873072624206543
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs_combined_lzf.hdf5 253.25
combined lzf shuffle
Compress time (shuffle_combined): 1.79245924949646
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs_combined_lzf_shuffle.hdf5 253.25
combined zlib
Compress time: 27.33644127845764
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/7/pdbs.hdf5 153.35
Retrying downloading 7lcs 1
Download time: 176.0704219341278
individual gzip
Compress time (individual): 8.679517269134521
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs_individual_gzip.hdf5 168.67
individual lzf
Compress time (individual): 2.6313188076019287
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs_individual_lzf.hdf5 254.29
individual lzf shuffle
Compress time (shuffle_individual): 2.649430513381958
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs_individual_lzf_shuffle.hdf5 254.29
combined gzip
Compress time (combined): 8.253647089004517
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs_combined_gzip.hdf5 152.12
combined lzf
Compress time (combined): 1.6141126155853271
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs_combined_lzf.hdf5 231.73
combined lzf shuffle
Compress time (shuffle_combined): 1.6149871349334717
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs_combined_lzf_shuffle.hdf5 231.73
combined zlib
Compress time: 25.073856592178345
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/0/pdbs.hdf5 139.86
Retrying downloading 2lcp 1
Download time: 172.5204393863678
individual gzip
Compress time (individual): 9.471012115478516
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs_individual_gzip.hdf5 177.06
individual lzf
Compress time (individual): 2.777259588241577
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs_individual_lzf.hdf5 267.26
individual lzf shuffle
Compress time (shuffle_individual): 2.718510150909424
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs_individual_lzf_shuffle.hdf5 267.26
combined gzip
Compress time (combined): 8.694792032241821
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs_combined_gzip.hdf5 160.34
combined lzf
Compress time (combined): 1.6806697845458984
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs_combined_lzf.hdf5 244.43
combined lzf shuffle
Compress time (shuffle_combined): 1.697434663772583
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs_combined_lzf_shuffle.hdf5 244.43
combined zlib
Compress time: 26.56005311012268
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/1/pdbs.hdf5 147.62
Total processing time 6: 114.06452345848083
Retrying downloading 1lmk 1
Retrying downloading 8pb5 1
Total processing time 4: 237.8919596672058
Total processing time 2: 215.83751487731934
Download time: 163.4844193458557
individual gzip
Compress time (individual): 9.176882982254028
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs_individual_gzip.hdf5 173.18
individual lzf
Compress time (individual): 2.729750633239746
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs_individual_lzf.hdf5 261.2
individual lzf shuffle
Compress time (shuffle_individual): 2.7659482955932617
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs_individual_lzf_shuffle.hdf5 261.2
combined gzip
Compress time (combined): 8.476070404052734
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs_combined_gzip.hdf5 156.95
combined lzf
Compress time (combined): 1.5406594276428223
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs_combined_lzf.hdf5 239.02
combined lzf shuffle
Compress time (shuffle_combined): 1.5622673034667969
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs_combined_lzf_shuffle.hdf5 239.02
combined zlib
Compress time: 25.9271342754364
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/2/pdbs.hdf5 144.47
Download time: 183.9232165813446
individual gzip
Compress time (individual): 9.457458734512329
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs_individual_gzip.hdf5 174.62
individual lzf
Compress time (individual): 2.867518901824951
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs_individual_lzf.hdf5 263.08
individual lzf shuffle
Compress time (shuffle_individual): 2.8551931381225586
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs_individual_lzf_shuffle.hdf5 263.08
combined gzip
Compress time (combined): 8.59383487701416
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs_combined_gzip.hdf5 157.31
combined lzf
Compress time (combined): 1.7046394348144531
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs_combined_lzf.hdf5 239.54
combined lzf shuffle
Compress time (shuffle_combined): 1.5941860675811768
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs_combined_lzf_shuffle.hdf5 239.54
combined zlib
Compress time: 26.08711886405945
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/4/pdbs.hdf5 144.65
Retrying downloading 7pyg 1
Download time: 54.93039345741272
individual gzip
Compress time (individual): 9.85055661201477
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs_individual_gzip.hdf5 186.86
individual lzf
Compress time (individual): 3.0021700859069824
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs_individual_lzf.hdf5 281.57
individual lzf shuffle
Compress time (shuffle_individual): 3.043093204498291
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs_individual_lzf_shuffle.hdf5 281.57
combined gzip
Compress time (combined): 9.187070369720459
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs_combined_gzip.hdf5 168.87
combined lzf
Compress time (combined): 1.7993779182434082
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs_combined_lzf.hdf5 256.83
combined lzf shuffle
Compress time (shuffle_combined): 1.808286190032959
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs_combined_lzf_shuffle.hdf5 256.83
combined zlib
Compress time: 28.313949584960938
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/5/pdbs.hdf5 155.45
Download time: 54.57556438446045
individual gzip
Compress time (individual): 9.232254266738892
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs_individual_gzip.hdf5 168.56
individual lzf
Compress time (individual): 2.65150785446167
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs_individual_lzf.hdf5 254.3
individual lzf shuffle
Compress time (shuffle_individual): 2.6797468662261963
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs_individual_lzf_shuffle.hdf5 254.3
combined gzip
Compress time (combined): 8.357633590698242
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs_combined_gzip.hdf5 153.45
combined lzf
Compress time (combined): 1.6917576789855957
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs_combined_lzf.hdf5 233.26
combined lzf shuffle
Compress time (shuffle_combined): 1.6612491607666016
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs_combined_lzf_shuffle.hdf5 233.26
combined zlib
Compress time: 25.18907904624939
/net/storage/pr3/plgrid/plggsano/tomaszlab/deepfri/dev_data/repo/PDB/all_/20240905_0042/structures/3/pdbs.hdf5 141.14
"""

compression_info = parse_compression_data(data)
print("Parsed compression info:")
for method, data in compression_info.items():
    print(f"{method}:")
    print(f"  Time data: {data['time']}")
    print(f"  Size data: {data['size']}")
    print(f"  Number of time entries: {len(data['time'])}")
    print(f"  Number of size entries: {len(data['size'])}")
    print()

averages = calculate_averages(compression_info)
write_csv(averages)

print("CSV file 'compression_results.csv' has been created with the average compression results.")