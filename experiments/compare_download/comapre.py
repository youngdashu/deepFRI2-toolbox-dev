import os
import csv
from pathlib import Path
import io
import re


def read_csv_data(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['name'] == 'retrieve_cif' or row['name'] == 'retrieve_binary_cif':
                retrieve_time = float(row['average_time_s'])
            elif row['name'] == 'cif_to_pdbs' or row['name'] == 'binary_cif_to_pdbs':
                conversion_time = float(row['average_time_s'])

            total_time = float(row['total_duration_seconds'])
    return retrieve_time, conversion_time, total_time


def extract_cores(filename):
    match = re.search(r'_(\d+)_', filename)
    return match.group(1) if match else "Unknown"


def compare_directories(text_dir, binary_dir):
    text_files = sorted([f for f in os.listdir(text_dir) if f.endswith('.csv')])
    binary_files = sorted([f for f in os.listdir(binary_dir) if f.endswith('.csv')])

    results = []
    for text_file, binary_file in zip(text_files, binary_files):
        text_path = Path(text_dir) / text_file
        binary_path = Path(binary_dir) / binary_file

        text_retrieve, text_convert, text_total = read_csv_data(text_path)
        binary_retrieve, binary_convert, binary_total = read_csv_data(binary_path)

        cores = extract_cores(text_file)

        retrieve_diff = (binary_retrieve - text_retrieve) / text_retrieve * 100
        convert_diff = (binary_convert - text_convert) / text_convert * 100
        total_diff = (binary_total - text_total) / text_total * 100

        results.append({
            'CPU cores': cores,
            'Text Retrieve (s)': f"{text_retrieve:.6f}",
            'Binary Retrieve (s)': f"{binary_retrieve:.6f}",
            'Retrieve Diff (%)': f"{retrieve_diff:.2f}",
            'Text Convert (s)': f"{text_convert:.6f}",
            'Binary Convert (s)': f"{binary_convert:.6f}",
            'Convert Diff (%)': f"{convert_diff:.2f}",
            'Text Total (s)': f"{text_total:.6f}",
            'Binary Total (s)': f"{binary_total:.6f}",
            'Total Diff (%)': f"{total_diff:.2f}"
        })

    return results


def save_results_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = results[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)


def csv_to_latex(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    latex_table = io.StringIO()
    latex_table.write("\\begin{table}[h]\n")
    latex_table.write("\\centering\n")
    latex_table.write("\\begin{tabular}{" + "l" * len(headers) + "}\n")
    latex_table.write("\\hline\n")
    latex_table.write(" & ".join(headers) + " \\\\\n")
    latex_table.write("\\hline\n")

    for row in rows:
        latex_table.write(" & ".join(row) + " \\\\\n")

    latex_table.write("\\hline\n")
    latex_table.write("\\end{tabular}\n")
    latex_table.write("\\caption{Comparison of Text and Binary Processing}\n")
    latex_table.write("\\label{tab:comparison}\n")
    latex_table.write("\\end{table}\n")

    return latex_table.getvalue()

# Usage
text_directory = '/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download_pdb_nb/results'
binary_directory = '/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download/results'
output_csv = 'comparison_results.csv'
output_latex = 'comparison_results.tex'

results = compare_directories(text_directory, binary_directory)
save_results_to_csv(results, output_csv)
latex_table = csv_to_latex(output_csv)

with open(output_latex, 'w') as f:
    f.write(latex_table)

print(f"Results saved to {output_csv}")
print(f"LaTeX table saved to {output_latex}")