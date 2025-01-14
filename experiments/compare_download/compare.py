import os
import csv
import statistics
from pathlib import Path
import io
import re


def read_csv_data(file_path):
    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["name"] == "retrieve_cif" or row["name"] == "retrieve_binary_cif":
                retrieve_time = float(row["average_time_s"])
            elif row["name"] == "cif_to_pdbs" or row["name"] == "binary_cif_to_pdbs":
                conversion_time = float(row["average_time_s"])

            total_time = float(row["total_duration_seconds"])
    return retrieve_time, conversion_time, total_time


def extract_cores(filename):
    match = re.search(r"_(\d+)_", filename)
    return match.group(1) if match else "Unknown"


def compare_directories(text_dir, binary_dir):
    text_files = sorted([f for f in os.listdir(text_dir) if f.endswith(".csv")])
    binary_files = sorted([f for f in os.listdir(binary_dir) if f.endswith(".csv")])

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

        results.append(
            {
                "CPU cores": cores,
                "Text Retrieve (s)": f"{text_retrieve:.6f}",
                "Binary Retrieve (s)": f"{binary_retrieve:.6f}",
                "Retrieve Diff (%)": f"{retrieve_diff:.2f}",
                "Text Convert (s)": f"{text_convert:.6f}",
                "Binary Convert (s)": f"{binary_convert:.6f}",
                "Convert Diff (%)": f"{convert_diff:.2f}",
                "Text Total (s)": f"{text_total:.6f}",
                "Binary Total (s)": f"{binary_total:.6f}",
                "Total Diff (%)": f"{total_diff:.2f}",
            }
        )

    return results


def save_results_to_csv(results, output_file):
    with open(output_file, "w", newline="") as csvfile:
        fieldnames = results[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)


def csv_to_latex(csv_file):
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Sort rows by CPU cores
    rows.sort(key=lambda x: int(x["CPU cores"]))

    latex_output = io.StringIO()

    # Calculate averages
    avg_text_retrieve = statistics.mean(float(row["Text Retrieve (s)"]) for row in rows)
    avg_binary_retrieve = statistics.mean(
        float(row["Binary Retrieve (s)"]) for row in rows
    )
    avg_text_convert = statistics.mean(float(row["Text Convert (s)"]) for row in rows)
    avg_binary_convert = statistics.mean(
        float(row["Binary Convert (s)"]) for row in rows
    )
    avg_retrieve_diff = statistics.mean(float(row["Retrieve Diff (%)"]) for row in rows)
    avg_convert_diff = statistics.mean(float(row["Convert Diff (%)"]) for row in rows)
    avg_text_total = statistics.mean(float(row["Text Total (s)"]) for row in rows)
    avg_binary_total = statistics.mean(float(row["Binary Total (s)"]) for row in rows)
    avg_total_diff = statistics.mean(float(row["Total Diff (%)"]) for row in rows)

    # First table
    latex_output.write("\\begin{table}[!htb]\n")
    latex_output.write("\\centering\n")
    latex_output.write("\\begin{tabular}{|l|rr|rr|rr|}\n")
    latex_output.write("\\hline\n")
    latex_output.write(
        "\\textbf{CPU cores} & \\multicolumn{2}{c|}{\\textbf{Retrieve (s)}} & \\multicolumn{2}{c|}{\\textbf{Convert (s)}} & \\multicolumn{2}{c|}{\\textbf{Diff (\\%)}}\\\\\n"
    )
    latex_output.write(
        "& \\textbf{Text} & \\textbf{Binary} & \\textbf{Text} & \\textbf{Binary} & \\textbf{Retrieve} & \\textbf{Convert} \\\\\n"
    )
    latex_output.write("\\hline\n")

    for row in rows:
        latex_output.write(
            f"{row['CPU cores']} & {float(row['Text Retrieve (s)']):.2f} & {float(row['Binary Retrieve (s)']):.2f} & "
        )
        latex_output.write(
            f"{float(row['Text Convert (s)']):.3f} & {float(row['Binary Convert (s)']):.3f} & "
        )
        latex_output.write(
            f"{float(row['Retrieve Diff (%)']):.2f} & {float(row['Convert Diff (%)']):.2f} \\\\\n"
        )

    latex_output.write("\\hline\n")
    latex_output.write(
        f"Average & {avg_text_retrieve:.2f} & {avg_binary_retrieve:.2f} & "
    )
    latex_output.write(f"{avg_text_convert:.3f} & {avg_binary_convert:.3f} & ")
    latex_output.write(f"{avg_retrieve_diff:.2f} & {avg_convert_diff:.2f} \\\\\n")
    latex_output.write("\\hline\n")
    latex_output.write("\\end{tabular}\n")
    latex_output.write(
        "\\caption{Comparison of Text and Binary Processing (Retrieve and Convert)}\n"
    )
    latex_output.write("\\label{tab:exp_download_pdb_comparison}\n")
    latex_output.write("\\end{table}\n\n")

    # Second table
    latex_output.write("\\begin{table}[!htb]\n")
    latex_output.write("\\centering\n")
    latex_output.write("\\begin{tabular}{|r|r|r|r|}\n")
    latex_output.write("\\hline\n")
    latex_output.write(
        "\\textbf{CPU cores} & \\textbf{Text Total (s)} & \\textbf{Binary Total (s)} & \\textbf{Diff (\\%)} \\\\\n"
    )
    latex_output.write("\\hline\n")

    for row in rows:
        latex_output.write(
            f"{row['CPU cores']} & {float(row['Text Total (s)']):.0f} & {float(row['Binary Total (s)']):.0f} & "
        )
        latex_output.write(f"{float(row['Total Diff (%)']):.2f} \\\\\n")

    latex_output.write("\\hline\n")
    latex_output.write(
        f"Average & {avg_text_total:.0f} & {avg_binary_total:.0f} & {avg_total_diff:.2f} \\\\\n"
    )
    latex_output.write("\\hline\n")
    latex_output.write("\\end{tabular}\n")
    latex_output.write(
        "\\caption{Comparison of total processing Time for text and binary download}\n"
    )
    latex_output.write("\\label{tab:exp_download_pdb_total_comparison}\n")
    latex_output.write("\\end{table}\n")

    return latex_output.getvalue()


# Usage
text_directory = (
    "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download_pdb_nb/results"
)
binary_directory = (
    "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download/results"
)
output_csv = "comparison_results.csv"
output_latex = "comparison_results.tex"

results = compare_directories(text_directory, binary_directory)
save_results_to_csv(results, output_csv)
latex_table = csv_to_latex(output_csv)

with open(output_latex, "w") as f:
    f.write(latex_table)

print(f"Results saved to {output_csv}")
print(f"LaTeX table saved to {output_latex}")
