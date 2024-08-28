import csv
import json
import os
import re

import dask.bag as db
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dask.distributed import get_client


def parse_html_to_json(html_file_path: str, parser='html.parser'):
    with open(html_file_path, 'r') as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, parser)

    # Extract duration
    duration_text = soup.find(string=re.compile(r'Duration:'))
    duration_minutes = parse_duration(duration_text)

    return soup.find('script', type='application/json'), duration_minutes


def parse_duration(duration_text):
    if duration_text:
        match = re.search(r'Duration:\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', duration_text)
        if match:
            hours = int(match.group(1) or 0)
            minutes = int(match.group(2) or 0)
            seconds = int(match.group(3) or 0)
            return hours * 60 + minutes + seconds / 60
    return None


def convert_json_to_schema(json_script: BeautifulSoup):
    if json_script:
        json_data = json.loads(json_script.string)
        return json_data
    return None


def analyze_single_task(data, task_name):
    task_times = []
    total_time = 0
    count = 0

    top_level_key = next(iter(data))
    roots = data[top_level_key]['roots']

    for reference in roots['references']:
        if 'attributes' in reference and 'data' in reference['attributes']:
            names = reference['attributes']['data'].get('name', [])
            durations = reference['attributes']['data'].get('duration', [])

            for name, duration in zip(names, durations):
                if name == task_name:
                    task_times.append(duration)
                    total_time += duration
                    count += 1

    average_time = total_time / count if count > 0 else 0
    return {
        "name": task_name,
        "task_times": task_times,
        "total_time": total_time,
        "count": count,
        "average_time": average_time
    }


def analyze_task_times(json_data, task_names):

    with get_client() as client:
        task_bag = db.from_sequence(task_names)
        results = task_bag.map(lambda name: analyze_single_task(json_data, name)).compute()
    return {result['name']: result for result in results}


def write_to_csv(results, output_file, duration):
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['name', 'average_time_ms', 'average_time_s', 'average_time_min', 'total_time_ms', 'total_time_s',
                      'total_time_min', 'count', 'total_duration_minutes']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for name, result in results.items():
            writer.writerow({
                'name': name,
                'average_time_ms': result['average_time'],
                'average_time_s': result['average_time'] / 1000,
                'average_time_min': result['average_time'] / 60000,
                'total_time_ms': result['total_time'],
                'total_time_s': result['total_time'] / 1000,
                'total_time_min': result['total_time'] / 60000,
                'count': result['count'],
                'total_duration_minutes': duration
            })



def extract_core_count(filename):
    match = re.search(r'_b_(\d+)_', filename)
    if match:
        return int(match.group(1))
    return None


def process_html_file(html_file_path, task_names, output_dir):
    json_script, duration = parse_html_to_json(html_file_path)
    json_data = convert_json_to_schema(json_script)

    if json_data is not None:
        results = analyze_task_times(json_data, task_names)

        base_name = os.path.splitext(os.path.basename(html_file_path))[0]
        output_file = os.path.join(output_dir, f"{base_name}_results.csv")
        write_to_csv(results, output_file, duration)
        print(f"Results for {html_file_path} written to {output_file}")
    else:
        print(f"Failed to extract JSON data from {html_file_path}")



def read_csv_files(directory):
    data = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            core_count = extract_core_count(filename)
            if core_count:
                df = pd.read_csv(os.path.join(directory, filename))
                df['cores'] = core_count
                data.append(df)
    return pd.concat(data, ignore_index=True)


def plot_scalability(df, plot_type='average'):
    tasks = df['name'].unique()
    colors = plt.cm.rainbow(np.linspace(0, 1, len(tasks)))

    plt.figure(figsize=(14, 10))

    if plot_type in ['average', 'total']:
        for task, color in zip(tasks, colors):
            task_df = df[df['name'] == task].sort_values('cores')

            if plot_type == 'average':
                y_values = task_df['average_time_s']
                y_label = 'Average Execution Time (seconds)'
            else:  # total
                y_values = task_df['total_time_s']
                y_label = 'Total Execution Time (seconds)'

            plt.plot(task_df['cores'], y_values, marker='o', label=task, color=color)

            # for _, row in task_df.iterrows():
            #     plt.annotate(f"{row['total_duration_minutes']:.1f}m",
            #                  (row['cores'], y_values.loc[_]),
            #                  textcoords="offset points",
            #                  xytext=(0, 10),
            #                  ha='center',
            #                  fontsize=8)

        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.title(f'Task Scalability ({plot_type.capitalize()} Time) with Total Duration')

    elif plot_type == 'duration':
        duration_df = df.groupby('cores')['total_duration_minutes'].first().reset_index()
        plt.plot(duration_df['cores'], duration_df['total_duration_minutes'], marker='o', color='blue')

        for _, row in duration_df.iterrows():
            plt.annotate(f"{row['total_duration_minutes']:.1f}m",
                         (row['cores'], row['total_duration_minutes']),
                         textcoords="offset points",
                         xytext=(0, 10),
                         ha='center',
                         fontsize=8)

        y_label = 'Total Job Duration (minutes)'
        plt.title('Total Job Duration Scalability')

    else:
        raise ValueError("plot_type must be 'average', 'total', or 'duration'")

    plt.xlabel('Number of Cores')
    plt.ylabel(y_label)
    # plt.xscale('log', base=2)
    # plt.yscale('log')
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.tight_layout()
    plt.savefig(f'scalability_plot_{plot_type}.pdf', dpi=1200, bbox_inches='tight')
    plt.show()


def main():
    html_files_dir = "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments/download/reports"  # replace with actual path
    task_names = [
        "retrieve_binary_cifs_to_pdbs",
        "aggregate_results",
        "create_zip_archive",
        "create_pdb_zip_archive",
        "compress_and_save_h5"
    ]
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)

    # Find all HTML files in the specified directory
    html_files = [os.path.join(html_files_dir, f) for f in os.listdir(html_files_dir) if f.endswith('.html')]

    # def process_file(html_file):
    #     process_html_file(html_file, task_names, output_dir)
    #
    # client = Client()
    #
    # # Process HTML files and generate CSVs
    # files = client.map(process_file, html_files)
    # dask.distributed.progress(files)

    # Read CSV files and create visualization
    df = read_csv_files(output_dir)

    # Plot average time
    plot_scalability(df, plot_type='average')

    # Plot total time
    plot_scalability(df, plot_type='total')

    # Plot total duration
    plot_scalability(df, plot_type='duration')


if __name__ == "__main__":
    main()