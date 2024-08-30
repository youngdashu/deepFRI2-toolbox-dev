import csv
import json
import os
import re
from typing import List

import dask.bag as db
import dask.distributed
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dask.distributed import get_client


BASE_CORE_COUNT=190

def parse_html_to_json(html_file_path: str, parser='html.parser'):
    with open(html_file_path, 'r') as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, parser)

    # Extract duration
    duration_text = soup.find(string=re.compile(r'Duration:'))
    duration_minutes, duration_seconds = parse_duration(duration_text)

    print(
        duration_minutes, duration_seconds
    )

    return soup.find('script', type='application/json'), duration_minutes, duration_seconds


def parse_duration(text):
    # First, try to match the format with decimal seconds
    simple_match = re.search(r'Duration:\s*(\d+(?:\.\d+)?)\s*s', text)
    if simple_match:
        total_seconds = float(simple_match.group(1))
        total_minutes = total_seconds / 60
        return round(total_minutes, 2), round(total_seconds, 2)

    # If simple format doesn't match, try the complex format
    complex_match = re.search(r'Duration:\s*((?:\d+\s*\w+\s*)+)', text)
    if complex_match:
        duration_string = complex_match.group(1)
        time_parts = re.findall(r'(\d+(?:\.\d+)?)\s*(\w+)', duration_string)

        total_seconds = 0
        for value, unit in time_parts:
            value = float(value)
            if unit.startswith('s'):
                total_seconds += value
            elif unit.startswith('m'):
                total_seconds += value * 60
            elif unit.startswith('h'):
                total_seconds += value * 3600
            elif unit.startswith('d'):
                total_seconds += value * 86400

        total_minutes = total_seconds / 60
        return round(total_minutes, 2), round(total_seconds, 2)

    return None, None


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


def write_to_csv(results, output_file, duration_mins, duration_secs):
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['name', 'average_time_ms', 'average_time_s', 'average_time_min', 'total_time_ms', 'total_time_s',
                      'total_time_min', 'count', 'total_duration_minutes', 'total_duration_seconds']
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
                'total_duration_minutes': duration_mins,
                'total_duration_seconds': duration_secs
            })


def extract_core_count(filename):
    match = re.search(r'(?:_b_|_)(\d+)_', filename)
    if match:
        return int(match.group(1))
    return None


def process_html_file(html_file_path, task_names, output_dir):
    json_script, duration_mins, duration_secs = parse_html_to_json(html_file_path)
    json_data = convert_json_to_schema(json_script)

    if json_data is not None:
        results = analyze_task_times(json_data, task_names)

        base_name = os.path.splitext(os.path.basename(html_file_path))[0]
        output_file = os.path.join(output_dir, f"{base_name}_results.csv")
        write_to_csv(results, output_file, duration_mins, duration_secs)
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

    if len(data) <= 1:
        return data[0]
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

        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.title(f'Task Scalability ({plot_type.capitalize()} Time) with Total Duration')

    elif plot_type == 'duration':
        duration_df = df.groupby('cores')['total_duration_minutes'].first().reset_index()
        if any(duration_df['total_duration_minutes'] < 1):
            duration_df['total_duration_seconds'] = df.groupby('cores')['total_duration_seconds'].first().reset_index()[
                'total_duration_seconds']
            plt.plot(duration_df['cores'], duration_df['total_duration_seconds'], marker='o', color='blue')
            y_label = 'Total Job Duration (seconds)'

            for _, row in duration_df.iterrows():
                plt.annotate(f"{row['total_duration_seconds']:.1f}s",
                             (row['cores'], row['total_duration_seconds']),
                             textcoords="offset points",
                             xytext=(0, 10),
                             ha='center',
                             fontsize=8)

        else:
            plt.plot(duration_df['cores'], duration_df['total_duration_minutes'], marker='o', color='blue')
            y_label = 'Total Job Duration (minutes)'

            for _, row in duration_df.iterrows():
                plt.annotate(f"{row['total_duration_minutes']:.1f}m",
                             (row['cores'], row['total_duration_minutes']),
                             textcoords="offset points",
                             xytext=(0, 10),
                             ha='center',
                             fontsize=8)

        plt.title('Total Job Duration Scalability')
    else:
        raise ValueError("plot_type must be 'average', 'total', or 'duration'")

    plt.xlabel('Number of Cores (P)')
    plt.ylabel(y_label)
    plt.xscale('linear')
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.xticks(ticks=df['cores'])
    plt.tight_layout()
    plt.savefig(f'scalability_plot_{plot_type}.pdf', dpi=1200, bbox_inches='tight')
    plt.show()


def plot_speedup(df):
    duration_df = df.groupby('cores')['total_duration_seconds'].first().reset_index()
    duration_df = duration_df.sort_values('cores')

    T1 = duration_df.loc[duration_df['cores'] == BASE_CORE_COUNT, 'total_duration_seconds'].values[0]
    duration_df['speedup'] = T1 / duration_df['total_duration_seconds']

    plt.figure(figsize=(10, 6))
    plt.plot(duration_df['cores'], duration_df['speedup'], marker='o', color='red')

    plt.xlabel('Number of Cores')
    plt.ylabel('Speedup')
    plt.title('Speedup vs Number of Cores')
    plt.legend()
    plt.xticks(ticks=df['cores'])
    plt.grid(True)
    plt.savefig('speedup_plot.pdf', dpi=1200, bbox_inches='tight')
    plt.show()


def plot_efficiency(df):
    duration_df = df.groupby('cores')['total_duration_seconds'].first().reset_index()
    duration_df = duration_df.sort_values('cores')

    T1 = duration_df.loc[duration_df['cores'] == BASE_CORE_COUNT, 'total_duration_seconds'].values[0]
    duration_df['speedup'] = T1 / duration_df['total_duration_seconds']
    duration_df['efficiency'] = duration_df['speedup'] / duration_df['cores']

    plt.figure(figsize=(10, 6))
    plt.plot(duration_df['cores'], duration_df['efficiency'], marker='o', color='green')

    plt.xlabel('Number of Cores')
    plt.ylabel('Efficiency')
    plt.title('Efficiency vs Number of Cores')
    plt.xticks(ticks=df['cores'])
    plt.grid(True)
    plt.savefig('efficiency_plot.pdf', dpi=1200, bbox_inches='tight')
    plt.show()


def plot_grid(df):
    duration_df = df.groupby('cores')['total_duration_seconds'].first().reset_index()
    duration_df = duration_df.sort_values('cores')

    T1 = duration_df.loc[duration_df['cores'] == BASE_CORE_COUNT, 'total_duration_seconds'].values[0]
    duration_df['speedup'] = T1 / duration_df['total_duration_seconds']
    duration_df['efficiency'] = duration_df['speedup'] / duration_df['cores']

    fig, axs = plt.subplots(2, 2, figsize=(20, 20))

    # Duration plot
    axs[0, 0].plot(duration_df['cores'], duration_df['total_duration_seconds'], marker='o', color='blue')
    axs[0, 0].set_xlabel('Number of Cores')
    axs[0, 0].set_ylabel('Total Job Duration (seconds)')
    axs[0, 0].set_title('Duration vs Number of Cores')
    axs[0, 0].set_xticks(df['cores'])
    axs[0, 0].grid(True)

    # Speedup plot
    axs[0, 1].plot(duration_df['cores'], duration_df['speedup'], marker='o', color='red')
    axs[0, 1].set_xlabel('Number of Cores')
    axs[0, 1].set_ylabel('Speedup')
    axs[0, 1].set_title('Speedup vs Number of Cores')
    axs[0, 1].set_xticks(df['cores'])
    axs[0, 1].grid(True)

    # Efficiency plot
    axs[1, 0].plot(duration_df['cores'], duration_df['efficiency'], marker='o', color='green')
    axs[1, 0].set_xlabel('Number of Cores')
    axs[1, 0].set_ylabel('Efficiency')
    axs[1, 0].set_title('Efficiency vs Number of Cores')
    axs[1, 0].set_xticks(df['cores'])
    axs[1, 0].grid(True)

    # Average time plot (all tasks)
    tasks = df['name'].unique()
    colors = plt.cm.rainbow(np.linspace(0, 1, len(tasks)))

    for task, color in zip(tasks, colors):
        task_df = df[df['name'] == task].sort_values('cores')
        axs[1, 1].plot(task_df['cores'], task_df['average_time_s'], marker='o', label=task, color=color)

    axs[1, 1].set_xlabel('Number of Cores')
    axs[1, 1].set_ylabel('Average Execution Time (seconds)')
    axs[1, 1].set_title('Average Time vs Number of Cores (All Tasks)')
    axs[1, 1].set_xticks(df['cores'])
    axs[1, 1].grid(True)
    axs[1, 1].legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()
    plt.savefig('grid_plot.pdf', dpi=1200, bbox_inches='tight')
    plt.show()


def plot_from_htmls(
        html_files_dir: str,
        task_names: List[str],
):
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)

    # Find all HTML files in the specified directory
    html_files = [os.path.join(html_files_dir, f) for f in os.listdir(html_files_dir) if f.endswith('.html')]

    def process_file(html_file):
        process_html_file(html_file, task_names, output_dir)

    client = dask.distributed.Client()

    # Process HTML files and generate CSVs
    files = client.map(process_file, html_files)
    dask.distributed.progress(files)

    # Read CSV files and create visualization
    df = read_csv_files(output_dir)

    # Plot average time
    plot_scalability(df, plot_type='average')

    # Plot total time
    plot_scalability(df, plot_type='total')

    # Plot total duration
    plot_scalability(df, plot_type='duration')

    # New plots
    plot_speedup(df)
    plot_efficiency(df)
    plot_grid(df)