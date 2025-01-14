import csv
import json
from collections import defaultdict
from dask.distributed import Client, LocalCluster
import dask.bag as db


def analyze_single_task(data, task_name):
    task_times = []
    total_time = 0
    count = 0

    # Get the first (and presumably only) key in the data dictionary
    top_level_key = next(iter(data))
    roots = data[top_level_key]["roots"]

    for reference in roots["references"]:
        if "attributes" in reference and "data" in reference["attributes"]:
            names = reference["attributes"]["data"].get("name", [])
            durations = reference["attributes"]["data"].get("duration", [])

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
        "average_time": average_time,
    }


def analyze_task_times(json_data, task_names):
    # Load JSON data
    if isinstance(json_data, str):
        with open(json_data, "r") as f:
            data = json.load(f)
    elif isinstance(json_data, dict):
        data = json_data
    else:
        raise ValueError("json_data must be either a file path or a dictionary")

    # Set up Dask local cluster and client
    with LocalCluster() as cluster, Client(cluster) as client:
        # Create a bag of task names
        task_bag = db.from_sequence(task_names)

        # Map the analyze_single_task function to each task name
        results = task_bag.map(lambda name: analyze_single_task(data, name)).compute()

    # Convert results to a dictionary
    return {result["name"]: result for result in results}


def print_analysis(results):
    print("Tasks analyzed:")
    for name, result in results.items():
        print(f"\n{name}:")
        print("Execution Times (ms):")
        for time in result["task_times"]:
            print(f"  {time:.2f}")

        print("\nSummary:")
        print(f"  Total execution time: {result['total_time']:.2f} ms")
        print(f"  Number of tasks: {result['count']}")
        if result["count"] > 0:
            print(f"  Average execution time: {result['average_time']:.2f} ms")


# Add function to write results to csv
def write_to_csv(results):
    with open("results.csv", "w", newline="") as csvfile:
        fieldnames = [
            "name",
            "average_time_ms",
            "average_time_s",
            "average_time_min",
            "total_time_ms",
            "total_time_s",
            "total_time_min",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for name, result in results.items():
            writer.writerow(
                {
                    "name": name,
                    "average_time_ms": result["average_time"],
                    "average_time_s": result["average_time"] / 1000,
                    "average_time_min": result["average_time"] / 60000,
                    "total_time_ms": result["total_time"],
                    "total_time_s": result["total_time"] / 1000,
                    "total_time_min": result["total_time"] / 60000,
                }
            )


# Example usage
if __name__ == "__main__":
    json_file = "pretty.json"
    task_names = [
        "retrieve_binary_cifs_to_pdbs",
        "aggregate_results",
        "create_zip_archive",
        "create_pdb_zip_archive",
        "compress_and_save_h5",
    ]

    results = analyze_task_times(json_file, task_names)
    print_analysis(results)
    write_to_csv(results)  # Added line to write results to csv
