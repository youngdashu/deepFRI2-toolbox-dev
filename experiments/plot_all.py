import os
import subprocess
import sys
import concurrent.futures


def run_script(script_path):
    print(f"Running: {script_path}")
    try:
        subprocess.run(
            [sys.executable, script_path], check=True, capture_output=True, text=True
        )
        print(f"Successfully completed: {script_path}")
        return f"Success: {script_path}"
    except subprocess.CalledProcessError as e:
        error_message = (
            f"Error running {script_path}: {e}\nSTDOUT: {e.stdout}\nSTDERR: {e.stderr}"
        )
        print(error_message)
        return error_message


def run_total_scripts(root_dir, max_workers=None):
    script_paths = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if "total.py" in filenames:
            script_paths.append(os.path.join(dirpath, "total.py"))

    print(f"Found {len(script_paths)} 'total.py' scripts to run.")

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_script, path) for path in script_paths]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())
            print("-" * 50)


if __name__ == "__main__":

    root_directory = "/Users/youngdashu/sano/deepFRI2-toolbox-dev/experiments"
    if not os.path.isdir(root_directory):
        print(f"Error: {root_directory} is not a valid directory")
        sys.exit(1)

    max_workers = 8

    run_total_scripts(root_directory, max_workers)
