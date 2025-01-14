import csv
import re


def main(input_string):
    download_times = re.findall(r"Download time:\s*([0-9.]+)", input_string)
    compress_times = re.findall(r"Compress \+ save time:\s*([0-9.]+)", input_string)

    with open("times.csv", "w", newline="") as csvfile:
        fieldnames = ["download_time", "compress_time"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(download_times)):
            writer.writerow(
                {"download_time": download_times[i], "compress_time": compress_times[i]}
            )


input = """

"""

if __name__ == "__main__":
    main(input)
