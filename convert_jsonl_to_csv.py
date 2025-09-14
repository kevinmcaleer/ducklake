import os
import glob
import json
import csv

INPUT_DIR = '_tmp_downloads'
PATTERN = 'all-visits-*.jsonl'

def convert_jsonl_to_csv(jsonl_path, csv_path):
    with open(jsonl_path, 'r', encoding='utf-8') as infile:
        lines = [json.loads(line) for line in infile if line.strip()]
    if not lines:
        print(f"No data in {jsonl_path}, skipping.")
        return
    # Get all unique keys across all records
    fieldnames = set()
    for row in lines:
        fieldnames.update(row.keys())
    fieldnames = sorted(fieldnames)
    with open(csv_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in lines:
            writer.writerow(row)
    print(f"Converted {jsonl_path} -> {csv_path}")

def main():
    pattern = os.path.join(INPUT_DIR, PATTERN)
    for jsonl_file in glob.glob(pattern):
        csv_file = jsonl_file[:-6] + '.csv'  # replace .jsonl with .csv
        convert_jsonl_to_csv(jsonl_file, csv_file)

if __name__ == '__main__':
    main()
