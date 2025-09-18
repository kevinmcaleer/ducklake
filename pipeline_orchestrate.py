import subprocess
import sys

def run(cmd):
    print(f'Running: {cmd}')
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f'Command failed: {cmd}')
        sys.exit(result.returncode)

if __name__ == '__main__':
    # 1. Wipe folders (assume already done)
    # 2. Split all-visits-full.csv into daily files
    run('python stage_all_visits_full.py')
    # 3. Stage daily files into data/raw/page_count
    run("python intake.py stage-downloads --source page_count --pattern 'all-visits-*.csv' --overwrite")
    # 4. Split and stage search-logs.csv into data/raw/search_logs/dt=YYYY-MM-DD/search_logs.csv
    run('python stage_search_logs_csv.py')
    # 5. Preprocess and bootstrap raw data to lake parquet
    run('python intake.py simple-refresh')
    # 6. Validate
    run('python intake.py simple-validate')
