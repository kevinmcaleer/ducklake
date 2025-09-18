import os
import re
import csv
from datetime import datetime

# Directory containing raw log files

import os
import re
import csv
from datetime import datetime

# Root directory containing nested log files
dir_logs = '_tmp_downloads/'
# Output directory for CSVs
csv_dir = '_tmp_downloads/search_logs_csv/'
os.makedirs(csv_dir, exist_ok=True)


# Regex pattern for log lines (matches: INFO:root:2025-09-03T10:15:28.736047 - IP: 192.168.1.10 - Query: raspberry pi pico)
LOG_PATTERN = re.compile(r'^INFO:root:(?P<timestamp>[^ ]+) - IP: (?P<ip>[^ ]+) - Query: ?(?P<query>.*)$')

def parse_log_line(line):
    match = LOG_PATTERN.match(line)
    if match:
        return match.group('timestamp'), match.group('ip'), match.group('query')
    return None, None, None

def convert_log_to_csv(log_path, csv_path):
    with open(log_path, 'r') as infile, open(csv_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['timestamp', 'ip', 'query'])
        for line in infile:
            line = line.strip()
            if not line:
                continue
            ts, ip, query = parse_log_line(line)
            if ts and ip and query is not None:
                writer.writerow([ts, ip, query])

def find_log_files(root_dir):
    log_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.endswith('.log'):
                log_files.append(os.path.join(dirpath, fname))
    return log_files

if __name__ == '__main__':
    log_files = find_log_files(dir_logs)
    for log_path in log_files:
        # Create a unique CSV name based on the log file's relative path
        rel_path = os.path.relpath(log_path, dir_logs)
        csv_name = rel_path.replace(os.sep, '_').replace('.log', '.csv')
        csv_path = os.path.join(csv_dir, csv_name)
        print(f'Converting {log_path} -> {csv_path}')
        convert_log_to_csv(log_path, csv_path)
    print('Done. CSVs are in', csv_dir)
