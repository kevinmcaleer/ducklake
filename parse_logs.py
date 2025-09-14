# INFO:root:2025-09-03T17:30:00.000000 - IP: 192.168.1.18 - Query: panda dataframe vs polars

# remove 'info:root:' from beginning of each line
# the next 10 characters are the timestamp, parse into a date time,
# the next section is the IP address ( beteen the - IP: and the next - )
# then remove Query: and capture the query to the end of the line.

# this is one search record

from datetime import datetime

line = "INFO:root:2025-09-03T17:30:00.000000 - IP: 192.168.1.18 - Query: panda dataframe vs polars"

timestamp = line[10:29]

my_date = datetime.fromisoformat(timestamp)
ip = line.split(" - ")[1][5:]
my_line = line[37:]
my_ip = my_line.split(" - ")[0][6:]
query = my_line.split(" - ")[1][7:]

print(f"timestamp: '{my_date}', my_ip: '{my_ip}', query: '{query}'")