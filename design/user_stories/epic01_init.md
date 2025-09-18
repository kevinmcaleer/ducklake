help me write an epic epic01_init.md. I want to create a one time setup of our datalake which will do the following:

wipe all existing data from our datalake so we are starting fresh.

# Simplified Pipeline Design
1. download all the historical data for page_count from http://page_count.kevsrobots.com/all-visits?since=2023-01-01?format=csv
2. download all the search_logs.log data from /Volumes/Search\ Logs/search-logs.log

3. setup our folders as per the simplified pipeline design
4. run a one time bootstrap to convert these initial from raw data into parquet files in the lake
5. run a one time bootstrap to convert these initial from raw data into the daily aggregate tables
6. generate the initial reports

# Verification
1. verify the row counts in the silver tables match the raw data counts
2. verify the reports match the expected values


