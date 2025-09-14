# Product Requirements Document (PRD) for DuckLake

## Overview
DuckLake is a data lakehouse built on DuckDB for ingesting, processing, and analyzing search logs. It provides a structured way to handle raw log data through bronze (raw), silver (cleaned/deduped), and gold (aggregated) layers, enabling efficient querying and reporting on user search behavior.

## Features
- **Data Ingestion**: Automated ingestion of search logs from files or URLs, supporting formats like log, JSON, CSV.
- **Data Processing**: Parsing logs using regex or simple methods, normalizing timestamps and IPs, and partitioning by event date (dt).
- **Deduplication**: Removal of duplicates based on primary keys (timestamp, IP, query) to ensure data quality.
- **Layered Architecture**: Bronze for raw data, Silver for cleaned data with dedup, Gold for aggregated views.
- **Reporting**: Generation of CSV reports for analytics, including daily, weekly, monthly summaries.
- **Analytics Capabilities**: Support for trending analysis, busiest times, geo-lookup from IPs, visit trends, and engagement metrics.

## Requirements
Derived from questions.md:
- Identify the most popular search keywords and phrases.
- Determine what is trending now in searches.
- Analyze busiest times, days of the week, and times of the day.
- Find the most popular locations via geo-lookup from IP addresses.
- Calculate the number of visits per day, per week, per month.
- Assess whether visits are increasing or decreasing over time.
- Measure what content is most engaging and least engaging.
- Track the most popular items this week: search terms, pages, and YouTube views.

## Architecture
- **Bronze Layer**: Raw data storage, ingested from sources with minimal processing.
- **Silver Layer**: Cleaned data with deduplication, partitioning by dt (event date), and computed fields like dt from timestamps.
- **Gold Layer**: Aggregated views for advanced analytics.
- **Reports**: Automated CSV exports for key metrics, stored in the reports/ directory.
- **Tools**: Python scripts for orchestration, DuckDB for querying and processing.

## Success Criteria
- Accurate and efficient deduplication of search events.
- Correct partitioning by event date to enable time-based queries.
- Fast performance on large datasets using DuckDB's columnar storage.
- Comprehensive reports that fully address the requirements from questions.md.
- Scalable architecture for handling growing log volumes.
