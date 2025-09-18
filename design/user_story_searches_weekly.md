# User Story: Weekly Search Volume Report

## Title
As a data analyst, I want to see the number of searches made per week (from search-logs), going back as far as data is available, so that I can analyze search trends and seasonality over time.

## Acceptance Criteria
- The report must aggregate the total number of searches per ISO week, for all available history in the search logs.
- The report must be updated automatically as new data is ingested.
- The report must be available as a CSV in the `reports/` directory (e.g., `searches_weekly.csv`).
- The report must include columns: `iso_week` (e.g., 2024-W01), `search_count` (total searches in that week).
- The report must be visualizable in dashboards and notebooks.

## Notes
- This supports trend analysis, anomaly detection, and seasonality studies for search activity.
- Should be included in the standard reporting pipeline.
