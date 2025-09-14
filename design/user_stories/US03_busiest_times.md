# User Story: Busiest Times

**As** a business analyst
**I want** to analyze the busiest times, days of the week, and hours of the day for both visits and searches
**So that** I can optimize system resources and plan content releases.

## Acceptance Criteria
- Reports exist for busiest days of the week and hours of the day.
- Each day of the week (Sun-Sat) and each hour of the day (00-23) is shown with total visits and total searches.
- All 7 days and all 24 hours must have non-zero visit and search counts, reflecting actual usage.
- Data is partitioned by event date (dt) and based on deduplicated silver data.
- Day and hour labels and counts are correct and complete for all 7 days and 24 hours.
