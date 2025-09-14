# User Story: Progress Bars for Data Import

**As** a data engineer
**I want** to see a progress bar during data import steps
**So that** I can monitor the status and estimate completion time for large ingests.

## Acceptance Criteria
- Progress bars are displayed for file-based and URL-based ingestion steps.
- Progress is updated as files are read and processed.
- The feature is implemented using a standard Python library (e.g., tqdm).
- Progress bars are visible in the CLI and do not interfere with logging output.
