# DuckLake Design

- Setup the datalake from scratch
    - create an initialisation script to create the DuckDB database and tables
    - create a virtual environment and install dependencies
    - define the structure of the bronze, silver, and gold layers
    - create a configuration file format for defining data sources
    - implement ingestion functions for local files, URLs, and SQLite databases
    - implement functions to build the silver layer from the bronze layer
    - implement functions to build the gold layer from the silver layer
    - create a CLI to orchestrate the workflow
- ingest data from the sources into the bronze layer
- build the silver and gold layers
- run reports and queries against the gold layer
- explore the data in Python or Jupyter
- add new data sources and automate ingestion
