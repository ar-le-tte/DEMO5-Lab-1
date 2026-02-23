# TMDB Movies Analytics Lab

## Overview
This lab implements an **end-to-end data analytics pipeline** using data from **The Movie Database (TMDB) API**.  
The pipeline follows a **Bronze → Silver → Gold** architecture and combines **Python, PySpark, and Matplotlib**.

A **single Jupyter notebook** is used for orchestration, while all reusable logic is encapsulated in Python scripts.

---

## Project Structure

```text
Lab 1/
├── notebook/
│   └── tmdb_movies_analysis_orchestrator.ipynb
│
├── src/
│   ├── logging_utils.py              # Adding logs for different jobs
│   ├── tmdb_client.py              # TMDB API client functions
│   ├── download_tmdb_bronze.py              # Fetching the raw data from the API
│   ├── bronze_to_spark.py          # Load Bronze JSON → Spark DataFrame
│   ├── silver_transform.py         # Cleaning & feature engineering logic
│   ├── gold_analysis.py            # KPI computation & aggregations
│   └── kpi_movies.py               # Helper functions for KPI ranking
│
├── requirements.txt
└── README.md
```
## Pipeline Architecture
### Bronze Layer
This is responsible for Raw Ingestion: Storing the movies, credits JSON files as received.

**Key scripts**: 
- [`TMDB Client`](src/tmdb_client.py)
- [`Movies Download`](src/download_tmdb_bronze.py)
- [`Bronze to Spark`](src/bronze_to_spark.py)

### Silver Layer
This is responsible tansforming raw JSON into an analysis-ready dataset: Cleaning the data.

**Key script**: 
- [`Silver Transform`](src/silver_transform.py)

### Gold Layer
This is responsible for computing analytical metrics and visualization

**Key scripts**: 
- [`Movie KPIs`](src/kpi_movies.py)
- [`Aggregation and Analysis`](src/gold_analysis.py)

## Running the Lab
### Main Dependencies
- pyspark
- requests
- python-dotenv
- matplotlib
### Requirements
Create and activate a Python environment, then install:
```bash
pip install -r requirements.txt
```
### TMDB API Key
Set your TMDB API key in `.env`

### Orchestrator
A notebook: Run cells sequentially from top to bottom.

**The notebook**: 
- [`Orchestrator`](notebook/tmb_movies_analysis_orchestrator.ipynb)
