# PySpark Retail ETL

This repository contains my Month 3 roadmap project: a small PySpark ETL pipeline built to strengthen my understanding of Spark DataFrames, transformations, aggregations, and basic data cleaning in a realistic but intentionally limited project scope.

The goal of this project was not to build a production-grade pipeline, but to finish a focused Spark project from start to GitHub and prove that I can work with Spark locally, clean transactional data, and generate simple analytical outputs.

## Why this project is intentionally small

According to the roadmap, Month 3 requires a first Spark ETL project in Python on GitHub as a visible proof point, alongside DEA-C01 preparation and Scala reading basics. Because of that, this project deliberately avoids adding larger platform concerns like orchestration, Docker, Kafka, cloud deployment, CI/CD, or advanced testing, since those belong to later and bigger roadmap projects.

The focus here is on learning the Spark workflow itself:
- Reading a CSV dataset into a Spark DataFrame.
- Cleaning important fields.
- Converting the invoice date into a proper timestamp with `to_timestamp()` using the dataset format.
- Filtering out invalid records such as nulls in critical columns, negative quantities, zero prices, and cancellations.
- Creating a revenue column and a few time-based columns.
- Writing cleaned and aggregated outputs back to disk.

## What the ETL does

The pipeline reads the online retail dataset, performs basic cleaning, enriches the data, and writes multiple outputs.

### Cleaning steps

- Trims text fields like `InvoiceNo`, `StockCode`, `Description`, and `Country`.
- Converts `Country` to uppercase for cleaner grouping.
- Parses `InvoiceDate` with `to_timestamp()` using the original date format from the dataset.
- Drops rows only when critical columns are missing by using `dropna(subset=...)`, which is more appropriate than blindly dropping every row with any null value.
- Removes rows where `Quantity <= 0`.
- Removes rows where `UnitPrice <= 0`.
- Removes cancellation rows where `InvoiceNo` starts with `C`.
- Removes duplicate rows.

### Enrichment steps

- Adds `Revenue = Quantity * UnitPrice`.
- Adds `InvoiceYear`, `InvoiceMonth`, `InvoiceDay`, and `InvoiceHour`.

### Outputs

- Cleaned retail dataset written as Parquet.
- Revenue by country written as CSV.
- Top products by revenue written as CSV.

## Project structure

```text
PYSparkETL RetailData/
├── .venv/
├── main.py
├── online-retail-dataset.csv
├── requirements.txt
├── README.md
└── output/
```

## How to run

```bash
source .venv/bin/activate
python main.py
```

## Why this matters in the roadmap

This project is meant to show a complete small win: local Spark setup, reading real data, applying transformations, handling common data-quality issues, and saving outputs in a structured way. It supports the Month 3 roadmap goal of having a Spark ETL project live on GitHub without turning the work into an oversized engineering project too early.

## Deliberately not included

The following parts were intentionally left out to keep the project aligned with the roadmap and focused on Spark understanding instead of premature complexity:

- Docker
- Airflow
- Kafka
- AWS deployment
- CI/CD pipelines
- Full unit and integration test setup
- Large architecture diagrams
- Production monitoring
- Advanced package structure

These topics are important, but they fit better into the later roadmap projects where the goal is production-grade infrastructure rather than a first Spark proof-of-understanding project.

## Tech stack

- Python
- PySpark
- Local file-based CSV input
- Parquet and CSV outputs

## Final note

This project is intentionally simple, but that is the point. It marks the end of my Month 3 Spark milestone and serves as a practical checkpoint before moving into the larger Month 4 topics like Kafka, Airflow, and the first streaming pipeline work.
