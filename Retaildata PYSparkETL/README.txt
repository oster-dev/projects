# Retail ETL with PySpark by https://github.com/oster-dev

Small ETL project built with PySpark on an online retail dataset.

RAW - Dataset Link: blob:https://github.com/58609e96-09f4-4e72-8b36-4f7435f4e277

## Goal
Load raw retail transaction data, clean data quality issues, enrich the dataset, and generate analytical outputs.

## Cleaning Rules
- Parsed InvoiceDate to timestamp
- Dropped rows with nulls in critical columns
- Removed cancellations (InvoiceNo starting with C)
- Removed rows with Quantity <= 0
- Removed rows with UnitPrice <= 0
- Removed duplicates

## Enrichment
- Revenue = Quantity * UnitPrice
- Derived InvoiceYear, InvoiceMonth, InvoiceDay, InvoiceHour

## Outputs
- Cleaned retail dataset as Parquet
- Revenue by country as CSV
- Top products as CSV

## Run
```bash
source .venv/bin/activate
python main.py
```