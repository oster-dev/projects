# Month 4 Project: Event Stream Pipeline

This is my Month 4 project from my roadmap: an end-to-end event stream pipeline built with Kafka, Airflow, and Postgres. It ingests user events from Kafka, stores them in a raw layer, validates them against clear business rules, routes invalid records to a dead-letter table, and aggregates daily metrics from the validated data.

## What it does

- Reads events from a Kafka topic.
- Writes every incoming event to `raw_events`.
- Validates records and routes them to either `clean_events` or `dead_letter_events`.
- Builds `daily_event_metrics` from the curated layer.


## Architecture

`Kafka Topic -> Airflow DAG -> raw_events -> Validation -> clean_events / dead_letter_events -> daily_event_metrics`

Kafka is the entry point for incoming events. The Airflow DAG orchestrates ingestion, validation, and aggregation. Raw events are stored unchanged in `raw_events`, valid events move into `clean_events`, invalid events are written to `dead_letter_events`, and the final metrics table is built from the curated data.

## Validation rules

The pipeline uses deterministic validation rules:

- Required fields: `event_id`, `event_type`, `user_id`, `event_ts`, `source`.
- Allowed `event_type` values: `click`, `page_view`, `purchase`.
- Allowed `source` values: `web`, `mobile`, `backend`.
- `event_ts` must be parseable.
- `amount` is only allowed for `purchase` events.
- Negative or non-numeric `amount` values are rejected.


## Data model

| Table | Purpose |
| :-- | :-- |
| `raw_events` | Stores the original event payload and Kafka metadata.|
| `clean_events` | Stores validated events in the curated layer.|
| `dead_letter_events` | Stores invalid events with an error reason for later analysis.|
| `daily_event_metrics` | Stores daily aggregates by date, event type, and source.|

## Test case

I verified the pipeline with 4 test events in `dev.user.events.raw.v2`: 2 valid and 2 invalid.

### Results

- `raw_events`: all 4 events were stored.
- `clean_events`: only the 2 valid events were stored.
- `dead_letter_events`: the 2 invalid events were stored with the correct error reasons.
- `daily_event_metrics`: the daily aggregates matched the expected outcome exactly.


## Why this matters

This project demonstrates a practical data and platform engineering workflow: ingestion, validation, failure handling, and aggregation in a reproducible setup. It also shows how Kafka, Airflow, and Postgres can be combined into a clean, testable streaming pipeline.

## Tech stack

- Kafka
- Apache Airflow
- Postgres
- Python
- Docker
- SQL


## Outcome

The pipeline runs end to end and has been tested successfully with both valid and invalid events. That makes it a solid Month 4 milestone in my roadmap for Data / Feature Infrastructure / ML Platform engineering.

