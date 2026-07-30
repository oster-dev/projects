from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from kafka import KafkaConsumer
from psycopg2.extras import Json

KAFKA_TOPIC = "dev.user.events.raw.v2"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

POSTGRES_CONN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

SQL_DIR = Path("/opt/airflow/infra/sql")

VALID_EVENT_TYPES = {"click", "page_view", "purchase"}
VALID_SOURCES = {"web", "mobile", "backend"}


def parse_amount(value):
    if value is None or value == "":
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return "INVALID"
    if amount < 0:
        return "INVALID"
    return amount


def validate_record(record):
    payload = record["payload_json"]

    required_fields = ["event_id", "event_type", "user_id", "event_ts", "source"]
    for field in required_fields:
        if not payload.get(field):
            return False, f"missing_required_field:{field}"

    if payload["event_type"] not in VALID_EVENT_TYPES:
        return False, f"invalid_event_type:{payload['event_type']}"

    if payload["source"] not in VALID_SOURCES:
        return False, f"invalid_source:{payload['source']}"

    try:
        datetime.fromisoformat(payload["event_ts"].replace("Z", "+00:00"))
    except Exception:
        return False, "invalid_event_ts"

    amount = parse_amount(payload.get("amount"))
    if amount == "INVALID":
        return False, "invalid_amount"

    if payload["event_type"] != "purchase" and payload.get("amount") not in (None, ""):
        return False, "amount_only_allowed_for_purchase"

    return True, None


@dag(
    dag_id="event_stream_pipeline",
    start_date=datetime(2026, 7, 29, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["event-stream", "kafka", "airflow"],
)
def event_stream_pipeline():
    @task
    def create_tables():
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        sql_files = [
            "001_create_raw_events.sql",
            "002_create_clean_events.sql",
            "003_create_dead_letter_events.sql",
            "004_create_daily_event_metrics.sql",
        ]

        for name in sql_files:
            sql_path = SQL_DIR / name
            with open(sql_path, "r", encoding="utf-8") as f:
                cur.execute(f.read())

        conn.commit()
        cur.close()
        conn.close()
        return "tables-ready"

    @task
    def extract_batch_from_kafka():
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="event_stream_pipeline_consumer",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        records = []

        for message in consumer:
            payload = message.value
            records.append(
                {
                    "kafka_topic": message.topic,
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                    "event_id": payload.get("event_id"),
                    "event_type": payload.get("event_type"),
                    "user_id": payload.get("user_id"),
                    "session_id": payload.get("session_id"),
                    "event_ts": payload.get("event_ts"),
                    "source": payload.get("source"),
                    "payload_json": payload,
                    "raw_record": payload,
                }
            )

        consumer.close()
        return records

    @task
    def load_raw_events(records):
        if not records:
            return 0

        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        insert_sql = """
        INSERT INTO raw_events (
            kafka_topic,
            kafka_partition,
            kafka_offset,
            event_id,
            event_type,
            user_id,
            session_id,
            event_ts,
            source,
            payload_json,
            raw_record
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::timestamptz, %s, %s, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """

        inserted = 0

        for record in records:
            cur.execute(
                insert_sql,
                (
                    record["kafka_topic"],
                    record["kafka_partition"],
                    record["kafka_offset"],
                    record["event_id"],
                    record["event_type"],
                    record["user_id"],
                    record["session_id"],
                    record["event_ts"],
                    record["source"],
                    Json(record["payload_json"]),
                    Json(record["raw_record"]),
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        return inserted

    @task
    def transform_events():
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                kafka_topic,
                kafka_partition,
                kafka_offset,
                event_id,
                event_type,
                user_id,
                session_id,
                event_ts,
                source,
                payload_json,
                ingestion_ts,
                raw_record
            FROM raw_events
            """
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        clean_records = []
        dead_letter_records = []

        for row in rows:
            record = {
                "kafka_topic": row[0],
                "kafka_partition": row[1],
                "kafka_offset": row[2],
                "event_id": row[3],
                "event_type": row[4],
                "user_id": row[5],
                "session_id": row[6],
                "event_ts": row[7],
                "source": row[8],
                "payload_json": row[9],
                "ingestion_ts": row[10],
                "raw_record": row[11],
            }

            is_valid, error_reason = validate_record(record)
            payload = record["payload_json"]

            if not is_valid:
                dead_letter_records.append(
                    {
                        "kafka_topic": record["kafka_topic"],
                        "kafka_partition": record["kafka_partition"],
                        "kafka_offset": record["kafka_offset"],
                        "event_id": payload.get("event_id"),
                        "error_reason": error_reason,
                        "raw_record": record["raw_record"],
                    }
                )
                continue

            amount = parse_amount(payload.get("amount"))
            clean_records.append(
                {
                    "event_id": payload["event_id"],
                    "event_type": payload["event_type"],
                    "user_id": payload["user_id"],
                    "session_id": payload.get("session_id"),
                    "event_ts": payload["event_ts"],
                    "event_date": payload["event_ts"][:10],
                    "source": payload["source"],
                    "page": payload.get("page"),
                    "product_id": payload.get("product_id"),
                    "amount": str(amount) if amount is not None else None,
                    "ingestion_ts": record["ingestion_ts"],
                }
            )

        return {
            "clean_records": clean_records,
            "dead_letter_records": dead_letter_records,
        }

    @task
    def load_clean_events(transformed):
        records = transformed["clean_records"]
        if not records:
            return 0

        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        insert_sql = """
        INSERT INTO clean_events (
            event_id,
            event_type,
            user_id,
            session_id,
            event_ts,
            event_date,
            source,
            page,
            product_id,
            amount,
            ingestion_ts
        )
        VALUES (%s, %s, %s, %s, %s::timestamptz, %s::date, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
            event_type = EXCLUDED.event_type,
            user_id = EXCLUDED.user_id,
            session_id = EXCLUDED.session_id,
            event_ts = EXCLUDED.event_ts,
            event_date = EXCLUDED.event_date,
            source = EXCLUDED.source,
            page = EXCLUDED.page,
            product_id = EXCLUDED.product_id,
            amount = EXCLUDED.amount,
            ingestion_ts = EXCLUDED.ingestion_ts,
            transformed_ts = NOW()
        """

        inserted = 0

        for record in records:
            cur.execute(
                insert_sql,
                (
                    record["event_id"],
                    record["event_type"],
                    record["user_id"],
                    record["session_id"],
                    record["event_ts"],
                    record["event_date"],
                    record["source"],
                    record["page"],
                    record["product_id"],
                    record["amount"],
                    record["ingestion_ts"],
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        return inserted

    @task
    def load_dead_letter_events(transformed):
        records = transformed["dead_letter_records"]
        if not records:
            return 0

        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        insert_sql = """
        INSERT INTO dead_letter_events (
            kafka_topic,
            kafka_partition,
            kafka_offset,
            event_id,
            error_reason,
            raw_record
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        inserted = 0

        for record in records:
            cur.execute(
                insert_sql,
                (
                    record["kafka_topic"],
                    record["kafka_partition"],
                    record["kafka_offset"],
                    record["event_id"],
                    record["error_reason"],
                    Json(record["raw_record"]),
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        return inserted

    @task
    def build_daily_event_metrics():
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE daily_event_metrics")

        cur.execute(
            """
            INSERT INTO daily_event_metrics (
                metric_date,
                event_type,
                source,
                total_events,
                unique_users,
                total_revenue
            )
            SELECT
                event_date AS metric_date,
                event_type,
                source,
                COUNT(*) AS total_events,
                COUNT(DISTINCT user_id) AS unique_users,
                COALESCE(SUM(amount), 0) AS total_revenue
            FROM clean_events
            GROUP BY event_date, event_type, source
            """
        )

        inserted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return inserted

    tables = create_tables()
    batch = extract_batch_from_kafka()
    raw_loaded = load_raw_events(batch)
    transformed = transform_events()
    clean_loaded = load_clean_events(transformed)
    dead_loaded = load_dead_letter_events(transformed)
    metrics = build_daily_event_metrics()

    tables >> batch >> raw_loaded >> transformed
    transformed >> [clean_loaded, dead_loaded]
    [clean_loaded, dead_loaded] >> metrics


pipeline = event_stream_pipeline()