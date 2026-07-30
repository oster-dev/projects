CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    event_id TEXT,
    event_type TEXT,
    user_id TEXT,
    session_id TEXT,
    event_ts TIMESTAMPTZ,
    source TEXT,
    payload_json JSONB NOT NULL,
    ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_record JSONB NOT NULL,
    CONSTRAINT raw_events_topic_partition_offset_uk
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);