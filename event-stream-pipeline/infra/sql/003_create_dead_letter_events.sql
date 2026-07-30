CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INT,
    kafka_offset BIGINT,
    event_id TEXT,
    error_reason TEXT NOT NULL,
    raw_record JSONB NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT dead_letter_topic_partition_offset_uk
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);