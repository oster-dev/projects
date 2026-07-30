CREATE TABLE IF NOT EXISTS clean_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT,
    event_ts TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    source TEXT NOT NULL,
    page TEXT,
    product_id TEXT,
    amount NUMERIC(12,2),
    ingestion_ts TIMESTAMPTZ NOT NULL,
    transformed_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);