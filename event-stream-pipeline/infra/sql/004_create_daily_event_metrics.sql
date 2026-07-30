CREATE TABLE IF NOT EXISTS daily_event_metrics (
    metric_date DATE NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    total_events BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    total_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, event_type, source)
);