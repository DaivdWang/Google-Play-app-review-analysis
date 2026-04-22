-- Append these tables/indexes to schema.sql

CREATE TABLE IF NOT EXISTS ingestion_run_item_metrics (
    run_item_id INTEGER PRIMARY KEY,
    runtime_seconds REAL,
    new_reviews INTEGER NOT NULL DEFAULT 0,
    existing_reviews INTEGER NOT NULL DEFAULT 0,
    duplicate_rate REAL,
    empty_text_count INTEGER NOT NULL DEFAULT 0,
    very_short_count INTEGER NOT NULL DEFAULT 0,
    low_signal_count INTEGER NOT NULL DEFAULT 0,
    missing_score_count INTEGER NOT NULL DEFAULT 0,
    abnormal_score_count INTEGER NOT NULL DEFAULT 0,
    reply_present_count INTEGER NOT NULL DEFAULT 0,
    rating_distribution_json TEXT,
    review_length_distribution_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(run_item_id) REFERENCES ingestion_run_items(run_item_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ingestion_run_metrics (
    run_id INTEGER PRIMARY KEY,
    runtime_seconds REAL,
    throughput_reviews_per_second REAL,
    new_reviews INTEGER NOT NULL DEFAULT 0,
    existing_reviews INTEGER NOT NULL DEFAULT 0,
    duplicate_rate REAL,
    empty_text_rate REAL,
    low_signal_rate REAL,
    missing_score_rate REAL,
    abnormal_score_rate REAL,
    reply_present_rate REAL,
    rating_distribution_json TEXT,
    review_length_distribution_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES ingestion_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS monitoring_alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    run_item_id INTEGER,
    alert_level TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL,
    threshold_value REAL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES ingestion_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY(run_item_id) REFERENCES ingestion_run_items(run_item_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_run_item_metrics_runtime
    ON ingestion_run_item_metrics(runtime_seconds);

CREATE INDEX IF NOT EXISTS idx_run_metrics_runtime
    ON ingestion_run_metrics(runtime_seconds);

CREATE INDEX IF NOT EXISTS idx_monitoring_alerts_run_id
    ON monitoring_alerts(run_id);

CREATE INDEX IF NOT EXISTS idx_monitoring_alerts_run_item_id
    ON monitoring_alerts(run_item_id);
