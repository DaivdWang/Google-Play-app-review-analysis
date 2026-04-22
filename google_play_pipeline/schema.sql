PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS data_sources (
    source_id INTEGER PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS apps (
    app_pk INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    external_app_id TEXT NOT NULL,
    title TEXT,
    developer_name TEXT,
    genre TEXT,
    category TEXT,
    url TEXT,
    icon_url TEXT,
    summary TEXT,
    description TEXT,
    score REAL,
    ratings_count INTEGER,
    reviews_count INTEGER,
    installs_text TEXT,
    min_installs INTEGER,
    max_installs INTEGER,
    price REAL,
    currency TEXT,
    is_free INTEGER,
    released_date TEXT,
    last_updated_source TEXT,
    current_version TEXT,
    histogram_json TEXT,
    raw_payload TEXT,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_id, external_app_id),
    FOREIGN KEY (source_id) REFERENCES data_sources(source_id)
);

CREATE TABLE IF NOT EXISTS app_snapshots (
    snapshot_id INTEGER PRIMARY KEY,
    app_pk INTEGER NOT NULL,
    ingested_at TEXT NOT NULL,
    score REAL,
    ratings_count INTEGER,
    reviews_count INTEGER,
    installs_text TEXT,
    min_installs INTEGER,
    max_installs INTEGER,
    current_version TEXT,
    histogram_json TEXT,
    raw_payload TEXT,
    FOREIGN KEY (app_pk) REFERENCES apps(app_pk) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    config_json TEXT,
    apps_targeted INTEGER NOT NULL DEFAULT 0,
    apps_succeeded INTEGER NOT NULL DEFAULT 0,
    apps_failed INTEGER NOT NULL DEFAULT 0,
    reviews_processed INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (source_id) REFERENCES data_sources(source_id)
);

CREATE TABLE IF NOT EXISTS ingestion_run_items (
    run_item_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    app_pk INTEGER,
    external_app_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    pages_fetched INTEGER NOT NULL DEFAULT 0,
    reviews_processed INTEGER NOT NULL DEFAULT 0,
    existing_pages_before_stop INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    FOREIGN KEY (run_id) REFERENCES ingestion_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (app_pk) REFERENCES apps(app_pk)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_pk INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    app_pk INTEGER NOT NULL,
    external_review_id TEXT NOT NULL,
    user_name TEXT,
    user_image_url TEXT,
    review_text TEXT,
    score INTEGER CHECK (score BETWEEN 1 AND 5),
    thumbs_up_count INTEGER,
    review_created_version TEXT,
    app_version TEXT,
    reviewed_at TEXT,
    lang_code TEXT,
    country_code TEXT,
    raw_payload TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    UNIQUE(source_id, external_review_id),
    FOREIGN KEY (source_id) REFERENCES data_sources(source_id),
    FOREIGN KEY (app_pk) REFERENCES apps(app_pk) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS review_replies (
    reply_pk INTEGER PRIMARY KEY,
    review_pk INTEGER NOT NULL UNIQUE,
    reply_text TEXT NOT NULL,
    replied_at TEXT,
    raw_payload TEXT,
    last_seen_at TEXT NOT NULL,
    FOREIGN KEY (review_pk) REFERENCES reviews(review_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_apps_source_external_id
    ON apps(source_id, external_app_id);

CREATE INDEX IF NOT EXISTS idx_reviews_app_reviewed_at
    ON reviews(app_pk, reviewed_at DESC);

CREATE INDEX IF NOT EXISTS idx_reviews_app_score
    ON reviews(app_pk, score);

CREATE INDEX IF NOT EXISTS idx_reviews_last_seen
    ON reviews(last_seen_at);

CREATE INDEX IF NOT EXISTS idx_app_snapshots_app_ingested
    ON app_snapshots(app_pk, ingested_at DESC);

CREATE VIEW IF NOT EXISTS vw_latest_app_snapshot AS
SELECT s.*
FROM app_snapshots s
JOIN (
    SELECT app_pk, MAX(ingested_at) AS max_ingested_at
    FROM app_snapshots
    GROUP BY app_pk
) latest
  ON s.app_pk = latest.app_pk
 AND s.ingested_at = latest.max_ingested_at;


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
