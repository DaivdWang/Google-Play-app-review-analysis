# Google Play Pipeline Bundle

This bundle includes a Google Play ingestion pipeline with a basic monitoring layer.

## Files

- `ingest_google_play.py` — monitored ingestion script
- `init_db.py` — initializes the SQLite database from `schema.sql`
- `schema.sql` — schema with monitoring tables included
- `config.example.json` — example run config
- `data/` — optional place to store the SQLite database

## Install dependency

```bash
pip install google-play-scraper
```

## Prepare config

Copy the example config and edit the app IDs as needed:

```bash
cp config.example.json config.json
```

## Initialize database

```bash
python init_db.py --db data/google_play_reviews.db --schema schema.sql
```

## Run ingestion

```bash
python ingest_google_play.py --db data/google_play_reviews.db --config config.json
```

## What monitoring is included

The pipeline writes monitoring data into these tables:

- `ingestion_run_metrics`
- `ingestion_run_item_metrics`
- `monitoring_alerts`

It tracks:

- ingestion success/failure
- total reviews processed
- new vs existing reviews
- duplicate rate
- empty / very short / low-signal review counts
- rating distribution
- review length distribution
- runtime and throughput

## Quick checks in SQLite

```bash
sqlite3 data/google_play_reviews.db
```

```sql
.tables

SELECT * FROM ingestion_run_metrics
ORDER BY run_id DESC
LIMIT 5;

SELECT * FROM ingestion_run_item_metrics
ORDER BY run_item_id DESC
LIMIT 10;

SELECT * FROM monitoring_alerts
ORDER BY alert_id DESC
LIMIT 10;
```
