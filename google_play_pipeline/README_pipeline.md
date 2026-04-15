# Google Play review ingestion pipeline (SQLite)

This module sets up a simple but production-friendly ingestion path:

1. **Source**: Google Play public app metadata and review pages
2. **Collector**: Python scraper client with pagination
3. **Storage**: SQLite for structured normalized storage
4. **Run tracking**: each ingestion job is logged in database tables

## Files

- `schema.sql` — relational schema
- `init_db.py` — initializes SQLite database and source registry
- `ingest_google_play.py` — fetches app metadata + paginated reviews and upserts them
- `config.example.json` — starter config
- `requirements.txt` — Python dependency

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.json
python init_db.py --db app_reviews.db
python ingest_google_play.py --db app_reviews.db --config config.json
```

## What gets stored

### `apps`
Current latest app metadata keyed by `(source_id, external_app_id)`.

### `app_snapshots`
Point-in-time app metrics snapshots so score, installs, or ratings can be trended over time.

### `reviews`
One row per external review ID. Records are **upserted**, not blindly appended, so edited reviews and changed metadata stay current.

### `review_replies`
Developer replies linked 1:1 with reviews.

### `ingestion_runs` and `ingestion_run_items`
Operational lineage for each ingestion execution.

## Incremental strategy

The script fetches reviews sorted by **newest** and stops early after a configurable number of pages where **every review already exists** in the database. This makes routine refreshes much cheaper than full backfills.

For a historical backfill, increase `max_pages` or temporarily set `stop_after_existing_pages` to a high number.

## Example queries

Top recent negative reviews for one app:

```sql
SELECT a.title, r.score, r.reviewed_at, r.review_text
FROM reviews r
JOIN apps a ON a.app_pk = r.app_pk
WHERE a.external_app_id = 'com.spotify.music'
  AND r.score <= 2
ORDER BY r.reviewed_at DESC
LIMIT 20;
```

Latest app snapshot per app:

```sql
SELECT a.external_app_id, a.title, s.score, s.ratings_count, s.reviews_count, s.ingested_at
FROM vw_latest_app_snapshot s
JOIN apps a ON a.app_pk = s.app_pk
ORDER BY s.ingested_at DESC;
```

## Next upgrades

- Add a scheduler (cron / GitHub Actions / Airflow)
- Add sentiment/model output tables downstream from `reviews`
- Add a staging/raw table if you want immutable raw event retention per fetch
- Add Apple App Store collector using the same schema with a second `source_id`
