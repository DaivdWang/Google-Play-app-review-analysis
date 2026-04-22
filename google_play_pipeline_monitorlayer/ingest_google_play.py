from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from init_db import initialize_database

try:
    from google_play_scraper import Sort, app as gp_app, reviews as gp_reviews
except ImportError as exc:  # pragma: no cover - runtime dependency check
    raise SystemExit(
        "Missing dependency: google-play-scraper. Install it with: pip install google-play-scraper"
    ) from exc


@dataclass
class AppConfig:
    app_id: str
    country: str
    lang: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return str(value)


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=json_default)


def to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return str(value)


def empty_metrics() -> dict[str, Any]:
    return {
        "new_reviews": 0,
        "existing_reviews": 0,
        "empty_text_count": 0,
        "very_short_count": 0,
        "low_signal_count": 0,
        "missing_score_count": 0,
        "abnormal_score_count": 0,
        "reply_present_count": 0,
        "rating_distribution": {str(i): 0 for i in range(1, 6)},
        "review_length_distribution": {
            "0_4": 0,
            "5_19": 0,
            "20_49": 0,
            "50_99": 0,
            "100_199": 0,
            "200_plus": 0,
        },
    }


def analyze_review_signal(review: dict[str, Any]) -> dict[str, Any]:
    text = (review.get("content") or "").strip()
    score = review.get("score")
    words = len(text.split()) if text else 0
    chars = len(text)

    if chars <= 4:
        length_bucket = "0_4"
    elif chars <= 19:
        length_bucket = "5_19"
    elif chars <= 49:
        length_bucket = "20_49"
    elif chars <= 99:
        length_bucket = "50_99"
    elif chars <= 199:
        length_bucket = "100_199"
    else:
        length_bucket = "200_plus"

    return {
        "is_empty_text": int(chars == 0),
        "is_very_short": int(chars < 5),
        "is_low_signal": int(words <= 2),
        "is_missing_score": int(score is None),
        "is_abnormal_score": int(score is not None and score not in {1, 2, 3, 4, 5}),
        "has_reply": int(bool(review.get("replyContent"))),
        "score": score,
        "length_bucket": length_bucket,
    }


def merge_metrics(total: dict[str, Any], part: dict[str, Any]) -> dict[str, Any]:
    total["new_reviews"] += part["new_reviews"]
    total["existing_reviews"] += part["existing_reviews"]
    total["empty_text_count"] += part["empty_text_count"]
    total["very_short_count"] += part["very_short_count"]
    total["low_signal_count"] += part["low_signal_count"]
    total["missing_score_count"] += part["missing_score_count"]
    total["abnormal_score_count"] += part["abnormal_score_count"]
    total["reply_present_count"] += part["reply_present_count"]

    for k, v in part["rating_distribution"].items():
        total["rating_distribution"][k] += v
    for k, v in part["review_length_distribution"].items():
        total["review_length_distribution"][k] += v

    return total


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def resolve_apps(config: dict[str, Any], cli_app_ids: list[str] | None) -> list[AppConfig]:
    default_country = config.get("default_country", "us")
    default_lang = config.get("default_lang", "en")

    if cli_app_ids:
        return [AppConfig(app_id=app_id, country=default_country, lang=default_lang) for app_id in cli_app_ids]

    apps = []
    for entry in config.get("apps", []):
        apps.append(
            AppConfig(
                app_id=entry["app_id"],
                country=entry.get("country", default_country),
                lang=entry.get("lang", default_lang),
            )
        )
    if not apps:
        raise ValueError("No apps provided. Add apps in config.json or pass --app-id.")
    return apps


class Database:
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")

    def close(self) -> None:
        self.conn.close()

    def get_source_id(self, source_name: str = "google_play") -> int:
        row = self.conn.execute(
            "SELECT source_id FROM data_sources WHERE source_name = ?", (source_name,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Source {source_name!r} not found. Run init_db.py first.")
        return int(row["source_id"])

    def start_run(self, source_id: int, config: dict[str, Any], apps_targeted: int) -> int:
        started_at = utc_now_iso()
        cur = self.conn.execute(
            """
            INSERT INTO ingestion_runs (
                source_id, started_at, status, config_json, apps_targeted
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (source_id, started_at, "running", to_json(config), apps_targeted),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(
        self,
        run_id: int,
        status: str,
        apps_succeeded: int,
        apps_failed: int,
        reviews_processed: int,
        notes: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE ingestion_runs
            SET finished_at = ?, status = ?, apps_succeeded = ?, apps_failed = ?,
                reviews_processed = ?, notes = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), status, apps_succeeded, apps_failed, reviews_processed, notes, run_id),
        )
        self.conn.commit()

    def start_run_item(self, run_id: int, external_app_id: str) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO ingestion_run_items (
                run_id, external_app_id, started_at, status
            ) VALUES (?, ?, ?, ?)
            """,
            (run_id, external_app_id, utc_now_iso(), "running"),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run_item(
        self,
        run_item_id: int,
        app_pk: int | None,
        status: str,
        pages_fetched: int,
        reviews_processed: int,
        existing_pages_before_stop: int,
        error_message: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE ingestion_run_items
            SET app_pk = ?, finished_at = ?, status = ?, pages_fetched = ?,
                reviews_processed = ?, existing_pages_before_stop = ?, error_message = ?
            WHERE run_item_id = ?
            """,
            (
                app_pk,
                utc_now_iso(),
                status,
                pages_fetched,
                reviews_processed,
                existing_pages_before_stop,
                error_message,
                run_item_id,
            ),
        )
        self.conn.commit()

    def upsert_app(self, source_id: int, external_app_id: str, payload: dict[str, Any]) -> int:
        histogram = payload.get("histogram")
        self.conn.execute(
            """
            INSERT INTO apps (
                source_id, external_app_id, title, developer_name, genre, category, url,
                icon_url, summary, description, score, ratings_count, reviews_count,
                installs_text, min_installs, max_installs, price, currency, is_free,
                released_date, last_updated_source, current_version, histogram_json,
                raw_payload, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, external_app_id) DO UPDATE SET
                title = excluded.title,
                developer_name = excluded.developer_name,
                genre = excluded.genre,
                category = excluded.category,
                url = excluded.url,
                icon_url = excluded.icon_url,
                summary = excluded.summary,
                description = excluded.description,
                score = excluded.score,
                ratings_count = excluded.ratings_count,
                reviews_count = excluded.reviews_count,
                installs_text = excluded.installs_text,
                min_installs = excluded.min_installs,
                max_installs = excluded.max_installs,
                price = excluded.price,
                currency = excluded.currency,
                is_free = excluded.is_free,
                released_date = excluded.released_date,
                last_updated_source = excluded.last_updated_source,
                current_version = excluded.current_version,
                histogram_json = excluded.histogram_json,
                raw_payload = excluded.raw_payload,
                last_seen_at = excluded.last_seen_at
            """,
            (
                source_id,
                external_app_id,
                payload.get("title"),
                payload.get("developer"),
                payload.get("genre"),
                payload.get("genreId"),
                payload.get("url"),
                payload.get("icon"),
                payload.get("summary"),
                payload.get("description"),
                payload.get("score"),
                payload.get("ratings"),
                payload.get("reviews"),
                payload.get("installs"),
                payload.get("minInstalls"),
                payload.get("realInstalls") or payload.get("maxInstalls"),
                payload.get("price"),
                payload.get("currency"),
                1 if payload.get("free") else 0,
                payload.get("released"),
                payload.get("updated") or payload.get("lastUpdatedOn"),
                payload.get("version"),
                to_json(histogram) if histogram is not None else None,
                to_json(payload),
                utc_now_iso(),
            ),
        )
        row = self.conn.execute(
            "SELECT app_pk FROM apps WHERE source_id = ? AND external_app_id = ?",
            (source_id, external_app_id),
        ).fetchone()
        self.conn.commit()
        return int(row["app_pk"])

    def insert_app_snapshot(self, app_pk: int, payload: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO app_snapshots (
                app_pk, ingested_at, score, ratings_count, reviews_count, installs_text,
                min_installs, max_installs, current_version, histogram_json, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                app_pk,
                utc_now_iso(),
                payload.get("score"),
                payload.get("ratings"),
                payload.get("reviews"),
                payload.get("installs"),
                payload.get("minInstalls"),
                payload.get("realInstalls") or payload.get("maxInstalls"),
                payload.get("version"),
                to_json(payload.get("histogram")) if payload.get("histogram") is not None else None,
                to_json(payload),
            ),
        )
        self.conn.commit()

    def review_exists(self, source_id: int, external_review_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM reviews WHERE source_id = ? AND external_review_id = ?",
            (source_id, external_review_id),
        ).fetchone()
        return row is not None

    def upsert_review(
        self,
        source_id: int,
        app_pk: int,
        external_app_id: str,
        lang_code: str,
        country_code: str,
        review: dict[str, Any],
    ) -> tuple[int, bool]:
        external_review_id = review["reviewId"]
        existed_before = self.review_exists(source_id, external_review_id)
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO reviews (
                source_id, app_pk, external_review_id, user_name, user_image_url,
                review_text, score, thumbs_up_count, review_created_version,
                app_version, reviewed_at, lang_code, country_code, raw_payload,
                first_seen_at, last_seen_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(source_id, external_review_id) DO UPDATE SET
                app_pk = excluded.app_pk,
                user_name = excluded.user_name,
                user_image_url = excluded.user_image_url,
                review_text = excluded.review_text,
                score = excluded.score,
                thumbs_up_count = excluded.thumbs_up_count,
                review_created_version = excluded.review_created_version,
                app_version = excluded.app_version,
                reviewed_at = excluded.reviewed_at,
                lang_code = excluded.lang_code,
                country_code = excluded.country_code,
                raw_payload = excluded.raw_payload,
                last_seen_at = excluded.last_seen_at,
                is_active = 1
            """,
            (
                source_id,
                app_pk,
                external_review_id,
                review.get("userName"),
                review.get("userImage"),
                review.get("content"),
                review.get("score"),
                review.get("thumbsUpCount"),
                review.get("reviewCreatedVersion"),
                review.get("appVersion"),
                to_iso(review.get("at")),
                lang_code,
                country_code,
                to_json({**review, "externalAppId": external_app_id}),
                now,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT review_pk FROM reviews WHERE source_id = ? AND external_review_id = ?",
            (source_id, external_review_id),
        ).fetchone()
        review_pk = int(row["review_pk"])

        reply_text = review.get("replyContent")
        if reply_text:
            self.conn.execute(
                """
                INSERT INTO review_replies (
                    review_pk, reply_text, replied_at, raw_payload, last_seen_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(review_pk) DO UPDATE SET
                    reply_text = excluded.reply_text,
                    replied_at = excluded.replied_at,
                    raw_payload = excluded.raw_payload,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    review_pk,
                    reply_text,
                    to_iso(review.get("repliedAt")),
                    to_json(
                        {
                            "replyContent": reply_text,
                            "repliedAt": to_iso(review.get("repliedAt")),
                            "externalReviewId": external_review_id,
                        }
                    ),
                    now,
                ),
            )
        else:
            self.conn.execute("DELETE FROM review_replies WHERE review_pk = ?", (review_pk,))

        self.conn.commit()
        return review_pk, existed_before


    def insert_run_item_metrics(
        self,
        run_item_id: int,
        runtime_seconds: float,
        metrics: dict[str, Any],
    ) -> None:
        processed = metrics["new_reviews"] + metrics["existing_reviews"]
        duplicate_rate = (metrics["existing_reviews"] / processed) if processed else None
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO ingestion_run_item_metrics (
                run_item_id, runtime_seconds, new_reviews, existing_reviews, duplicate_rate,
                empty_text_count, very_short_count, low_signal_count, missing_score_count,
                abnormal_score_count, reply_present_count, rating_distribution_json,
                review_length_distribution_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_item_id) DO UPDATE SET
                runtime_seconds = excluded.runtime_seconds,
                new_reviews = excluded.new_reviews,
                existing_reviews = excluded.existing_reviews,
                duplicate_rate = excluded.duplicate_rate,
                empty_text_count = excluded.empty_text_count,
                very_short_count = excluded.very_short_count,
                low_signal_count = excluded.low_signal_count,
                missing_score_count = excluded.missing_score_count,
                abnormal_score_count = excluded.abnormal_score_count,
                reply_present_count = excluded.reply_present_count,
                rating_distribution_json = excluded.rating_distribution_json,
                review_length_distribution_json = excluded.review_length_distribution_json,
                updated_at = excluded.updated_at
            """,
            (
                run_item_id,
                runtime_seconds,
                metrics["new_reviews"],
                metrics["existing_reviews"],
                duplicate_rate,
                metrics["empty_text_count"],
                metrics["very_short_count"],
                metrics["low_signal_count"],
                metrics["missing_score_count"],
                metrics["abnormal_score_count"],
                metrics["reply_present_count"],
                to_json(metrics["rating_distribution"]),
                to_json(metrics["review_length_distribution"]),
                now,
                now,
            ),
        )
        self.conn.commit()

    def insert_run_metrics(
        self,
        run_id: int,
        runtime_seconds: float,
        metrics: dict[str, Any],
    ) -> None:
        processed = metrics["new_reviews"] + metrics["existing_reviews"]
        duplicate_rate = (metrics["existing_reviews"] / processed) if processed else None
        empty_text_rate = (metrics["empty_text_count"] / processed) if processed else None
        low_signal_rate = (metrics["low_signal_count"] / processed) if processed else None
        missing_score_rate = (metrics["missing_score_count"] / processed) if processed else None
        abnormal_score_rate = (metrics["abnormal_score_count"] / processed) if processed else None
        reply_present_rate = (metrics["reply_present_count"] / processed) if processed else None
        throughput = (processed / runtime_seconds) if runtime_seconds > 0 else None
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO ingestion_run_metrics (
                run_id, runtime_seconds, throughput_reviews_per_second,
                new_reviews, existing_reviews, duplicate_rate, empty_text_rate,
                low_signal_rate, missing_score_rate, abnormal_score_rate,
                reply_present_rate, rating_distribution_json,
                review_length_distribution_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                runtime_seconds = excluded.runtime_seconds,
                throughput_reviews_per_second = excluded.throughput_reviews_per_second,
                new_reviews = excluded.new_reviews,
                existing_reviews = excluded.existing_reviews,
                duplicate_rate = excluded.duplicate_rate,
                empty_text_rate = excluded.empty_text_rate,
                low_signal_rate = excluded.low_signal_rate,
                missing_score_rate = excluded.missing_score_rate,
                abnormal_score_rate = excluded.abnormal_score_rate,
                reply_present_rate = excluded.reply_present_rate,
                rating_distribution_json = excluded.rating_distribution_json,
                review_length_distribution_json = excluded.review_length_distribution_json,
                updated_at = excluded.updated_at
            """,
            (
                run_id,
                runtime_seconds,
                throughput,
                metrics["new_reviews"],
                metrics["existing_reviews"],
                duplicate_rate,
                empty_text_rate,
                low_signal_rate,
                missing_score_rate,
                abnormal_score_rate,
                reply_present_rate,
                to_json(metrics["rating_distribution"]),
                to_json(metrics["review_length_distribution"]),
                now,
                now,
            ),
        )
        self.conn.commit()

    def insert_alert(
        self,
        run_id: int,
        run_item_id: int | None,
        alert_level: str,
        metric_name: str,
        metric_value: float | None,
        threshold_value: float | None,
        message: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO monitoring_alerts (
                run_id, run_item_id, alert_level, metric_name,
                metric_value, threshold_value, message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, run_item_id, alert_level, metric_name, metric_value, threshold_value, message, utc_now_iso()),
        )
        self.conn.commit()


class GooglePlayCollector:
    def __init__(self, sleep_seconds: float = 0.5):
        self.sleep_seconds = sleep_seconds

    def fetch_app_details(self, app_id: str, lang: str, country: str) -> dict[str, Any]:
        return gp_app(app_id, lang=lang, country=country)

    def fetch_review_page(
        self,
        app_id: str,
        lang: str,
        country: str,
        page_size: int,
        continuation_token: Any = None,
    ) -> tuple[list[dict[str, Any]], Any]:
        items, token = gp_reviews(
            app_id,
            lang=lang,
            country=country,
            sort=Sort.NEWEST,
            count=page_size,
            continuation_token=continuation_token,
        )
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)
        return items, token


def ingest_one_app(
    db: Database,
    collector: GooglePlayCollector,
    source_id: int,
    run_id: int,
    app_cfg: AppConfig,
    page_size: int,
    max_pages: int,
    stop_after_existing_pages: int,
) -> tuple[bool, int, dict[str, Any]]:
    run_item_id = db.start_run_item(run_id, app_cfg.app_id)
    pages_fetched = 0
    reviews_processed = 0
    existing_page_streak = 0
    app_pk: int | None = None
    item_started = time.perf_counter()
    metrics = empty_metrics()

    try:
        app_payload = collector.fetch_app_details(app_cfg.app_id, app_cfg.lang, app_cfg.country)
        app_pk = db.upsert_app(source_id, app_cfg.app_id, app_payload)
        db.insert_app_snapshot(app_pk, app_payload)

        continuation_token = None
        for _ in range(max_pages):
            page, continuation_token = collector.fetch_review_page(
                app_id=app_cfg.app_id,
                lang=app_cfg.lang,
                country=app_cfg.country,
                page_size=page_size,
                continuation_token=continuation_token,
            )
            if not page:
                break

            pages_fetched += 1
            page_existing_count = 0
            for review in page:
                _, existed_before = db.upsert_review(
                    source_id=source_id,
                    app_pk=app_pk,
                    external_app_id=app_cfg.app_id,
                    lang_code=app_cfg.lang,
                    country_code=app_cfg.country,
                    review=review,
                )
                signal = analyze_review_signal(review)

                reviews_processed += 1
                if existed_before:
                    metrics["existing_reviews"] += 1
                    page_existing_count += 1
                else:
                    metrics["new_reviews"] += 1

                metrics["empty_text_count"] += signal["is_empty_text"]
                metrics["very_short_count"] += signal["is_very_short"]
                metrics["low_signal_count"] += signal["is_low_signal"]
                metrics["missing_score_count"] += signal["is_missing_score"]
                metrics["abnormal_score_count"] += signal["is_abnormal_score"]
                metrics["reply_present_count"] += signal["has_reply"]
                metrics["review_length_distribution"][signal["length_bucket"]] += 1

                score = signal["score"]
                if score in {1, 2, 3, 4, 5}:
                    metrics["rating_distribution"][str(score)] += 1

            if page_existing_count == len(page):
                existing_page_streak += 1
            else:
                existing_page_streak = 0

            if stop_after_existing_pages > 0 and existing_page_streak >= stop_after_existing_pages:
                break

            if continuation_token is None:
                break

        runtime_seconds = time.perf_counter() - item_started
        db.finish_run_item(
            run_item_id=run_item_id,
            app_pk=app_pk,
            status="success",
            pages_fetched=pages_fetched,
            reviews_processed=reviews_processed,
            existing_pages_before_stop=existing_page_streak,
        )
        db.insert_run_item_metrics(
            run_item_id=run_item_id,
            runtime_seconds=runtime_seconds,
            metrics=metrics,
        )

        processed = metrics["new_reviews"] + metrics["existing_reviews"]
        duplicate_rate = (metrics["existing_reviews"] / processed) if processed else 0.0
        low_signal_rate = (metrics["low_signal_count"] / processed) if processed else 0.0

        if duplicate_rate > 0.80:
            db.insert_alert(
                run_id=run_id,
                run_item_id=run_item_id,
                alert_level="warning",
                metric_name="duplicate_rate",
                metric_value=duplicate_rate,
                threshold_value=0.80,
                message=f"High duplicate rate for app {app_cfg.app_id}",
            )

        if low_signal_rate > 0.35:
            db.insert_alert(
                run_id=run_id,
                run_item_id=run_item_id,
                alert_level="warning",
                metric_name="low_signal_rate",
                metric_value=low_signal_rate,
                threshold_value=0.35,
                message=f"High low-signal review share for app {app_cfg.app_id}",
            )

        return True, reviews_processed, metrics
    except Exception as exc:
        runtime_seconds = time.perf_counter() - item_started
        db.finish_run_item(
            run_item_id=run_item_id,
            app_pk=app_pk,
            status="failed",
            pages_fetched=pages_fetched,
            reviews_processed=reviews_processed,
            existing_pages_before_stop=existing_page_streak,
            error_message=str(exc),
        )
        if metrics["new_reviews"] + metrics["existing_reviews"] > 0:
            db.insert_run_item_metrics(
                run_item_id=run_item_id,
                runtime_seconds=runtime_seconds,
                metrics=metrics,
            )
        return False, reviews_processed, metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally ingest Google Play app metadata and reviews into SQLite.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--config", required=True, help="Path to config JSON file.")
    parser.add_argument("--app-id", action="append", help="Optional app ID(s) to ingest; overrides config apps list.")
    parser.add_argument("--page-size", type=int, default=None, help="Reviews per page, max recommended 200.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum review pages to fetch per app.")
    parser.add_argument(
        "--stop-after-existing-pages",
        type=int,
        default=None,
        help="Stop after this many fully-known pages in a row. Good for incremental refreshes.",
    )
    parser.add_argument("--sleep-seconds", type=float, default=None, help="Delay between review page requests.")
    args = parser.parse_args()

    db_path = Path(args.db)
    config_path = Path(args.config)
    config = load_config(config_path)

    initialize_database(db_path)
    db = Database(db_path)
    source_id = db.get_source_id("google_play")

    apps = resolve_apps(config, args.app_id)
    page_size = args.page_size or int(config.get("page_size", 100))
    max_pages = args.max_pages or int(config.get("max_pages", 10))
    stop_after_existing_pages = args.stop_after_existing_pages
    if stop_after_existing_pages is None:
        stop_after_existing_pages = int(config.get("stop_after_existing_pages", 2))
    sleep_seconds = args.sleep_seconds if args.sleep_seconds is not None else float(config.get("sleep_seconds", 0.5))

    collector = GooglePlayCollector(sleep_seconds=sleep_seconds)

    run_config = {
        "apps": [app.__dict__ for app in apps],
        "page_size": page_size,
        "max_pages": max_pages,
        "stop_after_existing_pages": stop_after_existing_pages,
        "sleep_seconds": sleep_seconds,
    }
    run_id = db.start_run(source_id=source_id, config=run_config, apps_targeted=len(apps))

    run_started = time.perf_counter()
    run_metrics = empty_metrics()

    apps_succeeded = 0
    apps_failed = 0
    total_reviews_processed = 0

    try:
        for app_cfg in apps:
            success, review_count, app_metrics = ingest_one_app(
                db=db,
                collector=collector,
                source_id=source_id,
                run_id=run_id,
                app_cfg=app_cfg,
                page_size=page_size,
                max_pages=max_pages,
                stop_after_existing_pages=stop_after_existing_pages,
            )
            run_metrics = merge_metrics(run_metrics, app_metrics)
            total_reviews_processed += review_count
            if success:
                apps_succeeded += 1
            else:
                apps_failed += 1

        run_runtime_seconds = time.perf_counter() - run_started
        db.insert_run_metrics(
            run_id=run_id,
            runtime_seconds=run_runtime_seconds,
            metrics=run_metrics,
        )

        processed = run_metrics["new_reviews"] + run_metrics["existing_reviews"]
        duplicate_rate = (run_metrics["existing_reviews"] / processed) if processed else 0.0
        low_signal_rate = (run_metrics["low_signal_count"] / processed) if processed else 0.0

        if duplicate_rate > 0.80:
            db.insert_alert(
                run_id=run_id,
                run_item_id=None,
                alert_level="warning",
                metric_name="duplicate_rate",
                metric_value=duplicate_rate,
                threshold_value=0.80,
                message="High duplicate rate for overall run",
            )

        if low_signal_rate > 0.35:
            db.insert_alert(
                run_id=run_id,
                run_item_id=None,
                alert_level="warning",
                metric_name="low_signal_rate",
                metric_value=low_signal_rate,
                threshold_value=0.35,
                message="High low-signal review share for overall run",
            )

        status = "success" if apps_failed == 0 else "partial_success"
        db.finish_run(
            run_id=run_id,
            status=status,
            apps_succeeded=apps_succeeded,
            apps_failed=apps_failed,
            reviews_processed=total_reviews_processed,
        )
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "status": status,
                    "apps_targeted": len(apps),
                    "apps_succeeded": apps_succeeded,
                    "apps_failed": apps_failed,
                    "reviews_processed": total_reviews_processed,
                    "runtime_seconds": round(run_runtime_seconds, 3),
                    "new_reviews": run_metrics["new_reviews"],
                    "existing_reviews": run_metrics["existing_reviews"],
                    "duplicate_rate": round(duplicate_rate, 4) if processed else None,
                    "low_signal_rate": round(low_signal_rate, 4) if processed else None,
                },
                indent=2,
            )
        )
    except Exception as exc:
        db.finish_run(
            run_id=run_id,
            status="failed",
            apps_succeeded=apps_succeeded,
            apps_failed=apps_failed + 1,
            reviews_processed=total_reviews_processed,
            notes=str(exc),
        )
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
