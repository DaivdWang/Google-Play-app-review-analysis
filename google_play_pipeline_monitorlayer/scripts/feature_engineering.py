import argparse
import hashlib
import json
import math
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PIPELINE_VERSION = "feature_engineering_v0.2"


ASPECT_KEYWORDS = {
    "performance": [
        "slow", "lag", "laggy", "freezing", "freeze", "stuck",
        "loading", "takes forever", "delay", "delayed", "buffering",
        "not responding", "too long", "unresponsive"
    ],
    "crash_stability": [
        "crash", "crashes", "crashing", "force close", "force closes",
        "not open", "doesn't open", "wont open", "won't open",
        "bug", "bugs", "glitch", "broken", "error", "errors"
    ],
    "login_account": [
        "login", "log in", "sign in", "signin", "account", "password",
        "verification", "verify", "otp", "cannot access", "locked out",
        "reset password", "authentication"
    ],
    "ui_ux": [
        "ui", "interface", "layout", "design", "button", "screen",
        "navigation", "hard to use", "confusing", "user friendly",
        "interface", "menu", "homepage"
    ],
    "ads_monetization": [
        "ad", "ads", "advertisement", "commercial", "too many ads",
        "premium", "subscription", "paywall", "sponsor", "pop up",
        "popup", "advertising"
    ],
    "notification": [
        "notification", "notifications", "alert", "alerts", "reminder",
        "push notification", "push notifications"
    ],
    "payment_billing": [
        "payment", "billing", "charged", "charge", "refund",
        "subscription", "purchase", "paid", "cancel subscription",
        "money", "price", "pricing"
    ],
    "update_version": [
        "update", "updated", "new version", "latest version",
        "after update", "since update", "new update", "version",
        "downgrade"
    ],
    "content_quality": [
        "content", "video", "music", "song", "recommendation",
        "recommendations", "feed", "search result", "quality",
        "algorithm", "relevant"
    ],
    "privacy_security": [
        "privacy", "security", "data", "permission", "permissions",
        "tracking", "scam", "spam", "hack", "hacked", "safe"
    ],
}


POSITIVE_WORDS = {
    "good", "great", "excellent", "amazing", "awesome", "nice", "love",
    "loved", "best", "perfect", "useful", "easy", "smooth", "fast",
    "helpful", "enjoy", "enjoyed", "recommend", "recommended", "favorite",
    "fantastic", "wonderful"
}

NEGATIVE_WORDS = {
    "bad", "terrible", "awful", "worst", "hate", "hated", "slow",
    "bug", "bugs", "crash", "crashes", "crashing", "broken", "annoying",
    "useless", "poor", "lag", "laggy", "freeze", "freezing", "problem",
    "issue", "issues", "disappointed", "difficult", "confusing", "error",
    "errors", "glitch", "glitches", "trash", "horrible"
}

BOILERPLATE_PHRASES = {
    "good", "great", "nice", "ok", "okay", "bad", "love it", "i love it",
    "excellent", "awesome", "cool", "perfect", "amazing", "best app",
    "worst app", "very good", "very bad", "super", "fine"
}


def table_exists(conn, table_name):
    result = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return result is not None


def table_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def sql_alias_for_candidates(table_alias, existing_cols, candidates, output_alias):
    for col in candidates:
        if col in existing_cols:
            return f"{table_alias}.{col} AS {output_alias}"
    return f"NULL AS {output_alias}"


def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(text):
    text = clean_text(text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def stable_hash(text):
    text = normalize_text(text)
    if not text:
        return ""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def stable_random_unit(value):
    value = str(value)
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def tokenize(text):
    return re.findall(r"[a-zA-Z']+", str(text).lower())


def is_punct_or_symbol_only(text):
    text = clean_text(text)
    if not text:
        return 0
    return int(re.search(r"[A-Za-z0-9]", text) is None)


def has_repeated_char_noise(text):
    text = clean_text(text)
    return int(bool(re.search(r"(.)\1{3,}", text)))


def is_all_caps(text):
    text = clean_text(text)
    letters = re.findall(r"[A-Za-z]", text)
    if len(letters) < 3:
        return 0
    return int("".join(letters).isupper())


def has_non_ascii(text):
    text = clean_text(text)
    return int(any(ord(ch) > 127 for ch in text))


def sentiment_features(text):
    tokens = tokenize(text)
    if not tokens:
        return 0.0, 0.0, "neutral"

    pos_count = sum(1 for token in tokens if token in POSITIVE_WORDS)
    neg_count = sum(1 for token in tokens if token in NEGATIVE_WORDS)

    signal_count = pos_count + neg_count
    polarity = 0.0 if signal_count == 0 else (pos_count - neg_count) / signal_count
    subjectivity = signal_count / len(tokens)

    if polarity > 0.15:
        bucket = "positive"
    elif polarity < -0.15:
        bucket = "negative"
    else:
        bucket = "neutral"

    return round(polarity, 4), round(subjectivity, 4), bucket


def rating_bucket(score):
    if pd.isna(score):
        return "unknown"

    try:
        score = int(score)
    except Exception:
        return "unknown"

    if score <= 2:
        return "negative"
    if score == 3:
        return "neutral"
    if score >= 4:
        return "positive"
    return "unknown"


def rating_sentiment_alignment(score, sentiment_bucket):
    rb = rating_bucket(score)

    if rb == "unknown":
        return "unknown"

    if rb == sentiment_bucket:
        return "aligned"

    if rb == "neutral" or sentiment_bucket == "neutral":
        return "mixed_or_weak"

    return "mismatch"


def keyword_count(text, keywords):
    text = normalize_text(text)
    count = 0

    for keyword in keywords:
        keyword_norm = keyword.lower().strip()

        if " " in keyword_norm or "'" in keyword_norm:
            count += text.count(keyword_norm)
        else:
            pattern = r"\b" + re.escape(keyword_norm) + r"\b"
            count += len(re.findall(pattern, text))

    return count


def extract_aspects(text):
    result = {}
    counts = {}

    for aspect, keywords in ASPECT_KEYWORDS.items():
        count = keyword_count(text, keywords)
        counts[aspect] = count
        result[f"aspect_{aspect}"] = int(count > 0)
        result[f"aspect_{aspect}_keyword_hits"] = count

    detected = [aspect for aspect, count in counts.items() if count > 0]
    total_hits = sum(counts.values())

    if detected:
        ranked = sorted(detected, key=lambda aspect: counts[aspect], reverse=True)
        primary_aspect = ranked[0]
        secondary_aspect = ranked[1] if len(ranked) > 1 else "none"
        aspect_confidence_score = counts[primary_aspect] / total_hits if total_hits else 0.0
    else:
        primary_aspect = "none"
        secondary_aspect = "none"
        aspect_confidence_score = 0.0

    result["primary_aspect"] = primary_aspect
    result["secondary_aspect"] = secondary_aspect
    result["num_aspects_detected"] = len(detected)
    result["aspect_keyword_total_hits"] = total_hits
    result["aspect_confidence_score"] = round(aspect_confidence_score, 4)
    result["aspect_extraction_method"] = "keyword_rules_v0.2"

    return result


def thumbs_up_bucket(value):
    try:
        value = int(value)
    except Exception:
        value = 0

    if value <= 0:
        return "none"
    if value <= 4:
        return "low_1_4"
    if value <= 19:
        return "medium_5_19"
    return "high_20_plus"


def parse_version(version):
    if pd.isna(version):
        return None, None, None

    version = str(version)
    nums = re.findall(r"\d+", version)

    major = int(nums[0]) if len(nums) >= 1 else None
    minor = int(nums[1]) if len(nums) >= 2 else None
    patch = int(nums[2]) if len(nums) >= 3 else None

    return major, minor, patch


def assign_random_split(row):
    key = row.get("external_review_id") or row.get("review_pk")
    value = stable_random_unit(key)

    if value < 0.70:
        return "train"
    if value < 0.85:
        return "validation"
    return "test"


def assign_chronological_split(features):
    features["chronological_split"] = "unknown"
    features["chronological_split_strategy"] = "reviewed_at_by_app_70_15_15_v0.2"

    valid = features[features["reviewed_at_dt"].notna()].copy()

    for _, group in valid.groupby("app_pk", dropna=False):
        sorted_idx = group.sort_values("reviewed_at_dt").index.tolist()
        n = len(sorted_idx)

        if n == 1:
            features.loc[sorted_idx[0], "chronological_split"] = "train"
            continue

        if n == 2:
            features.loc[sorted_idx[0], "chronological_split"] = "train"
            features.loc[sorted_idx[1], "chronological_split"] = "test"
            continue

        train_cut = max(1, int(n * 0.70))
        val_cut = max(train_cut + 1, int(n * 0.85))

        if val_cut >= n:
            val_cut = n - 1

        if train_cut >= val_cut:
            train_cut = val_cut - 1

        for position, idx in enumerate(sorted_idx):
            if position < train_cut:
                split = "train"
            elif position < val_cut:
                split = "validation"
            else:
                split = "test"

            features.loc[idx, "chronological_split"] = split

    return features


def add_duplicate_features(features):
    features["normalized_text_hash"] = features["normalized_text"].apply(stable_hash)

    non_empty = features["normalized_text"] != ""

    global_size = features.groupby("normalized_text")["review_pk"].transform("count")
    within_app_size = features.groupby(["app_pk", "normalized_text"])["review_pk"].transform("count")

    features["duplicate_group_size_global"] = global_size.where(non_empty, 0).astype(int)
    features["duplicate_group_size_within_app"] = within_app_size.where(non_empty, 0).astype(int)

    features["duplicate_text_global_flag"] = (
        (features["duplicate_group_size_global"] > 1) & non_empty
    ).astype(int)

    features["duplicate_text_within_app_flag"] = (
        (features["duplicate_group_size_within_app"] > 1) & non_empty
    ).astype(int)

    return features


def build_features(df):
    features = df.copy()

    for col in ["app_package", "app_name", "source_name", "app_category"]:
        if col not in features.columns:
            features[col] = None

    features["clean_text"] = features["review_text"].apply(clean_text)
    features["normalized_text"] = features["review_text"].apply(normalize_text)

    features["char_count"] = features["clean_text"].apply(len)
    features["word_count"] = features["clean_text"].apply(lambda text: len(text.split()) if text else 0)

    features["is_empty_text"] = (features["word_count"] == 0).astype(int)
    features["single_token_flag"] = (features["word_count"] == 1).astype(int)
    features["two_words_or_less_flag"] = (features["word_count"] <= 2).astype(int)
    features["very_short_chars_lt5_flag"] = (features["char_count"] < 5).astype(int)

    features["punct_or_symbol_only_flag"] = features["clean_text"].apply(is_punct_or_symbol_only)
    features["repeated_char_noise_flag"] = features["clean_text"].apply(has_repeated_char_noise)
    features["all_caps_flag"] = features["clean_text"].apply(is_all_caps)
    features["non_ascii_flag"] = features["clean_text"].apply(has_non_ascii)

    features["boilerplate_praise_flag"] = features["normalized_text"].apply(
        lambda text: int(text in BOILERPLATE_PHRASES)
    )

    low_signal_cols = [
        "is_empty_text",
        "single_token_flag",
        "two_words_or_less_flag",
        "very_short_chars_lt5_flag",
        "punct_or_symbol_only_flag",
        "repeated_char_noise_flag",
        "boilerplate_praise_flag",
    ]

    features["low_signal_flag"] = features[low_signal_cols].max(axis=1)

    sentiment_results = features["clean_text"].apply(sentiment_features)
    features["polarity_score"] = sentiment_results.apply(lambda item: item[0])
    features["subjectivity_score"] = sentiment_results.apply(lambda item: item[1])
    features["sentiment_bucket"] = sentiment_results.apply(lambda item: item[2])

    features["rating_bucket"] = features["score"].apply(rating_bucket)
    features["rating_sentiment_alignment"] = features.apply(
        lambda row: rating_sentiment_alignment(row["score"], row["sentiment_bucket"]),
        axis=1,
    )

    aspect_df = features["clean_text"].apply(extract_aspects).apply(pd.Series)
    features = pd.concat([features, aspect_df], axis=1)

    features = add_duplicate_features(features)

    features["reviewed_at_dt"] = pd.to_datetime(features["reviewed_at"], errors="coerce", utc=True)
    now = pd.Timestamp.now(tz="UTC")

    features["review_age_days"] = (now - features["reviewed_at_dt"]).dt.days
    features["review_month"] = features["reviewed_at_dt"].dt.strftime("%Y-%m")
    features["review_day_of_week"] = features["reviewed_at_dt"].dt.day_name()

    features["thumbs_up_count"] = pd.to_numeric(features["thumbs_up_count"], errors="coerce").fillna(0).astype(int)
    features["has_thumbs_up"] = (features["thumbs_up_count"] > 0).astype(int)
    features["thumbs_up_bucket"] = features["thumbs_up_count"].apply(thumbs_up_bucket)
    features["helpfulness_weight"] = features["thumbs_up_count"].apply(lambda value: round(math.log1p(value), 4))
    features["is_high_helpfulness_review"] = (features["thumbs_up_count"] >= 20).astype(int)

    features["has_app_version"] = features["app_version"].notna().astype(int)
    features["has_review_created_version"] = features["review_created_version"].notna().astype(int)
    features["has_app_package"] = features["app_package"].notna().astype(int)
    features["has_app_name"] = features["app_name"].notna().astype(int)

    parsed_versions = features["review_created_version"].fillna(features["app_version"]).apply(parse_version)
    features["version_major"] = parsed_versions.apply(lambda item: item[0])
    features["version_minor"] = parsed_versions.apply(lambda item: item[1])
    features["version_patch"] = parsed_versions.apply(lambda item: item[2])

    version_key = features["review_created_version"].fillna("unknown")
    features["version_review_count"] = (
        features.assign(version_key=version_key)
        .groupby(["app_pk", "version_key"])["review_pk"]
        .transform("count")
        .astype(int)
    )

    features = assign_chronological_split(features)
    features["random_split_baseline"] = features.apply(assign_random_split, axis=1)

    features["feature_pipeline_version"] = PIPELINE_VERSION
    features["features_created_at"] = datetime.now(timezone.utc).isoformat()

    base_cols = [
        "review_pk",
        "source_id",
        "source_name",
        "app_pk",
        "app_package",
        "app_name",
        "app_category",
        "external_review_id",
        "review_text",
        "clean_text",
        "normalized_text",
        "normalized_text_hash",
        "score",
        "rating_bucket",
        "thumbs_up_count",
        "thumbs_up_bucket",
        "helpfulness_weight",
        "is_high_helpfulness_review",
        "review_created_version",
        "app_version",
        "version_major",
        "version_minor",
        "version_patch",
        "version_review_count",
        "reviewed_at",
        "review_month",
        "review_day_of_week",
        "review_age_days",
        "lang_code",
        "country_code",
        "char_count",
        "word_count",
        "is_empty_text",
        "single_token_flag",
        "two_words_or_less_flag",
        "very_short_chars_lt5_flag",
        "punct_or_symbol_only_flag",
        "repeated_char_noise_flag",
        "all_caps_flag",
        "non_ascii_flag",
        "boilerplate_praise_flag",
        "low_signal_flag",
        "duplicate_text_global_flag",
        "duplicate_text_within_app_flag",
        "duplicate_group_size_global",
        "duplicate_group_size_within_app",
        "polarity_score",
        "subjectivity_score",
        "sentiment_bucket",
        "rating_sentiment_alignment",
        "primary_aspect",
        "secondary_aspect",
        "num_aspects_detected",
        "aspect_keyword_total_hits",
        "aspect_confidence_score",
        "aspect_extraction_method",
        "has_thumbs_up",
        "has_app_version",
        "has_review_created_version",
        "has_app_package",
        "has_app_name",
        "chronological_split",
        "chronological_split_strategy",
        "random_split_baseline",
        "feature_pipeline_version",
        "features_created_at",
    ]

    aspect_cols = sorted([col for col in features.columns if col.startswith("aspect_")])
    output_cols = []

    for col in base_cols + aspect_cols:
        if col in features.columns and col not in output_cols:
            output_cols.append(col)

    return features[output_cols]


def load_reviews(db_path, include_inactive=False):
    with sqlite3.connect(db_path) as conn:
        if not table_exists(conn, "reviews"):
            raise ValueError("The database does not contain a reviews table.")

        app_selects = [
            "NULL AS app_package",
            "NULL AS app_name",
            "NULL AS app_category",
        ]
        app_join = ""

        if table_exists(conn, "apps"):
            app_cols = table_columns(conn, "apps")

            if "app_pk" in app_cols:
                app_join = "LEFT JOIN apps a ON r.app_pk = a.app_pk"
                app_selects = [
                    sql_alias_for_candidates(
                        "a",
                        app_cols,
                        ["app_package", "package_name", "package", "package_id", "store_app_id", "external_app_id"],
                        "app_package",
                    ),
                    sql_alias_for_candidates(
                        "a",
                        app_cols,
                        ["app_name", "name", "title", "display_name"],
                        "app_name",
                    ),
                    sql_alias_for_candidates(
                        "a",
                        app_cols,
                        ["app_category", "category", "genre", "category_name"],
                        "app_category",
                    ),
                ]

        source_select = "NULL AS source_name"
        source_join = ""

        if table_exists(conn, "data_sources"):
            source_cols = table_columns(conn, "data_sources")

            if "source_id" in source_cols:
                source_join = "LEFT JOIN data_sources ds ON r.source_id = ds.source_id"
                source_select = sql_alias_for_candidates(
                    "ds",
                    source_cols,
                    ["source_name", "name", "platform", "source_type"],
                    "source_name",
                )

        where_clause = "" if include_inactive else "WHERE r.is_active = 1"

        query = f"""
            SELECT
                r.review_pk,
                r.source_id,
                {source_select},
                r.app_pk,
                {app_selects[0]},
                {app_selects[1]},
                {app_selects[2]},
                r.external_review_id,
                r.review_text,
                r.score,
                r.thumbs_up_count,
                r.review_created_version,
                r.app_version,
                r.reviewed_at,
                r.lang_code,
                r.country_code,
                r.first_seen_at,
                r.last_seen_at,
                r.is_active
            FROM reviews r
            {app_join}
            {source_join}
            {where_clause}
        """

        return pd.read_sql_query(query, conn)


def build_validation_summary_df(features):
    metrics = {
        "pipeline_version": PIPELINE_VERSION,
        "total_reviews": int(len(features)),
        "low_signal_rate": round(float(features["low_signal_flag"].mean()), 4),
        "duplicate_text_global_rate": round(float(features["duplicate_text_global_flag"].mean()), 4),
        "duplicate_text_within_app_rate": round(float(features["duplicate_text_within_app_flag"].mean()), 4),
        "avg_word_count": round(float(features["word_count"].mean()), 2),
        "median_word_count": round(float(features["word_count"].median()), 2),
        "negative_sentiment_rate": round(float((features["sentiment_bucket"] == "negative").mean()), 4),
        "rating_sentiment_mismatch_rate": round(float((features["rating_sentiment_alignment"] == "mismatch").mean()), 4),
        "review_with_detected_aspect_rate": round(float((features["primary_aspect"] != "none").mean()), 4),
        "high_helpfulness_review_rate": round(float(features["is_high_helpfulness_review"].mean()), 4),
        "top_primary_aspects": json.dumps(features["primary_aspect"].value_counts().head(10).to_dict()),
        "sentiment_distribution": json.dumps(features["sentiment_bucket"].value_counts().to_dict()),
        "chronological_split_distribution": json.dumps(features["chronological_split"].value_counts().to_dict()),
    }

    return pd.DataFrame(
        [{"metric_name": key, "metric_value": str(value)} for key, value in metrics.items()]
    )


def markdown_table_from_df(df):
    if df.empty:
        return "_No data available._"

    return df.to_markdown(index=False)


def write_validation_report(features, report_path):
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    total_reviews = len(features)
    generated_at = datetime.now(timezone.utc).isoformat()

    aspect_dist = (
        features["primary_aspect"]
        .fillna("missing")
        .value_counts()
        .head(10)
        .reset_index()
    )
    aspect_dist.columns = ["primary_aspect", "review_count"]

    sentiment_dist = (
        features["sentiment_bucket"]
        .fillna("missing")
        .value_counts()
        .reset_index()
    )
    sentiment_dist.columns = ["sentiment_bucket", "review_count"]

    split_dist = (
        features["chronological_split"]
        .fillna("missing")
        .value_counts()
        .reset_index()
    )
    split_dist.columns = ["chronological_split", "review_count"]

    negative_aspects = (
        features[features["rating_bucket"] == "negative"]["primary_aspect"]
        .fillna("missing")
        .value_counts()
        .head(10)
        .reset_index()
    )
    negative_aspects.columns = ["primary_aspect", "negative_review_count"]

    app_group_col = "app_package" if features["app_package"].notna().any() else "app_pk"

    app_summary = (
        features.groupby(app_group_col, dropna=False)
        .agg(
            review_count=("review_pk", "count"),
            low_signal_rate=("low_signal_flag", "mean"),
            duplicate_within_app_rate=("duplicate_text_within_app_flag", "mean"),
            avg_word_count=("word_count", "mean"),
            negative_sentiment_rate=("sentiment_bucket", lambda x: (x == "negative").mean()),
            avg_helpfulness_weight=("helpfulness_weight", "mean"),
        )
        .reset_index()
        .sort_values("review_count", ascending=False)
        .head(20)
    )

    for col in [
        "low_signal_rate",
        "duplicate_within_app_rate",
        "avg_word_count",
        "negative_sentiment_rate",
        "avg_helpfulness_weight",
    ]:
        app_summary[col] = app_summary[col].round(4)

    mismatch_examples = (
        features[features["rating_sentiment_alignment"] == "mismatch"]
        [["review_pk", "score", "sentiment_bucket", "primary_aspect", "clean_text"]]
        .head(10)
        .copy()
    )

    if not mismatch_examples.empty:
        mismatch_examples["clean_text"] = mismatch_examples["clean_text"].apply(
            lambda text: text[:120] + "..." if len(text) > 120 else text
        )

    lines = []
    lines.append("# Feature Validation Report\n")
    lines.append(f"- Generated at: `{generated_at}`")
    lines.append(f"- Pipeline version: `{PIPELINE_VERSION}`")
    lines.append(f"- Total reviews processed: `{total_reviews}`\n")

    lines.append("## Key Metrics\n")
    lines.append(f"- Low-signal review rate: `{features['low_signal_flag'].mean():.4f}`")
    lines.append(f"- Global duplicate text rate: `{features['duplicate_text_global_flag'].mean():.4f}`")
    lines.append(f"- Within-app duplicate text rate: `{features['duplicate_text_within_app_flag'].mean():.4f}`")
    lines.append(f"- Average word count: `{features['word_count'].mean():.2f}`")
    lines.append(f"- Rating-sentiment mismatch rate: `{(features['rating_sentiment_alignment'] == 'mismatch').mean():.4f}`")
    lines.append(f"- Reviews with detected aspect: `{(features['primary_aspect'] != 'none').mean():.4f}`")
    lines.append(f"- High helpfulness review rate: `{features['is_high_helpfulness_review'].mean():.4f}`\n")

    lines.append("## Primary Aspect Distribution\n")
    lines.append(markdown_table_from_df(aspect_dist))
    lines.append("\n## Sentiment Distribution\n")
    lines.append(markdown_table_from_df(sentiment_dist))
    lines.append("\n## Negative Reviews by Primary Aspect\n")
    lines.append(markdown_table_from_df(negative_aspects))
    lines.append("\n## Chronological Split Distribution\n")
    lines.append(markdown_table_from_df(split_dist))
    lines.append("\n## App-Level Summary\n")
    lines.append(markdown_table_from_df(app_summary))
    lines.append("\n## Rating-Sentiment Mismatch Examples\n")
    lines.append(markdown_table_from_df(mismatch_examples))

    lines.append("\n## Interpretation\n")
    lines.append(
        "This v0.2 feature table extends the raw reviews table with app/source metadata, "
        "text quality indicators, duplicate detection, sentiment features, aspect extraction, "
        "helpfulness weighting, version-aware fields, and model-ready train/validation/test splits. "
        "The validation metrics above help check whether the engineered features are meaningful for "
        "downstream pain point analysis and future ML modeling."
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")


def save_outputs(features, db_path, output_csv, report_path):
    output_csv = Path(output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    features.to_csv(output_csv, index=False)
    write_validation_report(features, report_path)

    validation_summary = build_validation_summary_df(features)

    with sqlite3.connect(db_path) as conn:
        features.to_sql("review_features", conn, if_exists="replace", index=False)
        validation_summary.to_sql("feature_validation_summary", conn, if_exists="replace", index=False)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_features_review_pk
            ON review_features(review_pk)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_features_app_pk
            ON review_features(app_pk)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_features_primary_aspect
            ON review_features(primary_aspect)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_features_chronological_split
            ON review_features(chronological_split)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_features_normalized_text_hash
            ON review_features(normalized_text_hash)
        """)


def main():
    parser = argparse.ArgumentParser(
        description="Build model-ready features from raw Google Play reviews."
    )

    parser.add_argument(
        "--db",
        default="data/google_play_reviews.db",
        help="Path to SQLite database."
    )

    parser.add_argument(
        "--out",
        default="data/features/review_features.csv",
        help="Output CSV path."
    )

    parser.add_argument(
        "--report",
        default="docs/feature_validation_report.md",
        help="Output validation report path."
    )

    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive reviews instead of only active reviews."
    )

    args = parser.parse_args()

    df = load_reviews(args.db, include_inactive=args.include_inactive)

    if df.empty:
        raise ValueError("No reviews found. Check database path or reviews table.")

    features = build_features(df)
    save_outputs(features, args.db, args.out, args.report)

    summary = {
        "status": "success",
        "pipeline_version": PIPELINE_VERSION,
        "reviews_processed": int(len(features)),
        "output_csv": str(args.out),
        "sqlite_output_table": "review_features",
        "validation_summary_table": "feature_validation_summary",
        "validation_report": str(args.report),
        "low_signal_rate": round(float(features["low_signal_flag"].mean()), 4),
        "duplicate_text_global_rate": round(float(features["duplicate_text_global_flag"].mean()), 4),
        "duplicate_text_within_app_rate": round(float(features["duplicate_text_within_app_flag"].mean()), 4),
        "avg_word_count": round(float(features["word_count"].mean()), 2),
        "top_primary_aspects": features["primary_aspect"].value_counts().head(10).to_dict(),
        "sentiment_distribution": features["sentiment_bucket"].value_counts().to_dict(),
        "chronological_split_distribution": features["chronological_split"].value_counts().to_dict(),
        "rating_sentiment_alignment": features["rating_sentiment_alignment"].value_counts().to_dict(),
    }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
