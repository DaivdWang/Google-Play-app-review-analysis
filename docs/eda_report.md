# Google Play Review EDA Report

## Dataset overview

- **rows**: 29999
- **columns**: 12
- **unique_apps**: 5
- **unique_review_ids**: 29999
- **min_date**: 2026-03-20 19:21:02
- **max_date**: 2026-04-06 02:08:43
- **top_3_app_share_pct**: 60.0
- **top_10_app_share_pct**: 100.0
- **lang_values**: ['en']
- **country_values**: ['us']

## Key distributions

- Rating **1.0**: 5920 reviews (19.73%)
- Rating **2.0**: 1256 reviews (4.19%)
- Rating **3.0**: 1470 reviews (4.90%)
- Rating **4.0**: 2362 reviews (7.87%)
- Rating **5.0**: 18991 reviews (63.31%)

### Review length summary

- Median word count: **4.00**
- Mean word count: **10.70**
- Median char count: **19.00**
- Mean char count: **55.94**

## Observable patterns

- Median words at rating **1.0**: **12.00**
- Median words at rating **2.0**: **14.00**
- Median words at rating **3.0**: **8.00**
- Median words at rating **4.0**: **4.00**
- Median words at rating **5.0**: **3.00**
- Reply rate at rating **1.0**: **22.92%**
- Reply rate at rating **2.0**: **27.07%**
- Reply rate at rating **3.0**: **22.18%**
- Reply rate at rating **4.0**: **16.93%**
- Reply rate at rating **5.0**: **16.71%**

## Data quality issues(in %)

- **empty_text_pct**: 0.0
- **single_token_pct**: 22.46
- **two_words_or_less_pct**: 38.21
- **very_short_chars_lt5_pct**: 15.21
- **boilerplate_praise_pct**: 15.97
- **punct_or_symbol_only_pct**: 2.01
- **repeated_char_noise_pct**: 1.16
- **all_caps_pct**: 0.88
- **non_ascii_pct**: 19.8
- **duplicate_review_id_pct**: 0.0
- **duplicate_text_global_pct**: 31.17
- **duplicate_text_within_app_pct**: 27.14
- **invalid_rating_zero_count**: 0.0
- **missing_reply_text_pct**: 81.34