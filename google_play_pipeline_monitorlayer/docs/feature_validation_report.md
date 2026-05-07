# Feature Validation Report

- Generated at: `2026-05-07T00:08:29.510167+00:00`
- Pipeline version: `feature_engineering_v0.2`
- Total reviews processed: `15000`

## Key Metrics

- Low-signal review rate: `0.3985`
- Global duplicate text rate: `0.3053`
- Within-app duplicate text rate: `0.2675`
- Average word count: `11.68`
- Rating-sentiment mismatch rate: `0.0425`
- Reviews with detected aspect: `0.2321`
- High helpfulness review rate: `0.0046`

## Primary Aspect Distribution

| primary_aspect   |   review_count |
|:-----------------|---------------:|
| none             |          11518 |
| content_quality  |           1161 |
| login_account    |            676 |
| ads_monetization |            647 |
| update_version   |            269 |
| performance      |            158 |
| ui_ux            |            156 |
| crash_stability  |            144 |
| privacy_security |            137 |
| payment_billing  |             97 |

## Sentiment Distribution

| sentiment_bucket   |   review_count |
|:-------------------|---------------:|
| positive           |           7298 |
| neutral            |           6409 |
| negative           |           1293 |

## Negative Reviews by Primary Aspect

| primary_aspect   |   negative_review_count |
|:-----------------|------------------------:|
| none             |                    1855 |
| content_quality  |                     484 |
| login_account    |                     451 |
| ads_monetization |                     373 |
| update_version   |                     161 |
| performance      |                     118 |
| crash_stability  |                      97 |
| ui_ux            |                      85 |
| privacy_security |                      66 |
| payment_billing  |                      59 |

## Chronological Split Distribution

| chronological_split   |   review_count |
|:----------------------|---------------:|
| train                 |          10500 |
| test                  |           2250 |
| validation            |           2250 |

## App-Level Summary

| app_package              |   review_count |   low_signal_rate |   duplicate_within_app_rate |   avg_word_count |   negative_sentiment_rate |   avg_helpfulness_weight |
|:-------------------------|---------------:|------------------:|----------------------------:|-----------------:|--------------------------:|-------------------------:|
| com.instagram.android    |           3000 |            0.4413 |                      0.2937 |          10.2717 |                    0.102  |                   0.0602 |
| com.snapchat.android     |           3000 |            0.4217 |                      0.28   |          10.5013 |                    0.0853 |                   0.2239 |
| com.spotify.music        |           3000 |            0.284  |                      0.1763 |          14.3613 |                    0.0793 |                   0.0758 |
| com.whatsapp             |           3000 |            0.5267 |                      0.3967 |           6.733  |                    0.0483 |                   0.0929 |
| com.zhiliaoapp.musically |           3000 |            0.3187 |                      0.191  |          16.5393 |                    0.116  |                   0.2982 |

## Rating-Sentiment Mismatch Examples

|   review_pk |   score | sentiment_bucket   | primary_aspect   | clean_text                                                                                                                  |
|------------:|--------:|:-------------------|:-----------------|:----------------------------------------------------------------------------------------------------------------------------|
|           5 |       2 | positive           | none             | I think Instagram is great, but my Reels and posts don't go viral and my followers don't increase.                          |
|          35 |       1 | positive           | login_account    | Guys This is good app but They are suspended many accounts with out any reason I have 2-3 account and They ban all accou... |
|          55 |       5 | negative           | content_quality  | instagram begin a problem now we can't use any filter on video call why??? before it was good now it's getting a problem... |
|          78 |       1 | positive           | content_quality  | this is a useful app for entertainnment and comminucation .but still its algorithm is designed to make you busy.            |
|          88 |       1 | positive           | content_quality  | STOP TRYING TO BE TIKTOK. Used to be great platform to share life moments. Now there is a Thousand ads in between. I DON... |
|         168 |       5 | negative           | none             | for my social media pleasure and entertainment it has everything for me . never and issue or lack of.                       |
|         179 |       1 | positive           | login_account    | Hello Instagram Support, Several of my Reels have completely disappeared from my profile grid and Reels tab without warn... |
|         192 |       2 | positive           | none             | My favorite, once they put appeal on made me sad been ages 😢                                                               |
|         193 |       1 | positive           | none             | good 👍                                                                                                                     |
|         205 |       5 | negative           | none             | Instagram Install problem                                                                                                   |

## Interpretation

This v0.2 feature table extends the raw reviews table with app/source metadata, text quality indicators, duplicate detection, sentiment features, aspect extraction, helpfulness weighting, version-aware fields, and model-ready train/validation/test splits. The validation metrics above help check whether the engineered features are meaningful for downstream pain point analysis and future ML modeling.