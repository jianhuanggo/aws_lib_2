SQL = """
{{
    config(
        materialized="incremental",
        unique_key="_id",
        partition_by = {
            "ds": "string"
        },
        tags=["tubidw", "801954"],
    )
}}

{% set id_columns = ["ds", "platform", "country", "city", "subdivision", "dma", "language", "autoplay_on", "content_genres", "content_ratings", "content_type", "device_type", "revenue_vertical", "ramp_id_type", "identity_data_source", "ad_opportunity_reason", "opt_out", "is_coppa", "coppa_enabled", "Ad_break_position", "user_gender", "targeted_seq_pos", "device_deal", "remnant_status", "autoplay_idx", "tracking_mode", "app_mode", "Logged_status", "postal_code", "user_age"] %}
{% set session_length = 24 * 60 %}

    SELECT
    date ds
  , platform
  , country
  , city
  , subdivision
  , dma
  , language
  , autoplay_on
  , content_genres
  , content_ratings
  , content_type
  , device_type
  , revenue_vertical
  , ramp_id_type
  , identity_data_source
  , ad_opportunity_reason
  , opt_out
  , is_coppa
  , coppa_enabled
  , CASE WHEN now_pos = 0 THEN 'PRE_ROLL' ELSE 'MID_ROLL' END Ad_break_position
  , user_gender
  , targeted_seq_pos
  , device_deal
  , remnant_status
  , autoplay_idx
  , tracking_mode
  , app_mode
  , CASE WHEN user_id IS NOT NULL THEN 'Logged In' ELSE 'Logged Out' END Logged_status
  , postal_code
  , user_age
  , COUNT(DISTINCT CASE
                       WHEN integration_type IN ('AMAZON_A9', 'ROKU_PS') THEN request_id
                       WHEN integration_type = 'GENERIC_VAST' THEN CONCAT(request_id, creative_id)
                       ELSE CONCAT(request_id, ortb_request_id) END) bid_requests
  , COUNT(DISTINCT request_id) ad_breaks
  , SUM(CASE
            WHEN result IN ('AUCTION_WON', 'AUCTION_LOST', 'AUCTION_WON_BUT_LOST', 'FILTERED_AD_VIDEO_PARENTAL_CONTROL',
                            'TRANSCODE_RESPONSE_NOT_PROCESSED', 'TRANSCODE_RESPONSE_PROCESSING',
                            'TRANSCODE_RESPONSE_DEACTIVATED', 'FILTERED_AD_VIDEO_REVIEWED',
                            'FILTERED_AD_VIDEO_DURATION', 'FILTERED_AD_VIDEO_PLATFORM_MATCH',
                            'FILTERED_AD_VIDEO_REVIEWED', 'FILTERED_BRAND_NAME_FREQUENCY_CAP',
                            'FILTERED_DOMAIN_BLACKLIST', 'FILTERED_PUBLISHER_DOMAIN_BLACKLIST',
                            'BID_RESPONSE_INVALID_PRICE_BELOW_FLOOR') THEN 1
            ELSE 0 END) total_bid_responses
  , SUM(CASE
            WHEN result IN ('AUCTION_WON', 'AUCTION_LOST', 'AUCTION_WON_BUT_LOST') THEN 1
            ELSE 0 END) valid_bid_responses
  , SUM(CASE WHEN result IN ('AUCTION_WON') THEN 1 ELSE 0 END) winning_bids
  , SUM(CASE WHEN result = 'AUCTION_WON' THEN auction_price / 1000.0 ELSE 0 END) revenue
  , {{ dbt_utils.generate_surrogate_key(id_columns) }} AS _id
FROM {{ source("datalake", "adserver_bid_events") }}
WHERE {{ datalake_date_filter("date", session_length) }} and
date >= '2022-01-01'
GROUP BY {{ ", ".join(id_columns) }}
"""