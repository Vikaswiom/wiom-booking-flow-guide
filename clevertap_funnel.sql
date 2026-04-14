WITH first_time_loaders_pre AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_homepage_loaded'
    AND TIMESTAMP >= '2026-02-20' AND TIMESTAMP < '2026-03-26'
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.booking_homepage_loaded.count"::STRING AS INT) = 1
),
first_time_loaders_post AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_homepage_loaded'
    AND TIMESTAMP >= '2026-03-28' AND TIMESTAMP < '2026-04-13'
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.booking_homepage_loaded.count"::STRING AS INT) = 1
),
all_events AS (
  SELECT USER_ID, EVENT_NAME, TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked','serviceable_page_loaded','unserviceable_page_loaded','how_does_it_work_clicked','how_to_get_started_clicked','cost_today_clicked','pay_100_to_move_forward_clicked','booking_fee_captured')
    AND TIMESTAMP >= '2026-02-13' AND TIMESTAMP < '2026-04-20'
),
pre_stages AS (
  SELECT
    'PRE' AS period,
    COUNT(DISTINCT b.USER_ID) AS homepage_loaded,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') THEN b.USER_ID END) AS check_clicked,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'serviceable_page_loaded' THEN b.USER_ID END) AS serviceable_loaded,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'unserviceable_page_loaded' THEN b.USER_ID END) AS unserviceable_loaded,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_does_it_work_clicked' THEN b.USER_ID END) AS how_works,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_to_get_started_clicked' THEN b.USER_ID END) AS get_started,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'cost_today_clicked' THEN b.USER_ID END) AS cost_today,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'pay_100_to_move_forward_clicked' THEN b.USER_ID END) AS pay_100,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'booking_fee_captured' THEN b.USER_ID END) AS booking_fee_captured
  FROM first_time_loaders_pre b
  LEFT JOIN all_events e ON e.USER_ID = b.USER_ID
),
post_stages AS (
  SELECT
    'POST' AS period,
    COUNT(DISTINCT b.USER_ID) AS homepage_loaded,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') THEN b.USER_ID END) AS check_clicked,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'serviceable_page_loaded' THEN b.USER_ID END) AS serviceable_loaded,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'unserviceable_page_loaded' THEN b.USER_ID END) AS unserviceable_loaded,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_does_it_work_clicked' THEN b.USER_ID END) AS how_works,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_to_get_started_clicked' THEN b.USER_ID END) AS get_started,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'cost_today_clicked' THEN b.USER_ID END) AS cost_today,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'pay_100_to_move_forward_clicked' THEN b.USER_ID END) AS pay_100,
    COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'booking_fee_captured' THEN b.USER_ID END) AS booking_fee_captured
  FROM first_time_loaders_post b
  LEFT JOIN all_events e ON e.USER_ID = b.USER_ID
)
SELECT * FROM pre_stages
UNION ALL
SELECT * FROM post_stages
