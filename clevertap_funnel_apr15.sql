WITH first_time_loaders AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_homepage_loaded'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < '2026-04-18'
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.booking_homepage_loaded.count"::STRING AS INT) = 1
),
all_events AS (
  SELECT USER_ID, EVENT_NAME
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked','serviceable_page_loaded','unserviceable_page_loaded','how_does_it_work_clicked','how_to_get_started_clicked','cost_today_clicked','pay_100_to_move_forward_clicked','I_AM_AT_INSTALL_LOCATION_CLICKED','booking_fee_captured')
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < '2026-04-18'
)
SELECT
  COUNT(DISTINCT b.USER_ID) AS homepage_loaded,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') THEN b.USER_ID END) AS check_clicked,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'serviceable_page_loaded' THEN b.USER_ID END) AS serviceable,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'unserviceable_page_loaded' THEN b.USER_ID END) AS unserviceable,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_does_it_work_clicked' THEN b.USER_ID END) AS how_works,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_to_get_started_clicked' THEN b.USER_ID END) AS get_started,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'cost_today_clicked' THEN b.USER_ID END) AS cost_today,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'pay_100_to_move_forward_clicked' THEN b.USER_ID END) AS pay_100,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' THEN b.USER_ID END) AS location_confirm,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'booking_fee_captured' THEN b.USER_ID END) AS fee_captured
FROM first_time_loaders b
LEFT JOIN all_events e ON e.USER_ID = b.USER_ID
