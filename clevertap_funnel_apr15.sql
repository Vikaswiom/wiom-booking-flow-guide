-- Section A top-of-funnel.
-- A1-A4: no variant filter (pre-variant steps, same for all users).
-- A5-A7: count users whose event itself carried cost_breakdown_flow in (A,B,C,D).
-- A8 (booking_fee_captured): does NOT carry the variant property, so attribute via
--     the user's last variant assignment on A5-A7.

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15'
    AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),
pre_events AS (
  -- A1-A4 + unserviceable + how_works: no variant filter
  SELECT USER_ID, EVENT_NAME
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME IN ('booking_homepage_loaded','check_serviceability_clicked','current_loc_serviceability_check_clicked',
                       'serviceable_page_loaded','unserviceable_page_loaded','how_does_it_work_clicked')
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
variant_events AS (
  -- A5-A7: per-event variant attribution
  SELECT USER_ID, EVENT_NAME, TIMESTAMP,
         UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME IN ('how_to_get_started_clicked','cost_today_clicked','pay_100_to_move_forward_clicked','I_AM_AT_INSTALL_LOCATION_CLICKED')
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
fee_users AS (
  -- A8: users who paid (booking_fee_captured carries no variant)
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_fee_captured'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
-- User-level last variant assignment (from A5-A7 events) — used to attribute A8
user_last_variant AS (
  SELECT USER_ID, variant
  FROM variant_events
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP DESC) = 1
)
SELECT
  COUNT(DISTINCT b.USER_ID) AS app_installed,
  -- A1-A4: pre-variant
  COUNT(DISTINCT CASE WHEN pe.EVENT_NAME = 'booking_homepage_loaded' THEN b.USER_ID END) AS homepage,
  COUNT(DISTINCT CASE WHEN pe.EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') THEN b.USER_ID END) AS check_clicked,
  COUNT(DISTINCT CASE WHEN pe.EVENT_NAME = 'serviceable_page_loaded' THEN b.USER_ID END) AS serviceable,
  COUNT(DISTINCT CASE WHEN pe.EVENT_NAME = 'unserviceable_page_loaded' THEN b.USER_ID END) AS unserviceable,
  COUNT(DISTINCT CASE WHEN pe.EVENT_NAME = 'how_does_it_work_clicked' THEN b.USER_ID END) AS how_works,
  -- A5-A7: per-event variant filter (variant on the event itself)
  COUNT(DISTINCT CASE WHEN ve.EVENT_NAME = 'how_to_get_started_clicked' THEN b.USER_ID END) AS get_started,
  COUNT(DISTINCT CASE WHEN ve.EVENT_NAME = 'cost_today_clicked' THEN b.USER_ID END) AS cost_today,
  COUNT(DISTINCT CASE WHEN ve.EVENT_NAME = 'pay_100_to_move_forward_clicked' THEN b.USER_ID END) AS pay_100,
  COUNT(DISTINCT CASE WHEN ve.EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' THEN b.USER_ID END) AS location_confirm,
  -- A8: users who paid AND have a variant assigned from earlier events
  COUNT(DISTINCT CASE WHEN fu.USER_ID IS NOT NULL AND ulv.variant IS NOT NULL THEN b.USER_ID END) AS fee_captured
FROM first_installers b
LEFT JOIN pre_events pe ON pe.USER_ID = b.USER_ID
LEFT JOIN variant_events ve ON ve.USER_ID = b.USER_ID
LEFT JOIN fee_users fu ON fu.USER_ID = b.USER_ID
LEFT JOIN user_last_variant ulv ON ulv.USER_ID = b.USER_ID;
