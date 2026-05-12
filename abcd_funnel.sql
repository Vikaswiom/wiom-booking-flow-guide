-- Variant funnel A5 → A8.
-- A5-A7 (get_started, cost_today, pay_100, location_confirm): variant from THAT event's property
-- A8 (booking_fee_captured): no variant on event itself, so attribute to user's last variant from A5-A7

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),
variant_events AS (
  SELECT c.USER_ID, c.EVENT_NAME, c.TIMESTAMP,
         UPPER(TRY_PARSE_JSON(c.PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER c
  JOIN first_installers fi ON fi.USER_ID = c.USER_ID
  WHERE c.EVENT_NAME IN ('how_to_get_started_clicked','cost_today_clicked','pay_100_to_move_forward_clicked','I_AM_AT_INSTALL_LOCATION_CLICKED')
    AND c.TIMESTAMP >= '2026-04-15' AND c.TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(c.PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
user_last_variant AS (
  -- For A8 attribution: each user's most recent A5-A7 variant
  SELECT USER_ID, variant
  FROM variant_events
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP DESC) = 1
),
fee_attributed AS (
  -- Users who paid, joined to their last variant
  SELECT ulv.USER_ID, ulv.variant
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER c
  JOIN user_last_variant ulv ON ulv.USER_ID = c.USER_ID
  WHERE c.EVENT_NAME = 'booking_fee_captured'
    AND c.TIMESTAMP >= '2026-04-15' AND c.TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
)
SELECT
  v.variant,
  (SELECT COUNT(DISTINCT USER_ID) FROM variant_events WHERE EVENT_NAME = 'how_to_get_started_clicked' AND variant = v.variant) AS get_started,
  (SELECT COUNT(DISTINCT USER_ID) FROM variant_events WHERE EVENT_NAME = 'cost_today_clicked' AND variant = v.variant) AS cost_today,
  (SELECT COUNT(DISTINCT USER_ID) FROM variant_events WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked' AND variant = v.variant) AS pay_100,
  (SELECT COUNT(DISTINCT USER_ID) FROM variant_events WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' AND variant = v.variant) AS location_confirm,
  (SELECT COUNT(DISTINCT USER_ID) FROM fee_attributed WHERE variant = v.variant) AS fee_captured
FROM (SELECT 'A' AS variant UNION ALL SELECT 'B' UNION ALL SELECT 'C' UNION ALL SELECT 'D') v
ORDER BY v.variant;
