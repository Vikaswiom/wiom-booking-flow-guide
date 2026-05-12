-- A/B/C/D variant funnel A5 → A8.
-- Variant is attributed PER EVENT: each event's own cost_breakdown_flow property
-- determines which variant gets the credit for that event firing.
-- Cohort = all first-time installers in window (variant only matters from A5+).

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15'
    AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),
variant_events AS (
  -- Each row: one event firing tagged with its own variant property
  SELECT USER_ID, EVENT_NAME,
         UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME IN ('how_to_get_started_clicked','cost_today_clicked',
                       'pay_100_to_move_forward_clicked','I_AM_AT_INSTALL_LOCATION_CLICKED',
                       'booking_fee_captured')
    AND TIMESTAMP >= '2026-04-15'
    AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
)
SELECT
  e.variant,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'how_to_get_started_clicked' THEN e.USER_ID END) AS get_started,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'cost_today_clicked' THEN e.USER_ID END) AS cost_today,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'pay_100_to_move_forward_clicked' THEN e.USER_ID END) AS pay_100,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' THEN e.USER_ID END) AS location_confirm,
  COUNT(DISTINCT CASE WHEN e.EVENT_NAME = 'booking_fee_captured' THEN e.USER_ID END) AS fee_captured
FROM first_installers b
JOIN variant_events e ON e.USER_ID = b.USER_ID
GROUP BY e.variant
ORDER BY e.variant;
