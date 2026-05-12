-- Variant funnel A5 → A8.
-- Each A5-A7 event has its OWN separate subquery with its own variant filter.
-- A8 attributed via user's last A5-A7 variant (booking_fee_captured carries no variant).

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),

e_get_started AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_to_get_started_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
e_cost_today AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'cost_today_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
e_pay_100 AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
e_location AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),

-- Restrict each event-CTE to first_installers
gs AS (SELECT e.USER_ID, e.variant FROM e_get_started e JOIN first_installers fi ON fi.USER_ID = e.USER_ID),
ct AS (SELECT e.USER_ID, e.variant FROM e_cost_today  e JOIN first_installers fi ON fi.USER_ID = e.USER_ID),
p1 AS (SELECT e.USER_ID, e.variant FROM e_pay_100     e JOIN first_installers fi ON fi.USER_ID = e.USER_ID),
lc AS (SELECT e.USER_ID, e.variant FROM e_location    e JOIN first_installers fi ON fi.USER_ID = e.USER_ID),

-- A8: attribute booking_fee_captured by user's last A5-A7 variant
e_fee AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_fee_captured'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
user_last_variant AS (
  SELECT USER_ID, variant FROM (
    SELECT USER_ID, variant, TIMESTAMP FROM e_get_started
    UNION ALL SELECT USER_ID, variant, TIMESTAMP FROM e_cost_today
    UNION ALL SELECT USER_ID, variant, TIMESTAMP FROM e_pay_100
    UNION ALL SELECT USER_ID, variant, TIMESTAMP FROM e_location
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP DESC) = 1
),
fee_attributed AS (
  SELECT ulv.USER_ID, ulv.variant
  FROM e_fee f
  JOIN first_installers fi ON fi.USER_ID = f.USER_ID
  JOIN user_last_variant ulv ON ulv.USER_ID = f.USER_ID
)

SELECT v.variant,
  (SELECT COUNT(DISTINCT USER_ID) FROM gs WHERE variant = v.variant) AS get_started,
  (SELECT COUNT(DISTINCT USER_ID) FROM ct WHERE variant = v.variant) AS cost_today,
  (SELECT COUNT(DISTINCT USER_ID) FROM p1 WHERE variant = v.variant) AS pay_100,
  (SELECT COUNT(DISTINCT USER_ID) FROM lc WHERE variant = v.variant) AS location_confirm,
  (SELECT COUNT(DISTINCT USER_ID) FROM fee_attributed WHERE variant = v.variant) AS fee_captured
FROM (SELECT 'A' AS variant UNION ALL SELECT 'B' UNION ALL SELECT 'C' UNION ALL SELECT 'D') v
ORDER BY v.variant;
