-- Variant funnel A5 → A8.
-- A5 (how_to_get_started_clicked): variant filter applied here — this defines the user's variant.
-- A6, A7, A7.5, A8: NO variant filter on event — instead, mapped to user via user_id back to A5 variant.

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),

-- A5: canonical variant assignment
user_variant AS (
  SELECT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_to_get_started_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP) = 1
),

-- A6-A8: just user_id, no variant filter — mapped to user_variant via USER_ID
e_cost_today AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'cost_today_clicked'              AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_pay_100    AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked' AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_location   AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_fee        AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_fee_captured'            AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),

-- Restrict each event to first_installers AND attach variant via user_variant
gs AS (SELECT uv.USER_ID, uv.variant FROM user_variant uv JOIN first_installers fi ON fi.USER_ID = uv.USER_ID),
ct AS (SELECT uv.USER_ID, uv.variant FROM user_variant uv JOIN first_installers fi ON fi.USER_ID = uv.USER_ID JOIN e_cost_today e ON e.USER_ID = uv.USER_ID),
p1 AS (SELECT uv.USER_ID, uv.variant FROM user_variant uv JOIN first_installers fi ON fi.USER_ID = uv.USER_ID JOIN e_pay_100    e ON e.USER_ID = uv.USER_ID),
lc AS (SELECT uv.USER_ID, uv.variant FROM user_variant uv JOIN first_installers fi ON fi.USER_ID = uv.USER_ID JOIN e_location   e ON e.USER_ID = uv.USER_ID),
fe AS (SELECT uv.USER_ID, uv.variant FROM user_variant uv JOIN first_installers fi ON fi.USER_ID = uv.USER_ID JOIN e_fee        e ON e.USER_ID = uv.USER_ID)

SELECT v.variant,
  (SELECT COUNT(DISTINCT USER_ID) FROM gs WHERE variant = v.variant) AS get_started,
  (SELECT COUNT(DISTINCT USER_ID) FROM ct WHERE variant = v.variant) AS cost_today,
  (SELECT COUNT(DISTINCT USER_ID) FROM p1 WHERE variant = v.variant) AS pay_100,
  (SELECT COUNT(DISTINCT USER_ID) FROM lc WHERE variant = v.variant) AS location_confirm,
  (SELECT COUNT(DISTINCT USER_ID) FROM fe WHERE variant = v.variant) AS fee_captured
FROM (SELECT 'A' AS variant UNION ALL SELECT 'B' UNION ALL SELECT 'C' UNION ALL SELECT 'D') v
ORDER BY v.variant;
