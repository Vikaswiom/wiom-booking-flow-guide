-- Variant funnel A5 → A8 (CleverTap-funnel style).
-- Cohort = users who fired how_to_get_started_clicked with variant A/B/C/D in the window.
-- No App Installed filter (matches CT funnel default behavior).
-- booking_fee_captured counted only if it happened AFTER the user's how_to_get_started_clicked.

WITH user_variant AS (
  -- A5: variant assignment per user (earliest get_started event)
  SELECT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP AS gs_time
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_to_get_started_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP) = 1
),

-- Downstream events: take earliest timestamp per user, dedup
u_cost_today AS (
  SELECT USER_ID, MIN(TIMESTAMP) AS t
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'cost_today_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
  GROUP BY 1
),
u_pay_100 AS (
  SELECT USER_ID, MIN(TIMESTAMP) AS t
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
  GROUP BY 1
),
u_location AS (
  SELECT USER_ID, MIN(TIMESTAMP) AS t
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
  GROUP BY 1
),
u_fee AS (
  SELECT USER_ID, MIN(TIMESTAMP) AS t
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_fee_captured'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
  GROUP BY 1
)

SELECT
  uv.variant,
  COUNT(DISTINCT uv.USER_ID) AS get_started,
  COUNT(DISTINCT CASE WHEN ct.t > uv.gs_time THEN uv.USER_ID END) AS cost_today,
  COUNT(DISTINCT CASE WHEN p1.t > uv.gs_time THEN uv.USER_ID END) AS pay_100,
  COUNT(DISTINCT CASE WHEN lc.t > uv.gs_time THEN uv.USER_ID END) AS location_confirm,
  COUNT(DISTINCT CASE WHEN fe.t > uv.gs_time THEN uv.USER_ID END) AS fee_captured
FROM user_variant uv
LEFT JOIN u_cost_today ct ON ct.USER_ID = uv.USER_ID
LEFT JOIN u_pay_100    p1 ON p1.USER_ID = uv.USER_ID
LEFT JOIN u_location   lc ON lc.USER_ID = uv.USER_ID
LEFT JOIN u_fee        fe ON fe.USER_ID = uv.USER_ID
GROUP BY uv.variant
ORDER BY uv.variant;
