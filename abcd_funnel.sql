-- Variant funnel A5 → A8 with STRICT sequential constraint.
-- Each step's join requires its timestamp > the previous step's timestamp.
-- get_started → cost_today → pay_100 → location_confirm → fee_captured (each AFTER prior)

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
  COUNT(DISTINCT ct.USER_ID) AS cost_today,
  COUNT(DISTINCT p1.USER_ID) AS pay_100,
  COUNT(DISTINCT lc.USER_ID) AS location_confirm,
  COUNT(DISTINCT fe.USER_ID) AS fee_captured
FROM user_variant uv
LEFT JOIN u_cost_today ct ON ct.USER_ID = uv.USER_ID AND ct.t > uv.gs_time
LEFT JOIN u_pay_100    p1 ON p1.USER_ID = uv.USER_ID AND p1.t > ct.t
LEFT JOIN u_location   lc ON lc.USER_ID = uv.USER_ID AND lc.t > p1.t
LEFT JOIN u_fee        fe ON fe.USER_ID = uv.USER_ID AND fe.t > p1.t
GROUP BY uv.variant
ORDER BY uv.variant;
