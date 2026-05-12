-- Section A top-of-funnel with STRICT sequential constraint on A5+ steps.
-- A1-A4: no variant, no install filter — just users who fired the event.
-- A5: variant assigned via how_to_get_started_clicked.
-- A6-A8: must happen AFTER the previous step (sequential join).

WITH user_variant AS (
  SELECT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP AS gs_time
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_to_get_started_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP) = 1
),

-- A1-A4 + unserviceable + how_works: distinct users (no variant, no install filter)
e_install_users AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'App Installed'          AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_homepage      AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_homepage_loaded' AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_check         AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_serviceable   AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'serviceable_page_loaded'   AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_unserviceable AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'unserviceable_page_loaded' AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_how_works     AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'how_does_it_work_clicked'  AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),

-- Downstream events: earliest TS per user
u_cost_today AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'cost_today_clicked'              AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),
u_pay_100    AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked' AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),
u_location   AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),
u_fee        AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_fee_captured'            AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),

-- Sequentially-constrained join chain for A5-A8
seq AS (
  SELECT
    uv.USER_ID,
    uv.variant,
    uv.gs_time,
    ct.t AS ct_time,
    p1.t AS p1_time,
    lc.t AS lc_time,
    fe.t AS fe_time
  FROM user_variant uv
  LEFT JOIN u_cost_today ct ON ct.USER_ID = uv.USER_ID AND ct.t > uv.gs_time
  LEFT JOIN u_pay_100    p1 ON p1.USER_ID = uv.USER_ID AND p1.t > ct.t
  LEFT JOIN u_location   lc ON lc.USER_ID = uv.USER_ID AND lc.t > p1.t
  LEFT JOIN u_fee        fe ON fe.USER_ID = uv.USER_ID AND fe.t > p1.t
)

SELECT
  (SELECT COUNT(*) FROM e_install_users) AS app_installed,
  (SELECT COUNT(*) FROM e_homepage)      AS homepage,
  (SELECT COUNT(*) FROM e_check)         AS check_clicked,
  (SELECT COUNT(*) FROM e_serviceable)   AS serviceable,
  (SELECT COUNT(*) FROM e_unserviceable) AS unserviceable,
  (SELECT COUNT(*) FROM e_how_works)     AS how_works,
  (SELECT COUNT(DISTINCT USER_ID) FROM seq)                       AS get_started,
  (SELECT COUNT(DISTINCT USER_ID) FROM seq WHERE ct_time IS NOT NULL) AS cost_today,
  (SELECT COUNT(DISTINCT USER_ID) FROM seq WHERE p1_time IS NOT NULL) AS pay_100,
  (SELECT COUNT(DISTINCT USER_ID) FROM seq WHERE lc_time IS NOT NULL) AS location_confirm,
  (SELECT COUNT(DISTINCT USER_ID) FROM seq WHERE fe_time IS NOT NULL) AS fee_captured;
