-- Section A top-of-funnel (CleverTap-funnel style).
-- A1-A4: no variant filter, no install filter — all users who fired the event in window.
-- A5: variant assigned via how_to_get_started_clicked.
-- A6/A7/A7.5/A8: counted only if event happened AFTER user's A5 timestamp.

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

-- A1-A4 + unserviceable + how_works: distinct users who fired these in window
e_install_users AS (
  SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
e_homepage      AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_homepage_loaded'  AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_check         AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_serviceable   AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'serviceable_page_loaded'   AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_unserviceable AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'unserviceable_page_loaded' AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_how_works     AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'how_does_it_work_clicked'  AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),

-- Downstream events: earliest per user
u_cost_today AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'cost_today_clicked'              AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),
u_pay_100    AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked' AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),
u_location   AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED' AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1),
u_fee        AS (SELECT USER_ID, MIN(TIMESTAMP) AS t FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_fee_captured'            AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()) GROUP BY 1)

SELECT
  (SELECT COUNT(*) FROM e_install_users) AS app_installed,
  (SELECT COUNT(*) FROM e_homepage)      AS homepage,
  (SELECT COUNT(*) FROM e_check)         AS check_clicked,
  (SELECT COUNT(*) FROM e_serviceable)   AS serviceable,
  (SELECT COUNT(*) FROM e_unserviceable) AS unserviceable,
  (SELECT COUNT(*) FROM e_how_works)     AS how_works,
  -- A5: total variant users
  (SELECT COUNT(DISTINCT USER_ID) FROM user_variant) AS get_started,
  -- A6-A8: must happen after gs_time
  (SELECT COUNT(DISTINCT uv.USER_ID) FROM user_variant uv JOIN u_cost_today ct ON ct.USER_ID = uv.USER_ID WHERE ct.t > uv.gs_time) AS cost_today,
  (SELECT COUNT(DISTINCT uv.USER_ID) FROM user_variant uv JOIN u_pay_100    p1 ON p1.USER_ID = uv.USER_ID WHERE p1.t > uv.gs_time) AS pay_100,
  (SELECT COUNT(DISTINCT uv.USER_ID) FROM user_variant uv JOIN u_location   lc ON lc.USER_ID = uv.USER_ID WHERE lc.t > uv.gs_time) AS location_confirm,
  (SELECT COUNT(DISTINCT uv.USER_ID) FROM user_variant uv JOIN u_fee        fe ON fe.USER_ID = uv.USER_ID WHERE fe.t > uv.gs_time) AS fee_captured;
