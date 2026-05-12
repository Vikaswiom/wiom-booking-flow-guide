-- Section A top-of-funnel.
-- A1-A4: no variant filter (pre-variant funnel, all installers).
-- A5: variant filter on `how_to_get_started_clicked` (where variant is assigned).
-- A6/A7/A7.5/A8: NO variant filter — joined via user_id back to user's A5 variant.

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15'
    AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),

-- A5: canonical variant assignment per user
user_variant AS (
  SELECT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_to_get_started_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY TIMESTAMP) = 1
),

-- A1-A4 / unserviceable / how_works: no variant filter
e_homepage      AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_homepage_loaded'  AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_check         AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked') AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_serviceable   AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'serviceable_page_loaded'   AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_unserviceable AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'unserviceable_page_loaded' AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_how_works     AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'how_does_it_work_clicked'  AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),

-- A6/A7/A7.5/A8: NO variant filter — just user_id of who fired the event
e_cost_today AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'cost_today_clicked'                 AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_pay_100    AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked'    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_location   AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED'   AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())),
e_fee        AS (SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER WHERE EVENT_NAME = 'booking_fee_captured'               AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE()))

SELECT
  COUNT(DISTINCT b.USER_ID) AS app_installed,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_homepage      x ON x.USER_ID = b.USER_ID) AS homepage,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_check         x ON x.USER_ID = b.USER_ID) AS check_clicked,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_serviceable   x ON x.USER_ID = b.USER_ID) AS serviceable,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_unserviceable x ON x.USER_ID = b.USER_ID) AS unserviceable,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_how_works     x ON x.USER_ID = b.USER_ID) AS how_works,
  -- A5: distinct users with variant assigned
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN user_variant    uv ON uv.USER_ID = b.USER_ID) AS get_started,
  -- A6-A8: count only users who have a variant assigned at A5 (mapping back via user_variant)
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN user_variant uv ON uv.USER_ID = b.USER_ID JOIN e_cost_today x ON x.USER_ID = b.USER_ID) AS cost_today,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN user_variant uv ON uv.USER_ID = b.USER_ID JOIN e_pay_100    x ON x.USER_ID = b.USER_ID) AS pay_100,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN user_variant uv ON uv.USER_ID = b.USER_ID JOIN e_location   x ON x.USER_ID = b.USER_ID) AS location_confirm,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN user_variant uv ON uv.USER_ID = b.USER_ID JOIN e_fee        x ON x.USER_ID = b.USER_ID) AS fee_captured
FROM first_installers b;
