-- Section A top-of-funnel.
-- A1-A4 (homepage, check, serviceable, unserviceable, how_works): NO variant filter.
-- A5-A7: each event has its OWN independent subquery with its own variant filter.
-- A8 (booking_fee_captured): no variant property on this event — attribute to user's last variant.

WITH first_installers AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed'
    AND TIMESTAMP >= '2026-04-15'
    AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::STRING AS INT) = 1
),

-- A1
e_homepage AS (
  SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_homepage_loaded'
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
-- A2
e_check AS (
  SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME IN ('check_serviceability_clicked','current_loc_serviceability_check_clicked')
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
-- A3 serviceable
e_serviceable AS (
  SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'serviceable_page_loaded'
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
-- A3 unserviceable
e_unserviceable AS (
  SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'unserviceable_page_loaded'
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),
-- A4
e_how_works AS (
  SELECT DISTINCT USER_ID FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_does_it_work_clicked'
    AND TIMESTAMP >= '2026-04-14' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
),

-- A5 with its OWN variant filter
e_get_started AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'how_to_get_started_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
-- A6 with its OWN variant filter
e_cost_today AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'cost_today_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
-- A7 with its OWN variant filter
e_pay_100 AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'pay_100_to_move_forward_clicked'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),
-- A7.5 location confirm with its OWN variant filter
e_location AS (
  SELECT DISTINCT USER_ID,
    UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) AS variant,
    TIMESTAMP
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'I_AM_AT_INSTALL_LOCATION_CLICKED'
    AND TIMESTAMP >= '2026-04-15' AND TIMESTAMP < DATEADD('day', -5, CURRENT_DATE())
    AND UPPER(TRY_PARSE_JSON(PROPERTIES):"event_props.cost_breakdown_flow"::STRING) IN ('A','B','C','D')
),

-- A8: booking_fee_captured has no variant property — attribute via user's last A5-A7 variant
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
)

SELECT
  COUNT(DISTINCT b.USER_ID) AS app_installed,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_homepage      x ON x.USER_ID = b.USER_ID) AS homepage,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_check         x ON x.USER_ID = b.USER_ID) AS check_clicked,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_serviceable   x ON x.USER_ID = b.USER_ID) AS serviceable,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_unserviceable x ON x.USER_ID = b.USER_ID) AS unserviceable,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_how_works     x ON x.USER_ID = b.USER_ID) AS how_works,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_get_started   x ON x.USER_ID = b.USER_ID) AS get_started,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_cost_today    x ON x.USER_ID = b.USER_ID) AS cost_today,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_pay_100       x ON x.USER_ID = b.USER_ID) AS pay_100,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_location      x ON x.USER_ID = b.USER_ID) AS location_confirm,
  (SELECT COUNT(DISTINCT b.USER_ID) FROM first_installers b JOIN e_fee f ON f.USER_ID = b.USER_ID
                                       JOIN user_last_variant ulv ON ulv.USER_ID = b.USER_ID) AS fee_captured
FROM first_installers b;
