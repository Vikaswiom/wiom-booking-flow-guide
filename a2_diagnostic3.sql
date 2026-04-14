WITH first_time_loaders_post AS (
  SELECT DISTINCT USER_ID
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'booking_homepage_loaded'
    AND TIMESTAMP >= '2026-03-28' AND TIMESTAMP < '2026-04-13'
    AND TRY_CAST(TRY_PARSE_JSON(PROPERTIES):"profile.events.booking_homepage_loaded.count"::STRING AS INT) = 1
),
events_per_user AS (
  SELECT
    b.USER_ID,
    MAX(CASE WHEN e.EVENT_NAME = 'check_serviceability_clicked' THEN 1 ELSE 0 END) AS clicked,
    MAX(CASE WHEN e.EVENT_NAME = 'checking_serviceablity_page_loaded' THEN 1 ELSE 0 END) AS loading_page,
    MAX(CASE WHEN e.EVENT_NAME = 'serviceable_page_loaded' THEN 1 ELSE 0 END) AS serviceable_page,
    MAX(CASE WHEN e.EVENT_NAME = 'unserviceable_page_loaded' THEN 1 ELSE 0 END) AS unserviceable_page
  FROM first_time_loaders_post b
  LEFT JOIN PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER e ON e.USER_ID = b.USER_ID
  WHERE e.TIMESTAMP >= '2026-03-28' AND e.TIMESTAMP < '2026-04-13'
  GROUP BY 1
)
SELECT
  COUNT(*) AS total_first_time,
  SUM(clicked) AS clicked,
  SUM(loading_page) AS loading_page,
  SUM(serviceable_page) AS serviceable_page,
  SUM(unserviceable_page) AS unserviceable_page,
  SUM(CASE WHEN clicked = 1 OR loading_page = 1 OR serviceable_page = 1 OR unserviceable_page = 1 THEN 1 ELSE 0 END) AS any_serviceability,
  SUM(CASE WHEN clicked = 0 AND (loading_page = 1 OR serviceable_page = 1 OR unserviceable_page = 1) THEN 1 ELSE 0 END) AS auto_no_click
FROM events_per_user
