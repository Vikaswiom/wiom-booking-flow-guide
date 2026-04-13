SELECT
  CASE
    WHEN TIMESTAMP >= '2026-02-20' AND TIMESTAMP < '2026-03-26' THEN 'PRE'
    WHEN TIMESTAMP >= '2026-03-28' AND TIMESTAMP < '2026-04-13' THEN 'POST'
  END AS period,
  EVENT_NAME,
  COUNT(DISTINCT USER_ID) AS distinct_users
FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
WHERE EVENT_NAME IN (
  'booking_homepage_loaded',
  'check_serviceability_clicked',
  'serviceable_page_loaded',
  'unserviceable_page_loaded',
  'how_does_it_work_clicked',
  'how_to_get_started_clicked',
  'cost_today_clicked',
  'pay_100_to_move_forward_clicked',
  'booking_fee_captured'
)
AND (
  (TIMESTAMP >= '2026-02-20' AND TIMESTAMP < '2026-03-26')
  OR
  (TIMESTAMP >= '2026-03-28' AND TIMESTAMP < '2026-04-13')
)
GROUP BY period, EVENT_NAME
ORDER BY period, distinct_users DESC
