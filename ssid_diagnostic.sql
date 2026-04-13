-- Diagnostic for Step 1: BFP → SSID Set
-- Try multiple SSID event variants and both PRE and POST periods
WITH bookings_pre AS (
  SELECT
    mobile,
    added_time AS booking_time,
    LEAD(added_time) OVER(PARTITION BY mobile ORDER BY added_time) AS next_booking_time
  FROM prod_db.public.booking_logs
  WHERE event_name = 'booking_fee_captured'
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-02-20 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-03-26 00:00:00'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY TRY_PARSE_JSON(data):transaction_id::STRING ORDER BY added_time DESC) = 1
),
bookings_post AS (
  SELECT
    mobile,
    added_time AS booking_time,
    LEAD(added_time) OVER(PARTITION BY mobile ORDER BY added_time) AS next_booking_time
  FROM prod_db.public.booking_logs
  WHERE event_name = 'booking_fee_captured'
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-03-28 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-04-13 00:00:00'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY TRY_PARSE_JSON(data):transaction_id::STRING ORDER BY added_time DESC) = 1
),
ssid_events AS (
  SELECT mobile, event_name, added_time AS event_time
  FROM prod_db.public.booking_logs
  WHERE event_name IN ('ssid_set','ssid_captured')
    AND mobile >= '5999999999'
),
pre_match AS (
  SELECT b.mobile, b.booking_time,
    MAX(CASE WHEN s.event_name = 'ssid_set' THEN 1 ELSE 0 END) AS got_ssid_set,
    MAX(CASE WHEN s.event_name = 'ssid_captured' THEN 1 ELSE 0 END) AS got_ssid_captured,
    MAX(CASE WHEN s.event_name IN ('ssid_set','ssid_captured') THEN 1 ELSE 0 END) AS got_any_ssid
  FROM bookings_pre b
  LEFT JOIN ssid_events s ON b.mobile = s.mobile
    AND (
      (b.next_booking_time IS NOT NULL AND s.event_time >= b.booking_time AND s.event_time < b.next_booking_time)
      OR
      (b.next_booking_time IS NULL AND s.event_time >= b.booking_time)
    )
  GROUP BY 1,2
),
post_match AS (
  SELECT b.mobile, b.booking_time,
    MAX(CASE WHEN s.event_name = 'ssid_set' THEN 1 ELSE 0 END) AS got_ssid_set,
    MAX(CASE WHEN s.event_name = 'ssid_captured' THEN 1 ELSE 0 END) AS got_ssid_captured,
    MAX(CASE WHEN s.event_name IN ('ssid_set','ssid_captured') THEN 1 ELSE 0 END) AS got_any_ssid
  FROM bookings_post b
  LEFT JOIN ssid_events s ON b.mobile = s.mobile
    AND (
      (b.next_booking_time IS NOT NULL AND s.event_time >= b.booking_time AND s.event_time < b.next_booking_time)
      OR
      (b.next_booking_time IS NULL AND s.event_time >= b.booking_time)
    )
  GROUP BY 1,2
)
SELECT
  'PRE' AS period,
  COUNT(*) AS total_bookings,
  SUM(got_ssid_set) AS ssid_set_cnt,
  SUM(got_ssid_captured) AS ssid_captured_cnt,
  SUM(got_any_ssid) AS any_ssid_cnt
FROM pre_match
UNION ALL
SELECT
  'POST' AS period,
  COUNT(*) AS total_bookings,
  SUM(got_ssid_set) AS ssid_set_cnt,
  SUM(got_ssid_captured) AS ssid_captured_cnt,
  SUM(got_any_ssid) AS any_ssid_cnt
FROM post_match
