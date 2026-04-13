WITH bookings AS (
  SELECT mobile, MIN(added_time) AS first_booking_time
  FROM prod_db.public.booking_logs
  WHERE event_name = 'booking_fee_captured'
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-02-20 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-03-26 00:00:00'
  GROUP BY mobile
),
first_ssid AS (
  SELECT b.mobile, MIN(s.added_time) AS first_ssid_time
  FROM bookings b
  JOIN prod_db.public.booking_logs s ON s.mobile = b.mobile
  WHERE s.event_name = 'ssid_set'
    AND s.added_time >= b.first_booking_time
  GROUP BY b.mobile
),
first_cancel AS (
  SELECT b.mobile, MIN(c.added_time) AS first_cancel_time
  FROM bookings b
  JOIN prod_db.public.booking_logs c ON c.mobile = b.mobile
  WHERE c.event_name IN ('cancelled', 'refund_initiated')
    AND c.added_time >= b.first_booking_time
  GROUP BY b.mobile
),
abandoned AS (
  SELECT b.mobile
  FROM bookings b
  LEFT JOIN first_ssid s ON s.mobile = b.mobile
  JOIN first_cancel c ON c.mobile = b.mobile
  WHERE s.first_ssid_time IS NULL OR c.first_cancel_time < s.first_ssid_time
),
clean_bookings AS (
  SELECT b.mobile
  FROM bookings b
  LEFT JOIN abandoned a ON a.mobile = b.mobile
  WHERE a.mobile IS NULL
),
bl_events AS (
  SELECT mobile, event_name
  FROM prod_db.public.booking_logs
  WHERE event_name IN ('ssid_set','address_updated','booking_verified','cancelled')
    AND mobile >= '5999999999'
),
tl_events AS (
  SELECT mobile, event_name
  FROM prod_db.public.task_logs
  WHERE event_name IN ('NOTIF_SENT','INTERESTED','SLOT_SELECTED','CUSTOMER_SLOT_CONFIRMED','ASSIGNED','OTP_VERIFIED')
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-02-13 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-04-13 00:00:00'
),
all_events AS (
  SELECT mobile, event_name FROM bl_events
  UNION ALL
  SELECT mobile, event_name FROM tl_events
),
customer_stages AS (
  SELECT
    b.mobile,
    MAX(CASE WHEN e.event_name = 'ssid_set' THEN 1 ELSE 0 END) AS got_ssid,
    MAX(CASE WHEN e.event_name = 'address_updated' THEN 1 ELSE 0 END) AS got_address,
    MAX(CASE WHEN e.event_name = 'booking_verified' THEN 1 ELSE 0 END) AS got_verified,
    MAX(CASE WHEN e.event_name = 'NOTIF_SENT' THEN 1 ELSE 0 END) AS got_notif,
    MAX(CASE WHEN e.event_name = 'INTERESTED' THEN 1 ELSE 0 END) AS got_interested,
    MAX(CASE WHEN e.event_name = 'SLOT_SELECTED' THEN 1 ELSE 0 END) AS got_slot,
    MAX(CASE WHEN e.event_name = 'CUSTOMER_SLOT_CONFIRMED' THEN 1 ELSE 0 END) AS got_confirmed,
    MAX(CASE WHEN e.event_name = 'ASSIGNED' THEN 1 ELSE 0 END) AS got_assigned,
    MAX(CASE WHEN e.event_name = 'OTP_VERIFIED' THEN 1 ELSE 0 END) AS got_otp,
    MAX(CASE WHEN e.event_name = 'cancelled' THEN 1 ELSE 0 END) AS got_cancelled
  FROM clean_bookings b
  LEFT JOIN all_events e ON e.mobile = b.mobile
  GROUP BY b.mobile
)
SELECT
  COUNT(*) AS total_customers,
  SUM(got_ssid) AS ssid_set,
  SUM(got_address) AS address_updated,
  SUM(got_verified) AS booking_verified,
  SUM(got_notif) AS notif_sent,
  SUM(got_interested) AS interested,
  SUM(got_slot) AS slot_selected,
  SUM(got_confirmed) AS customer_slot_confirmed,
  SUM(got_assigned) AS assigned,
  SUM(got_otp) AS otp_verified,
  SUM(got_cancelled) AS cancelled
FROM customer_stages
