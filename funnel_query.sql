WITH bookings AS (
  SELECT DISTINCT mobile
  FROM prod_db.public.booking_logs
  WHERE event_name = 'booking_fee_captured'
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-02-20 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-03-26 00:00:00'
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
  FROM bookings b
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
