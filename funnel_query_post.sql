WITH bookings AS (
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
bl_events AS (
  SELECT mobile, event_name, added_time AS event_time
  FROM prod_db.public.booking_logs
  WHERE event_name IN ('ssid_set','address_updated','booking_verified','unservisable','cancelled')
    AND mobile >= '5999999999'
),
tl_events AS (
  SELECT mobile, event_name, added_time AS event_time
  FROM prod_db.public.task_logs
  WHERE event_name IN ('NOTIF_SENT','INTERESTED','SLOT_SELECTED','CUSTOMER_SLOT_CONFIRMED','ASSIGNED','OTP_VERIFIED')
    AND mobile >= '5999999999'
),
all_events AS (
  SELECT mobile, event_name, event_time FROM bl_events
  UNION ALL
  SELECT mobile, event_name, event_time FROM tl_events
),
mapped AS (
  SELECT b.mobile, b.booking_time, e.event_name
  FROM bookings b
  LEFT JOIN all_events e
    ON b.mobile = e.mobile
    AND (
      (b.next_booking_time IS NOT NULL AND e.event_time >= b.booking_time AND e.event_time < b.next_booking_time)
      OR
      (b.next_booking_time IS NULL AND e.event_time >= b.booking_time)
    )
),
booking_stages AS (
  SELECT
    mobile,
    booking_time,
    MAX(CASE WHEN event_name = 'ssid_set' THEN 1 ELSE 0 END) AS got_ssid,
    MAX(CASE WHEN event_name = 'address_updated' THEN 1 ELSE 0 END) AS got_address,
    MAX(CASE WHEN event_name = 'booking_verified' THEN 1 ELSE 0 END) AS got_booking_verified,
    MAX(CASE WHEN event_name = 'NOTIF_SENT' THEN 1 ELSE 0 END) AS got_notif_sent,
    MAX(CASE WHEN event_name = 'INTERESTED' THEN 1 ELSE 0 END) AS got_interested,
    MAX(CASE WHEN event_name = 'SLOT_SELECTED' THEN 1 ELSE 0 END) AS got_slot_selected,
    MAX(CASE WHEN event_name = 'CUSTOMER_SLOT_CONFIRMED' THEN 1 ELSE 0 END) AS got_confirmed,
    MAX(CASE WHEN event_name = 'ASSIGNED' THEN 1 ELSE 0 END) AS got_assigned,
    MAX(CASE WHEN event_name = 'OTP_VERIFIED' THEN 1 ELSE 0 END) AS got_otp_verified,
    MAX(CASE WHEN event_name = 'cancelled' THEN 1 ELSE 0 END) AS got_cancelled
  FROM mapped
  GROUP BY 1, 2
)
SELECT
  COUNT(*) AS total_bookings,
  SUM(got_ssid) AS ssid_set,
  SUM(got_address) AS address_updated,
  SUM(got_booking_verified) AS booking_verified,
  SUM(got_notif_sent) AS notif_sent,
  SUM(got_interested) AS interested,
  SUM(got_slot_selected) AS slot_selected,
  SUM(got_confirmed) AS customer_slot_confirmed,
  SUM(got_assigned) AS assigned,
  SUM(got_otp_verified) AS otp_verified,
  SUM(got_cancelled) AS cancelled
FROM booking_stages
