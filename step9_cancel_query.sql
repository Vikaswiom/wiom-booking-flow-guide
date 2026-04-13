WITH bookings AS (
  SELECT mobile, added_time AS booking_time,
    LEAD(added_time) OVER(PARTITION BY mobile ORDER BY added_time) AS next_booking_time
  FROM prod_db.public.booking_logs
  WHERE event_name = 'booking_fee_captured'
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-02-20 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-03-26 00:00:00'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY TRY_PARSE_JSON(data):transaction_id::STRING ORDER BY added_time DESC) = 1
),
assigned_events AS (
  SELECT mobile, added_time AS assigned_time
  FROM prod_db.public.task_logs
  WHERE event_name = 'ASSIGNED' AND mobile >= '5999999999'
),
cancel_events AS (
  SELECT mobile, added_time AS cancelled_time, TRY_PARSE_JSON(data):initiated_by::STRING AS initiated_by
  FROM prod_db.public.booking_logs
  WHERE event_name = 'cancelled' AND mobile >= '5999999999'
),
assigned_for_bookings AS (
  SELECT b.mobile, b.booking_time, MIN(a.assigned_time) AS assigned_time
  FROM bookings b
  JOIN assigned_events a ON b.mobile = a.mobile
    AND a.assigned_time >= b.booking_time
    AND (b.next_booking_time IS NULL OR a.assigned_time < b.next_booking_time)
  GROUP BY 1, 2
),
cancels_for_bookings AS (
  SELECT b.mobile, b.booking_time, MIN(c.cancelled_time) AS cancelled_time, MIN(c.initiated_by) AS initiated_by
  FROM bookings b
  JOIN cancel_events c ON b.mobile = c.mobile
    AND c.cancelled_time >= b.booking_time
    AND (b.next_booking_time IS NULL OR c.cancelled_time < b.next_booking_time)
  GROUP BY 1, 2
)
SELECT
  c.initiated_by,
  COUNT(*) AS cnt,
  ROUND(AVG(DATEDIFF('hour', a.assigned_time, c.cancelled_time)), 0) AS avg_hours
FROM assigned_for_bookings a
JOIN cancels_for_bookings c ON a.mobile = c.mobile AND a.booking_time = c.booking_time
GROUP BY 1
