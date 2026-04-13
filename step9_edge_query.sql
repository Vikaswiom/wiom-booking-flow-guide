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
  SELECT mobile, 'ASSIGNED' AS event_name, added_time AS event_time, NULL AS data
  FROM prod_db.public.task_logs
  WHERE event_name = 'ASSIGNED' AND mobile >= '5999999999'
),
reached_events AS (
  SELECT mobile, 'REACHED_HOME' AS event_name, added_time AS event_time, NULL AS data
  FROM prod_db.public.task_logs
  WHERE event_name = 'REACHED_HOME' AND mobile >= '5999999999'
),
cancel_events AS (
  SELECT mobile, 'cancelled' AS event_name, added_time AS event_time, data
  FROM prod_db.public.booking_logs
  WHERE event_name = 'cancelled' AND mobile >= '5999999999'
),
all_events AS (
  SELECT * FROM assigned_events
  UNION ALL SELECT * FROM reached_events
  UNION ALL SELECT * FROM cancel_events
),
mapped AS (
  SELECT b.mobile, b.booking_time, e.event_name, e.event_time, e.data
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
    MAX(CASE WHEN event_name = 'ASSIGNED' THEN 1 ELSE 0 END) AS got_assigned,
    MAX(CASE WHEN event_name = 'REACHED_HOME' THEN 1 ELSE 0 END) AS got_reached,
    MAX(CASE WHEN event_name = 'cancelled' THEN 1 ELSE 0 END) AS got_cancelled,
    MIN(CASE WHEN event_name = 'ASSIGNED' THEN event_time END) AS assigned_time,
    MIN(CASE WHEN event_name = 'cancelled' THEN event_time END) AS cancelled_time,
    MAX(CASE WHEN event_name = 'cancelled' AND TRY_PARSE_JSON(data):initiated_by::STRING = 'customer' THEN 1 ELSE 0 END) AS cust_cancel,
    MAX(CASE WHEN event_name = 'cancelled' AND TRY_PARSE_JSON(data):initiated_by::STRING = 'cops' THEN 1 ELSE 0 END) AS sys_cancel,
    MAX(CASE WHEN event_name = 'cancelled' AND (TRY_PARSE_JSON(data):reason::STRING ILIKE '%delay%' OR TRY_PARSE_JSON(data):reason_type::STRING ILIKE '%delay%') THEN 1 ELSE 0 END) AS delay_reason
  FROM mapped
  GROUP BY 1, 2
)
SELECT
  SUM(CASE WHEN got_assigned = 1 AND got_cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_at_rohit_stage,
  SUM(CASE WHEN got_assigned = 1 AND got_cancelled = 1 AND got_reached = 1 THEN 1 ELSE 0 END) AS reached_and_cancelled,
  ROUND(AVG(CASE WHEN got_assigned = 1 AND got_cancelled = 1 AND cust_cancel = 1 THEN DATEDIFF('hour', assigned_time, cancelled_time) END), 0) AS avg_hours_cust_cancel,
  ROUND(AVG(CASE WHEN got_assigned = 1 AND got_cancelled = 1 AND sys_cancel = 1 THEN DATEDIFF('hour', assigned_time, cancelled_time) END), 0) AS avg_hours_sys_cancel,
  SUM(CASE WHEN got_assigned = 1 AND got_cancelled = 1 AND delay_reason = 1 THEN 1 ELSE 0 END) AS delay_reason_count
FROM booking_stages
