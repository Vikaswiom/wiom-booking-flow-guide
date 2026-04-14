WITH bookings AS (
  SELECT mobile, MIN(added_time) AS booking_time
  FROM prod_db.public.booking_logs
  WHERE event_name = 'booking_fee_captured'
    AND mobile >= '5999999999'
    AND DATEADD('minute', 330, added_time) >= '2026-03-28 00:00:00'
    AND DATEADD('minute', 330, added_time) < '2026-04-05 00:00:00'
  GROUP BY mobile
),
stage_times AS (
  SELECT
    b.mobile,
    b.booking_time,
    MIN(CASE WHEN bl.event_name = 'ssid_set' AND bl.added_time >= b.booking_time THEN bl.added_time END) AS t_ssid,
    MIN(CASE WHEN bl.event_name = 'address_updated' AND bl.added_time >= b.booking_time THEN bl.added_time END) AS t_address,
    MIN(CASE WHEN bl.event_name = 'booking_verified' AND bl.added_time >= b.booking_time THEN bl.added_time END) AS t_verified
  FROM bookings b
  LEFT JOIN prod_db.public.booking_logs bl ON bl.mobile = b.mobile
  GROUP BY 1, 2
),
task_times AS (
  SELECT
    b.mobile,
    MIN(CASE WHEN tl.event_name = 'NOTIF_SENT' AND tl.added_time >= b.booking_time THEN tl.added_time END) AS t_notif,
    MIN(CASE WHEN tl.event_name = 'INTERESTED' AND tl.added_time >= b.booking_time THEN tl.added_time END) AS t_interested,
    MIN(CASE WHEN tl.event_name = 'SLOT_SELECTED' AND tl.added_time >= b.booking_time THEN tl.added_time END) AS t_slot,
    MIN(CASE WHEN tl.event_name = 'CUSTOMER_SLOT_CONFIRMED' AND tl.added_time >= b.booking_time THEN tl.added_time END) AS t_confirmed,
    MIN(CASE WHEN tl.event_name = 'ASSIGNED' AND tl.added_time >= b.booking_time THEN tl.added_time END) AS t_assigned,
    MIN(CASE WHEN tl.event_name = 'OTP_VERIFIED' AND tl.added_time >= b.booking_time THEN tl.added_time END) AS t_otp
  FROM bookings b
  LEFT JOIN prod_db.public.task_logs tl ON tl.mobile = b.mobile
  GROUP BY 1
),
cancel_info AS (
  SELECT
    b.mobile,
    MIN(CASE WHEN c.event_name = 'cancelled' AND TRY_PARSE_JSON(c.data):initiated_by::STRING = 'customer' AND c.added_time >= b.booking_time THEN c.added_time END) AS t_cust_cancel,
    MIN(CASE WHEN c.event_name = 'cancelled' AND TRY_PARSE_JSON(c.data):initiated_by::STRING LIKE 'cops%' AND c.added_time >= b.booking_time THEN c.added_time END) AS t_sys_cancel
  FROM bookings b
  LEFT JOIN prod_db.public.booking_logs c ON c.mobile = b.mobile
  GROUP BY 1
),
joined AS (
  SELECT
    b.mobile, b.booking_time,
    s.t_ssid, s.t_address, s.t_verified,
    t.t_notif, t.t_interested, t.t_slot, t.t_confirmed, t.t_assigned, t.t_otp,
    c.t_cust_cancel, c.t_sys_cancel
  FROM bookings b
  LEFT JOIN stage_times s ON s.mobile = b.mobile
  LEFT JOIN task_times t ON t.mobile = b.mobile
  LEFT JOIN cancel_info c ON c.mobile = b.mobile
),
analysis AS (
  SELECT
    mobile,
    CASE
      WHEN t_ssid IS NULL THEN 1
      WHEN t_address IS NULL THEN 2
      WHEN t_verified IS NULL THEN 3
      WHEN t_notif IS NULL THEN 4
      WHEN t_interested IS NULL THEN 5
      WHEN t_slot IS NULL THEN 6
      WHEN t_confirmed IS NULL THEN 7
      WHEN t_assigned IS NULL THEN 8
      WHEN t_otp IS NULL THEN 9
      ELSE 99
    END AS stuck_stage,
    CASE WHEN t_cust_cancel IS NOT NULL THEN 1 ELSE 0 END AS did_cust_cancel,
    CASE WHEN t_sys_cancel IS NOT NULL THEN 1 ELSE 0 END AS did_sys_cancel,
    CASE WHEN t_verified IS NOT NULL AND t_notif IS NULL AND DATEDIFF('hour', t_verified, COALESCE(t_cust_cancel, t_sys_cancel, CURRENT_TIMESTAMP())) >= 48 THEN 1 ELSE 0 END AS s4_pred,
    CASE WHEN t_notif IS NOT NULL AND t_interested IS NULL AND DATEDIFF('hour', t_notif, COALESCE(t_cust_cancel, t_sys_cancel, CURRENT_TIMESTAMP())) >= 48 THEN 1 ELSE 0 END AS s5_pred,
    CASE WHEN t_interested IS NOT NULL AND t_slot IS NULL AND DATEDIFF('hour', t_interested, COALESCE(t_cust_cancel, t_sys_cancel, CURRENT_TIMESTAMP())) >= 48 THEN 1 ELSE 0 END AS s6_pred,
    CASE WHEN t_slot IS NOT NULL AND t_confirmed IS NULL AND DATEDIFF('hour', t_slot, COALESCE(t_cust_cancel, t_sys_cancel, CURRENT_TIMESTAMP())) >= 6 THEN 1 ELSE 0 END AS s7_pred,
    CASE WHEN t_confirmed IS NOT NULL AND t_assigned IS NULL AND DATEDIFF('hour', t_confirmed, COALESCE(t_cust_cancel, t_sys_cancel, CURRENT_TIMESTAMP())) >= 24 THEN 1 ELSE 0 END AS s8_pred,
    CASE WHEN t_assigned IS NOT NULL AND t_otp IS NULL AND DATEDIFF('hour', t_assigned, COALESCE(t_cust_cancel, t_sys_cancel, CURRENT_TIMESTAMP())) >= 24 THEN 1 ELSE 0 END AS s9_pred
  FROM joined
),
stage_counts AS (
  SELECT
    SUM(CASE WHEN stuck_stage >= 1 THEN 1 ELSE 0 END) AS s1_entry,
    SUM(CASE WHEN stuck_stage >= 2 THEN 1 ELSE 0 END) AS s2_entry,
    SUM(CASE WHEN stuck_stage >= 3 THEN 1 ELSE 0 END) AS s3_entry,
    SUM(CASE WHEN stuck_stage >= 4 THEN 1 ELSE 0 END) AS s4_entry,
    SUM(CASE WHEN stuck_stage >= 5 THEN 1 ELSE 0 END) AS s5_entry,
    SUM(CASE WHEN stuck_stage >= 6 THEN 1 ELSE 0 END) AS s6_entry,
    SUM(CASE WHEN stuck_stage >= 7 THEN 1 ELSE 0 END) AS s7_entry,
    SUM(CASE WHEN stuck_stage >= 8 THEN 1 ELSE 0 END) AS s8_entry,
    SUM(CASE WHEN stuck_stage >= 9 THEN 1 ELSE 0 END) AS s9_entry,
    SUM(CASE WHEN stuck_stage = 1 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s1_cust,
    SUM(CASE WHEN stuck_stage = 2 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s2_cust,
    SUM(CASE WHEN stuck_stage = 3 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s3_cust,
    SUM(CASE WHEN stuck_stage = 4 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s4_cust,
    SUM(CASE WHEN stuck_stage = 5 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s5_cust,
    SUM(CASE WHEN stuck_stage = 6 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s6_cust,
    SUM(CASE WHEN stuck_stage = 7 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s7_cust,
    SUM(CASE WHEN stuck_stage = 8 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s8_cust,
    SUM(CASE WHEN stuck_stage = 9 AND did_cust_cancel = 1 THEN 1 ELSE 0 END) AS s9_cust,
    SUM(CASE WHEN stuck_stage = 1 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s1_sys,
    SUM(CASE WHEN stuck_stage = 2 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s2_sys,
    SUM(CASE WHEN stuck_stage = 3 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s3_sys,
    SUM(CASE WHEN stuck_stage = 4 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s4_sys,
    SUM(CASE WHEN stuck_stage = 5 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s5_sys,
    SUM(CASE WHEN stuck_stage = 6 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s6_sys,
    SUM(CASE WHEN stuck_stage = 7 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s7_sys,
    SUM(CASE WHEN stuck_stage = 8 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s8_sys,
    SUM(CASE WHEN stuck_stage = 9 AND did_sys_cancel = 1 THEN 1 ELSE 0 END) AS s9_sys,
    SUM(s4_pred) AS s4_pred,
    SUM(s5_pred) AS s5_pred,
    SUM(s6_pred) AS s6_pred,
    SUM(s7_pred) AS s7_pred,
    SUM(s8_pred) AS s8_pred,
    SUM(s9_pred) AS s9_pred
  FROM analysis
)
SELECT * FROM stage_counts
