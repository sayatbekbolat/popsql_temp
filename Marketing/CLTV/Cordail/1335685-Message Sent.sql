SELECT DISTINCT 
    DATETIME(TIMESTAMP(time)) AS Sent_datetime_UTC, 
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) Sent_Datetime_PST,
    email, action, message_name, mdtID, msID, experiment, variant
FROM `gusa-bronto-dwh.Cordail_Data.message_activity_sent`
LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate       
  AND email IS NOT NULL 
  AND DATETIME(TIMESTAMP(time)) IS NOT NULL